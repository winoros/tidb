// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregate

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestStatementRUInputRowsOverflow(t *testing.T) {
	total := int64(3)
	if !addStatementRUInputRows(&total, 2) || total != 5 {
		t.Fatalf("normal addition failed: %d", total)
	}

	total = math.MaxInt64 - 1
	if addStatementRUInputRows(&total, 2) {
		t.Fatal("overflowing addition must fail closed")
	}
	if total != math.MaxInt64-1 {
		t.Fatalf("overflowing addition changed total: %d", total)
	}
}

func TestStatementRUCountCheckedAdd(t *testing.T) {
	total := int64(3)
	require.True(t, addStatementRUCount(&total, 2))
	require.Equal(t, int64(5), total)

	total = math.MaxInt64 - 1
	require.False(t, addStatementRUCount(&total, 2))
	require.Equal(t, int64(math.MaxInt64-1), total)
	require.False(t, addStatementRUCount(&total, -1))
	require.Equal(t, int64(math.MaxInt64-1), total)
}

// StatementRUHashStateCommittedForTest reports whether the current HashAgg
// generation published its terminal hash-state observation.
func StatementRUHashStateCommittedForTest(aggExec *HashAggExec) bool {
	statementRU := aggExec.statementRU.Load()
	return statementRU != nil && statementRU.hashState.state.Load() == hashAggStatementRUCommitted
}

func newStatementRUOverflowHashAgg(t *testing.T) (*HashAggExec, *statementru.Statement) {
	ctx := mock.NewContext()
	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	child := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		Ctx:        ctx,
		DataSchema: expression.NewSchema(column),
		Rows:       1,
		Ndvs:       []int{1},
	})
	child.PrepareChunks()
	aggExec := &HashAggExec{
		BaseExecutor:          exec.NewBaseExecutor(ctx, expression.NewSchema(), 0, child),
		Sc:                    ctx.GetSessionVars().StmtCtx,
		GroupByItems:          []expression.Expression{column},
		IsUnparallelExec:      true,
		FileNamePrefixForTest: t.Name(),
	}
	aggExec.SetChildren(0, child)
	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	weights := statementru.Weights{
		statementru.CPUWork:       1,
		statementru.HashStateRows: 1,
	}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: requiredUnits,
		Weights:       &weights,
	}))
	require.True(t, exec.ConfigureStatementRUExecutor(aggExec, sc, exec.StatementRUExecutorConfig{
		CPUWorkMultiplier: 1,
		NeedsUnitRecorder: true,
	}))
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)
	require.NoError(t, aggExec.Open(context.Background()))
	return aggExec, statement
}

func finishInvalidStatementRUHashAgg(t *testing.T, statement *statementru.Statement, invalidUnit statementru.UnitKind) statementru.UnitValues {
	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	require.True(t, statement.EvidenceRecorder().MarkPresent(requiredUnits))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.False(t, finish.Result.HasTotal())
	require.Equal(t, statementru.Outcome{
		State:  statementru.StateInvalid,
		Reason: statementru.ReasonInvalidObservation,
	}, finish.Result.Outcome())
	coverage, ok := finish.Result.Coverage()
	require.True(t, ok)
	require.Equal(t, invalidUnit.Mask(), coverage.InvalidUnits)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	return units
}

func drainStatementRUHashAgg(t *testing.T, aggExec *HashAggExec) {
	for {
		chk := exec.NewFirstChunk(aggExec)
		require.NoError(t, aggExec.Next(context.Background(), chk))
		if chk.NumRows() == 0 {
			return
		}
	}
}

func TestStatementRUHashAggOverflowInvalidation(t *testing.T) {
	t.Run("input rows invalidate CPU work at child EOF", func(t *testing.T) {
		aggExec, statement := newStatementRUOverflowHashAgg(t)
		defer func() { require.NoError(t, aggExec.Close()) }()
		statementRU := aggExec.statementRU.Load()
		require.NotNil(t, statementRU)
		statementRU.inputRows = math.MaxInt64

		drainStatementRUHashAgg(t, aggExec)

		units := finishInvalidStatementRUHashAgg(t, statement, statementru.CPUWork)
		require.Equal(t, float64(1), units[statementru.HashStateRows])
	})

	t.Run("serial hash state invalidates only at output EOF", func(t *testing.T) {
		aggExec, statement := newStatementRUOverflowHashAgg(t)
		defer func() { require.NoError(t, aggExec.Close()) }()
		statementRU := aggExec.statementRU.Load()
		require.NotNil(t, statementRU)
		statementRU.hashStateRows = math.MaxInt64

		chk := exec.NewFirstChunk(aggExec)
		require.NoError(t, aggExec.Next(context.Background(), chk))
		require.Positive(t, chk.NumRows())
		require.Equal(t, hashAggStatementRUOpen, statementRU.hashState.state.Load())
		drainStatementRUHashAgg(t, aggExec)

		units := finishInvalidStatementRUHashAgg(t, statement, statementru.HashStateRows)
		require.Equal(t, float64(1), units[statementru.CPUWork])
	})

	t.Run("parallel final worker sum cannot wrap", func(t *testing.T) {
		aggExec, statement := newStatementRUOverflowHashAgg(t)
		require.NoError(t, aggExec.Close())
		statementRU := newHashAggStatementRURuntime(2)
		statementRU.completeFinalWorker(0, math.MaxInt64, true)
		statementRU.completeFinalWorker(1, 1, true)

		statementRU.commitParallelHashStateRows(aggExec, false)

		finishInvalidStatementRUHashAgg(t, statement, statementru.HashStateRows)
	})
}

type blockingStatementRUHashAggChild struct {
	exec.BaseExecutor
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingStatementRUHashAggChild(ctx *mock.Context, schema *expression.Schema) *blockingStatementRUHashAggChild {
	return &blockingStatementRUHashAggChild{
		BaseExecutor: exec.NewBaseExecutor(ctx, schema, 0),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (c *blockingStatementRUHashAggChild) Next(_ context.Context, req *chunk.Chunk) error {
	c.enteredOnce.Do(func() { close(c.entered) })
	<-c.release
	req.Reset()
	return nil
}

func (c *blockingStatementRUHashAggChild) Close() error {
	c.releaseOnce.Do(func() { close(c.release) })
	return nil
}

func newBlockedParallelStatementRUHashAgg(t *testing.T) (*HashAggExec, *statementru.Statement, *blockingStatementRUHashAggChild) {
	ctx := mock.NewContext()
	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	childSchema := expression.NewSchema(column)
	child := newBlockingStatementRUHashAggChild(ctx, childSchema)
	aggExec := &HashAggExec{
		BaseExecutor:          exec.NewBaseExecutor(ctx, expression.NewSchema(), 0, child),
		Sc:                    ctx.GetSessionVars().StmtCtx,
		GroupByItems:          []expression.Expression{column},
		FileNamePrefixForTest: t.Name(),
	}
	aggExec.SetChildren(0, child)
	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	weights := statementru.Weights{
		statementru.CPUWork:       1,
		statementru.HashStateRows: 1,
	}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: requiredUnits,
		Weights:       &weights,
	}))
	require.True(t, exec.ConfigureStatementRUExecutor(aggExec, sc, exec.StatementRUExecutorConfig{
		CPUWorkMultiplier: 1,
		NeedsUnitRecorder: true,
	}))
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)
	require.NoError(t, aggExec.Open(context.Background()))
	return aggExec, statement, child
}

func TestStatementRUParallelHashAggCloseAbortsBlockedNext(t *testing.T) {
	aggExec, statement, child := newBlockedParallelStatementRUHashAgg(t)
	statementRU := aggExec.statementRU.Load()
	require.NotNil(t, statementRU)
	require.True(t, statement.UnitRecorder().Add(statementru.CPUWork, 11))
	require.True(t, statement.UnitRecorder().Add(statementru.HashStateRows, 7))

	nextDone := make(chan error, 1)
	go func() {
		nextDone <- aggExec.Next(context.Background(), exec.NewFirstChunk(aggExec))
	}()
	select {
	case <-child.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("parallel HashAgg Next did not block in its child")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- aggExec.Close() }()
	require.Eventually(t, func() bool {
		return statementRU.cpuWork.state.Load() == hashAggStatementRUAborted &&
			statementRU.hashState.state.Load() == hashAggStatementRUAborted
	}, 10*time.Second, time.Millisecond)
	require.NoError(t, child.Close())
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("parallel HashAgg Close did not return")
	}
	select {
	case err := <-nextDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("parallel HashAgg Next remained blocked after Close")
	}

	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	require.True(t, statement.EvidenceRecorder().MarkPresent(requiredUnits))
	finish, first := statement.Finish(statementru.TerminalCanceled)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(11), units[statementru.CPUWork])
	require.Equal(t, float64(7), units[statementru.HashStateRows])
	coverage, ok := finish.Result.Coverage()
	require.True(t, ok)
	require.Zero(t, coverage.InvalidUnits)
}

func TestStatementRUHashAggOpenGenerationIsolation(t *testing.T) {
	aggExec, statement := newStatementRUOverflowHashAgg(t)
	oldStatementRU := aggExec.statementRU.Load()
	require.NotNil(t, oldStatementRU)
	oldStatementRU.inputRows = math.MaxInt64
	oldStatementRU.addInputRows(1)
	oldStatementRU.hashStateRows = math.MaxInt64
	oldStatementRU.addHashStateRows(1)
	require.False(t, oldStatementRU.inputValid)
	require.False(t, oldStatementRU.hashStateValid)

	require.NoError(t, aggExec.Close())
	require.Equal(t, hashAggStatementRUAborted, oldStatementRU.cpuWork.state.Load())
	require.Equal(t, hashAggStatementRUAborted, oldStatementRU.hashState.state.Load())
	require.NoError(t, aggExec.Open(context.Background()))
	currentStatementRU := aggExec.statementRU.Load()
	require.NotNil(t, currentStatementRU)
	require.NotSame(t, oldStatementRU, currentStatementRU)

	oldStatementRU.commitCPUWork(aggExec)
	oldStatementRU.commitSerialHashStateRows(aggExec)
	currentStatementRU.addInputRows(2)
	currentStatementRU.commitCPUWork(aggExec)
	currentStatementRU.addHashStateRows(3)
	currentStatementRU.commitSerialHashStateRows(aggExec)
	require.NoError(t, aggExec.Close())

	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	require.True(t, statement.EvidenceRecorder().MarkPresent(requiredUnits))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.Equal(t, statementru.Outcome{State: statementru.StateComplete, Reason: statementru.ReasonNone}, finish.Result.Outcome())
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(2), units[statementru.CPUWork])
	require.Equal(t, float64(3), units[statementru.HashStateRows])
	coverage, ok := finish.Result.Coverage()
	require.True(t, ok)
	require.Zero(t, coverage.InvalidUnits)
}

func TestStatementRUHashAggCloseWaitsForPublication(t *testing.T) {
	aggExec, statement := newStatementRUOverflowHashAgg(t)
	statementRU := aggExec.statementRU.Load()
	require.NotNil(t, statementRU)
	publicationStarted := make(chan struct{})
	releasePublication := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releasePublication) }) })
	publicationDone := make(chan bool, 1)
	go func() {
		publicationDone <- statementRU.cpuWork.commit(func() {
			close(publicationStarted)
			<-releasePublication
			aggExec.RecordStatementRUCPUWork64(5)
		})
	}()
	<-publicationStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- aggExec.Close() }()
	require.Eventually(t, func() bool { return aggExec.statementRU.Load() == nil }, 10*time.Second, time.Millisecond)
	select {
	case err := <-closeDone:
		require.NoError(t, err)
		t.Fatal("HashAgg Close returned before the in-flight RU publication completed")
	default:
	}
	releaseOnce.Do(func() { close(releasePublication) })
	require.True(t, <-publicationDone)
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("HashAgg Close did not finish after RU publication completed")
	}

	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	require.True(t, statement.EvidenceRecorder().MarkPresent(requiredUnits))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(5), units[statementru.CPUWork])
	require.Zero(t, units[statementru.HashStateRows])
}

type reopenStatementRUHashAggChild struct {
	exec.BaseExecutor
	mu        sync.Mutex
	emitted   bool
	nextCalls atomic.Int64
	openHook  func()
}

func (c *reopenStatementRUHashAggChild) Open(ctx context.Context) error {
	if c.openHook != nil {
		c.openHook()
	}
	c.mu.Lock()
	c.emitted = false
	c.mu.Unlock()
	return c.BaseExecutor.Open(ctx)
}

func (c *reopenStatementRUHashAggChild) Next(_ context.Context, req *chunk.Chunk) error {
	c.nextCalls.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	req.Reset()
	if !c.emitted {
		req.AppendInt64(0, 1)
		c.emitted = true
	}
	return nil
}

func newReopenStatementRUHashAgg(t *testing.T) (*HashAggExec, *statementru.Statement, *reopenStatementRUHashAggChild) {
	ctx := mock.NewContext()
	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	childSchema := expression.NewSchema(column)
	child := &reopenStatementRUHashAggChild{BaseExecutor: exec.NewBaseExecutor(ctx, childSchema, 0)}
	aggExec := &HashAggExec{
		BaseExecutor:          exec.NewBaseExecutor(ctx, expression.NewSchema(), 0, child),
		Sc:                    ctx.GetSessionVars().StmtCtx,
		GroupByItems:          []expression.Expression{column},
		FileNamePrefixForTest: t.Name(),
	}
	aggExec.SetChildren(0, child)
	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	weights := statementru.Weights{
		statementru.CPUWork:       1,
		statementru.HashStateRows: 1,
	}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: requiredUnits,
		Weights:       &weights,
	}))
	require.True(t, exec.ConfigureStatementRUExecutor(aggExec, sc, exec.StatementRUExecutorConfig{
		CPUWorkMultiplier: 1,
		NeedsUnitRecorder: true,
	}))
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)
	require.NoError(t, aggExec.Open(context.Background()))
	return aggExec, statement, child
}

func TestStatementRUParallelHashAggOldNextCannotStartReopenedGeneration(t *testing.T) {
	aggExec, statement, child := newReopenStatementRUHashAgg(t)
	oldStatementRU := aggExec.statementRU.Load()
	require.NotNil(t, oldStatementRU)
	oldNextCaptured := make(chan struct{})
	resumeOldNext := make(chan struct{})
	var captureOnce sync.Once
	var resumeOnce sync.Once
	t.Cleanup(func() { resumeOnce.Do(func() { close(resumeOldNext) }) })
	oldStatementRU.parallelStartForTest = func() {
		captureOnce.Do(func() { close(oldNextCaptured) })
		<-resumeOldNext
	}
	oldNextDone := make(chan error, 1)
	go func() {
		oldNextDone <- aggExec.Next(context.Background(), exec.NewFirstChunk(aggExec))
	}()
	select {
	case <-oldNextCaptured:
	case <-time.After(10 * time.Second):
		t.Fatal("old HashAgg Next did not capture its RU generation")
	}

	require.NoError(t, aggExec.Close())
	require.NoError(t, aggExec.Open(context.Background()))
	currentStatementRU := aggExec.statementRU.Load()
	require.NotNil(t, currentStatementRU)
	require.NotSame(t, oldStatementRU, currentStatementRU)
	require.False(t, aggExec.prepared.Load())
	require.Zero(t, child.nextCalls.Load())
	resumeOnce.Do(func() { close(resumeOldNext) })
	select {
	case err := <-oldNextDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("old HashAgg Next did not exit after the reopened generation was published")
	}
	require.False(t, aggExec.prepared.Load())
	require.Zero(t, child.nextCalls.Load())

	drainStatementRUHashAgg(t, aggExec)
	require.NoError(t, aggExec.Close())
	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	require.True(t, statement.EvidenceRecorder().MarkPresent(requiredUnits))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(1), units[statementru.CPUWork])
	require.Equal(t, float64(1), units[statementru.HashStateRows])
}

func TestStatementRUEnabledNilRuntimeNextIsNoop(t *testing.T) {
	aggExec, statement, child := newReopenStatementRUHashAgg(t)
	require.NoError(t, aggExec.Close())
	require.Nil(t, aggExec.statementRU.Load())
	preparedAfterClose := aggExec.prepared.Load()
	require.NoError(t, aggExec.Next(context.Background(), exec.NewFirstChunk(aggExec)))
	require.Equal(t, preparedAfterClose, aggExec.prepared.Load())
	require.Zero(t, child.nextCalls.Load())

	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOpen) }) })
	child.openHook = func() {
		close(openStarted)
		<-releaseOpen
	}
	openDone := make(chan error, 1)
	go func() { openDone <- aggExec.Open(context.Background()) }()
	select {
	case <-openStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("HashAgg Open did not reach child initialization")
	}
	require.Nil(t, aggExec.statementRU.Load())
	nextDone := make(chan error, 1)
	go func() { nextDone <- aggExec.Next(context.Background(), exec.NewFirstChunk(aggExec)) }()
	select {
	case err := <-nextDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("enabled HashAgg Next blocked on an unpublished runtime")
	}
	require.Equal(t, preparedAfterClose, aggExec.prepared.Load())
	require.Zero(t, child.nextCalls.Load())
	releaseOnce.Do(func() { close(releaseOpen) })
	select {
	case err := <-openDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("HashAgg Open did not finish after child initialization resumed")
	}

	drainStatementRUHashAgg(t, aggExec)
	require.NoError(t, aggExec.Close())
	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	require.True(t, statement.EvidenceRecorder().MarkPresent(requiredUnits))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(1), units[statementru.CPUWork])
	require.Equal(t, float64(1), units[statementru.HashStateRows])
}

type serialGenerationStatementRUHashAggChild struct {
	exec.BaseExecutor
	mu           sync.Mutex
	generation   int
	emitted      bool
	entered      chan struct{}
	release      chan struct{}
	released     chan struct{}
	allowReturn  chan struct{}
	enteredOnce  sync.Once
	releaseOnce  sync.Once
	releasedOnce sync.Once
}

func newSerialGenerationStatementRUHashAggChild(ctx *mock.Context, schema *expression.Schema) *serialGenerationStatementRUHashAggChild {
	return &serialGenerationStatementRUHashAggChild{
		BaseExecutor: exec.NewBaseExecutor(ctx, schema, 0),
		entered:      make(chan struct{}),
		release:      make(chan struct{}),
		released:     make(chan struct{}),
		allowReturn:  make(chan struct{}),
	}
}

func (c *serialGenerationStatementRUHashAggChild) Open(ctx context.Context) error {
	c.mu.Lock()
	c.generation++
	c.emitted = false
	c.mu.Unlock()
	return c.BaseExecutor.Open(ctx)
}

func (c *serialGenerationStatementRUHashAggChild) Next(_ context.Context, req *chunk.Chunk) error {
	c.mu.Lock()
	generation := c.generation
	emitted := c.emitted
	if generation > 1 && !emitted {
		c.emitted = true
	}
	c.mu.Unlock()
	req.Reset()
	if generation == 1 {
		c.enteredOnce.Do(func() { close(c.entered) })
		<-c.release
		c.releasedOnce.Do(func() { close(c.released) })
		<-c.allowReturn
		return nil
	}
	if !emitted {
		req.AppendInt64(0, 1)
	}
	return nil
}

func (c *serialGenerationStatementRUHashAggChild) Close() error {
	c.mu.Lock()
	generation := c.generation
	c.mu.Unlock()
	if generation == 1 {
		c.releaseOnce.Do(func() { close(c.release) })
	}
	return c.BaseExecutor.Close()
}

func newSerialGenerationStatementRUHashAgg(t *testing.T) (*HashAggExec, *statementru.Statement, *serialGenerationStatementRUHashAggChild) {
	ctx := mock.NewContext()
	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	childSchema := expression.NewSchema(column)
	child := newSerialGenerationStatementRUHashAggChild(ctx, childSchema)
	aggExec := &HashAggExec{
		BaseExecutor:          exec.NewBaseExecutor(ctx, expression.NewSchema(), 0, child),
		Sc:                    ctx.GetSessionVars().StmtCtx,
		GroupByItems:          []expression.Expression{column},
		IsUnparallelExec:      true,
		FileNamePrefixForTest: t.Name(),
	}
	aggExec.SetChildren(0, child)
	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	weights := statementru.Weights{
		statementru.CPUWork:       1,
		statementru.HashStateRows: 1,
	}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: requiredUnits,
		Weights:       &weights,
	}))
	require.True(t, exec.ConfigureStatementRUExecutor(aggExec, sc, exec.StatementRUExecutorConfig{
		CPUWorkMultiplier: 1,
		NeedsUnitRecorder: true,
	}))
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)
	require.NoError(t, aggExec.Open(context.Background()))
	return aggExec, statement, child
}

func TestStatementRUSerialHashAggCloseWaitsBeforeCleanupAndReopen(t *testing.T) {
	aggExec, statement, child := newSerialGenerationStatementRUHashAgg(t)
	var allowReturnOnce sync.Once
	t.Cleanup(func() { allowReturnOnce.Do(func() { close(child.allowReturn) }) })
	oldNextDone := make(chan error, 1)
	go func() {
		oldNextDone <- aggExec.Next(context.Background(), exec.NewFirstChunk(aggExec))
	}()
	select {
	case <-child.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("serial HashAgg Next did not block in its child")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- aggExec.Close() }()
	select {
	case <-child.released:
	case <-time.After(10 * time.Second):
		t.Fatal("serial HashAgg Close did not release the blocked child")
	}
	require.NotNil(t, aggExec.childResult)
	select {
	case err := <-closeDone:
		require.NoError(t, err)
		t.Fatal("serial HashAgg Close cleaned up before the active Next returned")
	default:
	}
	allowReturnOnce.Do(func() { close(child.allowReturn) })
	select {
	case err := <-oldNextDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("old serial HashAgg Next did not return after its child was released")
	}
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("serial HashAgg Close did not finish after the active Next returned")
	}
	require.Nil(t, aggExec.childResult)

	require.NoError(t, aggExec.Open(context.Background()))
	drainStatementRUHashAgg(t, aggExec)
	require.NoError(t, aggExec.Close())
	requiredUnits := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask()
	require.True(t, statement.EvidenceRecorder().MarkPresent(requiredUnits))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(1), units[statementru.CPUWork])
	require.Equal(t, float64(1), units[statementru.HashStateRows])
}
