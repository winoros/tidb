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

package join

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannerbase "github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var errStatementRUJoinChild = errors.New("statement RU join child error")

type statementRUJoinErrorDataSource struct {
	*testutil.MockDataSource
	failNext bool
}

func (s *statementRUJoinErrorDataSource) Next(ctx context.Context, req *chunk.Chunk) error {
	if s.failNext {
		s.failNext = false
		return errStatementRUJoinChild
	}
	return s.MockDataSource.Next(ctx, req)
}

func newStatementRUJoinContext() *mock.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 2
	ctx.GetSessionVars().MaxChunkSize = 2
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)
	ctx.GetSessionVars().DiskTracker = disk.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.DiskTracker.AttachTo(ctx.GetSessionVars().DiskTracker)
	return ctx
}

func newStatementRUJoinDataSource(ctx sessionctx.Context, failNext bool) exec.Executor {
	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	dataSource := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		Ctx:        ctx,
		DataSchema: expression.NewSchema(column),
		Rows:       3,
		GenDataFunc: func(row int, _ *types.FieldType) any {
			return int64(row + 1)
		},
	})
	dataSource.PrepareChunks()
	if failNext {
		return &statementRUJoinErrorDataSource{MockDataSource: dataSource, failNext: true}
	}
	return dataSource
}

func configureStatementRUJoin(t testing.TB, ctx sessionctx.Context, executor exec.Executor, multiplier int) *statementru.Statement {
	additionalUnits := statementru.JoinOutputRows.Mask()
	synchronousCPUWork := false
	switch executor.(type) {
	case *exec.BaseExecutor, *HashJoinV1Exec, *HashJoinV2Exec:
		additionalUnits |= statementru.HashStateRows.Mask()
	case *MergeJoinExec:
		synchronousCPUWork = true
	}
	required := statementru.CPUWork.Mask() | additionalUnits
	weights := statementru.Weights{
		statementru.CPUWork:        1,
		statementru.HashStateRows:  1,
		statementru.JoinOutputRows: 1,
	}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: required,
		Weights:       &weights,
	}))
	require.True(t, exec.ConfigureStatementRUExecutor(executor, sc, exec.StatementRUExecutorConfig{
		CPUWorkMultiplier:  multiplier,
		AdditionalUnits:    additionalUnits,
		SynchronousCPUWork: synchronousCPUWork,
	}))
	require.True(t, exec.CompleteStatementRUCPUWorkInventory(sc))
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)
	return statement
}

func finishStatementRUJoin(t testing.TB, statement *statementru.Statement, terminal statementru.TerminalStatus) statementru.UnitValues {
	require.True(t, statement.EvidenceRecorder().MarkPresent(
		statement.UnitRecorder().CollectedUnits()&^statementru.CPUWork.Mask(),
	))
	finish, first := statement.Finish(terminal)
	require.True(t, first)
	if finish.Result.Outcome().State == statementru.StateComplete {
		require.True(t, finish.Result.HasTotal())
	} else {
		require.False(t, finish.Result.HasTotal())
	}
	units, ok := finish.Result.Units()
	require.True(t, ok)
	return units
}

func drainStatementRUJoin(t testing.TB, executor exec.Executor) int {
	ctx := context.Background()
	require.NoError(t, exec.Open(ctx, executor))
	defer func() { require.NoError(t, exec.Close(executor)) }()
	req := exec.NewFirstChunk(executor)
	rows := 0
	for {
		require.NoError(t, exec.Next(ctx, executor, req))
		if req.NumRows() == 0 {
			return rows
		}
		rows += req.NumRows()
	}
}

func TestStatementRUJoinRuntimeCompletion(t *testing.T) {
	require.Nil(t, newStatementRUJoinRuntime(false))
	require.NotNil(t, newStatementRUJoinRuntime(true))

	for _, test := range []struct {
		name     string
		complete func(current, old *statementRUJoinRuntime, executor *exec.BaseExecutor)
		expected float64
	}{
		{
			name: "clean EOF commits generation-local rows",
			complete: func(current, _ *statementRUJoinRuntime, executor *exec.BaseExecutor) {
				current.buildRows.Store(2)
				current.probeRows.Store(3)
				current.recordTerminal(executor, false)
			},
			expected: 10,
		},
		{
			name: "error does not commit",
			complete: func(current, _ *statementRUJoinRuntime, executor *exec.BaseExecutor) {
				current.buildRows.Store(5)
				current.markFailed()
				current.recordTerminal(executor, false)
			},
		},
		{
			name: "early close does not commit",
			complete: func(current, _ *statementRUJoinRuntime, _ *exec.BaseExecutor) {
				current.buildRows.Store(5)
			},
		},
		{
			name: "late old generation merge does not pollute current commit",
			complete: func(current, old *statementRUJoinRuntime, executor *exec.BaseExecutor) {
				current.buildRows.Store(2)
				current.recordTerminal(executor, false)
				old.buildRows.Add(100)
			},
			expected: 4,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := newStatementRUJoinContext()
			executor := exec.NewBaseExecutor(ctx, expression.NewSchema(), 0)
			statement := configureStatementRUJoin(t, ctx, &executor, 2)
			test.complete(&statementRUJoinRuntime{}, &statementRUJoinRuntime{}, &executor)
			units := finishStatementRUJoin(t, statement, statementru.TerminalSuccess)
			require.Equal(t, test.expected, units[statementru.CPUWork])
		})
	}

	t.Run("execution failure suppresses terminal units without invalidating arithmetic", func(t *testing.T) {
		ctx := newStatementRUJoinContext()
		executor := exec.NewBaseExecutor(ctx, expression.NewSchema(), 0)
		statement := configureStatementRUJoin(t, ctx, &executor, 2)
		runtime := &statementRUJoinRuntime{}
		runtime.buildRows.Store(5)
		runtime.markFailed()
		runtime.recordTerminal(&executor, true)

		required := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask() | statementru.JoinOutputRows.Mask()
		require.True(t, statement.EvidenceRecorder().MarkPresent(required))
		finish, first := statement.Finish(statementru.TerminalError)
		require.True(t, first)
		require.False(t, finish.Result.HasTotal())
		require.Equal(t, statementru.Outcome{
			State:  statementru.StatePartial,
			Reason: statementru.ReasonUnsupported,
		}, finish.Result.Outcome())
	})

	for _, test := range []struct {
		name       string
		invalid    statementru.UnitKind
		invalidate func(*statementRUJoinRuntime)
	}{
		{
			name:    "CPU work overflow invalidates successful statement",
			invalid: statementru.CPUWork,
			invalidate: func(runtime *statementRUJoinRuntime) {
				runtime.buildRows.Store(math.MaxInt64)
				runtime.addBuildRows(1)
			},
		},
		{
			name:    "CPU terminal sum overflow invalidates successful statement",
			invalid: statementru.CPUWork,
			invalidate: func(runtime *statementRUJoinRuntime) {
				runtime.buildRows.Store(math.MaxInt64)
				runtime.probeRows.Store(1)
			},
		},
		{
			name:    "hash state overflow invalidates successful statement",
			invalid: statementru.HashStateRows,
			invalidate: func(runtime *statementRUJoinRuntime) {
				runtime.hashStateRows.Store(math.MaxUint64)
				runtime.addHashStateRows(1)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := newStatementRUJoinContext()
			executor := exec.NewBaseExecutor(ctx, expression.NewSchema(), 0)
			statement := configureStatementRUJoin(t, ctx, &executor, 2)
			runtime := &statementRUJoinRuntime{}
			runtime.buildRows.Store(3)
			runtime.hashStateRows.Store(7)
			test.invalidate(runtime)
			runtime.recordTerminal(&executor, true)

			required := statementru.CPUWork.Mask() | statementru.HashStateRows.Mask() | statementru.JoinOutputRows.Mask()
			require.True(t, statement.EvidenceRecorder().MarkPresent(required))
			finish, first := statement.Finish(statementru.TerminalSuccess)
			require.True(t, first)
			require.False(t, finish.Result.HasTotal())
			require.Equal(t, statementru.StateInvalid, finish.Result.Outcome().State)
			units, ok := finish.Result.Units()
			require.True(t, ok)
			if test.invalid == statementru.CPUWork {
				require.Equal(t, float64(7), units[statementru.HashStateRows])
			} else {
				require.Equal(t, float64(6), units[statementru.CPUWork])
			}
		})
	}

	t.Run("V2 successful rebuild-round accumulator seam", func(t *testing.T) {
		// The spill suites own the end-to-end V2 restore fixture. This seam test
		// isolates the generation-local accumulation used after each successful
		// hash-table build without duplicating that failpoint-heavy fixture here.
		ctx := newStatementRUJoinContext()
		executor := exec.NewBaseExecutor(ctx, expression.NewSchema(), 0)
		statement := configureStatementRUJoin(t, ctx, &executor, 2)
		runtime := &statementRUJoinRuntime{}
		runtime.addHashStateRows(2)
		runtime.addHashStateRows(3)
		runtime.recordTerminal(&executor, true)

		units := finishStatementRUJoin(t, statement, statementru.TerminalSuccess)
		require.Equal(t, float64(5), units[statementru.HashStateRows])
	})

	t.Run("V1 null-aware bucket state helper seam", func(t *testing.T) {
		// NAAJ build tests own null-row routing. This seam assertion covers the RU
		// helper's additional admitted-state bucket without rebuilding that fixture.
		container := &hashRowContainer{
			hashTable:        NewConcurrentMapHashTable(),
			hashNANullBucket: &hashNANullBucket{entries: make([]*naEntry, 3)},
		}
		rows, ok := statementRUHashStateRowsV1(container)
		require.True(t, ok)
		require.Equal(t, uint64(3), rows)
	})
}

func TestStatementRUIndexJoinTerminalBarriers(t *testing.T) {
	type barrierFixture struct {
		executor      exec.Executor
		signalEOF     func()
		finishWorkers func(*statementRUJoinRuntime)
	}
	for _, test := range []struct {
		name string
		new  func(sessionctx.Context, *statementRUJoinRuntime) barrierFixture
	}{
		{name: "IndexLookUpJoin", new: func(ctx sessionctx.Context, runtime *statementRUJoinRuntime) barrierFixture {
			resultCh := make(chan *lookUpJoinTask)
			workerWG := &sync.WaitGroup{}
			workerWG.Add(2)
			executor := &IndexLookUpJoin{
				BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(), 0),
				WorkerWg:     workerWG,
				prepared:     true,
				resultCh:     resultCh,
				statementRU:  runtime,
			}
			executor.JoinResult = exec.NewFirstChunk(executor)
			return barrierFixture{
				executor: executor,
				signalEOF: func() {
					resultCh <- nil
					close(resultCh)
				},
				finishWorkers: func(runtime *statementRUJoinRuntime) {
					runtime.mergeBuildRowsAndDone(2, workerWG)
					runtime.mergeProbeRowsAndDone(3, workerWG)
				},
			}
		}},
		{name: "IndexNestedLoopHashJoin", new: func(ctx sessionctx.Context, runtime *statementRUJoinRuntime) barrierFixture {
			resultCh := make(chan *indexHashJoinResult)
			workerWG := &sync.WaitGroup{}
			workerWG.Add(1)
			executor := &IndexNestedLoopHashJoin{
				IndexLookUpJoin: IndexLookUpJoin{
					BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(), 0),
					WorkerWg:     workerWG,
					statementRU:  runtime,
				},
				resultCh:      resultCh,
				prepared:      true,
				ctxWithCancel: context.Background(),
			}
			go executor.wait4JoinWorkers()
			return barrierFixture{
				executor:  executor,
				signalEOF: func() {},
				finishWorkers: func(runtime *statementRUJoinRuntime) {
					runtime.addBuildRows(2)
					runtime.addProbeRows(3)
					workerWG.Done()
				},
			}
		}},
		{name: "IndexLookUpMergeJoin", new: func(ctx sessionctx.Context, runtime *statementRUJoinRuntime) barrierFixture {
			resultCh := make(chan *lookUpMergeJoinTask)
			workerWG := &sync.WaitGroup{}
			workerWG.Add(2)
			executor := &IndexLookUpMergeJoin{
				BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(), 0),
				WorkerWg:     workerWG,
				prepared:     true,
				resultCh:     resultCh,
				statementRU:  runtime,
			}
			return barrierFixture{
				executor: executor,
				signalEOF: func() {
					resultCh <- nil
					close(resultCh)
				},
				finishWorkers: func(runtime *statementRUJoinRuntime) {
					runtime.mergeBuildRowsAndDone(2, workerWG)
					runtime.mergeProbeRowsAndDone(3, workerWG)
				},
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := newStatementRUJoinContext()
			runtime := &statementRUJoinRuntime{}
			fixture := test.new(ctx, runtime)
			statement := configureStatementRUJoin(t, ctx, fixture.executor, 2)
			releaseWorker := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
			t.Cleanup(release)
			go func() {
				<-releaseWorker
				fixture.finishWorkers(runtime)
			}()

			req := exec.NewFirstChunk(fixture.executor)
			nextStarted := make(chan struct{})
			nextDone := make(chan error, 1)
			go func() {
				close(nextStarted)
				nextDone <- exec.Next(context.Background(), fixture.executor, req)
			}()
			<-nextStarted
			fixture.signalEOF()
			require.Never(t, func() bool {
				select {
				case <-nextDone:
					return true
				default:
					return false
				}
			}, 50*time.Millisecond, time.Millisecond, "Next returned before worker-local RU counters crossed the terminal barrier")

			release()
			select {
			case err := <-nextDone:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Next did not return after the terminal worker barrier was released")
			}
			require.Zero(t, req.NumRows())

			units := finishStatementRUJoin(t, statement, statementru.TerminalSuccess)
			require.Equal(t, float64(10), units[statementru.CPUWork])
			require.Zero(t, units[statementru.JoinOutputRows])
		})
	}
}

func TestStatementRUConcurrentCloseSuppressesAsyncTerminal(t *testing.T) {
	type closeFixture struct {
		executor  exec.Executor
		statement *statementru.Statement
		runtime   *statementRUJoinRuntime
		armClose  func(<-chan struct{})
	}

	newFinished := func() *atomic.Value {
		finished := &atomic.Value{}
		finished.Store(false)
		return finished
	}

	for _, test := range []struct {
		name string
		new  func(*testing.T, sessionctx.Context) closeFixture
	}{
		{name: "HashJoinV1", new: func(t *testing.T, ctx sessionctx.Context) closeFixture {
			executor := newStatementRUHashJoinV1(ctx, false)
			statement := configureStatementRUJoin(t, ctx, executor, 2)
			require.NoError(t, exec.Open(context.Background(), executor))
			hashCtx := &HashContext{
				AllTypes:    executor.BuildTypes,
				KeyColIdx:   executor.BuildWorker.BuildKeyColIdx,
				NaKeyColIdx: executor.BuildWorker.BuildNAKeyColIdx,
			}
			executor.RowContainer = newHashRowContainer(executor.Ctx(), hashCtx, exec.RetTypes(executor.BuildWorker.BuildSideExec))
			executor.initializeForProbe()
			executor.Prepared = true
			return closeFixture{
				executor:  executor,
				statement: statement,
				runtime:   executor.statementRU,
				armClose: func(nextReturned <-chan struct{}) {
					go func() {
						<-executor.closeCh
						close(executor.joinResultCh)
						for _, resultCh := range executor.ProbeSideTupleFetcher.probeResultChs {
							close(resultCh)
						}
						<-nextReturned
					}()
				},
			}
		}},
		{name: "HashJoinV2", new: func(t *testing.T, ctx sessionctx.Context) closeFixture {
			executor := newStatementRUHashJoinV2(ctx, false)
			statement := configureStatementRUJoin(t, ctx, executor, 2)
			require.NoError(t, exec.Open(context.Background(), executor))
			executor.initHashTableContext()
			executor.initializeForProbe()
			executor.prepared = true
			return closeFixture{
				executor:  executor,
				statement: statement,
				runtime:   executor.statementRU,
				armClose: func(nextReturned <-chan struct{}) {
					go func() {
						<-executor.closeCh
						close(executor.joinResultCh)
						for _, resultCh := range executor.ProbeSideTupleFetcher.probeResultChs {
							close(resultCh)
						}
						<-nextReturned
					}()
				},
			}
		}},
		{name: "IndexLookUpJoin", new: func(t *testing.T, ctx sessionctx.Context) closeFixture {
			resultCh := make(chan *lookUpJoinTask)
			runtime := &statementRUJoinRuntime{}
			executor := &IndexLookUpJoin{
				BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(), 0),
				WorkerWg:     &sync.WaitGroup{},
				Finished:     newFinished(),
				prepared:     true,
				resultCh:     resultCh,
				statementRU:  runtime,
			}
			executor.JoinResult = exec.NewFirstChunk(executor)
			statement := configureStatementRUJoin(t, ctx, executor, 2)
			return closeFixture{
				executor:  executor,
				statement: statement,
				runtime:   runtime,
				armClose: func(nextReturned <-chan struct{}) {
					executor.cancelFunc = func() {
						close(resultCh)
						<-nextReturned
					}
				},
			}
		}},
		{name: "IndexNestedLoopHashJoin", new: func(t *testing.T, ctx sessionctx.Context) closeFixture {
			resultCh := make(chan *indexHashJoinResult)
			runtime := &statementRUJoinRuntime{}
			executor := &IndexNestedLoopHashJoin{
				IndexLookUpJoin: IndexLookUpJoin{
					BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(), 0),
					WorkerWg:     &sync.WaitGroup{},
					Finished:     newFinished(),
					statementRU:  runtime,
				},
				resultCh:      resultCh,
				prepared:      true,
				ctxWithCancel: context.Background(),
			}
			statement := configureStatementRUJoin(t, ctx, executor, 2)
			return closeFixture{
				executor:  executor,
				statement: statement,
				runtime:   runtime,
				armClose: func(nextReturned <-chan struct{}) {
					executor.cancelFunc = func() {
						close(resultCh)
						<-nextReturned
					}
				},
			}
		}},
		{name: "IndexLookUpMergeJoin", new: func(t *testing.T, ctx sessionctx.Context) closeFixture {
			resultCh := make(chan *lookUpMergeJoinTask)
			runtime := &statementRUJoinRuntime{}
			executor := &IndexLookUpMergeJoin{
				BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(), 0),
				WorkerWg:     &sync.WaitGroup{},
				prepared:     true,
				resultCh:     resultCh,
				statementRU:  runtime,
			}
			statement := configureStatementRUJoin(t, ctx, executor, 2)
			return closeFixture{
				executor:  executor,
				statement: statement,
				runtime:   runtime,
				armClose: func(nextReturned <-chan struct{}) {
					executor.cancelFunc = func() {
						close(resultCh)
						<-nextReturned
					}
				},
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := newStatementRUJoinContext()
			fixture := test.new(t, ctx)
			fixture.runtime.addBuildRows(2)
			fixture.runtime.addProbeRows(3)
			fixture.runtime.setHashStateRows(7)

			req := exec.NewFirstChunk(fixture.executor)
			nextStarted := make(chan struct{})
			nextReturned := make(chan struct{})
			nextErr := make(chan error, 1)
			fixture.armClose(nextReturned)
			go func() {
				close(nextStarted)
				nextErr <- exec.Next(context.Background(), fixture.executor, req)
				close(nextReturned)
			}()
			<-nextStarted
			require.Never(t, func() bool {
				select {
				case <-nextReturned:
					return true
				default:
					return false
				}
			}, 20*time.Millisecond, time.Millisecond, "Next was not blocked before concurrent Close")

			closeErr := make(chan error, 1)
			go func() { closeErr <- exec.Close(fixture.executor) }()
			select {
			case <-nextReturned:
			case <-time.After(5 * time.Second):
				t.Fatal("blocked Next did not return after concurrent Close")
			}
			require.NoError(t, <-nextErr)
			select {
			case err := <-closeErr:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("concurrent Close did not return")
			}

			require.True(t, fixture.statement.EvidenceRecorder().MarkPresent(fixture.statement.UnitRecorder().CollectedUnits()))
			finish, first := fixture.statement.Finish(statementru.TerminalCanceled)
			require.True(t, first)
			require.False(t, finish.Result.HasTotal())
			require.Equal(t, statementru.Outcome{
				State:  statementru.StatePartial,
				Reason: statementru.ReasonUnsupported,
			}, finish.Result.Outcome())
			units, ok := finish.Result.Units()
			require.True(t, ok)
			require.Zero(t, units[statementru.CPUWork])
			require.Zero(t, units[statementru.HashStateRows])
			require.Zero(t, units[statementru.JoinOutputRows])
		})
	}
}

func TestStatementRUMergeJoin(t *testing.T) {
	ctx := newStatementRUJoinContext()
	outer := newStatementRUJoinDataSource(ctx, false)
	inner := newStatementRUJoinDataSource(ctx, false)
	outerKey := outer.Schema().Columns[0]
	innerKey := inner.Schema().Columns[0]
	joinSchema := expression.NewSchema(outerKey, innerKey)
	executor := &MergeJoinExec{
		BaseExecutor: exec.NewBaseExecutor(ctx, joinSchema, 0, outer, inner),
		StmtCtx:      ctx.GetSessionVars().StmtCtx,
		CompareFuncs: []expression.CompareFunc{expression.GetCmpFunction(ctx.GetExprCtx(), outerKey, innerKey)},
		Joiner: NewJoiner(ctx, plannerbase.InnerJoin, false, nil, nil,
			exec.RetTypes(outer), exec.RetTypes(inner), nil, false),
		InnerTable: &MergeJoinTable{IsInner: true, ChildIndex: 1, JoinKeys: []*expression.Column{innerKey}},
		OuterTable: &MergeJoinTable{ChildIndex: 0, JoinKeys: []*expression.Column{outerKey}},
	}
	statement := configureStatementRUJoin(t, ctx, executor, 2)
	require.Equal(t, 3, drainStatementRUJoin(t, executor))
	units := finishStatementRUJoin(t, statement, statementru.TerminalSuccess)
	require.Equal(t, float64(12), units[statementru.CPUWork])
	require.Equal(t, float64(3), units[statementru.JoinOutputRows])
}

func newStatementRUHashJoinV1(ctx sessionctx.Context, failBuild bool) *HashJoinV1Exec {
	probe := newStatementRUJoinDataSource(ctx, false)
	build := newStatementRUJoinDataSource(ctx, failBuild)
	hashCtx := &HashJoinCtxV1{
		ProbeTypes: exec.RetTypes(probe),
		BuildTypes: exec.RetTypes(build),
	}
	hashCtx.SessCtx = ctx
	hashCtx.JoinType = plannerbase.InnerJoin
	hashCtx.Concurrency = 1
	executor := &HashJoinV1Exec{
		BaseExecutor:          exec.NewBaseExecutor(ctx, expression.NewSchema(build.Schema().Columns[0], probe.Schema().Columns[0]), 0, build, probe),
		HashJoinCtxV1:         hashCtx,
		ProbeSideTupleFetcher: &ProbeSideTupleFetcherV1{},
		ProbeWorkers:          make([]*ProbeWorkerV1, 1),
		BuildWorker:           &BuildWorkerV1{},
	}
	hashCtx.ChunkAllocPool = executor.AllocPool
	executor.ProbeSideTupleFetcher.ProbeSideExec = probe
	executor.BuildWorker.BuildSideExec = build
	executor.BuildWorker.BuildKeyColIdx = []int{0}
	executor.BuildWorker.HashJoinCtx = hashCtx
	executor.ProbeWorkers[0] = &ProbeWorkerV1{
		HashJoinCtx:    hashCtx,
		ProbeKeyColIdx: []int{0},
		Joiner: NewJoiner(ctx, plannerbase.InnerJoin, false, nil, nil,
			exec.RetTypes(build), exec.RetTypes(probe), nil, false),
	}
	executor.ProbeWorkers[0].WorkerID = 0
	return executor
}

func newStatementRUHashJoinV2(ctx sessionctx.Context, failBuild bool) *HashJoinV2Exec {
	left := newStatementRUJoinDataSource(ctx, false)
	right := newStatementRUJoinDataSource(ctx, failBuild)
	return buildHashJoinV2Exec(&hashJoinInfo{
		ctx:              ctx,
		schema:           expression.NewSchema(left.Schema().Columns[0], right.Schema().Columns[0]),
		leftExec:         left,
		rightExec:        right,
		joinType:         plannerbase.InnerJoin,
		rightAsBuildSide: true,
		buildKeys:        []*expression.Column{right.Schema().Columns[0]},
		probeKeys:        []*expression.Column{left.Schema().Columns[0]},
		lUsed:            []int{0},
		rUsed:            []int{0},
	})
}

func TestStatementRUHashJoinCompletion(t *testing.T) {
	builders := []struct {
		name string
		new  func(sessionctx.Context, bool) exec.Executor
	}{
		{name: "V1", new: func(ctx sessionctx.Context, fail bool) exec.Executor { return newStatementRUHashJoinV1(ctx, fail) }},
		{name: "V2", new: func(ctx sessionctx.Context, fail bool) exec.Executor { return newStatementRUHashJoinV2(ctx, fail) }},
	}
	for _, builder := range builders {
		t.Run(builder.name+" off mode allocates no runtime", func(t *testing.T) {
			ctx := newStatementRUJoinContext()
			executor := builder.new(ctx, false)
			require.NoError(t, exec.Open(context.Background(), executor))
			switch join := executor.(type) {
			case *HashJoinV1Exec:
				require.Nil(t, join.statementRU)
			case *HashJoinV2Exec:
				require.Nil(t, join.statementRU)
			default:
				t.Fatalf("unexpected hash join %T", executor)
			}
			require.NoError(t, exec.Close(executor))
		})

		t.Run(builder.name+" normal", func(t *testing.T) {
			ctx := newStatementRUJoinContext()
			executor := builder.new(ctx, false)
			statement := configureStatementRUJoin(t, ctx, executor, 2)
			require.Equal(t, 3, drainStatementRUJoin(t, executor))
			units := finishStatementRUJoin(t, statement, statementru.TerminalSuccess)
			require.Equal(t, float64(12), units[statementru.CPUWork])
			require.Equal(t, float64(3), units[statementru.HashStateRows])
			require.Equal(t, float64(3), units[statementru.JoinOutputRows])
		})

		t.Run(builder.name+" build error", func(t *testing.T) {
			ctx := newStatementRUJoinContext()
			executor := builder.new(ctx, true)
			statement := configureStatementRUJoin(t, ctx, executor, 2)
			require.NoError(t, exec.Open(context.Background(), executor))
			require.ErrorIs(t, exec.Next(context.Background(), executor, exec.NewFirstChunk(executor)), errStatementRUJoinChild)
			require.NoError(t, exec.Close(executor))
			units := finishStatementRUJoin(t, statement, statementru.TerminalError)
			require.Zero(t, units[statementru.CPUWork])
			require.Zero(t, units[statementru.HashStateRows])
			require.Zero(t, units[statementru.JoinOutputRows])
		})

		t.Run(builder.name+" upper early close", func(t *testing.T) {
			ctx := newStatementRUJoinContext()
			executor := builder.new(ctx, false)
			statement := configureStatementRUJoin(t, ctx, executor, 2)
			require.NoError(t, exec.Open(context.Background(), executor))
			req := exec.NewFirstChunk(executor)
			require.NoError(t, exec.Next(context.Background(), executor, req))
			require.NotZero(t, req.NumRows())
			require.NoError(t, exec.Close(executor))
			units := finishStatementRUJoin(t, statement, statementru.TerminalSuccess)
			require.Zero(t, units[statementru.CPUWork])
			require.Zero(t, units[statementru.HashStateRows])
			require.Equal(t, float64(req.NumRows()), units[statementru.JoinOutputRows])
		})
	}
}

func prepareStatementRUJoinDataSources(executor exec.Executor) {
	for _, child := range executor.AllChildren() {
		switch dataSource := child.(type) {
		case *testutil.MockDataSource:
			dataSource.PrepareChunks()
		case *statementRUJoinErrorDataSource:
			dataSource.MockDataSource.PrepareChunks()
		}
	}
}

func BenchmarkStatementRUHashJoinAccounting(b *testing.B) {
	for _, builder := range []struct {
		name string
		new  func(sessionctx.Context, bool) exec.Executor
	}{
		{name: "V1", new: func(ctx sessionctx.Context, fail bool) exec.Executor { return newStatementRUHashJoinV1(ctx, fail) }},
		{name: "V2", new: func(ctx sessionctx.Context, fail bool) exec.Executor { return newStatementRUHashJoinV2(ctx, fail) }},
	} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/enabled=%t", builder.name, enabled), func(b *testing.B) {
				ctx := newStatementRUJoinContext()
				executor := builder.new(ctx, false)
				var statement *statementru.Statement
				if enabled {
					statement = configureStatementRUJoin(b, ctx, executor, 2)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					prepareStatementRUJoinDataSources(executor)
					b.StartTimer()
					require.Equal(b, 3, drainStatementRUJoin(b, executor))
				}
				b.StopTimer()

				if statement != nil {
					units := finishStatementRUJoin(b, statement, statementru.TerminalSuccess)
					require.Positive(b, units[statementru.CPUWork])
					require.Positive(b, units[statementru.HashStateRows])
					require.Positive(b, units[statementru.JoinOutputRows])
				}
			})
		}
	}
}

func BenchmarkStatementRUHashJoinOpenClose(b *testing.B) {
	for _, builder := range []struct {
		name string
		new  func(sessionctx.Context, bool) exec.Executor
	}{
		{name: "V1", new: func(ctx sessionctx.Context, fail bool) exec.Executor { return newStatementRUHashJoinV1(ctx, fail) }},
		{name: "V2", new: func(ctx sessionctx.Context, fail bool) exec.Executor { return newStatementRUHashJoinV2(ctx, fail) }},
	} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/enabled=%t", builder.name, enabled), func(b *testing.B) {
				ctx := newStatementRUJoinContext()
				executor := builder.new(ctx, false)
				if enabled {
					require.NotNil(b, configureStatementRUJoin(b, ctx, executor, 2))
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := exec.Open(context.Background(), executor); err != nil {
						b.Fatal(err)
					}
					if err := exec.Close(executor); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
