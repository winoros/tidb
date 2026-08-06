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

package executor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

type statementRUBlockingSnapshot struct {
	kv.Snapshot
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

type statementRUPanickingSnapshot struct {
	kv.Snapshot
}

type statementRUSharedSnapshotSlot struct {
	mu         sync.Mutex
	current    any
	clearCount int
}

type statementRUSharedSnapshot struct {
	kv.Snapshot
	slot *statementRUSharedSnapshotSlot
}

func (s *statementRUSharedSnapshot) SetOption(opt int, value any) {
	if opt != kv.CollectRuntimeStats {
		return
	}
	s.slot.mu.Lock()
	defer s.slot.mu.Unlock()
	s.slot.current = value
	if value == nil {
		s.slot.clearCount++
	}
}

func (*statementRUPanickingSnapshot) Get(
	context.Context,
	kv.Key,
	...kv.GetOption,
) (kv.ValueEntry, error) {
	panic("statement RU point-read test panic")
}

func (s *statementRUBlockingSnapshot) Get(
	ctx context.Context,
	key kv.Key,
	options ...kv.GetOption,
) (kv.ValueEntry, error) {
	s.startOnce.Do(func() { close(s.started) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return kv.ValueEntry{}, ctx.Err()
	}
	return s.Snapshot.Get(ctx, key, options...)
}

// StatementRUPointGetReadStatsForTest exposes point-read test state to the
// external-package SQL lifecycle tests without widening the production API.
func StatementRUPointGetReadStatsForTest(e *PointGetExecutor) (*txnsnapshot.SnapshotRuntimeStats, bool) {
	return e.statementRUPointRead.stats, e.statementRUPointRead.owner != nil
}

// StatementRUBatchPointGetReadStatsForTest is the BatchPointGet counterpart
// of StatementRUPointGetReadStatsForTest.
func StatementRUBatchPointGetReadStatsForTest(e *BatchPointGetExec) (*txnsnapshot.SnapshotRuntimeStats, bool) {
	return e.statementRUPointRead.stats, e.statementRUPointRead.owner != nil
}

// BlockStatementRUPointGetForTest pauses the next snapshot Get so an
// external-package test can exercise the real executor's concurrent
// Next/Close lifecycle.
func BlockStatementRUPointGetForTest(e *PointGetExecutor) (<-chan struct{}, func()) {
	if e == nil || e.snapshot == nil {
		return nil, nil
	}
	blocking := &statementRUBlockingSnapshot{
		Snapshot: e.snapshot,
		started:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	e.snapshot = blocking
	var releaseOnce sync.Once
	return blocking.started, func() {
		releaseOnce.Do(func() { close(blocking.release) })
	}
}

// StatementRUPointGetClosingForTest reports whether Close has entered the
// generation owner, making the concurrent Next/Close assertion deterministic.
func StatementRUPointGetClosingForTest(e *PointGetExecutor) bool {
	if e == nil || e.statementRUPointRead.owner == nil {
		return false
	}
	e.statementRUPointRead.owner.mu.Lock()
	defer e.statementRUPointRead.owner.mu.Unlock()
	return e.statementRUPointRead.owner.closing
}

// PanicStatementRUPointGetForTest makes the next snapshot Get panic so the
// external lifecycle test can verify that a recovered executor panic is not
// submitted as a successful RU contribution.
func PanicStatementRUPointGetForTest(e *PointGetExecutor) {
	if e != nil && e.snapshot != nil {
		e.snapshot = &statementRUPanickingSnapshot{Snapshot: e.snapshot}
	}
}

func completePointReadOperationWithCounts(
	totalVersions, processedVersions, totalSize, processedSize uint64,
) txnsnapshot.PointReadOperationStats {
	detail := txnsnapshot.PointReadScanDetail{
		TotalVersions:         totalVersions,
		TotalVersionsSize:     totalSize,
		ProcessedVersions:     processedVersions,
		ProcessedVersionsSize: processedSize,
	}
	if totalSize == 0 {
		detail.FallbackTotalVersions = totalVersions
		detail.FallbackProcessedVersions = processedVersions
		detail.FallbackProcessedVersionsSize = processedSize
	}
	return txnsnapshot.PointReadOperationStats{
		ScanDetail:          detail,
		StartedOperations:   1,
		CompletedOperations: 1,
		RPCAttempts:         1,
		CompletedResponses:  1,
		ScanDetailResponses: 1,
	}
}

func completePointReadOperation(totalSize, processedSize uint64) txnsnapshot.PointReadOperationStats {
	return completePointReadOperationWithCounts(1, 1, totalSize, processedSize)
}

func TestCompletePointReadScanBytes(t *testing.T) {
	stats := txnsnapshot.PointReadRuntimeStats{
		Get:      completePointReadOperation(100, 80),
		BatchGet: completePointReadOperationWithCounts(4, 2, 0, 40),
	}
	bytes, complete, usable := completePointReadScanBytes(stats)
	require.Equal(t, float64(180), bytes)
	require.True(t, complete)
	require.True(t, usable)

	stats.Get = completePointReadOperation(0, 0)
	stats.BatchGet = txnsnapshot.PointReadOperationStats{}
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.True(t, usable)

	stats.Get = completePointReadOperationWithCounts(2, 0, 0, 0)
	bytes, complete, usable = completePointReadScanBytes(stats)
	require.Zero(t, bytes)
	require.True(t, complete)
	require.True(t, usable)

	stats.Get = completePointReadOperationWithCounts(3, 2, 0, 5)
	bytes, complete, usable = completePointReadScanBytes(stats)
	require.Equal(t, 7.5, bytes)
	require.True(t, complete)
	require.True(t, usable)

	stats.Get = txnsnapshot.PointReadOperationStats{
		ScanDetail: txnsnapshot.PointReadScanDetail{
			TotalVersions:                 9,
			TotalVersionsSize:             50,
			ProcessedVersions:             5,
			ProcessedVersionsSize:         50,
			FallbackTotalVersions:         4,
			FallbackProcessedVersions:     2,
			FallbackProcessedVersionsSize: 20,
		},
		StartedOperations:   1,
		CompletedOperations: 1,
		RPCAttempts:         2,
		CompletedResponses:  2,
		ScanDetailResponses: 2,
	}
	bytes, complete, usable = completePointReadScanBytes(stats)
	require.Equal(t, float64(90), bytes)
	require.True(t, complete)
	require.True(t, usable)

	stats.Get = completePointReadOperationWithCounts(2, 1, 10, 20)
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.True(t, usable)

	stats.Get = txnsnapshot.PointReadOperationStats{
		StartedOperations:   1,
		CompletedOperations: 1,
	}
	stats.BatchGet = txnsnapshot.PointReadOperationStats{}
	bytes, complete, usable = completePointReadScanBytes(stats)
	require.Zero(t, bytes)
	require.True(t, complete)
	require.False(t, usable)

	stats.Get = txnsnapshot.PointReadOperationStats{}
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.False(t, usable)

	stats.Get = txnsnapshot.PointReadOperationStats{
		StartedOperations:   1,
		CompletedOperations: 1,
		RPCAttempts:         1,
	}
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.False(t, usable)

	stats.Get.CompletedResponses = 1
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.True(t, usable)

	stats.Get = completePointReadOperation(1, 1)
	stats.BufferBatchGet.StartedOperations = 1
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.True(t, usable)

	stats.BufferBatchGet = txnsnapshot.PointReadOperationStats{}
	stats.BatchGet = txnsnapshot.PointReadOperationStats{CompletedResponses: 1}
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.True(t, usable)

	stats.BatchGet = txnsnapshot.PointReadOperationStats{}
	stats.Get = completePointReadOperation(maxExactPointReadRUInteger, 1)
	stats.BatchGet = completePointReadOperation(1, 1)
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.True(t, usable)

	stats.BatchGet = txnsnapshot.PointReadOperationStats{}
	stats.Get = completePointReadOperationWithCounts(2, 1, 0, maxExactPointReadRUInteger)
	_, complete, usable = completePointReadScanBytes(stats)
	require.False(t, complete)
	require.True(t, usable)
}

func TestSubmitPointReadScanCompleteNonzero(t *testing.T) {
	weights := statementru.Weights{statementru.ScanBytes: 1}
	statement := statementru.NewStatement(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.ScanBytes.Mask(),
		Weights:       &weights,
	})
	scan := statement.UnitContributorRegistrar().RegisterUnitContributor(statementru.ScanBytes.Mask())
	submitPointReadScan(scan, txnsnapshot.PointReadRuntimeStats{
		Get: completePointReadOperation(100, 80),
	}, true, false)
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.Equal(t, statementru.StateComplete, finish.Result.Outcome().State)
	units, retained := finish.Result.Units()
	require.True(t, retained)
	require.Equal(t, float64(100), units[statementru.ScanBytes])
}

func TestStatementRUPointReadWaitsForReadPhase(t *testing.T) {
	weights := statementru.Weights{statementru.ScanBytes: 1}
	statement := statementru.NewStatement(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.ScanBytes.Mask(),
		Weights:       &weights,
	})
	stats := &txnsnapshot.SnapshotRuntimeStats{}
	stats.EnablePointReadStats()
	owner := &statementRUPointReadOwner{
		stats: stats,
		scan:  statement.UnitContributorRegistrar().RegisterUnitContributor(statementru.ScanBytes.Mask()),
	}
	require.True(t, owner.beginNext())
	owner.finishCall(true, false)
	owner.mu.Lock()
	require.False(t, owner.done)
	require.Zero(t, owner.activeNext)
	owner.mu.Unlock()

	require.True(t, owner.beginNext())
	owner.finishCall(true, true)
	owner.close()
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.Equal(t, statementru.StateUnavailable, finish.Result.Outcome().State)
}

func TestStatementRUPointReadIgnoresUnrequestedScanBytes(t *testing.T) {
	weights := statementru.Weights{statementru.CPUWork: 1}
	for _, test := range []struct {
		name      string
		collected statementru.UnitMask
		wantOwner bool
	}{
		{name: "zero defaults to required"},
		{name: "explicit required-only mask", collected: statementru.CPUWork.Mask()},
		{
			name:      "optional collected scan activates owner",
			collected: statementru.CPUWork.Mask() | statementru.ScanBytes.Mask(),
			wantOwner: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sc := stmtctx.NewStmtCtx()
			require.True(t, sc.ConfigureStatementRU(statementru.Selection{
				Mode:           statementru.ModeCalibration,
				Applicable:     true,
				RequiredUnits:  statementru.CPUWork.Mask(),
				CollectedUnits: test.collected,
				Weights:        &weights,
			}))
			attachment := prepareStatementRUPointRead(sc, nil, true)
			require.Equal(t, test.wantOwner, attachment.owner != nil)
			require.Equal(t, test.wantOwner, attachment.stats != nil)
			require.Equal(t, test.wantOwner, attachment.registry != nil)

			statement := sc.TakeStatementRUForExecution()
			require.NotNil(t, statement)
			cpu := statement.UnitContributorRegistrar().RegisterUnitContributor(statementru.CPUWork.Mask())
			require.NotNil(t, cpu)
			var values statementru.UnitValues
			values[statementru.CPUWork] = 7
			require.True(t, cpu.Complete(values))
			if attachment.owner != nil {
				attachment.owner.finish(false)
			}
			finish, first := statement.Finish(statementru.TerminalSuccess)
			require.True(t, first)
			require.Equal(t, statementru.Outcome{State: statementru.StateComplete}, finish.Result.Outcome())
			total, ok := finish.Result.TotalRU()
			require.True(t, ok)
			require.Equal(t, float64(7), total)
			coverage, ok := finish.Result.Coverage()
			require.True(t, ok)
			if test.wantOwner {
				require.Equal(t, statementru.ScanBytes.Mask(), coverage.UnavailableUnits)
			} else {
				require.Zero(t, coverage.UnavailableUnits)
			}
		})
	}
}

func TestStatementRUPointReadSharedSnapshotFailsClosed(t *testing.T) {
	weights := statementru.Weights{statementru.ScanBytes: 1}
	sc := stmtctx.NewStmtCtx()
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.ScanBytes.Mask(),
		Weights:       &weights,
	}))
	first := prepareStatementRUPointRead(sc, nil, true)
	second := prepareStatementRUPointRead(sc, nil, true)
	slot := &statementRUSharedSnapshotSlot{}
	firstSnapshot := &statementRUSharedSnapshot{slot: slot}
	secondSnapshot := &statementRUSharedSnapshot{slot: slot}

	first.install(firstSnapshot)
	require.True(t, first.beginNext())
	first.finishCall(true, true)
	second.install(secondSnapshot)
	require.True(t, second.beginNext())
	second.finishCall(true, true)

	first.owner.mu.Lock()
	require.True(t, first.owner.snapshotContended)
	first.owner.mu.Unlock()
	second.owner.mu.Lock()
	require.True(t, second.owner.snapshotContended)
	second.owner.mu.Unlock()

	first.close()
	slot.mu.Lock()
	require.NotNil(t, slot.current)
	require.Zero(t, slot.clearCount)
	slot.mu.Unlock()
	second.close()
	slot.mu.Lock()
	require.Nil(t, slot.current)
	require.Equal(t, 2, slot.clearCount)
	slot.mu.Unlock()

	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)
	result, firstFinish := statement.Finish(statementru.TerminalSuccess)
	require.True(t, firstFinish)
	require.False(t, result.Result.HasTotal())
	require.Equal(t, statementru.StateUnavailable, result.Result.Outcome().State)
}

func TestStatementRUPointReadUnobservedUnavailable(t *testing.T) {
	weights := statementru.Weights{statementru.ScanBytes: 1}
	statement := statementru.NewStatement(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.ScanBytes.Mask(),
		Weights:       &weights,
	})
	stats := &txnsnapshot.SnapshotRuntimeStats{}
	stats.EnablePointReadStats()
	owner := &statementRUPointReadOwner{
		stats: stats,
		scan:  statement.UnitContributorRegistrar().RegisterUnitContributor(statementru.ScanBytes.Mask()),
	}
	owner.finish(true)
	result, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.False(t, result.Result.HasTotal())
	require.Equal(t, statementru.StateUnavailable, result.Result.Outcome().State)
}

func TestStatementRUPointReadUnsupportedLockScan(t *testing.T) {
	weights := statementru.Weights{statementru.ScanBytes: 1}
	sc := stmtctx.NewStmtCtx()
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.ScanBytes.Mask(),
		Weights:       &weights,
	}))
	attachment := prepareStatementRUPointRead(sc, nil, false)
	require.Nil(t, attachment.stats)
	require.Nil(t, attachment.owner)
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.False(t, finish.Result.HasTotal())
	require.Equal(t, statementru.ReasonUnsupported, finish.Result.Outcome().Reason)
}

func TestStatementRUPointReadCloseWaitsForNext(t *testing.T) {
	weights := statementru.Weights{statementru.ScanBytes: 1}
	statement := statementru.NewStatement(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.ScanBytes.Mask(),
		Weights:       &weights,
	})
	stats := &txnsnapshot.SnapshotRuntimeStats{}
	stats.EnablePointReadStats()
	owner := &statementRUPointReadOwner{
		stats: stats,
		scan:  statement.UnitContributorRegistrar().RegisterUnitContributor(statementru.ScanBytes.Mask()),
	}
	require.True(t, owner.beginNext())
	closed := make(chan struct{})
	go func() {
		owner.close()
		close(closed)
	}()
	require.Eventually(t, func() bool {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		return owner.closing
	}, 5*time.Second, time.Millisecond)
	select {
	case <-closed:
		t.Fatal("Close returned while Next was active")
	default:
	}

	owner.finishCall(true, true)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after Next quiesced")
	}
	require.False(t, owner.beginNext())
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.Equal(t, statementru.StateUnavailable, finish.Result.Outcome().State)
}
