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
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlmock "github.com/pingcap/tidb/pkg/util/topsql/collector/mock"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	metastorage "github.com/tikv/pd/client/clients/metastorage"
	"github.com/tikv/pd/client/opt"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type stmtStatsTestContext struct {
	*mock.Context
	stmtStats *stmtstats.StatementStats
}

type sharedLockMemBufferForTest struct {
	kv.MemBuffer
	getLocal func(key []byte) ([]byte, error)
	rLocks   int
	rUnlocks int
}

func (m *sharedLockMemBufferForTest) GetLocal(_ context.Context, key []byte) ([]byte, error) {
	return m.getLocal(key)
}

func (m *sharedLockMemBufferForTest) RLock() {
	m.rLocks++
}

func (m *sharedLockMemBufferForTest) RUnlock() {
	m.rUnlocks++
}

type sharedLockTxnForTest struct {
	kv.Transaction
	memBuffer kv.MemBuffer
}

type recordSetTerminalExecutorForTest struct {
	exec.BaseExecutor
	next  func(*chunk.Chunk) error
	close func() error
}

func (e *recordSetTerminalExecutorForTest) Next(_ context.Context, req *chunk.Chunk) error {
	if e.next != nil {
		return e.next(req)
	}
	req.Reset()
	return nil
}

func (e *recordSetTerminalExecutorForTest) Close() error {
	if e.close != nil {
		return e.close()
	}
	return nil
}

type recordSetTerminalReporterForTest struct {
	reports []statementru.Report
}

func (r *recordSetTerminalReporterForTest) ReportStatementRU(report statementru.Report) {
	r.reports = append(r.reports, report)
}

func newStatementRURecordSetForTest(t *testing.T) (*recordSet, *recordSetTerminalReporterForTest) {
	t.Helper()
	sctx := mock.NewContext()
	sctx.GetSessionVars().StartTime = time.Now()
	reporter := &recordSetTerminalReporterForTest{}
	weights := statementru.Weights{statementru.FrontendCompileBytes: 1}
	statementRU := statementru.NewStatement(statementru.Selection{
		Mode:          statementru.ModeResultOnly,
		Applicable:    true,
		RequiredUnits: statementru.FrontendCompileBytes.Mask(),
		Weights:       &weights,
		Reporter:      reporter,
	})
	require.NotNil(t, statementRU)
	require.True(t, statementRU.UnitRecorder().Add(statementru.FrontendCompileBytes, 1))
	require.True(t, statementRU.EvidenceRecorder().MarkPresent(statementru.FrontendCompileBytes.Mask()))

	executorUnderTest := &recordSetTerminalExecutorForTest{
		BaseExecutor: exec.NewBaseExecutor(sctx, expression.NewSchema(), 0),
	}
	plan := physicalop.PhysicalTableDual{}.Init(sctx.GetPlanCtx(), &property.StatsInfo{}, 0)
	plan.SetSchema(expression.NewSchema())
	stmt := &ExecStmt{
		Ctx:         sctx,
		GoCtx:       context.Background(),
		Plan:        plan,
		StmtNode:    &ast.SelectStmt{},
		statementRU: statementRU,
	}
	return &recordSet{executor: executorUnderTest, stmt: stmt}, reporter
}

func (t *sharedLockTxnForTest) GetMemBuffer() kv.MemBuffer {
	return t.memBuffer
}

func (c *stmtStatsTestContext) GetStmtStats() *stmtstats.StatementStats {
	return c.stmtStats
}

func resetTopProfilingStateForTest(t *testing.T) {
	t.Helper()
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	t.Cleanup(func() {
		topsqlstate.DisableTopSQL()
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})
}

func newExecStmtWithStmtStatsForTest(goCtx context.Context, t *testing.T) (*ExecStmt, *stmtstats.StatementStats) {
	t.Helper()

	stats := stmtstats.CreateStatementStats()
	t.Cleanup(stats.SetFinished)

	sctx := mock.NewContext()
	sctx.GetSessionVars().User = &auth.UserIdentity{Username: "u1", Hostname: "%"}
	sc := sctx.GetSessionVars().StmtCtx
	sc.OriginalSQL = "select * from t where a = 1"
	_, sqlDigest := sc.SQLDigest()
	require.NotNil(t, sqlDigest)
	const normalizedPlan = "TableReader(table:t)->Selection(eq(test.t.a, ?))"
	planDigest := parser.NewDigest([]byte("topru-plan-digest"))
	sc.SetPlanDigest(normalizedPlan, planDigest)

	return &ExecStmt{
		Ctx: &stmtStatsTestContext{
			Context:   sctx,
			stmtStats: stats,
		},
		GoCtx: goCtx,
	}, stats
}

func newFinishedRecordSetForTest() *recordSet {
	ft := types.NewFieldType(mysql.TypeLonglong)
	return &recordSet{
		schema: expression.NewSchema(&expression.Column{RetType: ft}),
		stmt:   &ExecStmt{Ctx: mock.NewContext()},
	}
}

func TestRecordSetNewChunkAfterFinish(t *testing.T) {
	rs := newFinishedRecordSetForTest()

	req := rs.NewChunk(nil)
	require.NotNil(t, req)
	require.Equal(t, 1, req.NumCols())

	req = rs.NewChunk(chunk.NewAllocator())
	require.NotNil(t, req)
	require.Equal(t, 1, req.NumCols())
}

func TestRecordSetNextAfterFinish(t *testing.T) {
	rs := newFinishedRecordSetForTest()

	err := rs.Next(context.Background(), chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1))
	require.Error(t, err)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(err), err)
}

func TestRecordSetStatementRUTerminal(t *testing.T) {
	assertFrozenTerminal := func(
		t *testing.T,
		rs *recordSet,
		reporter *recordSetTerminalReporterForTest,
		terminal statementru.TerminalStatus,
	) {
		t.Helper()
		require.Empty(t, reporter.reports)
		finish, first := rs.stmt.statementRU.Finish(statementru.TerminalSuccess)
		require.False(t, first)
		require.Equal(t, terminal, finish.Terminal)
	}

	t.Run("kill after Finish but before Close", func(t *testing.T) {
		rs, reporter := newStatementRURecordSetForTest(t)
		require.NoError(t, rs.Next(context.Background(), rs.NewChunk(nil)))
		require.NoError(t, rs.Finish())

		killer := &rs.stmt.Ctx.GetSessionVars().SQLKiller
		killer.SendKillSignal(sqlkiller.QueryInterrupted)
		t.Cleanup(killer.Reset)
		require.NoError(t, rs.Close())
		assertFrozenTerminal(t, rs, reporter, statementru.TerminalCanceled)
	})

	t.Run("kill before Next", func(t *testing.T) {
		rs, reporter := newStatementRURecordSetForTest(t)
		killer := &rs.stmt.Ctx.GetSessionVars().SQLKiller
		killer.SendKillSignal(sqlkiller.QueryInterrupted)
		t.Cleanup(killer.Reset)

		require.Error(t, rs.Next(context.Background(), rs.NewChunk(nil)))
		require.NoError(t, rs.Close())
		assertFrozenTerminal(t, rs, reporter, statementru.TerminalCanceled)
	})

	t.Run("recovered outer Next panic", func(t *testing.T) {
		rs, reporter := newStatementRURecordSetForTest(t)
		testfailpoint.EnableCall(
			t,
			"github.com/pingcap/tidb/pkg/executor/statementRURecordSetNextAfterExecForTest",
			func(stmt *ExecStmt) {
				if stmt == rs.stmt {
					panic("record-set outer Next panic")
				}
			},
		)

		require.Error(t, rs.Next(context.Background(), rs.NewChunk(nil)))
		require.NoError(t, rs.Close())
		assertFrozenTerminal(t, rs, reporter, statementru.TerminalError)
	})

	t.Run("recovered Finish panic", func(t *testing.T) {
		rs, reporter := newStatementRURecordSetForTest(t)
		panicErr := exeerrors.ErrQueryInterrupted.GenWithStackByArgs()
		testfailpoint.EnableCall(
			t,
			"github.com/pingcap/tidb/pkg/executor/statementRURecordSetFinishAfterExecutorCloseForTest",
			func(stmt *ExecStmt) {
				if stmt == rs.stmt {
					panic(panicErr)
				}
			},
		)

		var recovered any
		func() {
			defer func() { recovered = recover() }()
			_ = rs.Finish()
		}()
		recoveredErr, ok := recovered.(error)
		require.True(t, ok, "recovered value %T", recovered)
		require.True(t, exeerrors.ErrQueryInterrupted.Equal(recoveredErr), recoveredErr)
		require.NoError(t, rs.Close())
		assertFrozenTerminal(t, rs, reporter, statementru.TerminalCanceled)
	})

	t.Run("Finish publishes terminal evidence before once completion", func(t *testing.T) {
		rs, reporter := newStatementRURecordSetForTest(t)
		executorUnderTest := rs.executor.(*recordSetTerminalExecutorForTest)
		finishErr := errors.New("record-set executor close failed")
		executorUnderTest.close = func() error { return finishErr }
		terminalVisibleAfterOnce := make(chan bool, 1)
		testfailpoint.EnableCall(
			t,
			"github.com/pingcap/tidb/pkg/executor/statementRURecordSetFinishAfterOnceForTest",
			func(stmt *ExecStmt) {
				if stmt == rs.stmt {
					terminalVisibleAfterOnce <- rs.hasTerminalErr()
				}
			},
		)

		require.ErrorIs(t, rs.Finish(), finishErr)
		select {
		case visible := <-terminalVisibleAfterOnce:
			require.True(t, visible, "sync.Once published completion before terminal evidence")
		case <-time.After(5 * time.Second):
			t.Fatal("Finish did not reach the post-once boundary")
		}
		require.NoError(t, rs.Close())
		assertFrozenTerminal(t, rs, reporter, statementru.TerminalError)
	})

	t.Run("Finish waits for admitted Next executor snapshot", func(t *testing.T) {
		rs, reporter := newStatementRURecordSetForTest(t)
		executorUnderTest := rs.executor.(*recordSetTerminalExecutorForTest)
		beforeExecutorSnapshot := make(chan struct{})
		beforeExecutorTake := make(chan struct{})
		releaseExecutorSnapshot := make(chan struct{})
		executorClosed := make(chan bool, 1)
		var releaseSnapshotOnce sync.Once
		t.Cleanup(func() {
			releaseSnapshotOnce.Do(func() { close(releaseExecutorSnapshot) })
		})
		var executorSnapshotReleased atomic.Bool
		executorUnderTest.close = func() error {
			executorClosed <- executorSnapshotReleased.Load()
			return nil
		}
		testfailpoint.EnableCall(
			t,
			"github.com/pingcap/tidb/pkg/executor/statementRURecordSetBeforeExecutorSnapshotForTest",
			func(stmt *ExecStmt) {
				if stmt != rs.stmt {
					return
				}
				close(beforeExecutorSnapshot)
				<-releaseExecutorSnapshot
				executorSnapshotReleased.Store(true)
			},
		)
		testfailpoint.EnableCall(
			t,
			"github.com/pingcap/tidb/pkg/executor/statementRURecordSetBeforeExecutorTakeForTest",
			func(stmt *ExecStmt) {
				if stmt == rs.stmt {
					close(beforeExecutorTake)
				}
			},
		)

		req := rs.NewChunk(nil)
		nextErr := make(chan error, 1)
		go func() { nextErr <- rs.Next(context.Background(), req) }()
		select {
		case <-beforeExecutorSnapshot:
		case <-time.After(5 * time.Second):
			t.Fatal("Next did not reach the executor snapshot boundary")
		}

		closeErr := make(chan error, 1)
		go func() { closeErr <- rs.Close() }()
		select {
		case <-beforeExecutorTake:
		case <-time.After(5 * time.Second):
			t.Fatal("Finish did not reach the executor take boundary")
		}
		releaseSnapshotOnce.Do(func() { close(releaseExecutorSnapshot) })
		select {
		case snapshotReleased := <-executorClosed:
			require.True(t, snapshotReleased, "Finish took the executor before the admitted Next snapshot")
		case <-time.After(5 * time.Second):
			t.Fatal("Finish did not close the executor after the snapshot was released")
		}
		select {
		case err := <-nextErr:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Next did not return after the executor snapshot was released")
		}
		select {
		case err := <-closeErr:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return after the admitted Next completed")
		}
		require.Len(t, reporter.reports, 1)
		finish, first := rs.stmt.statementRU.Finish(statementru.TerminalError)
		require.False(t, first)
		require.Equal(t, statementru.TerminalSuccess, finish.Terminal)
	})

	t.Run("kill while concurrent Close waits for outer Next", func(t *testing.T) {
		rs, reporter := newStatementRURecordSetForTest(t)
		executorUnderTest := rs.executor.(*recordSetTerminalExecutorForTest)
		afterExec := make(chan struct{})
		executorCloseCalled := make(chan struct{})
		beforeTerminalWait := make(chan struct{})
		terminalBarrierAcquired := make(chan bool, 1)
		releaseNext := make(chan struct{})
		releaseTerminalWait := make(chan struct{})
		var releaseNextOnce sync.Once
		var releaseTerminalWaitOnce sync.Once
		t.Cleanup(func() {
			releaseNextOnce.Do(func() { close(releaseNext) })
			releaseTerminalWaitOnce.Do(func() { close(releaseTerminalWait) })
		})
		var outerNextReleased atomic.Bool
		var executorNextCalls atomic.Int32
		executorUnderTest.next = func(req *chunk.Chunk) error {
			executorNextCalls.Add(1)
			req.Reset()
			return nil
		}
		executorUnderTest.close = func() error {
			close(executorCloseCalled)
			return nil
		}
		testfailpoint.EnableCall(
			t,
			"github.com/pingcap/tidb/pkg/executor/statementRURecordSetNextAfterExecForTest",
			func(stmt *ExecStmt) {
				if stmt != rs.stmt {
					return
				}
				close(afterExec)
				<-releaseNext
				outerNextReleased.Store(true)
			},
		)
		testfailpoint.EnableCall(
			t,
			"github.com/pingcap/tidb/pkg/executor/statementRURecordSetBeforeTerminalWaitForTest",
			func(stmt *ExecStmt) {
				if stmt != rs.stmt {
					return
				}
				close(beforeTerminalWait)
				<-releaseTerminalWait
			},
		)
		testfailpoint.EnableCall(
			t,
			"github.com/pingcap/tidb/pkg/executor/statementRURecordSetTerminalBarrierAcquiredForTest",
			func(stmt *ExecStmt) {
				if stmt == rs.stmt {
					terminalBarrierAcquired <- outerNextReleased.Load()
				}
			},
		)

		nextErr := make(chan error, 1)
		go func() {
			nextErr <- rs.Next(context.Background(), rs.NewChunk(nil))
		}()
		select {
		case <-afterExec:
		case <-time.After(5 * time.Second):
			t.Fatal("outer Next did not reach the post-executor boundary")
		}

		closeErr := make(chan error, 1)
		go func() {
			closeErr <- rs.Close()
		}()
		select {
		case <-executorCloseCalled:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not close the executor before waiting for outer Next")
		}
		select {
		case <-beforeTerminalWait:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not reach the statement-RU terminal wait boundary")
		}
		rejectedNextErr := make(chan error, 1)
		go func() {
			rejectedNextErr <- rs.Next(context.Background(), rs.NewChunk(nil))
		}()
		select {
		case err := <-rejectedNextErr:
			require.True(t, exeerrors.ErrQueryInterrupted.Equal(err), err)
		case <-time.After(5 * time.Second):
			t.Fatal("Next ingress was not rejected after Close began")
		}
		require.Equal(t, int32(1), executorNextCalls.Load())
		killer := &rs.stmt.Ctx.GetSessionVars().SQLKiller
		killer.SendKillSignal(sqlkiller.QueryInterrupted)
		t.Cleanup(killer.Reset)
		releaseTerminalWaitOnce.Do(func() { close(releaseTerminalWait) })

		releaseNextOnce.Do(func() { close(releaseNext) })
		select {
		case err := <-nextErr:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("outer Next did not return after release")
		}
		select {
		case nextReleased := <-terminalBarrierAcquired:
			require.True(t, nextReleased, "Close acquired the terminal barrier before outer Next released it")
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not acquire the statement-RU terminal barrier")
		}
		select {
		case err := <-closeErr:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return after outer Next completed and kill was sampled")
		}
		assertFrozenTerminal(t, rs, reporter, statementru.TerminalCanceled)
	})
}

func ruKeyForStmt(t *testing.T, stmt *ExecStmt) stmtstats.RUKey {
	t.Helper()

	sqlDigest, planDigest := stmt.getSQLPlanDigest()
	require.NotNil(t, sqlDigest)
	require.NotNil(t, planDigest)
	return stmtstats.RUKey{
		User:       stmt.Ctx.GetSessionVars().User.String(),
		SQLDigest:  stmtstats.BinaryDigest(sqlDigest),
		PlanDigest: stmtstats.BinaryDigest(planDigest),
	}
}

func TestMoveWrittenSharedLockKeysToExclusive(t *testing.T) {
	injectedErr := errors.New("injected get local error")

	tests := []struct {
		name              string
		exclusiveKeys     []kv.Key
		sharedKeys        []kv.Key
		writtenKeys       map[string]struct{}
		getLocalErrors    map[string]error
		wantExclusiveKeys []kv.Key
		wantSharedKeys    []kv.Key
		wantErr           error
	}{
		{
			name:              "no shared keys",
			exclusiveKeys:     []kv.Key{kv.Key("exclusive")},
			wantExclusiveKeys: []kv.Key{kv.Key("exclusive")},
		},
		{
			name:          "deduplicate exclusive and promote written keys",
			exclusiveKeys: []kv.Key{kv.Key("exclusive")},
			sharedKeys: []kv.Key{
				kv.Key("exclusive"),
				kv.Key("written"),
				kv.Key("shared"),
			},
			writtenKeys: map[string]struct{}{
				"written": {},
			},
			wantExclusiveKeys: []kv.Key{kv.Key("exclusive"), kv.Key("written")},
			wantSharedKeys:    []kv.Key{kv.Key("shared")},
		},
		{
			name: "propagate get local error",
			sharedKeys: []kv.Key{
				kv.Key("bad"),
			},
			getLocalErrors: map[string]error{
				"bad": injectedErr,
			},
			wantErr: injectedErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memBuffer := &sharedLockMemBufferForTest{
				getLocal: func(key []byte) ([]byte, error) {
					if err, ok := tt.getLocalErrors[string(key)]; ok {
						return nil, err
					}
					if _, ok := tt.writtenKeys[string(key)]; ok {
						return []byte("value"), nil
					}
					return nil, kv.ErrNotExist
				},
			}
			txn := &sharedLockTxnForTest{memBuffer: memBuffer}

			exclusiveKeys, sharedKeys, err := moveWrittenSharedLockKeysToExclusive(
				context.Background(),
				txn,
				tt.exclusiveKeys,
				tt.sharedKeys,
			)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, exclusiveKeys)
				require.Nil(t, sharedKeys)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantExclusiveKeys, exclusiveKeys)
				require.Equal(t, tt.wantSharedKeys, sharedKeys)
			}
			if len(tt.sharedKeys) > 0 {
				require.Equal(t, 1, memBuffer.rLocks)
				require.Equal(t, 1, memBuffer.rUnlocks)
			} else {
				require.Zero(t, memBuffer.rLocks)
				require.Zero(t, memBuffer.rUnlocks)
			}
		})
	}
}

// TestObserveStmtBeginOnTopProfiling verifies SQL and plan registration on profiling begin.
func TestObserveStmtBeginOnTopProfiling(t *testing.T) {
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}
	topsqlstate.EnableTopRU()
	t.Cleanup(func() {
		topsqlstate.DisableTopSQL()
		for topsqlstate.TopRUEnabled() {
			topsqlstate.DisableTopRU()
		}
	})

	topCollector := topsqlmock.NewTopSQLCollector()
	topsql.SetupTopProfilingForTest(topCollector)

	sctx := mock.NewContext()
	sc := sctx.GetSessionVars().StmtCtx
	sc.OriginalSQL = "select * from t where a = 1"
	normalizedSQL, sqlDigest := sc.SQLDigest()
	require.NotNil(t, sqlDigest)
	const normalizedPlan = "TableReader(table:t)->Selection(eq(test.t.a, ?))"
	planDigest := parser.NewDigest([]byte("topru-plan-digest"))
	sc.SetPlanDigest(normalizedPlan, planDigest)

	stmt := &ExecStmt{
		Ctx:   sctx,
		GoCtx: context.Background(),
	}
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	require.Equal(t, normalizedSQL, topCollector.GetSQL(sqlDigest.Bytes()))
	require.Equal(t, normalizedPlan, topCollector.GetPlan(planDigest.Bytes()))
}

func TestObserveStmtBeginOnTopProfilingRUV2Wiring(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	t.Run("domain ru version v2 drives top ru sampling", func(t *testing.T) {
		stmt, stats := newExecStmtWithStmtStatsForTest(context.Background(), t)
		testCtx := stmt.Ctx.(*stmtStatsTestContext)
		testCtx.BindDomainAndSchValidator(newMockDomainWithRUVersion(t, rmclient.RUVersionV2), nil)

		vars := stmt.Ctx.GetSessionVars()
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddPlanCnt(3)
		vars.RUV2Metrics = metrics
		expectedRU := metrics.TotalRU(vars.RUV2Weights(), 0, 0)

		_ = stmt.observeStmtBeginForTopProfiling(context.Background())

		key := ruKeyForStmt(t, stmt)
		m := stats.MergeRUInto()
		require.Len(t, m, 1)
		require.Equal(t, uint64(1), m[key].ExecCount)
		require.InDelta(t, expectedRU, m[key].TotalRU, 1e-9)
	})

	t.Run("nil domain falls back to default ru version", func(t *testing.T) {
		stmt, stats := newExecStmtWithStmtStatsForTest(context.Background(), t)

		vars := stmt.Ctx.GetSessionVars()
		metrics := execdetails.NewRUV2Metrics()
		metrics.AddPlanCnt(3)
		vars.RUV2Metrics = metrics

		_ = stmt.observeStmtBeginForTopProfiling(context.Background())

		key := ruKeyForStmt(t, stmt)
		m := stats.MergeRUInto()
		require.Len(t, m, 1)
		require.Equal(t, uint64(1), m[key].ExecCount)
		require.InDelta(t, 0.0, m[key].TotalRU, 1e-9)
	})
}

// TestObserveStmtFinishedOnTopProfiling verifies stale RU exec context is cleared
// before the first tick after re-enable.
// Flow: begin-on -> disable -> finish -> re-enable -> tick-before-new-begin
func TestObserveStmtFinishedOnTopProfiling(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	ru := util.NewRUDetailsWith(0, 0, 0)
	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, ru), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	ru.Merge(util.NewRUDetailsWith(10, 0, 0))
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	topsqlstate.EnableTopRU()
	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)

	// Use a non-zero sentinel RU bump (5 is arbitrary) to prove stale execCtx
	// has been cleared; otherwise the next MergeRUInto would leak a positive delta.
	ru.Merge(util.NewRUDetailsWith(5, 0, 0))
	require.Len(t, stats.MergeRUInto(), 0)
}

// TestObserveStmtFinishedOnTopProfilingDoes verifies stale baseline is not reused
// across TopRU toggle windows.
// Flow: begin-on -> disable -> finish -> begin-off(same key) -> re-enable -> finish-on
func TestObserveStmtFinishedOnTopProfilingDoes(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	ruA := util.NewRUDetailsWith(0, 0, 0)
	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, ruA), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	ruA.Merge(util.NewRUDetailsWith(10, 0, 0))
	topsqlstate.DisableTopSQL()
	for topsqlstate.TopRUEnabled() {
		topsqlstate.DisableTopRU()
	}

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	// TopRU is still disabled here; begin-off must not create/reuse an RU execCtx.
	ruB := util.NewRUDetailsWith(20, 0, 0)
	stmt.GoCtx = context.WithValue(context.Background(), util.RUDetailsCtxKey, ruB)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	topsqlstate.EnableTopRU()
	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	stmt.observeStmtFinishedForTopProfiling()

	// Expect only the begin-based execution count and no RU delta from stale baseline.
	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)
}

// TestObserveStmtFinishedOnTopProfilingKeeps verifies TopSQL-only finish stats are
// preserved when TopRU is disabled.
// Flow: begin(topSQL-on/topRU-off) -> finish -> verify duration/network-out stats
func TestObserveStmtFinishedOnTopProfilingKeeps(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopSQL()

	stmt, stats := newExecStmtWithStmtStatsForTest(context.Background(), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())

	vars := stmt.Ctx.GetSessionVars()
	vars.OutPacketBytes.Store(123)
	vars.StartTime = time.Now().Add(-time.Second)

	stmt.observeStmtFinishedForTopProfiling()

	data := stats.Take()
	require.Len(t, data, 1)
	for _, item := range data {
		require.Equal(t, uint64(1), item.ExecCount)
		require.Equal(t, uint64(1), item.DurationCount)
		require.Greater(t, item.SumDurationNs, uint64(0))
		require.Equal(t, uint64(123), item.NetworkOutBytes)
	}
}

// TestObserveStmtFinishedOnTopProfilingIgnores verifies unexpected RUDetails
// context types do not panic and keep TopRU sampling stable.
// Flow: begin(topRU-on, bad RUDetails type) -> finish -> no panic -> zero RU delta
func TestObserveStmtFinishedOnTopProfilingIgnores(t *testing.T) {
	resetTopProfilingStateForTest(t)
	topsqlstate.EnableTopRU()

	stmt, stats := newExecStmtWithStmtStatsForTest(context.WithValue(context.Background(), util.RUDetailsCtxKey, "bad-type"), t)
	_ = stmt.observeStmtBeginForTopProfiling(context.Background())
	key := ruKeyForStmt(t, stmt)

	stmt.Ctx.GetSessionVars().StartTime = time.Now().Add(-time.Second)
	require.NotPanics(t, func() {
		stmt.observeStmtFinishedForTopProfiling()
	})

	m := stats.MergeRUInto()
	require.Len(t, m, 1)
	incr, ok := m[key]
	require.True(t, ok)
	require.Equal(t, uint64(1), incr.ExecCount)
	require.InDelta(t, 0.0, incr.TotalRU, 1e-9)
}

type mockResourceGroupProvider struct {
	config *rmclient.Config
}

func newMockDomainWithRUVersion(t *testing.T, version rmclient.RUVersion) *domain.Domain {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := rmclient.DefaultConfig()
	cfg.RUVersionPolicy = &rmclient.RUVersionPolicy{Default: version}
	provider := &mockResourceGroupProvider{config: cfg}
	controller, err := rmclient.NewResourceGroupController(ctx, 1, provider, nil, 1)
	require.NoError(t, err)

	do := domain.NewMockDomain()
	do.SetResourceGroupsController(controller)
	return do
}

func (m *mockResourceGroupProvider) Get(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	value, err := json.Marshal(m.config)
	if err != nil {
		return nil, err
	}
	return &meta_storagepb.GetResponse{
		Kvs: []*meta_storagepb.KeyValue{{Value: value}},
	}, nil
}

func (*mockResourceGroupProvider) Watch(ctx context.Context, key []byte, opts ...opt.MetaStorageOption) (chan *metastorage.WatchResponse, error) {
	ch := make(chan *metastorage.WatchResponse)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (*mockResourceGroupProvider) Put(context.Context, []byte, []byte, ...opt.MetaStorageOption) (*meta_storagepb.PutResponse, error) {
	return &meta_storagepb.PutResponse{}, nil
}

func (*mockResourceGroupProvider) GetResourceGroup(context.Context, string, ...pd.GetResourceGroupOption) (*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (*mockResourceGroupProvider) ListResourceGroups(context.Context, ...pd.GetResourceGroupOption) ([]*rmpb.ResourceGroup, error) {
	return nil, nil
}

func (*mockResourceGroupProvider) AddResourceGroup(context.Context, *rmpb.ResourceGroup) (string, error) {
	return "", nil
}

func (*mockResourceGroupProvider) ModifyResourceGroup(context.Context, *rmpb.ResourceGroup) (string, error) {
	return "", nil
}

func (*mockResourceGroupProvider) DeleteResourceGroup(context.Context, string) (string, error) {
	return "", nil
}

func (*mockResourceGroupProvider) AcquireTokenBuckets(context.Context, *rmpb.TokenBucketsRequest) ([]*rmpb.TokenBucketResponse, error) {
	return nil, nil
}

func (*mockResourceGroupProvider) LoadResourceGroups(context.Context) ([]*rmpb.ResourceGroup, int64, error) {
	return nil, 0, nil
}

var (
	_ metastorage.Client             = (*mockResourceGroupProvider)(nil)
	_ rmclient.ResourceGroupProvider = (*mockResourceGroupProvider)(nil)
)
