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

package executor_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor"
	execinternal "github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	tikvutil "github.com/tikv/client-go/v2/util"
)

type statementRUPointReporter struct {
	reports []statementru.Report
}

func (r *statementRUPointReporter) ReportStatementRU(report statementru.Report) {
	r.reports = append(r.reports, report)
}

type statementRUExecutorRecordSet interface {
	sqlexec.RecordSet
	GetExecutor4Test() any
}

func requireStatementRUPointReadOwner(
	t *testing.T,
	executorUnderTest any,
) *txnsnapshot.SnapshotRuntimeStats {
	t.Helper()
	var stats *txnsnapshot.SnapshotRuntimeStats
	var hasOwner bool
	switch e := executorUnderTest.(type) {
	case *executor.PointGetExecutor:
		stats, hasOwner = executor.StatementRUPointGetReadStatsForTest(e)
	case *executor.BatchPointGetExec:
		stats, hasOwner = executor.StatementRUBatchPointGetReadStatsForTest(e)
	default:
		t.Fatalf("unexpected point executor type %T", executorUnderTest)
	}
	require.True(t, hasOwner)
	require.NotNil(t, stats)
	return stats
}

func compileStatementRUPointRecordSet(
	t *testing.T,
	tk *testkit.TestKit,
	sql string,
	collectRuntimeStats ...bool,
) (sqlexec.RecordSet, *statementRUPointReporter, *executor.ExecStmt) {
	t.Helper()
	stmtNode, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), stmtNode))
	require.NoError(t, executor.ResetContextOfStmt(tk.Session(), stmtNode))
	if len(collectRuntimeStats) != 0 && collectRuntimeStats[0] {
		tk.Session().GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	}
	reporter := &statementRUPointReporter{}
	weights := statementru.Weights{statementru.ScanBytes: 1}
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeResultOnly,
		Applicable:    true,
		RequiredUnits: statementru.ScanBytes.Mask(),
		Weights:       &weights,
		Reporter:      reporter,
	}))
	execStmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(context.Background(), stmtNode)
	require.NoError(t, err)
	rs, err := execStmt.Exec(context.Background())
	require.NoError(t, err)
	return rs, reporter, execStmt
}

func executeStatementRUPointRecordSet(
	t *testing.T,
	tk *testkit.TestKit,
	sql string,
) (sqlexec.RecordSet, *statementRUPointReporter) {
	t.Helper()
	rs, reporter, _ := compileStatementRUPointRecordSet(t, tk, sql)
	return rs, reporter
}

func openStatementRUPointRecordSet(
	t *testing.T,
	tk *testkit.TestKit,
	sql string,
) (statementRUExecutorRecordSet, *statementRUPointReporter) {
	t.Helper()
	rs, reporter, _ := compileStatementRUPointRecordSet(t, tk, sql)
	ret, ok := rs.(statementRUExecutorRecordSet)
	require.True(t, ok, "record set type %T", rs)
	return ret, reporter
}

func drainStatementRUPointRecordSet(t *testing.T, rs sqlexec.RecordSet) {
	t.Helper()
	req := rs.NewChunk(nil)
	for {
		require.NoError(t, rs.Next(context.Background(), req))
		if req.NumRows() == 0 {
			break
		}
	}
	require.NoError(t, rs.Close())
}

func requireMissingPointReadScanDetail(
	t *testing.T,
	operation txnsnapshot.PointReadOperationStats,
) {
	t.Helper()
	require.Positive(t, operation.StartedOperations)
	require.Equal(t, operation.StartedOperations, operation.CompletedOperations)
	require.Positive(t, operation.RPCAttempts)
	require.Positive(t, operation.CompletedResponses)
	require.Zero(t, operation.ScanDetailResponses)
	require.False(t, operation.ScanDetailComplete())
}

func TestStatementRUPointExecutorsRealWiringFailsClosedWithoutScanDetail(t *testing.T) {
	for _, test := range []struct {
		name      string
		sql       string
		kind      any
		operation func(txnsnapshot.PointReadRuntimeStats) txnsnapshot.PointReadOperationStats
	}{
		{
			name: "PointGet", sql: "select * from t where id = 1", kind: &executor.PointGetExecutor{},
			operation: func(stats txnsnapshot.PointReadRuntimeStats) txnsnapshot.PointReadOperationStats {
				return stats.Get
			},
		},
		{
			name: "BatchPointGet", sql: "select * from t where id in (1, 2)", kind: &executor.BatchPointGetExec{},
			operation: func(stats txnsnapshot.PointReadRuntimeStats) txnsnapshot.PointReadOperationStats {
				return stats.BatchGet
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t (id bigint primary key, v bigint)")
			tk.MustExec("insert into t values (1, 10), (2, 20)")

			rs, reporter := openStatementRUPointRecordSet(t, tk, test.sql)
			require.IsType(t, test.kind, rs.GetExecutor4Test())
			stats := requireStatementRUPointReadOwner(t, rs.GetExecutor4Test())
			drainStatementRUPointRecordSet(t, rs)
			requireMissingPointReadScanDetail(t, test.operation(stats.GetPointReadStats()))
			require.Empty(t, reporter.reports)
		})
	}
}

func TestStatementRUNetworkResponseContextPaths(t *testing.T) {
	for _, test := range []struct {
		name                string
		prepareSQL          string
		pointGet            bool
		querySQL            string
		expectAuthoritative bool
	}{
		{name: "ordinary Exec and lazy Next", querySQL: "select * from t where id in (1, 2)", expectAuthoritative: true},
		{
			name:                "prepared PointGet and lazy Next",
			prepareSQL:          "prepare point_stmt from 'select * from t where id = 1'",
			pointGet:            true,
			querySQL:            "execute point_stmt",
			expectAuthoritative: true,
		},
		{
			name:                "prepared range Exec and lazy Next",
			prepareSQL:          "prepare range_stmt from 'select * from t where id in (1, 2)'",
			querySQL:            "execute range_stmt",
			expectAuthoritative: true,
		},
		{
			name:       "prepared unaudited SHOW fails closed",
			prepareSQL: "prepare show_stmt from 'show tables'",
			querySQL:   "execute show_stmt",
		},
		{name: "unaudited admin path fails closed", querySQL: "admin show ddl jobs"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t (id bigint primary key, v bigint)")
			tk.MustExec("insert into t values (1, 10), (2, 20)")
			if test.prepareSQL != "" {
				tk.MustExec(test.prepareSQL)
			}

			stmtNode, err := parser.New().ParseOneStmt(test.querySQL, "", "")
			require.NoError(t, err)
			base := context.Background()
			require.NoError(t, tk.Session().PrepareTxnCtx(base, stmtNode))
			require.NoError(t, executor.ResetContextOfStmt(tk.Session(), stmtNode))
			sc := tk.Session().GetSessionVars().StmtCtx
			weights := statementru.Weights{statementru.NetworkBytes: 1}
			reporter := &statementRUPointReporter{}
			require.True(t, sc.ConfigureStatementRU(statementru.Selection{
				Mode:          statementru.ModeResultOnly,
				Applicable:    true,
				RequiredUnits: statementru.NetworkBytes.Mask(),
				Weights:       &weights,
				Reporter:      reporter,
			}))
			preparedCtx := executor.PrepareStatementRUNetworkContext(base, sc)
			execStmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(preparedCtx, stmtNode)
			require.NoError(t, err)
			beforeExecution := tikvutil.NetworkResponseEvidenceFromContext(execStmt.GoCtx)
			require.True(t, beforeExecution.Enabled)

			executionSibling := context.WithValue(context.Background(), struct{}{}, "execution")
			var rs sqlexec.RecordSet
			if test.pointGet {
				require.NotNil(t, execStmt.PsStmt)
				rs, err = execStmt.PointGet(executionSibling)
			} else {
				if test.prepareSQL != "" {
					require.Nil(t, execStmt.PsStmt)
				}
				rs, err = execStmt.Exec(executionSibling)
			}
			require.NoError(t, err)
			afterOpen := tikvutil.NetworkResponseEvidenceFromContext(execStmt.GoCtx)

			req := rs.NewChunk(nil)
			nextSibling := context.WithValue(context.Background(), struct{ next bool }{}, true)
			require.NoError(t, rs.Next(nextSibling, req))
			afterNext := tikvutil.NetworkResponseEvidenceFromContext(execStmt.GoCtx)
			if test.expectAuthoritative {
				require.Positive(t, req.NumRows())
				require.Greater(t, afterNext.Started, afterOpen.Started)
				require.Greater(t, afterNext.Started, beforeExecution.Started)
			}

			for req.NumRows() != 0 {
				require.NoError(t, rs.Next(nextSibling, req))
			}
			require.NoError(t, rs.Close())
			execStmt.FinishExecuteStmt(0, nil, false)
			final := tikvutil.NetworkResponseEvidenceFromContext(execStmt.GoCtx)
			if test.expectAuthoritative {
				require.Equal(t, final.Started, final.Finished)
				require.True(t, final.Complete())
				require.Positive(t, final.ResponseBytes)
				require.Equal(t, []statementru.Report{{TotalRU: float64(final.ResponseBytes)}}, reporter.reports)
			} else {
				require.Empty(t, reporter.reports)
			}
		})
	}
}

func TestStatementRUPointExecutorsFailClosed(t *testing.T) {
	t.Run("TiDB mem buffer bypass is unavailable", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (id bigint primary key, v bigint)")
		tk.MustExec("begin")
		tk.MustExec("insert into t values (1, 10)")

		rs, reporter := openStatementRUPointRecordSet(t, tk, "select * from t where id = 1")
		require.IsType(t, &executor.PointGetExecutor{}, rs.GetExecutor4Test())
		stats := requireStatementRUPointReadOwner(t, rs.GetExecutor4Test())
		drainStatementRUPointRecordSet(t, rs)
		require.Zero(t, stats.GetPointReadStats().Get.StartedOperations)
		require.Empty(t, reporter.reports)
		tk.MustExec("rollback")
	})

	t.Run("locking point read is unsupported", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (id bigint primary key, v bigint)")
		tk.MustExec("insert into t values (1, 10)")
		tk.MustExec("begin pessimistic")

		rs, reporter := executeStatementRUPointRecordSet(t, tk, "select * from t where id = 1 for update")
		drainStatementRUPointRecordSet(t, rs)
		require.Empty(t, reporter.reports)
		tk.MustExec("rollback")
	})

	t.Run("canceled read is not reported", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (id bigint primary key, v bigint)")
		tk.MustExec("insert into t values (1, 10)")

		rs, reporter := openStatementRUPointRecordSet(t, tk, "select * from t where id = 1")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, rs.Next(ctx, rs.NewChunk(nil)))
		require.NoError(t, rs.Close())
		require.Empty(t, reporter.reports)
	})

	t.Run("recovered executor panic is not reported", func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (id bigint primary key, v bigint)")
		tk.MustExec("insert into t values (1, 10)")

		rs, reporter := openStatementRUPointRecordSet(t, tk, "select * from t where id = 1")
		pointGet, ok := rs.GetExecutor4Test().(*executor.PointGetExecutor)
		require.True(t, ok)
		executor.PanicStatementRUPointGetForTest(pointGet)
		require.Error(t, rs.Next(context.Background(), rs.NewChunk(nil)))
		require.NoError(t, rs.Close())
		require.Empty(t, reporter.reports)
	})

	t.Run("entrance Open failure cleanup is safe", func(t *testing.T) {
		for _, test := range []struct {
			name      string
			sql       string
			failpoint string
			openClose func(*testing.T, any)
		}{
			{
				name:      "PointGet",
				sql:       "select * from t where id = 1",
				failpoint: "github.com/pingcap/tidb/pkg/executor/statementRUPointGetOpenError",
				openClose: func(t *testing.T, executorUnderTest any) {
					t.Helper()
					e := executorUnderTest.(*executor.PointGetExecutor)
					require.Error(t, e.Open(context.Background()))
					require.NoError(t, e.Close())
				},
			},
			{
				name:      "BatchPointGet",
				sql:       "select * from t where id in (1, 2)",
				failpoint: "github.com/pingcap/tidb/pkg/executor/statementRUBatchPointGetOpenError",
				openClose: func(t *testing.T, executorUnderTest any) {
					t.Helper()
					e := executorUnderTest.(*executor.BatchPointGetExec)
					require.Error(t, e.Open(context.Background()))
					require.NoError(t, e.Close())
				},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				store := testkit.CreateMockStore(t)
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("create table t (id bigint primary key, v bigint)")
				tk.MustExec("insert into t values (1, 10), (2, 20)")
				rs, _, _ := compileStatementRUPointRecordSet(t, tk, test.sql, true)
				recordSet, ok := rs.(statementRUExecutorRecordSet)
				require.True(t, ok)
				executorUnderTest := recordSet.GetExecutor4Test()
				drainStatementRUPointRecordSet(t, rs)

				testfailpoint.Enable(t, test.failpoint, "return(true)")
				require.NotPanics(t, func() {
					test.openClose(t, executorUnderTest)
				})
			})
		}
	})
}

func TestStatementRUPointGetSnapshotCacheReportsAuthoritativeZero(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bigint primary key, v bigint)")
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("begin")
	tk.MustExec("begin")

	remote, remoteReporter := openStatementRUPointRecordSet(t, tk, "select * from t where id = 1")
	remoteStats := requireStatementRUPointReadOwner(t, remote.GetExecutor4Test())
	drainStatementRUPointRecordSet(t, remote)
	requireMissingPointReadScanDetail(t, remoteStats.GetPointReadStats().Get)
	require.Empty(t, remoteReporter.reports)

	cached, cachedReporter := openStatementRUPointRecordSet(t, tk, "select * from t where id = 1")
	cachedStats := requireStatementRUPointReadOwner(t, cached.GetExecutor4Test())
	drainStatementRUPointRecordSet(t, cached)
	cachedOperation := cachedStats.GetPointReadStats().Get
	require.Equal(t, uint64(1), cachedOperation.StartedOperations)
	require.Equal(t, cachedOperation.StartedOperations, cachedOperation.CompletedOperations)
	require.Zero(t, cachedOperation.RPCAttempts)
	require.True(t, cachedOperation.ScanDetailComplete())
	require.Len(t, cachedReporter.reports, 1)
	require.Zero(t, cachedReporter.reports[0].TotalRU)
	tk.MustExec("rollback")
}

func TestStatementRUBatchPointGetMixedLocalAndSnapshotEvidenceFailsClosed(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bigint primary key, v bigint)")
	tk.MustExec("insert into t values (1, 10), (2, 20)")
	tk.MustExec("begin")

	// Warm the transaction snapshot cache first, so the snapshot half of the
	// mixed BatchPointGet has complete authoritative-zero evidence.
	tk.MustQuery("select * from t where id in (1, 2)").Sort().Check(testkit.Rows("1 10", "2 20"))
	tk.MustExec("insert into t values (3, 30)")

	rs, reporter := openStatementRUPointRecordSet(t, tk, "select * from t where id in (1, 3)")
	batchPointGet, ok := rs.GetExecutor4Test().(*executor.BatchPointGetExec)
	require.True(t, ok)
	stats := requireStatementRUPointReadOwner(t, batchPointGet)
	drainStatementRUPointRecordSet(t, rs)
	operation := stats.GetPointReadStats().BatchGet
	require.Equal(t, uint64(1), operation.StartedOperations)
	require.Equal(t, operation.StartedOperations, operation.CompletedOperations)
	require.Zero(t, operation.RPCAttempts)
	require.True(t, operation.ScanDetailComplete())
	// The other key came from TiDB's mem buffer, so snapshot evidence alone
	// cannot produce a complete statement RU result.
	require.Empty(t, reporter.reports)
	tk.MustExec("rollback")
}

func TestStatementRUPointGetCloseWaitsForConcurrentNext(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bigint primary key, v bigint)")
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("begin")
	tk.MustQuery("select * from t where id = 1").Check(testkit.Rows("1 10"))

	rs, reporter := openStatementRUPointRecordSet(t, tk, "select * from t where id = 1")
	pointGet, ok := rs.GetExecutor4Test().(*executor.PointGetExecutor)
	require.True(t, ok)
	started, release := executor.BlockStatementRUPointGetForTest(pointGet)
	require.NotNil(t, started)
	require.NotNil(t, release)

	nextErr := make(chan error, 1)
	go func() {
		nextErr <- rs.Next(context.Background(), rs.NewChunk(nil))
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("point Get did not start")
	}

	closeErr := make(chan error, 1)
	go func() {
		closeErr <- rs.Close()
	}()
	require.Eventually(t, func() bool {
		return executor.StatementRUPointGetClosingForTest(pointGet)
	}, 5*time.Second, time.Millisecond)
	select {
	case <-closeErr:
		t.Fatal("Close returned while Next was active")
	default:
	}

	release()
	select {
	case err := <-nextErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not return after releasing snapshot Get")
	}
	select {
	case err := <-closeErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after Next quiesced")
	}
	require.Len(t, reporter.reports, 1)
	require.Zero(t, reporter.reports[0].TotalRU)
	tk.MustExec("rollback")
}

func TestStatementRUPointGetRecreatedUsesIndependentGeneration(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bigint primary key, v bigint)")
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("begin")

	const sql = "select * from t where id = 1"
	rs, _, execStmt := compileStatementRUPointRecordSet(t, tk, sql)
	recordSet, ok := rs.(statementRUExecutorRecordSet)
	require.True(t, ok)
	pointGet, ok := recordSet.GetExecutor4Test().(*executor.PointGetExecutor)
	require.True(t, ok)
	firstStats := requireStatementRUPointReadOwner(t, pointGet)
	drainStatementRUPointRecordSet(t, rs)
	plan, ok := execStmt.Plan.(*physicalop.PointGetPlan)
	require.True(t, ok)

	stmtNode, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), stmtNode))
	require.NoError(t, executor.ResetContextOfStmt(tk.Session(), stmtNode))
	reporter := &statementRUPointReporter{}
	weights := statementru.Weights{statementru.ScanBytes: 1}
	require.True(t, tk.Session().GetSessionVars().StmtCtx.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeResultOnly,
		Applicable:    true,
		RequiredUnits: statementru.ScanBytes.Mask(),
		Weights:       &weights,
		Reporter:      reporter,
	}))
	// Compiler.Compile normally establishes this before executor construction;
	// this test recreates the executor directly from the retained plan.
	tk.Session().GetSessionVars().StmtCtx.IsReadOnly = true
	pointGet.Recreated(plan, tk.Session())
	stats, hasOwner := executor.StatementRUPointGetReadStatsForTest(pointGet)
	require.Nil(t, stats)
	require.False(t, hasOwner)
	require.NoError(t, pointGet.Open(context.Background()))
	secondStats := requireStatementRUPointReadOwner(t, pointGet)
	require.NotSame(t, firstStats, secondStats)
	chk := execinternal.NewFirstChunk(pointGet)
	require.NoError(t, pointGet.Next(context.Background(), chk))
	require.Equal(t, 1, chk.NumRows())
	require.NoError(t, pointGet.Next(context.Background(), chk))
	require.Zero(t, chk.NumRows())
	require.NoError(t, pointGet.Close())
	statement := tk.Session().GetSessionVars().StmtCtx.TakeStatementRUForExecution()
	require.NotNil(t, statement)
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.True(t, finish.Result.HasTotal())
	require.Len(t, reporter.reports, 1)
	require.Zero(t, reporter.reports[0].TotalRU)

	// Recreate once more with runtime stats enabled, then force Open to fail
	// before the RU attachment is installed. Adapter-style Close cleanup must
	// remain safe and must not retain the previous generation's owner.
	require.NoError(t, tk.Session().PrepareTxnCtx(context.Background(), stmtNode))
	require.NoError(t, executor.ResetContextOfStmt(tk.Session(), stmtNode))
	tk.Session().GetSessionVars().StmtCtx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	pointGet.Recreated(plan, tk.Session())
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/statementRUPointGetOpenError", "return(true)")
	require.Error(t, pointGet.Open(context.Background()))
	require.NotPanics(t, func() {
		require.NoError(t, pointGet.Close())
	})
	tk.MustExec("rollback")
}

func TestStatementRUPreparedPointGetUsesIndependentOwners(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bigint primary key, v bigint)")
	tk.MustExec("insert into t values (1, 10)")
	tk.MustExec("prepare point_stmt from 'select * from t where id = 1'")

	first, firstReporter := openStatementRUPointRecordSet(t, tk, "execute point_stmt")
	firstExecutor, ok := first.GetExecutor4Test().(*executor.PointGetExecutor)
	require.True(t, ok)
	firstStats := requireStatementRUPointReadOwner(t, firstExecutor)
	drainStatementRUPointRecordSet(t, first)
	requireMissingPointReadScanDetail(t, firstStats.GetPointReadStats().Get)
	require.Empty(t, firstReporter.reports)

	second, secondReporter := openStatementRUPointRecordSet(t, tk, "execute point_stmt")
	secondExecutor, ok := second.GetExecutor4Test().(*executor.PointGetExecutor)
	require.True(t, ok)
	require.NotSame(t, firstExecutor, secondExecutor)
	secondStats := requireStatementRUPointReadOwner(t, secondExecutor)
	require.NotSame(t, firstStats, secondStats)
	drainStatementRUPointRecordSet(t, second)
	requireMissingPointReadScanDetail(t, secondStats.GetPointReadStats().Get)
	require.Empty(t, secondReporter.reports)
}

type statementRUPointBenchmarkReporter struct {
	reports int
}

func (r *statementRUPointBenchmarkReporter) ReportStatementRU(statementru.Report) {
	r.reports++
}

func executeStatementRUPointBenchmarkQuery(
	b *testing.B,
	tk *testkit.TestKit,
	sql string,
	collectRuntimeStats bool,
	reporter *statementRUPointBenchmarkReporter,
	weights *statementru.Weights,
) {
	b.Helper()
	stmtNode, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	if err = tk.Session().PrepareTxnCtx(ctx, stmtNode); err != nil {
		b.Fatal(err)
	}
	if err = executor.ResetContextOfStmt(tk.Session(), stmtNode); err != nil {
		b.Fatal(err)
	}
	sc := tk.Session().GetSessionVars().StmtCtx
	if collectRuntimeStats {
		sc.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(nil)
	}
	if reporter != nil {
		if !sc.ConfigureStatementRU(statementru.Selection{
			Mode:          statementru.ModeResultOnly,
			Applicable:    true,
			RequiredUnits: statementru.ScanBytes.Mask(),
			Weights:       weights,
			Reporter:      reporter,
		}) {
			b.Fatal("statement RU configuration rejected")
		}
	}
	execStmt, err := (&executor.Compiler{Ctx: tk.Session()}).Compile(ctx, stmtNode)
	if err != nil {
		b.Fatal(err)
	}
	rs, err := execStmt.Exec(ctx)
	if err != nil {
		b.Fatal(err)
	}
	req := rs.NewChunk(nil)
	for {
		if err = rs.Next(ctx, req); err != nil {
			b.Fatal(err)
		}
		if req.NumRows() == 0 {
			break
		}
	}
	if err = rs.Close(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkStatementRUPointReadMatrix measures end-to-end statement
// activation and execution. Cached means the transaction snapshot cache is
// warm; MockStoreUncached starts a fresh transaction each iteration and is not
// a real network benchmark. Stable reporter and weight fixtures are allocated
// outside the timed loop so the RU modes only include production setup work.
func BenchmarkStatementRUPointReadMatrix(b *testing.B) {
	queries := []struct {
		name string
		sql  string
	}{
		{name: "PointGet", sql: "select * from t where id = 1"},
		{name: "BatchPointGet", sql: "select * from t where id in (1, 2)"},
	}
	modes := []struct {
		name                string
		collectRuntimeStats bool
		collectStatementRU  bool
	}{
		{name: "Off"},
		{name: "ScanBytesResultOnly", collectStatementRU: true},
		{name: "RuntimeStats", collectRuntimeStats: true},
		{name: "RuntimeStatsScanBytesResultOnly", collectRuntimeStats: true, collectStatementRU: true},
	}
	for _, query := range queries {
		for _, source := range []struct {
			name      string
			rotateTxn bool
		}{
			{name: "SnapshotCached"},
			{name: "MockStoreUncached", rotateTxn: true},
		} {
			for _, mode := range modes {
				modeName := mode.name
				if mode.collectStatementRU {
					if source.rotateTxn {
						modeName += "Unavailable"
					} else {
						modeName += "Complete"
					}
				}
				b.Run(fmt.Sprintf("%s/%s/%s", query.name, source.name, modeName), func(b *testing.B) {
					store := testkit.CreateMockStore(b)
					tk := testkit.NewTestKit(b, store)
					tk.MustExec("use test")
					tk.MustExec("create table t (id bigint primary key, v bigint)")
					tk.MustExec("insert into t values (1, 10), (2, 20)")
					tk.MustExec("begin")
					if !source.rotateTxn {
						executeStatementRUPointBenchmarkQuery(b, tk, query.sql, false, nil, nil)
					}

					var reporter *statementRUPointBenchmarkReporter
					var weights *statementru.Weights
					if mode.collectStatementRU {
						reporter = &statementRUPointBenchmarkReporter{}
						weights = &statementru.Weights{statementru.ScanBytes: 1}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						executeStatementRUPointBenchmarkQuery(
							b, tk, query.sql, mode.collectRuntimeStats, reporter, weights,
						)
						if source.rotateTxn {
							b.StopTimer()
							tk.MustExec("rollback")
							tk.MustExec("begin")
							b.StartTimer()
						}
					}
					b.StopTimer()
					tk.MustExec("rollback")
					expectedReports := 0
					if !source.rotateTxn && mode.collectStatementRU {
						expectedReports = b.N
					}
					reportCount := 0
					if reporter != nil {
						reportCount = reporter.reports
					}
					if reportCount != expectedReports {
						b.Fatalf("got %d reports, expected %d", reportCount, expectedReports)
					}
				})
			}
		}
	}
}
