// Copyright 2024 PingCAP, Inc.
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

package session

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	kvstore "github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestGetStartMode(t *testing.T) {
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion))
	require.Equal(t, ddl.Normal, getStartMode(currentBootstrapVersion+1))
	require.Equal(t, ddl.Upgrade, getStartMode(currentBootstrapVersion-1))
	require.Equal(t, ddl.Bootstrap, getStartMode(0))
}

func TestStatementRUMutationPreparationCollection(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	inner, err := store.Begin()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = inner.Rollback()
	})

	weights := statementru.Weights{statementru.CPUWork: 1}
	sc := stmtctx.NewStmtCtx()
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))
	vars := &variable.SessionVars{StmtCtx: sc}
	lazyTxn := &LazyTxn{Transaction: inner}
	lazyTxn.bindStatementRU(vars)

	buffer := lazyTxn.GetMemBuffer()
	require.Same(t, buffer, lazyTxn.GetMemBuffer())
	require.NoError(t, buffer.Set(kv.Key("k1"), []byte("v1")))
	require.NoError(t, buffer.SetWithFlags(kv.Key("k2"), []byte("v2")))
	require.NoError(t, buffer.Delete(kv.Key("k1")))
	require.NoError(t, buffer.DeleteWithFlags(kv.Key("k2")))
	require.NoError(t, lazyTxn.Set(kv.Key("k3"), []byte("v3")))
	require.NoError(t, lazyTxn.Delete(kv.Key("k3")))

	statement := sc.TakeStatementRUForExecution()
	require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(6), units[statementru.CPUWork])

	offSC := stmtctx.NewStmtCtx()
	vars.StmtCtx = offSC
	require.Same(t, inner.GetMemBuffer(), lazyTxn.GetMemBuffer())

	history := stmtctx.NewStmtCtx()
	target := stmtctx.NewStmtCtx()
	require.True(t, history.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))
	historyStatement := history.TakeStatementRUForExecution()
	_, first = historyStatement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.True(t, target.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))
	vars.StmtCtx = history
	retainedBuffer := lazyTxn.GetMemBuffer()
	require.NoError(t, history.WithStatementRUProducerOverride(target, func() error {
		require.Same(t, retainedBuffer, lazyTxn.GetMemBuffer())
		require.NoError(t, retainedBuffer.Set(kv.Key("retry"), []byte("value")))
		return nil
	}))
	targetStatement := target.TakeStatementRUForExecution()
	require.True(t, targetStatement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	targetFinish, first := targetStatement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	targetUnits, ok := targetFinish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(1), targetUnits[statementru.CPUWork])

	allocations := testing.AllocsPerRun(100, func() {
		_ = lazyTxn.GetMemBuffer()
	})
	require.Zero(t, allocations)

	const readers = 4
	var waitGroup sync.WaitGroup
	waitGroup.Add(readers)
	for range readers {
		go func() {
			defer waitGroup.Done()
			for range 100 {
				retainedBuffer.RLock()
				_ = retainedBuffer.Len()
				retainedBuffer.RUnlock()
			}
		}()
	}
	for range 100 {
		require.Same(t, retainedBuffer, lazyTxn.GetMemBuffer())
	}
	waitGroup.Wait()

	secondInner, err := store.Begin()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = secondInner.Rollback()
	})
	retainedBuffer.RLock()
	require.False(t, lazyTxn.statementRUMutationBuffer.targetMu.TryLock())
	reinitStarted := make(chan struct{})
	reinitDone := make(chan struct{})
	go func() {
		close(reinitStarted)
		lazyTxn.setTransaction(secondInner)
		close(reinitDone)
	}()
	<-reinitStarted
	select {
	case <-reinitDone:
		require.Fail(t, "transaction replacement completed while a retained reader pinned the old buffer")
	default:
	}
	_ = retainedBuffer.Len()
	retainedBuffer.RUnlock()
	<-reinitDone
	require.Same(t, retainedBuffer, lazyTxn.GetMemBuffer())
	require.Same(t, secondInner.GetMemBuffer(), lazyTxn.statementRUMutationBuffer.MemBuffer)
}

func TestStatementRUReplayCoordinator(t *testing.T) {
	weights := statementru.Weights{statementru.CPUWork: 1}
	selection := statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}
	history := stmtctx.NewStmtCtx()
	target := stmtctx.NewStmtCtx()
	require.True(t, history.ConfigureStatementRU(selection))
	historyRecorder := history.StatementRUUnitRecorder()
	require.True(t, target.ConfigureStatementRU(selection))
	targetRecorder := target.StatementRUUnitRecorder()
	vars := &variable.SessionVars{StmtCtx: target, FoundInPlanCache: true}
	replayErr := errors.New("replay failed")

	err := withStatementRUProducerOverrideForRetry(vars, history, target, func() error {
		require.Same(t, history, vars.StmtCtx)
		require.False(t, vars.FoundInPlanCache)
		require.Equal(t, targetRecorder, history.StatementRUUnitRecorder())
		history.ResetForRetry()
		require.True(t, history.StatementRUUnitRecorder().Add(statementru.CPUWork, 2))
		return replayErr
	})
	require.ErrorIs(t, err, replayErr)
	require.Equal(t, historyRecorder, history.StatementRUUnitRecorder())

	targetStatement := target.TakeStatementRUForExecution()
	require.True(t, targetStatement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := targetStatement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(2), units[statementru.CPUWork])

	vars.FoundInPlanCache = true
	require.NoError(t, withStatementRUProducerOverrideForRetry(vars, history, nil, func() error {
		require.False(t, vars.FoundInPlanCache)
		require.Nil(t, history.StatementRUUnitRecorder())
		return nil
	}))
	require.Equal(t, historyRecorder, history.StatementRUUnitRecorder())
}

func TestStatementRUWholeTxnRetryLifecycle(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})

	weights := statementru.Weights{statementru.CPUWork: 1}
	selection := statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}
	newStatements := func(t *testing.T, se *session, table string) (*stmtctx.StatementContext, *stmtctx.StatementContext, *statementru.Statement) {
		MustExec(t, se, "use test")
		MustExec(t, se, "set @@session.tidb_txn_mode = 'optimistic'")
		MustExec(t, se, "set @@session.tidb_retry_limit = 2")
		MustExec(t, se, "drop table if exists "+table)
		MustExec(t, se, "create table "+table+" (id int primary key, v int)")
		MustExec(t, se, "insert into "+table+" values (1, 1)")
		MustExec(t, se, "begin")
		MustExec(t, se, "update "+table+" set v = v + 1 where id = 1")

		history := se.sessionVars.StmtCtx
		require.True(t, history.ConfigureStatementRU(selection))
		require.True(t, history.StatementRUUnitRecorder().Add(statementru.CPUWork, 7))
		historyStatement := history.TakeStatementRUForExecution()
		require.NotNil(t, historyStatement)
		require.True(t, historyStatement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
		historyFinish, first := historyStatement.Finish(statementru.TerminalSuccess)
		require.True(t, first)
		historyUnits, ok := historyFinish.Result.Units()
		require.True(t, ok)
		require.Equal(t, float64(7), historyUnits[statementru.CPUWork])

		target := stmtctx.NewStmtCtx()
		require.True(t, target.ConfigureStatementRU(selection))
		se.sessionVars.StmtCtx = target
		return history, target, historyStatement
	}

	t.Run("successful replay routes work and marks the whole transaction", func(t *testing.T) {
		se, err := createSession(store)
		require.NoError(t, err)
		t.Cleanup(func() { se.Close() })
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/sessiontxn/isolation/injectOptimisticTxnRetryable", `return(true)`)
		history, target, historyStatement := newStatements(t, se, "statement_ru_retry_success")
		require.NotSame(t, history, target)

		ResetMockAutoRandIDRetryCount(1)
		t.Cleanup(func() { ResetMockAutoRandIDRetryCount(0) })
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/mockCommitRetryForAutoRandID", `return(true)`)
		require.NoError(t, se.CommitTxn(context.Background()))

		require.Same(t, target, se.sessionVars.StmtCtx)
		require.False(t, history.StatementRUWholeTxnRetried())
		require.True(t, target.StatementRUWholeTxnRetried())
		require.Equal(t, uint64(1), target.ExecRetryCount)
		historyFinish, first := historyStatement.Finish(statementru.TerminalSuccess)
		require.False(t, first)
		historyUnits, ok := historyFinish.Result.Units()
		require.True(t, ok)
		require.Equal(t, float64(7), historyUnits[statementru.CPUWork])

		statement := target.TakeStatementRUForExecution()
		require.NotNil(t, statement)
		require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
		finish, first := statement.Finish(statementru.TerminalSuccess)
		require.True(t, first)
		units, ok := finish.Result.Units()
		require.True(t, ok)
		require.Positive(t, units[statementru.CPUWork])

		rs, err := exec(se, "select v from statement_ru_retry_success where id = 1")
		require.NoError(t, err)
		require.NotNil(t, rs)
		rows := rs.NewChunk(nil)
		require.NoError(t, rs.Next(context.Background(), rows))
		require.Equal(t, 1, rows.NumRows())
		require.Equal(t, int64(2), rows.GetRow(0).GetInt64(0))
		require.NoError(t, rs.Close())
	})

	t.Run("pre-exec replay error restores the historical producer", func(t *testing.T) {
		se, err := createSession(store)
		require.NoError(t, err)
		t.Cleanup(func() { se.Close() })
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/sessiontxn/isolation/injectOptimisticTxnRetryable", `return(true)`)
		history, target, historyStatement := newStatements(t, se, "statement_ru_retry_error")
		historyRecorder := history.StatementRUUnitRecorder()
		targetRecorder := target.StatementRUUnitRecorder()

		ResetMockAutoRandIDRetryCount(1)
		t.Cleanup(func() { ResetMockAutoRandIDRetryCount(0) })
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/mockCommitRetryForAutoRandID", `return(true)`)
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/session/txnRetryPreExecError", `return(true)`)
		err = se.CommitTxn(context.Background())
		require.ErrorContains(t, err, "mock txn retry pre-exec error")

		require.Same(t, target, se.sessionVars.StmtCtx)
		require.Equal(t, historyRecorder, history.StatementRUUnitRecorder())
		require.Equal(t, targetRecorder, target.StatementRUUnitRecorder())
		require.False(t, history.StatementRUWholeTxnRetried())
		require.True(t, target.StatementRUWholeTxnRetried())
		require.Zero(t, target.ExecRetryCount)
		historyFinish, first := historyStatement.Finish(statementru.TerminalSuccess)
		require.False(t, first)
		historyUnits, ok := historyFinish.Result.Units()
		require.True(t, ok)
		require.Equal(t, float64(7), historyUnits[statementru.CPUWork])
	})
}

func TestMustGetStoreBootstrapVersionRetriesTransaction(t *testing.T) {
	store, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.EmbedUnistore))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	txn, err := store.Begin()
	require.NoError(t, err)
	require.NoError(t, meta.NewMutator(txn).FinishBootstrap(currentBootstrapVersion))
	require.NoError(t, txn.Commit(context.Background()))

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/kv/mockCommitErrorInNewTxn", `return("retry_once")`)
	conf := new(log.Config)
	lg, p, err := log.InitLogger(conf, zap.WithFatalHook(zapcore.WriteThenPanic))
	require.NoError(t, err)
	restoreLog := log.ReplaceGlobals(lg, p)
	defer restoreLog()

	require.NotPanics(t, func() {
		require.Equal(t, currentBootstrapVersion, mustGetStoreBootstrapVersion(store))
	})
}

func TestWaitSystemBootVersion(t *testing.T) {
	const systemKeyspaceID uint32 = 0xFFFFFF - 1
	store, err := mockstore.NewMockStore(
		mockstore.WithStoreType(mockstore.EmbedUnistore),
		mockstore.WithCurrentKeyspaceMeta(&keyspacepb.KeyspaceMeta{
			Id:   systemKeyspaceID,
			Name: keyspace.System,
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	originSystemStore := kvstore.GetSystemStorage()
	kvstore.SetSystemStorage(store)
	t.Cleanup(func() {
		kvstore.SetSystemStorage(originSystemStore)
	})

	t.Run("get the version after retry", func(t *testing.T) {
		// Unistore was opened outside the synctest bubble, so keep its write path
		// outside too to avoid mixing its WaitGroups across the bubble boundary.
		bootstrapCh := make(chan struct{})
		bootstrapErrCh := make(chan error, 1)
		stopCh := make(chan struct{})
		defer close(stopCh)
		go func() {
			select {
			case <-bootstrapCh:
			case <-stopCh:
				return
			}

			txn, err := store.Begin()
			if err == nil {
				err = meta.NewMutator(txn).FinishBootstrap(currentBootstrapVersion)
			}
			if err == nil {
				err = txn.Commit(context.Background())
			}
			bootstrapErrCh <- err
		}()

		synctest.Test(t, func(t *testing.T) {
			core, logs := observer.New(zap.InfoLevel)
			restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{})
			defer restoreLog()

			versionCh := make(chan int64, 1)
			go func() {
				versionCh <- waitSystemBootVersion()
			}()

			require.Eventually(t, func() bool {
				return logs.FilterMessage("waiting for the SYSTEM keyspace bootstrap to complete").Len() > 0
			}, 30*time.Second, 10*time.Millisecond)

			bootstrapCh <- struct{}{}
			require.NoError(t, <-bootstrapErrCh)

			select {
			case version := <-versionCh:
				require.Equal(t, currentBootstrapVersion, version)
			case <-time.After(30 * time.Second):
				require.Fail(t, "timed out waiting for SYSTEM keyspace bootstrap version")
			}
		})
	})

	t.Run("exhaust all retry budget", func(t *testing.T) {
		// reset the boot status
		txn, err := store.Begin()
		require.NoError(t, err)
		require.NoError(t, meta.NewMutator(txn).FinishBootstrap(0))
		require.NoError(t, txn.Commit(context.Background()))

		core, _ := observer.New(zap.InfoLevel)
		restoreLog := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{})
		defer restoreLog()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			require.Equal(t, int64(notBootstrapped), waitSystemBootVersion())
			require.Greater(t, time.Since(start), 29*time.Minute)
		})
	})
}

func TestBootstrapSessionImplUserKSVersionGuard(t *testing.T) {
	if kerneltype.IsClassic() {
		t.Skip("keyspace guard only applies to next-gen kernel")
	}

	const (
		systemKeyspaceID uint32 = 0xFFFFFF - 1
		userKeyspaceID   uint32 = 0xFFFFFF - 2
	)

	newKSStore := func(t *testing.T, keyspaceID uint32, keyspaceName string) kv.Storage {
		t.Helper()
		store, err := mockstore.NewMockStore(
			mockstore.WithStoreType(mockstore.EmbedUnistore),
			mockstore.WithCurrentKeyspaceMeta(&keyspacepb.KeyspaceMeta{
				Id:   keyspaceID,
				Name: keyspaceName,
			}),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, store.Close())
		})
		return store
	}

	setBootstrapVersion := func(t *testing.T, store kv.Storage, ver int64) {
		t.Helper()
		txn, err := store.Begin()
		require.NoError(t, err)
		err = meta.NewMutator(txn).FinishBootstrap(ver)
		require.NoError(t, err)
		require.NoError(t, txn.Commit(context.Background()))
		store.SetOption(StoreBootstrappedKey, nil)
	}

	t.Run("fatal when user target version is ahead of system version", func(t *testing.T) {
		systemStore := newKSStore(t, systemKeyspaceID, keyspace.System)
		userStore := newKSStore(t, userKeyspaceID, "user_keyspace_guard_fatal")
		setBootstrapVersion(t, systemStore, currentBootstrapVersion-1)
		setBootstrapVersion(t, userStore, currentBootstrapVersion-1)

		originSystemStore := kvstore.GetSystemStorage()
		kvstore.SetSystemStorage(systemStore)
		t.Cleanup(func() {
			kvstore.SetSystemStorage(originSystemStore)
		})

		createSessionCalled := false
		createSessionStub := func(_ kv.Storage, _ int) ([]*session, error) {
			createSessionCalled = true
			return nil, errors.New("must-not-be-called")
		}

		conf := new(log.Config)
		lg, p, err := log.InitLogger(conf, zap.WithFatalHook(zapcore.WriteThenPanic))
		require.NoError(t, err)
		restoreLog := log.ReplaceGlobals(lg, p)
		defer restoreLog()

		var panicVal any
		_, _ = func() (_ *domain.Domain, err error) {
			defer func() {
				panicVal = recover()
			}()
			return bootstrapSessionImpl(context.Background(), userStore, createSessionStub)
		}()
		require.NotNil(t, panicVal)
		panicMsg := fmt.Sprint(panicVal)
		require.True(t, strings.HasPrefix(panicMsg, "bootstrap version of user keyspace must be smaller or equal"))
		require.False(t, createSessionCalled)
	})
}

func TestDDLTableVersionTables(t *testing.T) {
	require.True(t, slices.IsSortedFunc(ddlTableVersionTables, func(a, b versionedDDLTables) int {
		return cmp.Compare(a.ver, b.ver)
	}), "ddlTableVersionTables should be sorted by version")
	allDDLTables := make([]TableBasicInfo, 0, len(ddlTableVersionTables)*2)
	for _, v := range ddlTableVersionTables {
		allDDLTables = append(allDDLTables, v.tables...)
	}
	testTableBasicInfoSlice(t, allDDLTables, " mysql.%s (")
}

func testTableBasicInfoSlice(t *testing.T, allTables []TableBasicInfo, sqlFmt string) {
	t.Helper()
	require.True(t, slices.IsSortedFunc(allTables, func(a, b TableBasicInfo) int {
		if a.ID == b.ID {
			t.Errorf("table IDs should be unique, a=%d, b=%d", a.ID, b.ID)
		}
		if a.Name == b.Name {
			t.Errorf("table names should be unique, a=%s, b=%s", a.Name, b.Name)
		}
		return cmp.Compare(b.ID, a.ID)
	}), "tables should be sorted by table ID in descending order")
	for _, vt := range allTables {
		require.Greater(t, vt.ID, metadef.ReservedGlobalIDLowerBound, "table ID should be greater than ReservedGlobalIDLowerBound")
		require.LessOrEqual(t, vt.ID, metadef.ReservedGlobalIDUpperBound, "table ID should be less than or equal to ReservedGlobalIDUpperBound")
		require.Equal(t, strings.ToLower(vt.Name), vt.Name, "table name should be in lower case")
		require.Contains(t, vt.SQL, fmt.Sprintf(sqlFmt, vt.Name),
			fmt.Sprintf("table SQL should contain table name and follow the format %s", sqlFmt))
	}
}

func TestMemArbitratorSession(t *testing.T) {
	require.Equal(t, int64(15), approxParseSQLTokenCnt("/*select * from **/SELECT x FROM `t\\`` # abc \nwhere a = 1.23 and b = 'abc\"d\\'e' -- abc \nand c_1_2 in \"abc'd\\\"e\" # (1,2,3)\n"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("select @@version @a")) // not select ... from ...
	require.Equal(t, int64(0), approxParseSQLTokenCnt("set @a=1"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("desc analyze table t"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("analyze table t"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("/*select * from **/explain show warnings"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("/*select * from **/desc show columns from t"))
	require.Equal(t, int64(5), approxParseSQLTokenCnt("insert into t values 1"))
	require.Equal(t, int64(5), approxParseSQLTokenCnt("update t set a=1"))
	require.Equal(t, int64(6), approxParseSQLTokenCnt("delete from t where a=1"))
	require.Equal(t, int64(5), approxParseSQLTokenCnt("replace into t values 1"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("prepare stmt1 from 'select * from t where a=? and b=?'"))
	require.Equal(t, int64(0), approxParseSQLTokenCnt("execute stmt1 using @a,@b,@c"))
	require.Equal(t, int64(10), approxParseSQLTokenCnt("select * from `a_1`.`b_2` where c1 = ? and c2 = ?"))
	require.Equal(t, int64(9), approxCompilePlanTokenCnt("select * from `a_1`.`b_2` where c1 = ? and c2 = ?", true))
	require.Equal(t, int64(0), approxCompilePlanTokenCnt("select @@version @a", true))
	require.Equal(t, int64(3), approxCompilePlanTokenCnt("select @@version @a", false))

	normalizedSQL := "select * from `t` where `a` = ?"
	db1DigestID := buildMemArbitratorDigestID(normalizedSQL, []stmtctx.TableEntry{{DB: "db1", Table: "t"}}, "db1")
	db2DigestID := buildMemArbitratorDigestID(normalizedSQL, []stmtctx.TableEntry{{DB: "db2", Table: "t"}}, "db2")
	require.NotEqual(t, db1DigestID, db2DigestID)

	explicitDBSQL := "select * from `db3`.`t` where `a` = ?"
	db3Table := []stmtctx.TableEntry{{DB: "db3", Table: "t"}}
	require.Equal(t,
		buildMemArbitratorDigestID(explicitDBSQL, db3Table, "db1"),
		buildMemArbitratorDigestID(explicitDBSQL, db3Table, "db2"))
	require.Equal(t,
		buildMemArbitratorDigestID(explicitDBSQL, db3Table, "db1"),
		buildMemArbitratorDigestID(explicitDBSQL, []stmtctx.TableEntry{{DB: "DB3", Table: "T"}}, "db1"))

	require.NotEqual(t,
		buildMemArbitratorDigestID(normalizedSQL, nil, "db1"),
		buildMemArbitratorDigestID(normalizedSQL, nil, "db2"))
	require.Equal(t, memory.InvalidDigestID, buildMemArbitratorDigestID("", db3Table, "db1"))
}
