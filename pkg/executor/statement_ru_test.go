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
	stderrors "errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	windowexec "github.com/pingcap/tidb/pkg/executor/windows"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

type statementRUReporterForTest struct {
	reports []statementru.Report
}

type panickingStatementRUMemoryAction struct {
	memory.BaseOOMAction
}

type statementRUEmptyGetter struct{}

func (statementRUEmptyGetter) Get(context.Context, kv.Key, ...kv.GetOption) (kv.ValueEntry, error) {
	return kv.ValueEntry{}, kv.ErrNotExist
}

func (*panickingStatementRUMemoryAction) Action(*memory.Tracker) {
	panic(stderrors.New("selection memory accounting failed"))
}

func (*panickingStatementRUMemoryAction) GetPriority() int64 {
	return memory.DefPanicPriority
}

func (r *statementRUReporterForTest) ReportStatementRU(report statementru.Report) {
	r.reports = append(r.reports, report)
}

func TestFinishExecuteStmtReportsStatementRUOnce(t *testing.T) {
	weights := statementru.Weights{statementru.CPUWork: 2}
	reporter := &statementRUReporterForTest{}
	selection := statementru.Selection{
		Mode:          statementru.ModeResultOnly,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
		Reporter:      reporter,
	}

	ctx := mock.NewContext()
	sessVars := ctx.GetSessionVars()
	require.Nil(t, takeStatementRUForExecution(sessVars.StmtCtx))
	sessVars.StartTime = time.Now()
	sessVars.StmtCtx.StmtType = "Select"
	sessVars.StmtCtx.OriginalSQL = "select 1"
	sessVars.StmtCtx.ResetSQLDigest(sessVars.StmtCtx.OriginalSQL)
	require.True(t, sessVars.StmtCtx.ConfigureStatementRU(selection))
	unitRecorder := sessVars.StmtCtx.StatementRUUnitRecorder()
	evidenceRecorder := sessVars.StmtCtx.StatementRUEvidenceRecorder()
	require.NotNil(t, unitRecorder)
	require.NotNil(t, evidenceRecorder)
	require.True(t, unitRecorder.Add(statementru.CPUWork, 3))
	require.True(t, evidenceRecorder.MarkPresent(statementru.CPUWork.Mask()))
	execStmt, err := finishCompileWithStatementRU(&ExecStmt{
		Ctx:      ctx,
		GoCtx:    execdetails.ContextWithInitializedExecDetails(context.Background()),
		StmtNode: &ast.SelectStmt{},
	}, sessVars.StmtCtx, nil)
	require.NoError(t, err)
	require.NotNil(t, execStmt.statementRU)
	require.Nil(t, takeStatementRUForExecution(sessVars.StmtCtx))
	require.Equal(t, unitRecorder, sessVars.StmtCtx.StatementRUUnitRecorder())

	counter := metrics.StatementRUFinishCounter.WithLabelValues("success", "complete", "none", "true")
	before := readStatementRUCounter(t, counter)
	// hasMoreResults is a slow-log protocol attribute; FinishExecuteStmt is
	// already terminal for this logical statement.
	execStmt.FinishExecuteStmt(0, nil, true)
	execStmt.FinishExecuteStmt(0, nil, false)

	require.Equal(t, []statementru.Report{{TotalRU: 6}}, reporter.reports)
	require.Equal(t, float64(1), readStatementRUCounter(t, counter)-before)
}

func TestFinishCompileWithStatementRUTransfersAfterInitialization(t *testing.T) {
	weights := statementru.Weights{statementru.CPUWork: 1}
	newSelection := func() statementru.Selection {
		return statementru.Selection{
			Mode:          statementru.ModeResultOnly,
			Applicable:    true,
			RequiredUnits: statementru.CPUWork.Mask(),
			Weights:       &weights,
		}
	}

	t.Run("initialization error leaves owner available", func(t *testing.T) {
		sc := mock.NewContext().GetSessionVars().StmtCtx
		require.True(t, sc.ConfigureStatementRU(newSelection()))
		unitRecorder := sc.StatementRUUnitRecorder()
		initializationErr := stderrors.New("warm-up failed")
		execStmt, err := finishCompileWithStatementRU(&ExecStmt{}, sc, initializationErr)
		require.ErrorIs(t, err, initializationErr)
		require.Nil(t, execStmt)
		require.Equal(t, unitRecorder, sc.StatementRUUnitRecorder())
		execStmt, err = finishCompileWithStatementRU(&ExecStmt{}, sc, nil)
		require.NoError(t, err)
		require.NotNil(t, execStmt.statementRU)
	})

	t.Run("successful initialization transfers owner once", func(t *testing.T) {
		sc := mock.NewContext().GetSessionVars().StmtCtx
		require.True(t, sc.ConfigureStatementRU(newSelection()))
		unitRecorder := sc.StatementRUUnitRecorder()
		execStmt, err := finishCompileWithStatementRU(&ExecStmt{}, sc, nil)
		require.NoError(t, err)
		require.NotNil(t, execStmt.statementRU)
		require.Nil(t, takeStatementRUForExecution(sc))
		require.Equal(t, unitRecorder, sc.StatementRUUnitRecorder())
	})

}

func TestStatementRUFrontendCompileCollection(t *testing.T) {
	weights := statementru.Weights{statementru.FrontendCompileBytes: 1}
	newStatement := func(t testing.TB) (*stmtctx.StatementContext, *statementru.Statement) {
		t.Helper()
		sc := stmtctx.NewStmtCtx()
		require.True(t, sc.ConfigureStatementRU(statementru.Selection{
			Mode:          statementru.ModeCalibration,
			Applicable:    true,
			RequiredUnits: statementru.FrontendCompileBytes.Mask(),
			Weights:       &weights,
		}))
		return sc, sc.TakeStatementRUForExecution()
	}
	finish := func(t testing.TB, statement *statementru.Statement) (statementru.UnitValues, statementru.Coverage) {
		t.Helper()
		result, first := statement.Finish(statementru.TerminalSuccess)
		require.True(t, first)
		units, ok := result.Result.Units()
		require.True(t, ok)
		coverage, ok := result.Result.Coverage()
		require.True(t, ok)
		return units, coverage
	}

	t.Run("ordinary miss records original bytes", func(t *testing.T) {
		sc, statement := newStatement(t)
		stmt := &ast.SelectStmt{}
		stmt.SetText(nil, "select 你好")
		recordStatementRUFrontendCompile(sc, false, stmt, nil)
		units, coverage := finish(t, statement)
		require.Equal(t, float64(len("select 你好")), units[statementru.FrontendCompileBytes])
		require.Equal(t, statementru.FrontendCompileBytes.Mask(), coverage.PresentUnits)
	})

	t.Run("prepared miss records template instead of execute", func(t *testing.T) {
		sc, statement := newStatement(t)
		template := &ast.SelectStmt{}
		template.SetText(nil, "select ?")
		execute := &ast.ExecuteStmt{Name: "s"}
		execute.SetText(nil, "execute s using @a")
		recordStatementRUFrontendCompile(sc, false, execute, &plannercore.PlanCacheStmt{
			PreparedAst: &ast.Prepared{Stmt: template},
		})
		units, coverage := finish(t, statement)
		require.Equal(t, float64(len("select ?")), units[statementru.FrontendCompileBytes])
		require.Equal(t, statementru.FrontendCompileBytes.Mask(), coverage.PresentUnits)
	})

	t.Run("cache hit is authoritative zero", func(t *testing.T) {
		sc, statement := newStatement(t)
		recordStatementRUFrontendCompile(sc, true, &ast.ExecuteStmt{}, nil)
		units, coverage := finish(t, statement)
		require.Zero(t, units[statementru.FrontendCompileBytes])
		require.Equal(t, statementru.FrontendCompileBytes.Mask(), coverage.PresentUnits)
		require.Zero(t, coverage.UnavailableUnits)
	})

	t.Run("missing template is unavailable", func(t *testing.T) {
		sc, statement := newStatement(t)
		recordStatementRUFrontendCompile(sc, false, &ast.ExecuteStmt{}, nil)
		units, coverage := finish(t, statement)
		require.Zero(t, units[statementru.FrontendCompileBytes])
		require.Equal(t, statementru.FrontendCompileBytes.Mask(), coverage.UnavailableUnits)
		require.Zero(t, coverage.PresentUnits)
	})

	t.Run("replay routes compile work to trigger statement", func(t *testing.T) {
		history, historyStatement := newStatement(t)
		history.OriginalSQL = "select ?"
		_, first := historyStatement.Finish(statementru.TerminalSuccess)
		require.True(t, first)
		target, targetStatement := newStatement(t)

		require.NoError(t, history.WithStatementRUProducerOverride(target, func() error {
			recordStatementRUFrontendCompile(history, false, &ast.ExecuteStmt{}, nil)
			recordStatementRUFrontendCompile(history, true, &ast.ExecuteStmt{}, nil)
			return nil
		}))

		units, coverage := finish(t, targetStatement)
		require.Equal(t, float64(len("select ?")), units[statementru.FrontendCompileBytes])
		require.Equal(t, statementru.FrontendCompileBytes.Mask(), coverage.PresentUnits)
	})
}

func TestStatementRUWriteDetailCollection(t *testing.T) {
	writeUnits := statementru.WriteKeys.Mask() | statementru.WriteBytes.Mask()
	weights := statementru.Weights{
		statementru.WriteKeys:  1,
		statementru.WriteBytes: 1,
	}
	tests := []struct {
		name                string
		stmtNode            ast.StmtNode
		detail              *tikvutil.CommitDetails
		finalErr            error
		pipelined           bool
		wholeTxnRetried     bool
		preparedDDL         bool
		expectedKeys        float64
		expectedBytes       float64
		expectedPresent     statementru.UnitMask
		expectedMissing     statementru.UnitMask
		expectedUnsupported statementru.UnitMask
	}{
		{
			name:            "accepted positive final attempt",
			stmtNode:        &ast.InsertStmt{},
			detail:          &tikvutil.CommitDetails{WriteKeys: 3, WriteSize: 21},
			expectedKeys:    3,
			expectedBytes:   21,
			expectedPresent: writeUnits,
		},
		{name: "ambiguous zero", stmtNode: &ast.CommitStmt{}, detail: &tikvutil.CommitDetails{}, expectedMissing: writeUnits},
		{name: "missing detail", stmtNode: &ast.CommitStmt{}, expectedMissing: writeUnits},
		{
			name:            "whole transaction retry",
			stmtNode:        &ast.CommitStmt{},
			detail:          &tikvutil.CommitDetails{WriteKeys: 3, WriteSize: 21, TxnRetry: 1},
			expectedMissing: writeUnits,
		},
		{
			name:            "TiDB retry before client detail",
			stmtNode:        &ast.CommitStmt{},
			detail:          &tikvutil.CommitDetails{WriteKeys: 3, WriteSize: 21},
			wholeTxnRetried: true,
			expectedMissing: writeUnits,
		},
		{
			name:                "pipelined false zero",
			stmtNode:            &ast.InsertStmt{},
			detail:              &tikvutil.CommitDetails{},
			pipelined:           true,
			expectedMissing:     writeUnits,
			expectedUnsupported: writeUnits,
		},
		{
			name:                "DDL does not consume implicit commit detail",
			stmtNode:            &ast.CreateTableStmt{},
			detail:              &tikvutil.CommitDetails{WriteKeys: 3, WriteSize: 21},
			expectedMissing:     writeUnits,
			expectedUnsupported: writeUnits,
		},
		{
			name:                "prepared DDL does not consume implicit commit detail",
			stmtNode:            &ast.ExecuteStmt{},
			detail:              &tikvutil.CommitDetails{WriteKeys: 3, WriteSize: 21},
			preparedDDL:         true,
			expectedMissing:     writeUnits,
			expectedUnsupported: writeUnits,
		},
		{
			name:            "failed statement records no writes",
			stmtNode:        &ast.InsertStmt{},
			detail:          &tikvutil.CommitDetails{WriteKeys: 3, WriteSize: 21},
			finalErr:        stderrors.New("commit failed"),
			expectedMissing: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := statementru.NewStatement(statementru.Selection{
				Mode:          statementru.ModeCalibration,
				Applicable:    true,
				RequiredUnits: writeUnits,
				Weights:       &weights,
			})
			execStmt := &ExecStmt{StmtNode: tt.stmtNode, statementRU: statement}
			if tt.preparedDDL {
				execStmt.Plan = &plannercore.Execute{Plan: &plannercore.DDL{}}
			}
			execStmt.recordStatementRUWriteDetails(tt.detail, tt.finalErr, statementRUCommitEvidence{
				pipelined:       tt.pipelined,
				wholeTxnRetried: tt.wholeTxnRetried,
			})
			finish, first := statement.Finish(statementRUTerminalStatus(tt.finalErr))
			require.True(t, first)
			units, ok := finish.Result.Units()
			require.True(t, ok)
			coverage, ok := finish.Result.Coverage()
			require.True(t, ok)
			require.Equal(t, tt.expectedKeys, units[statementru.WriteKeys])
			require.Equal(t, tt.expectedBytes, units[statementru.WriteBytes])
			require.Equal(t, tt.expectedPresent, coverage.PresentUnits&writeUnits)
			require.Equal(t, tt.expectedMissing, coverage.UnavailableUnits&writeUnits)
			require.Equal(t, tt.expectedUnsupported, coverage.UnsupportedUnits&writeUnits)
		})
	}

	t.Run("ExecStmt retains original context commit evidence", func(t *testing.T) {
		original := stmtctx.NewStmtCtx()
		require.True(t, original.ConfigureStatementRU(statementru.Selection{
			Mode:          statementru.ModeCalibration,
			Applicable:    true,
			RequiredUnits: writeUnits,
			Weights:       &weights,
		}))
		original.MarkStatementRUWholeTxnRetried()

		execStmt, err := finishCompileWithStatementRU(&ExecStmt{StmtNode: &ast.CommitStmt{}}, original, nil)
		require.NoError(t, err)
		statement := execStmt.statementRU
		require.NotNil(t, statement)
		execStmt.recordStatementRUWriteDetails(
			&tikvutil.CommitDetails{WriteKeys: 3, WriteSize: 21},
			nil,
			execStmt.readStatementRUCommitEvidence(),
		)
		finish, first := statement.Finish(statementru.TerminalSuccess)
		require.True(t, first)
		units, ok := finish.Result.Units()
		require.True(t, ok)
		coverage, ok := finish.Result.Coverage()
		require.True(t, ok)
		require.Zero(t, units[statementru.WriteKeys])
		require.Zero(t, units[statementru.WriteBytes])
		require.Equal(t, writeUnits, coverage.UnavailableUnits&writeUnits)
	})
}

func TestStatementRUTerminalClassificationAndLabels(t *testing.T) {
	terminalTests := []struct {
		name string
		err  error
		want statementru.TerminalStatus
	}{
		{name: "success", want: statementru.TerminalSuccess},
		{name: "error", err: stderrors.New("execution failed"), want: statementru.TerminalError},
		{name: "context canceled", err: context.Canceled, want: statementru.TerminalCanceled},
		{name: "wrapped deadline", err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded), want: statementru.TerminalCanceled},
		{name: "TiDB query interrupted", err: exeerrors.ErrQueryInterrupted.GenWithStackByArgs(), want: statementru.TerminalCanceled},
		{name: "TiDB max execution time", err: exeerrors.ErrMaxExecTimeExceeded.GenWithStackByArgs(), want: statementru.TerminalCanceled},
	}
	for _, tt := range terminalTests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, statementRUTerminalStatus(tt.err))
		})
	}

	for status, label := range map[statementru.TerminalStatus]string{
		statementru.TerminalSuccess:     "success",
		statementru.TerminalError:       "error",
		statementru.TerminalCanceled:    "canceled",
		statementru.TerminalStatus(255): "unknown",
	} {
		require.Equal(t, label, statementRUTerminalLabel(status))
	}
	for state, label := range map[statementru.CollectionState]string{
		statementru.StateUnavailable:     "unavailable",
		statementru.StatePartial:         "partial",
		statementru.StateComplete:        "complete",
		statementru.StateInvalid:         "invalid",
		statementru.CollectionState(255): "unknown",
	} {
		require.Equal(t, label, statementRUStateLabel(state))
	}
	for reason, label := range map[statementru.Reason]string{
		statementru.ReasonNone:                 "none",
		statementru.ReasonMissingEvidence:      "missing_evidence",
		statementru.ReasonIncompleteEvidence:   "incomplete_evidence",
		statementru.ReasonUnsupported:          "unsupported",
		statementru.ReasonWeightsUnavailable:   "weights_unavailable",
		statementru.ReasonInvalidConfiguration: "invalid_configuration",
		statementru.ReasonInvalidObservation:   "invalid_observation",
		statementru.ReasonArithmeticOverflow:   "arithmetic_overflow",
		statementru.Reason(255):                "unknown",
	} {
		require.Equal(t, label, statementRUReasonLabel(reason))
	}
	require.Equal(t, "true", statementRUReportSelectedLabel(true))
	require.Equal(t, "false", statementRUReportSelectedLabel(false))
}

func readStatementRUCounter(t *testing.T, counter interface{ Write(*dto.Metric) error }) float64 {
	metric := &dto.Metric{}
	require.NoError(t, counter.Write(metric))
	return metric.GetCounter().GetValue()
}

func BenchmarkStatementRUOffHooks(b *testing.B) {
	sc := mock.NewContext().GetSessionVars().StmtCtx
	execStmt := &ExecStmt{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		execStmt.statementRU = takeStatementRUForExecution(sc)
		execStmt.finishStatementRU(nil)
	}
}

func TestStatementRUSynchronousOperatorEligibilityAndWindowFormula(t *testing.T) {
	t.Run("only root synchronous builder is eligible", func(t *testing.T) {
		builder := &executorBuilder{}
		require.True(t, builder.statementRUCPUWorkEligible())
		builder.forDataReaderBuilder = true
		require.False(t, builder.statementRUCPUWorkEligible())
		builder.forDataReaderBuilder = false
		builder.buildingShuffleWorker = true
		require.False(t, builder.statementRUCPUWorkEligible())

		for _, initial := range []bool{false, true} {
			builder := &executorBuilder{buildingShuffleWorker: initial}
			builder.buildShuffleAsyncExecutor(nil)
			require.Equal(t, initial, builder.buildingShuffleWorker)
		}
	})

	t.Run("parallel projection is deferred", func(t *testing.T) {
		require.True(t, statementRUProjectionCPUWorkEligible(0, true))
		require.True(t, statementRUProjectionCPUWorkEligible(-1, true))
		require.True(t, statementRUProjectionCPUWorkEligible(4, false))
		require.False(t, statementRUProjectionCPUWorkEligible(4, true))
	})

	t.Run("window formula includes frozen expression slots", func(t *testing.T) {
		window := &physicalop.PhysicalWindow{}
		require.Equal(t, 0, statementRUWindowCPUWorkMultiplier(window))

		window.WindowFuncDescs = append(window.WindowFuncDescs, nil, nil)
		window.PartitionBy = make([]property.SortItem, 1)
		window.OrderBy = make([]property.SortItem, 2)
		window.Frame = &logicalop.WindowFrame{
			Start: &logicalop.FrameBound{CalcFuncs: make([]expression.Expression, 2)},
			End:   &logicalop.FrameBound{CalcFuncs: make([]expression.Expression, 1)},
		}
		require.Equal(t, 8, statementRUWindowCPUWorkMultiplier(window))

		window.Frame.Start = nil
		require.Equal(t, 6, statementRUWindowCPUWorkMultiplier(window))
	})
}

func TestStatementRUStatefulJoinExpressionCount(t *testing.T) {
	col := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	baseJoin := physicalop.BasePhysicalJoin{
		LeftConditions:  expression.CNFExprs{col},
		RightConditions: expression.CNFExprs{col},
		OtherConditions: expression.CNFExprs{col},
		LeftJoinKeys:    []*expression.Column{col, col},
		RightJoinKeys:   []*expression.Column{col, col},
		OuterJoinKeys:   []*expression.Column{col, col},
		InnerJoinKeys:   []*expression.Column{col, col},
	}
	compareFilters := &physicalop.ColWithCmpFuncManager{OpType: []string{"gt", "lt"}}

	hashJoin := &physicalop.PhysicalHashJoin{
		BasePhysicalJoin:  baseJoin,
		EqualConditions:   []*expression.ScalarFunction{{}, {}},
		NAEqualConditions: []*expression.ScalarFunction{{}},
	}
	count, ok := statementRUHashJoinExpressionCount(hashJoin)
	require.True(t, ok)
	require.Equal(t, 6, count)

	mergeJoin := &physicalop.PhysicalMergeJoin{BasePhysicalJoin: baseJoin, CompareFuncs: []expression.CompareFunc{nil, nil}}
	count, ok = statementRUMergeJoinExpressionCount(mergeJoin)
	require.True(t, ok)
	require.Equal(t, 5, count)

	indexJoin := &physicalop.PhysicalIndexJoin{BasePhysicalJoin: baseJoin, CompareFilters: compareFilters}
	count, ok = statementRUIndexJoinExpressionCount(indexJoin)
	require.True(t, ok)
	require.Equal(t, 7, count)

	indexHashJoin := &physicalop.PhysicalIndexHashJoin{PhysicalIndexJoin: physicalop.PhysicalIndexJoin{
		BasePhysicalJoin: baseJoin,
		OuterHashKeys:    []*expression.Column{col, col, col},
		InnerHashKeys:    []*expression.Column{col, col, col},
		CompareFilters:   compareFilters,
	}}
	count, ok = statementRUIndexHashJoinExpressionCount(indexHashJoin)
	require.True(t, ok)
	require.Equal(t, 8, count)

	indexMergeJoin := &physicalop.PhysicalIndexMergeJoin{PhysicalIndexJoin: physicalop.PhysicalIndexJoin{
		BasePhysicalJoin: baseJoin,
		CompareFilters:   compareFilters,
	}, CompareFuncs: []expression.CompareFunc{nil, nil}, OuterCompareFuncs: []expression.CompareFunc{nil}, NeedOuterSort: true}
	count, ok = statementRUIndexMergeJoinExpressionCount(indexMergeJoin)
	require.True(t, ok)
	require.Equal(t, 8, count)

	invalidHashJoin := *hashJoin
	invalidHashJoin.RightJoinKeys = invalidHashJoin.RightJoinKeys[:1]
	_, ok = statementRUHashJoinExpressionCount(&invalidHashJoin)
	require.False(t, ok)

	invalidIndexJoin := *indexJoin
	invalidIndexJoin.InnerJoinKeys = invalidIndexJoin.InnerJoinKeys[:1]
	_, ok = statementRUIndexJoinExpressionCount(&invalidIndexJoin)
	require.False(t, ok)

	invalidIndexHashJoin := *indexHashJoin
	invalidIndexHashJoin.InnerHashKeys = invalidIndexHashJoin.InnerHashKeys[:1]
	_, ok = statementRUIndexHashJoinExpressionCount(&invalidIndexHashJoin)
	require.False(t, ok)

	indexMergeJoinWithoutOuterSort := *indexMergeJoin
	indexMergeJoinWithoutOuterSort.NeedOuterSort = false
	count, ok = statementRUIndexMergeJoinExpressionCount(&indexMergeJoinWithoutOuterSort)
	require.True(t, ok)
	require.Equal(t, 7, count)
}

func TestConfigureStatementRUStatefulConcreteExecutors(t *testing.T) {
	tests := []struct {
		name string
		new  func(exec.BaseExecutor) exec.Executor
	}{
		{name: "Sort", new: func(base exec.BaseExecutor) exec.Executor {
			return &sortexec.SortExec{BaseExecutor: base}
		}},
		{name: "TopN", new: func(base exec.BaseExecutor) exec.Executor {
			return &sortexec.TopNExec{SortExec: sortexec.SortExec{BaseExecutor: base}}
		}},
		{name: "HashAgg", new: func(base exec.BaseExecutor) exec.Executor {
			return &aggregate.HashAggExec{BaseExecutor: base}
		}},
		{name: "HashJoinV1", new: func(base exec.BaseExecutor) exec.Executor {
			return &join.HashJoinV1Exec{BaseExecutor: base}
		}},
		{name: "HashJoinV2", new: func(base exec.BaseExecutor) exec.Executor {
			return &join.HashJoinV2Exec{BaseExecutor: base}
		}},
		{name: "MergeJoin", new: func(base exec.BaseExecutor) exec.Executor {
			return &join.MergeJoinExec{BaseExecutor: base}
		}},
		{name: "IndexJoin", new: func(base exec.BaseExecutor) exec.Executor {
			return &join.IndexLookUpJoin{BaseExecutor: base}
		}},
		{name: "IndexHashJoin", new: func(base exec.BaseExecutor) exec.Executor {
			return &join.IndexNestedLoopHashJoin{IndexLookUpJoin: join.IndexLookUpJoin{BaseExecutor: base}}
		}},
		{name: "IndexMergeJoin", new: func(base exec.BaseExecutor) exec.Executor {
			return &join.IndexLookUpMergeJoin{BaseExecutor: base}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := mock.NewContext()
			weights := statementru.Weights{
				statementru.CPUWork:       1,
				statementru.HashStateRows: 1,
			}
			sc := ctx.GetSessionVars().StmtCtx
			require.True(t, sc.ConfigureStatementRU(statementru.Selection{
				Mode:          statementru.ModeCalibration,
				Applicable:    true,
				RequiredUnits: statementru.CPUWork.Mask() | statementru.HashStateRows.Mask(),
				Weights:       &weights,
			}))
			executorUnderTest := tt.new(exec.NewBaseExecutor(ctx, expression.NewSchema(), 0))
			builder := &executorBuilder{sctx: ctx}

			builder.configureStatementRUStateful(executorUnderTest, 1)

			require.NoError(t, builder.err)
			provider, ok := executorUnderTest.(interface{ StatementRUEnabled() bool })
			require.True(t, ok)
			require.True(t, provider.StatementRUEnabled())
			require.False(t, exec.ConfigureStatementRUExecutor(executorUnderTest, sc, exec.StatementRUExecutorConfig{
				CPUWorkMultiplier: 1,
				NeedsUnitRecorder: true,
			}))
		})
	}
}

func TestStatementRUSelectedRecordSetDoesNotDetach(t *testing.T) {
	weights := statementru.Weights{statementru.CPUWork: 1}
	recordSet := &recordSet{stmt: &ExecStmt{statementRU: statementru.NewStatement(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	})}}

	detached, ok, err := recordSet.TryDetach()
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, detached)
}

func TestStatementRUShuffleAsyncSubtreesExcluded(t *testing.T) {
	ctx := mock.NewContext()
	weights := statementru.Weights{statementru.CPUWork: 1}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))

	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(column)
	condition := &expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeTiny)}
	newDual := func() *physicalop.PhysicalTableDual {
		dual := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx.GetPlanCtx(), nil, 0)
		dual.SetSchema(schema)
		return dual
	}
	newSelection := func(child base.PhysicalPlan) *physicalop.PhysicalSelection {
		selection := physicalop.PhysicalSelection{Conditions: []expression.Expression{condition}}.Init(ctx.GetPlanCtx(), nil, 0)
		selection.SetChildren(child)
		return selection
	}

	dataSourceSelection := newSelection(newDual())
	workerSelection := newSelection(newDual())
	shufflePlan := physicalop.PhysicalShuffle{
		Concurrency:  1,
		Tails:        []base.PhysicalPlan{workerSelection},
		DataSources:  []base.PhysicalPlan{dataSourceSelection},
		SplitterType: physicalop.PartitionHashSplitterType,
		ByItemArrays: [][]expression.Expression{{}},
	}.Init(ctx.GetPlanCtx(), nil, 0)
	shufflePlan.SetChildren(workerSelection)

	builder := newExecutorBuilder(context.Background(), ctx, nil, nil)
	built := builder.build(shufflePlan)
	require.NoError(t, builder.err)
	shuffle := built.(*ShuffleExec)
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)

	shuffle.dataSources[0].(*SelectionExec).RecordStatementRUCPUWork(5)
	shuffle.workers[0].childExec.(*SelectionExec).RecordStatementRUCPUWork(7)
	require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(0), units[statementru.CPUWork])
}

func TestStatementRUSelectionRecordsBeforeMemoryPanic(t *testing.T) {
	ctx := mock.NewContext()
	const (
		initialRows = 32
		rows        = 64
	)
	ctx.GetSessionVars().InitChunkSize = initialRows
	ctx.GetSessionVars().MaxChunkSize = rows
	weights := statementru.Weights{statementru.CPUWork: 1}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))

	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(column)
	dataSource := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		Ctx:        ctx,
		DataSchema: schema,
		Rows:       rows,
		GenDataFunc: func(row int, _ *types.FieldType) any {
			return int64(row)
		},
	})
	dataSource.PrepareChunks()
	condition := &expression.Constant{Value: types.NewIntDatum(1), RetType: types.NewFieldType(mysql.TypeTiny)}
	selectionPlan := physicalop.PhysicalSelection{Conditions: []expression.Expression{condition}}.Init(ctx.GetPlanCtx(), nil, 0)
	selectionPlan.SetChildren(testutil.BuildMockDataPhysicalPlan(ctx, dataSource))
	builder := newExecutorBuilder(context.Background(), ctx, nil, nil)
	built := builder.build(selectionPlan)
	require.NoError(t, builder.err)
	selection := built.(*SelectionExec)
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)

	goCtx := context.Background()
	require.NoError(t, exec.Open(goCtx, selection))
	selection.memTracker.SetBytesLimit(selection.memTracker.BytesConsumed() + 1)
	selection.memTracker.SetActionOnExceed(&panickingStatementRUMemoryAction{})
	require.Error(t, exec.Next(goCtx, selection, exec.NewFirstChunk(selection)))
	selection.memTracker.SetBytesLimit(-1)
	selection.memTracker.SetActionOnExceed(nil)
	require.NoError(t, exec.Close(selection))

	require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := statement.Finish(statementru.TerminalError)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, float64(rows), units[statementru.CPUWork])
}

func configureStatementRUCPUWorkTest(t testing.TB, ctx sessionctx.Context) {
	weights := statementru.Weights{statementru.CPUWork: 1}
	require.True(t, ctx.GetSessionVars().StmtCtx.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))
}

func newStatementRUCPUWorkDataSource(ctx sessionctx.Context, schema *expression.Schema, rows int) *testutil.MockDataSource {
	dataSource := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		Ctx:        ctx,
		DataSchema: schema,
		Rows:       rows,
		GenDataFunc: func(row int, _ *types.FieldType) any {
			return int64(row + 1)
		},
	})
	dataSource.PrepareChunks()
	return dataSource
}

func drainStatementRUCPUWorkExecutor(t testing.TB, executor exec.Executor) int {
	goCtx := context.Background()
	require.NoError(t, exec.Open(goCtx, executor))
	result := exec.NewFirstChunk(executor)
	rows := 0
	for {
		require.NoError(t, exec.Next(goCtx, executor, result))
		if result.NumRows() == 0 {
			break
		}
		rows += result.NumRows()
	}
	require.NoError(t, exec.Close(executor))
	return rows
}

func finishStatementRUCPUWorkTest(t testing.TB, statement *statementru.Statement, expected float64) {
	require.NotNil(t, statement)
	require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	require.Equal(t, expected, units[statementru.CPUWork])
}

func TestStatementRUSynchronousOperatorAccounting(t *testing.T) {
	const (
		chunkSize = 2
		inputRows = 5
	)

	newContext := func(t testing.TB) *mock.Context {
		ctx := mock.NewContext()
		ctx.GetSessionVars().InitChunkSize = chunkSize
		ctx.GetSessionVars().MaxChunkSize = chunkSize
		configureStatementRUCPUWorkTest(t, ctx)
		return ctx
	}

	t.Run("limit", func(t *testing.T) {
		ctx := newContext(t)
		column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
		schema := expression.NewSchema(column)
		dataSource := newStatementRUCPUWorkDataSource(ctx, schema, inputRows)
		plan := physicalop.PhysicalLimit{Offset: 1, Count: 10}.Init(ctx.GetPlanCtx(), nil, 0)
		plan.SetSchema(schema)
		plan.SetChildren(testutil.BuildMockDataPhysicalPlan(ctx, dataSource))

		builder := newExecutorBuilder(context.Background(), ctx, nil, nil)
		built := builder.build(plan)
		require.NoError(t, builder.err)
		statement := ctx.GetSessionVars().StmtCtx.TakeStatementRUForExecution()
		require.Equal(t, inputRows-1, drainStatementRUCPUWorkExecutor(t, built))
		finishStatementRUCPUWorkTest(t, statement, inputRows)
	})

	for _, test := range []struct {
		name        string
		concurrency int
		expected    float64
	}{
		{name: "serial projection", concurrency: 0, expected: inputRows * 2},
		{name: "parallel projection excluded", concurrency: 2, expected: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := newContext(t)
			ctx.GetSessionVars().SetProjectionConcurrency(test.concurrency)
			inputColumn := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
			inputSchema := expression.NewSchema(inputColumn)
			outputSchema := expression.NewSchema(
				&expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
				&expression.Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
			)
			dataSource := newStatementRUCPUWorkDataSource(ctx, inputSchema, inputRows)
			plan := physicalop.PhysicalProjection{
				Exprs: []expression.Expression{inputColumn, inputColumn},
			}.Init(ctx.GetPlanCtx(), &property.StatsInfo{RowCount: 100}, 0)
			plan.SetSchema(outputSchema)
			plan.SetChildren(testutil.BuildMockDataPhysicalPlan(ctx, dataSource))

			builder := newExecutorBuilder(context.Background(), ctx, nil, nil)
			built := builder.build(plan)
			require.NoError(t, builder.err)
			projection := built.(*ProjectionExec)
			if test.concurrency > 0 {
				require.Positive(t, projection.numWorkers)
			} else {
				require.Zero(t, projection.numWorkers)
			}
			statement := ctx.GetSessionVars().StmtCtx.TakeStatementRUForExecution()
			require.Equal(t, inputRows, drainStatementRUCPUWorkExecutor(t, built))
			finishStatementRUCPUWorkTest(t, statement, test.expected)
		})
	}

	t.Run("stream aggregation", func(t *testing.T) {
		ctx := newContext(t)
		inputColumn := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
		inputSchema := expression.NewSchema(inputColumn)
		dataSource := newStatementRUCPUWorkDataSource(ctx, inputSchema, inputRows)
		aggDesc, err := aggregation.NewAggFuncDesc(
			ctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{inputColumn}, false,
		)
		require.NoError(t, err)
		plan := new(physicalop.PhysicalStreamAgg)
		plan.AggFuncs = []*aggregation.AggFuncDesc{aggDesc}
		plan.GroupByItems = []expression.Expression{inputColumn}
		plan.SetSchema(expression.NewSchema(&expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}))
		plan.Init(ctx.GetPlanCtx(), nil, 0)
		plan.SetChildren(testutil.BuildMockDataPhysicalPlan(ctx, dataSource))

		builder := newExecutorBuilder(context.Background(), ctx, nil, nil)
		built := builder.build(plan)
		require.NoError(t, builder.err)
		statement := ctx.GetSessionVars().StmtCtx.TakeStatementRUForExecution()
		require.Equal(t, inputRows, drainStatementRUCPUWorkExecutor(t, built))
		finishStatementRUCPUWorkTest(t, statement, inputRows*2)
	})

	for _, pipelined := range []bool{false, true} {
		name := "regular window"
		if pipelined {
			name = "pipelined window"
		}
		t.Run(name, func(t *testing.T) {
			ctx := newContext(t)
			ctx.GetSessionVars().EnablePipelinedWindowExec = pipelined
			inputColumn := &expression.Column{Index: 0, UniqueID: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}
			inputSchema := expression.NewSchema(inputColumn)
			dataSource := newStatementRUCPUWorkDataSource(ctx, inputSchema, inputRows)
			windowFunc, err := aggregation.NewWindowFuncDesc(ctx.GetExprCtx(), ast.WindowFuncRowNumber, nil, false)
			require.NoError(t, err)
			outputSchema := inputSchema.Clone()
			outputSchema.Append(&expression.Column{Index: 1, UniqueID: 2, RetType: types.NewFieldType(mysql.TypeLonglong)})
			plan := physicalop.PhysicalWindow{
				WindowFuncDescs: []*aggregation.WindowFuncDesc{windowFunc},
				PartitionBy:     []property.SortItem{{Col: inputColumn}},
			}.Init(ctx.GetPlanCtx(), nil, 0)
			plan.SetSchema(outputSchema)
			plan.SetChildren(testutil.BuildMockDataPhysicalPlan(ctx, dataSource))

			builder := newExecutorBuilder(context.Background(), ctx, nil, nil)
			built := builder.build(plan)
			require.NoError(t, builder.err)
			if pipelined {
				require.IsType(t, &windowexec.PipelinedWindowExec{}, built)
			} else {
				require.IsType(t, &windowexec.WindowExec{}, built)
			}
			statement := ctx.GetSessionVars().StmtCtx.TakeStatementRUForExecution()
			require.Equal(t, inputRows, drainStatementRUCPUWorkExecutor(t, built))
			finishStatementRUCPUWorkTest(t, statement, inputRows*2)
		})
	}

	t.Run("union scan snapshot child", func(t *testing.T) {
		ctx := mock.NewContext()
		ctx.GetSessionVars().InitChunkSize = chunkSize
		ctx.GetSessionVars().MaxChunkSize = chunkSize
		configureStatementRUCPUWorkTest(t, ctx)

		columnInfo := &model.ColumnInfo{
			ID:        1,
			Name:      ast.NewCIStr("id"),
			Offset:    0,
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}
		tableInfo := &model.TableInfo{
			ID:         1,
			Name:       ast.NewCIStr("t"),
			Columns:    []*model.ColumnInfo{columnInfo},
			PKIsHandle: true,
			State:      model.StatePublic,
		}
		tbl := tables.MockTableFromMeta(tableInfo)
		column := &expression.Column{
			Index: 0, ID: columnInfo.ID, UniqueID: 1, RetType: &columnInfo.FieldType,
		}
		schema := expression.NewSchema(column)
		dataSource := newStatementRUCPUWorkDataSource(ctx, schema, inputRows)
		unionScan := &UnionScanExec{
			BaseExecutor:        exec.NewBaseExecutor(ctx, schema, 1, dataSource),
			memBufSnap:          statementRUEmptyGetter{},
			columns:             tableInfo.Columns,
			table:               tbl,
			snapshotChunkBuffer: exec.TryNewCacheChunk(dataSource),
			physTblIDIdx:        -1,
			compareExec: compareExec{
				handleCols: plannerutil.NewIntHandleCols(column),
			},
		}
		builder := newExecutorBuilder(context.Background(), ctx, nil, nil)
		builder.configureStatementRUCPUWork(unionScan, 1)
		require.NoError(t, builder.err)
		statement := ctx.GetSessionVars().StmtCtx.TakeStatementRUForExecution()

		rows := 0
		for {
			row, err := unionScan.getSnapshotRow(context.Background())
			require.NoError(t, err)
			if row == nil {
				break
			}
			rows++
			unionScan.cursor4SnapshotRows++
		}
		require.Equal(t, inputRows, rows)
		finishStatementRUCPUWorkTest(t, statement, inputRows)
	})
}
