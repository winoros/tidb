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

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/mock"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

type statementRUReporterForTest struct {
	reports []statementru.Report
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
