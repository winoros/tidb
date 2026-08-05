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

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func takeStatementRUForExecution(sc *stmtctx.StatementContext) *statementru.Statement {
	if sc == nil {
		return nil
	}
	return sc.TakeStatementRUForExecution()
}

// finishCompileWithStatementRU transfers finalization ownership only after the
// final fallible compile initialization succeeds.
func finishCompileWithStatementRU(stmt *ExecStmt, sc *stmtctx.StatementContext, initializationErr error) (*ExecStmt, error) {
	if initializationErr != nil {
		return nil, initializationErr
	}
	stmt.statementRUContext = sc
	stmt.statementRU = takeStatementRUForExecution(sc)
	return stmt, nil
}

func recordStatementRUFrontendCompile(
	sc *stmtctx.StatementContext,
	cacheHit bool,
	stmtNode ast.StmtNode,
	preparedObj *plannercore.PlanCacheStmt,
) {
	if sc == nil {
		return
	}
	unitRecorder := sc.StatementRUUnitRecorder()
	if unitRecorder == nil {
		return
	}
	evidenceRecorder := sc.StatementRUEvidenceRecorder()
	if evidenceRecorder == nil {
		return
	}
	unitMask := statementru.FrontendCompileBytes.Mask()
	if cacheHit {
		evidenceRecorder.MarkPresent(unitMask)
		return
	}

	source := statementRUCompileSource(sc, stmtNode, preparedObj)
	if source == "" {
		evidenceRecorder.MarkUnavailable(unitMask)
		return
	}
	if unitRecorder.Add(statementru.FrontendCompileBytes, float64(len(source))) {
		evidenceRecorder.MarkPresent(unitMask)
	}
}

func statementRUCompileSource(
	sc *stmtctx.StatementContext,
	stmtNode ast.StmtNode,
	preparedObj *plannercore.PlanCacheStmt,
) string {
	if preparedObj != nil && preparedObj.PreparedAst != nil && preparedObj.PreparedAst.Stmt != nil {
		if source := preparedObj.PreparedAst.Stmt.OriginalText(); source != "" {
			return source
		}
		if sc.OriginalSQL != "" {
			return sc.OriginalSQL
		}
		return preparedObj.PreparedAst.Stmt.Text()
	}
	if stmtNode == nil {
		return sc.OriginalSQL
	}
	if _, preparedExecution := stmtNode.(*ast.ExecuteStmt); preparedExecution {
		// ResetContextOfStmt stores the resolved template in OriginalSQL. Never
		// substitute the outer EXECUTE text for missing template evidence.
		return sc.OriginalSQL
	}
	if source := stmtNode.OriginalText(); source != "" {
		return source
	}
	if sc.OriginalSQL != "" {
		return sc.OriginalSQL
	}
	return stmtNode.Text()
}

func (a *ExecStmt) readStatementRUCommitEvidence() statementRUCommitEvidence {
	if a == nil || a.statementRUContext == nil {
		return statementRUCommitEvidence{}
	}
	return statementRUCommitEvidence{
		pipelined:       a.statementRUContext.StatementRUCommitPipelined(),
		wholeTxnRetried: a.statementRUContext.StatementRUWholeTxnRetried(),
	}
}

func (a *ExecStmt) recordStatementRUWriteDetails(
	commitDetail *tikvutil.CommitDetails,
	finalErr error,
	commitEvidence statementRUCommitEvidence,
) {
	if a == nil || a.statementRU == nil || finalErr != nil {
		return
	}
	unitRecorder := a.statementRU.UnitRecorder()
	evidenceRecorder := a.statementRU.EvidenceRecorder()
	writeUnits := statementru.WriteKeys.Mask() | statementru.WriteBytes.Mask()
	if unitRecorder == nil || evidenceRecorder == nil {
		return
	}
	if a.statementRUOwnsDDL() {
		evidenceRecorder.MarkUnsupported(writeUnits)
		return
	}
	if commitEvidence.pipelined {
		evidenceRecorder.MarkUnsupported(writeUnits)
		return
	}
	// Current client-go does not expose authoritative zero, complete pipelined
	// flush totals, or a final-successful-attempt presence bit. TiDB can also
	// replay before client-go creates CommitDetails. Keep every such path
	// unavailable until that dependency contract lands; only a positive,
	// non-retried ordinary pair is complete in this layer.
	if commitEvidence.wholeTxnRetried || commitDetail == nil || commitDetail.TxnRetry > 0 ||
		commitDetail.WriteKeys <= 0 || commitDetail.WriteSize <= 0 {
		evidenceRecorder.MarkUnavailable(writeUnits)
		return
	}
	keysAccepted := unitRecorder.Add(statementru.WriteKeys, float64(commitDetail.WriteKeys))
	bytesAccepted := unitRecorder.Add(statementru.WriteBytes, float64(commitDetail.WriteSize))
	if keysAccepted && bytesAccepted {
		evidenceRecorder.MarkPresent(writeUnits)
	}
}

type statementRUCommitEvidence struct {
	pipelined       bool
	wholeTxnRetried bool
}

func (a *ExecStmt) statementRUOwnsDDL() bool {
	if _, ddl := a.StmtNode.(ast.DDLNode); ddl {
		return true
	}
	plan := a.Plan
	if execute, ok := plan.(*plannercore.Execute); ok {
		plan = execute.Plan
	}
	_, ddl := plan.(*plannercore.DDL)
	return ddl
}

func (a *ExecStmt) finishStatementRU(finalErr error) {
	if a == nil || a.statementRU == nil {
		return
	}
	finish, first := a.statementRU.Finish(statementRUTerminalStatus(finalErr))
	if !first {
		return
	}
	outcome := finish.Result.Outcome()
	metrics.StatementRUFinishCounter.WithLabelValues(
		statementRUTerminalLabel(finish.Terminal),
		statementRUStateLabel(outcome.State),
		statementRUReasonLabel(outcome.Reason),
		statementRUReportSelectedLabel(finish.ReportSelected),
	).Inc()
}

func statementRUTerminalStatus(finalErr error) statementru.TerminalStatus {
	if finalErr == nil {
		return statementru.TerminalSuccess
	}
	if stderrors.Is(finalErr, context.Canceled) ||
		stderrors.Is(finalErr, context.DeadlineExceeded) ||
		exeerrors.ErrQueryInterrupted.Equal(finalErr) ||
		exeerrors.ErrMaxExecTimeExceeded.Equal(finalErr) {
		return statementru.TerminalCanceled
	}
	return statementru.TerminalError
}

func statementRUTerminalLabel(status statementru.TerminalStatus) string {
	switch status {
	case statementru.TerminalSuccess:
		return "success"
	case statementru.TerminalError:
		return "error"
	case statementru.TerminalCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

func statementRUStateLabel(state statementru.CollectionState) string {
	switch state {
	case statementru.StateUnavailable:
		return "unavailable"
	case statementru.StatePartial:
		return "partial"
	case statementru.StateComplete:
		return "complete"
	case statementru.StateInvalid:
		return "invalid"
	default:
		return "unknown"
	}
}

func statementRUReasonLabel(reason statementru.Reason) string {
	switch reason {
	case statementru.ReasonNone:
		return "none"
	case statementru.ReasonMissingEvidence:
		return "missing_evidence"
	case statementru.ReasonIncompleteEvidence:
		return "incomplete_evidence"
	case statementru.ReasonUnsupported:
		return "unsupported"
	case statementru.ReasonWeightsUnavailable:
		return "weights_unavailable"
	case statementru.ReasonInvalidConfiguration:
		return "invalid_configuration"
	case statementru.ReasonInvalidObservation:
		return "invalid_observation"
	case statementru.ReasonArithmeticOverflow:
		return "arithmetic_overflow"
	default:
		return "unknown"
	}
}

func statementRUReportSelectedLabel(selected bool) string {
	if selected {
		return "true"
	}
	return "false"
}
