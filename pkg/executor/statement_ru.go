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

const maxExactStatementRUNetworkBytes = uint64(1 << 53)

type statementRUNetworkAttachment struct {
	contributor statementru.UnitContributor
	collect     bool
}

type statementRUNetworkAttachmentContextKey struct{}

// PrepareStatementRUNetworkContext installs one response-network owner before
// compile can issue storage requests. It is a true no-op unless the statement
// selection requires NetworkBytes. Callers must keep using the returned
// context; calling it again on that context is idempotent.
func PrepareStatementRUNetworkContext(ctx context.Context, sc *stmtctx.StatementContext) context.Context {
	if ctx == nil || sc == nil {
		return ctx
	}
	if attachment, _ := ctx.Value(statementRUNetworkAttachmentContextKey{}).(*statementRUNetworkAttachment); attachment != nil {
		return ctx
	}
	registrar := sc.StatementRUUnitContributorRegistrar()
	if registrar == nil || registrar.CollectedUnits()&statementru.NetworkBytes.Mask() == 0 {
		return ctx
	}
	attachment := &statementRUNetworkAttachment{
		contributor: registrar.RegisterUnitContributor(statementru.NetworkBytes.Mask()),
	}
	if attachment.contributor == nil {
		return ctx
	}
	// A pre-existing owner belongs to an outer or otherwise unknown
	// attribution boundary. Do not reuse it for a new statement.
	if tikvutil.NetworkResponseEvidenceFromContext(ctx).Enabled {
		attachment.contributor.Unavailable()
		return context.WithValue(ctx, statementRUNetworkAttachmentContextKey{}, attachment)
	}
	attachment.collect = true
	ctx = tikvutil.ContextWithNetworkResponseEvidence(ctx)
	return context.WithValue(ctx, statementRUNetworkAttachmentContextKey{}, attachment)
}

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
	if stmt.statementRU != nil && stmt.GoCtx != nil {
		attachment, _ := stmt.GoCtx.Value(statementRUNetworkAttachmentContextKey{}).(*statementRUNetworkAttachment)
		if attachment != nil && attachment.collect {
			stmt.statementRUNetworkContributor = attachment.contributor
		}
	}
	return stmt, nil
}

func (a *ExecStmt) recordStatementRUNetworkBytes() {
	if a == nil || a.statementRUNetworkContributor == nil {
		return
	}
	contributor := a.statementRUNetworkContributor
	a.statementRUNetworkContributor = nil
	snapshot := tikvutil.NetworkResponseEvidenceFromContext(a.GoCtx)
	usable := snapshot.SizedBodies > 0

	if a.statementRUContext == nil {
		contributor.Unavailable()
		return
	}
	if a.statementRUNetworkConflict.Load() {
		if usable {
			contributor.Partial()
		} else {
			contributor.Unavailable()
		}
		return
	}
	// The current client-go domain covers only unary TiKV read/Cop responses.
	// Writes and TiFlash remain fail-closed even when they happen to issue no
	// response observed by that subset.
	if !a.statementRUContext.IsReadOnly || !statementRUNetworkSupportedStatement(a) ||
		a.statementRUContext.IsTiFlash.Load() || snapshot.Unsupported > 0 {
		contributor.Unsupported()
		return
	}
	if snapshot.Complete() && snapshot.ResponseBytes <= maxExactStatementRUNetworkBytes {
		var values statementru.UnitValues
		values[statementru.NetworkBytes] = float64(snapshot.ResponseBytes)
		contributor.Complete(values)
		return
	}
	if usable {
		contributor.Partial()
	} else {
		contributor.Unavailable()
	}
}

// statementRUNetworkSupportedStatement is the currently audited statement
// subset. Other read-only statements may issue storage requests through paths
// that do not preserve the execution context, so an untouched owner cannot
// prove zero. EXECUTE is classified by its resolved template, never by the
// outer wrapper.
func statementRUNetworkSupportedStatement(stmt *ExecStmt) bool {
	if stmt == nil {
		return false
	}
	node := stmt.StmtNode
	if _, ok := node.(*ast.ExecuteStmt); ok {
		node = stmt.statementRUNetworkResolvedNode
	}
	switch node.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.ExplainStmt:
		return true
	default:
		return false
	}
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
	unitMask := statementru.FrontendCompileBytes.Mask()
	if unitRecorder.CollectedUnits()&unitMask == 0 {
		return
	}
	evidenceRecorder := sc.StatementRUEvidenceRecorder()
	if evidenceRecorder == nil {
		return
	}
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
	collectedWriteUnits := unitRecorder.CollectedUnits() & writeUnits
	if collectedWriteUnits == 0 {
		return
	}
	if a.statementRUOwnsDDL() {
		evidenceRecorder.MarkUnsupported(collectedWriteUnits)
		return
	}
	if commitEvidence.pipelined {
		evidenceRecorder.MarkUnsupported(collectedWriteUnits)
		return
	}
	// Current client-go does not expose authoritative zero, complete pipelined
	// flush totals, or a final-successful-attempt presence bit. TiDB can also
	// replay before client-go creates CommitDetails. Keep every such path
	// unavailable until that dependency contract lands. On an ordinary,
	// non-retried commit, each selected positive detail is independently
	// complete; an absent detail for one unit does not discard the other.
	if commitEvidence.wholeTxnRetried || commitDetail == nil || commitDetail.TxnRetry > 0 {
		evidenceRecorder.MarkUnavailable(collectedWriteUnits)
		return
	}
	var present, unavailable statementru.UnitMask
	if collectedWriteUnits&statementru.WriteKeys.Mask() != 0 {
		if commitDetail.WriteKeys > 0 && unitRecorder.Add(statementru.WriteKeys, float64(commitDetail.WriteKeys)) {
			present |= statementru.WriteKeys.Mask()
		} else {
			unavailable |= statementru.WriteKeys.Mask()
		}
	}
	if collectedWriteUnits&statementru.WriteBytes.Mask() != 0 {
		if commitDetail.WriteSize > 0 && unitRecorder.Add(statementru.WriteBytes, float64(commitDetail.WriteSize)) {
			present |= statementru.WriteBytes.Mask()
		} else {
			unavailable |= statementru.WriteBytes.Mask()
		}
	}
	evidenceRecorder.MarkPresent(present)
	evidenceRecorder.MarkUnavailable(unavailable)
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
