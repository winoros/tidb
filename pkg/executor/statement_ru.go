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
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
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
	stmt.statementRU = takeStatementRUForExecution(sc)
	return stmt, nil
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
