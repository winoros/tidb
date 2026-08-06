// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statementru

import (
	"sync"
	"sync/atomic"
)

// Mode controls how much statement RU state is collected and retained.
type Mode uint8

const (
	// ModeOff performs no collection and allocates no statement owner.
	ModeOff Mode = iota
	// ModeResultOnly calculates an authoritative total without retaining units.
	ModeResultOnly
	// ModeCalibration retains bounded unit and coverage details for calibration.
	ModeCalibration
	// ModeExplain retains bounded details for a later operator-attribution layer.
	ModeExplain
)

func (m Mode) valid() bool {
	return m <= ModeExplain
}

// Report is the complete production payload. A zero TotalRU is an
// authoritative zero, not missing evidence.
type Report struct {
	TotalRU float64
}

// Reporter receives one complete production result. Statement
// identity is supplied by the reporter's surrounding transport context. A
// reporter panic is contained because reporting must not interrupt statement
// completion bookkeeping.
type Reporter interface {
	ReportStatementRU(Report)
}

// Selection is the statement-local decision frozen before execution. Applicable
// distinguishes lifecycle eligibility from evidence support: an inapplicable
// statement selects true Off, while an applicable statement whose selected unit
// producer is unsupported uses EvidenceRecorder.MarkUnsupported. RequiredUnits
// is the nonempty candidate-total mask. CollectedUnits may be its superset for
// optional diagnostics; zero defaults to RequiredUnits.
type Selection struct {
	Mode           Mode
	Applicable     bool
	RequiredUnits  UnitMask
	CollectedUnits UnitMask
	Weights        *Weights
	Reporter       Reporter
}

// TerminalStatus is the bounded final logical-statement status.
type TerminalStatus uint8

const (
	// TerminalSuccess means the logical statement completed without an error.
	TerminalSuccess TerminalStatus = iota
	// TerminalError means the logical statement failed for a reason other than cancellation.
	TerminalError
	// TerminalCanceled means the statement was canceled or exceeded its deadline.
	TerminalCanceled
)

// FinishResult is the immutable collection result plus its terminal reporting
// decision. ReportSelected means this result passed the production reporting
// gate. It does not describe callback completion or downstream delivery.
type FinishResult struct {
	Result         Result
	Terminal       TerminalStatus
	ReportSelected bool
}

// Statement owns one logical statement's collector and exactly-once finish.
// Transparent retries keep using the same Statement.
type Statement struct {
	mode                 Mode
	requiredUnits        UnitMask
	collectedUnits       UnitMask
	producerCapabilities bool
	collector            *Collector
	reporter             Reporter
	unit                 statementUnitRecorder
	evidence             statementEvidenceRecorder
	contributors         atomic.Pointer[contributorCoordinator]
	registrar            statementContributorRegistrar

	finishOnce sync.Once
	finish     FinishResult
}

// The concrete capability adapters deliberately expose only their respective
// interfaces. Returning Collector directly would let producers type-assert to
// Finalize or to the other recorder interface.
type statementUnitRecorder struct {
	statement *Statement
}

func (r *statementUnitRecorder) CollectedUnits() UnitMask {
	if r == nil || r.statement == nil {
		return 0
	}
	return r.statement.collectedUnits
}

func (r *statementUnitRecorder) Add(kind UnitKind, delta float64) bool {
	if r == nil || r.statement == nil {
		return false
	}
	return r.statement.collector.Add(kind, delta)
}

type statementEvidenceRecorder struct {
	collector *Collector
}

func (r *statementEvidenceRecorder) MarkPresent(units UnitMask) bool {
	return r.collector.MarkPresent(units)
}

func (r *statementEvidenceRecorder) MarkPartial(units UnitMask) bool {
	return r.collector.MarkPartial(units)
}

func (r *statementEvidenceRecorder) MarkUnavailable(units UnitMask) bool {
	return r.collector.MarkUnavailable(units)
}

func (r *statementEvidenceRecorder) MarkUnsupported(units UnitMask) bool {
	return r.collector.MarkUnsupported(units)
}

// NewStatement translates a frozen selection into a statement owner. Off,
// inapplicable, and invalid modes return nil without allocating. A valid enabled
// mode with invalid unit configuration returns an owner whose Finish result is
// InvalidConfiguration, but exposes no producer capabilities.
func NewStatement(selection Selection) *Statement {
	if selection.Mode == ModeOff || !selection.Mode.valid() || !selection.Applicable {
		return nil
	}
	required, collected, _ := normalizeUnitSelection(selection.RequiredUnits, selection.CollectedUnits)
	retainDetails := selection.Mode == ModeCalibration || selection.Mode == ModeExplain
	collector := NewCollector(Config{
		RequiredUnits:  selection.RequiredUnits,
		CollectedUnits: selection.CollectedUnits,
		Weights:        selection.Weights,
		RetainDetails:  retainDetails,
	})
	statement := &Statement{
		mode:                 selection.Mode,
		requiredUnits:        required,
		collectedUnits:       collected,
		producerCapabilities: collector.configurationValid,
		collector:            collector,
		reporter:             selection.Reporter,
	}
	statement.unit.statement = statement
	statement.evidence.collector = collector
	statement.registrar.statement = statement
	return statement
}

// Mode returns the frozen collection mode. A nil Statement is Off.
func (s *Statement) Mode() Mode {
	if s == nil {
		return ModeOff
	}
	return s.mode
}

// UnitRecorder returns the narrow value-recording seam for unit producers.
func (s *Statement) UnitRecorder() UnitRecorder {
	if s == nil || !s.producerCapabilities {
		return nil
	}
	return &s.unit
}

// EvidenceRecorder returns the statement-level coverage-coordination seam.
func (s *Statement) EvidenceRecorder() EvidenceRecorder {
	if s == nil || !s.producerCapabilities {
		return nil
	}
	return &s.evidence
}

// UnitContributorRegistrar returns the statement-owned contributor lifecycle
// capability. It does not expose finalization or direct evidence mutation.
func (s *Statement) UnitContributorRegistrar() UnitContributorRegistrar {
	if s == nil || !s.producerCapabilities {
		return nil
	}
	return &s.registrar
}

// Finish freezes the result and applies the production reporting gate exactly
// once. The caller translates its final error into a bounded terminal status.
// The bool is true only for the caller that performed finalization.
func (s *Statement) Finish(terminal TerminalStatus) (FinishResult, bool) {
	if s == nil {
		return FinishResult{}, false
	}
	finished := false
	s.finishOnce.Do(func() {
		finished = true
		s.sealUnitContributors()
		s.finish.Result = s.collector.Finalize()
		s.finish.Terminal = terminal
		if s.finish.Terminal != TerminalSuccess || s.mode != ModeResultOnly || s.reporter == nil {
			return
		}
		if !s.finish.Result.HasTotal() {
			return
		}
		s.finish.ReportSelected = true
	})
	if finished && s.finish.ReportSelected {
		total, _ := s.finish.Result.TotalRU()
		// The result and report decision are already frozen. Keeping the callback
		// outside sync.Once allows a reporter to inspect Finish without deadlock.
		callStatementRUReporter(s.reporter, Report{TotalRU: total})
	}
	return s.finish, finished
}

func callStatementRUReporter(reporter Reporter, report Report) {
	defer func() {
		// A reporting sink must not suppress slow log, statement summary, and
		// the remaining logical-statement completion work.
		_ = recover()
	}()
	reporter.ReportStatementRU(report)
}
