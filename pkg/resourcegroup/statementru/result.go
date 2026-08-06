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

// Result is an immutable-by-copy statement RU collection result.
type Result struct {
	initialized bool
	totalRU     float64
	hasTotal    bool
	outcome     Outcome
	details     *resultDetails
}

type resultDetails struct {
	units    UnitValues
	coverage Coverage
}

// Diagnostic is an in-process value view of one frozen Calibration or Explain
// result. It is not a wire schema and deliberately carries no statement or
// weight-profile identity. A future transport must pair it with identity and
// profile metadata frozen at statement selection rather than read mutable
// global configuration at finish time.
//
// The candidate total is non-authoritative. Only a production Report selected
// by FinishResult.ReportSelected carries an authoritative statement RU value.
type Diagnostic struct {
	terminal            TerminalStatus
	outcome             Outcome
	units               UnitValues
	coverage            Coverage
	candidateTotalRU    float64
	hasCandidateTotalRU bool
}

// Diagnostic projects the same immutable result frozen by Statement.Finish.
// It is available only when the selected mode retained bounded details.
func (f FinishResult) Diagnostic() (Diagnostic, bool) {
	units, hasUnits := f.Result.Units()
	coverage, hasCoverage := f.Result.Coverage()
	if !hasUnits || !hasCoverage {
		return Diagnostic{}, false
	}
	candidateTotalRU, hasCandidateTotalRU := f.Result.TotalRU()
	return Diagnostic{
		terminal:            f.Terminal,
		outcome:             f.Result.Outcome(),
		units:               units,
		coverage:            coverage,
		candidateTotalRU:    candidateTotalRU,
		hasCandidateTotalRU: hasCandidateTotalRU,
	}, true
}

// Terminal returns the final logical-statement status. It is orthogonal to the
// collection Outcome and candidate-total availability.
func (d Diagnostic) Terminal() TerminalStatus {
	return d.terminal
}

// Outcome returns the bounded collection outcome.
func (d Diagnostic) Outcome() Outcome {
	return d.outcome
}

// Units returns a value copy of the frozen raw units.
func (d Diagnostic) Units() UnitValues {
	return d.units
}

// Coverage returns a value copy of the frozen evidence coverage.
func (d Diagnostic) Coverage() Coverage {
	return d.coverage
}

// CandidateTotalRU returns the non-authoritative weighted candidate and
// whether it exists. A returned zero with ok=true is a present candidate zero,
// not missing evidence.
func (d Diagnostic) CandidateTotalRU() (total float64, ok bool) {
	return d.candidateTotalRU, d.hasCandidateTotalRU
}

// TotalRU returns the formula-complete weighted total and whether one exists.
// A returned zero with ok=true is a present zero. In Calibration and Explain
// this value is only a candidate; production authority is represented by
// FinishResult.ReportSelected and the separate Report payload.
func (r Result) TotalRU() (total float64, ok bool) {
	return r.totalRU, r.hasTotal
}

// HasTotal reports whether the result contains a formula-complete weighted
// total. The selected mode and terminal reporting gate determine authority.
func (r Result) HasTotal() bool {
	return r.hasTotal
}

// Outcome returns the internal collection outcome.
func (r Result) Outcome() Outcome {
	if !r.initialized {
		return Outcome{State: StateUnavailable, Reason: ReasonMissingEvidence}
	}
	return r.outcome
}

// Units returns a value copy of the raw units when detail retention was requested.
func (r Result) Units() (UnitValues, bool) {
	if r.details == nil {
		return UnitValues{}, false
	}
	return r.details.units, true
}

// Coverage returns a value copy of coverage when detail retention was requested.
func (r Result) Coverage() (Coverage, bool) {
	if r.details == nil {
		return Coverage{}, false
	}
	return r.details.coverage, true
}
