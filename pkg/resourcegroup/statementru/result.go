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

// TotalRU returns the authoritative total and whether one exists. A returned
// zero with ok=true is an authoritative zero.
func (r Result) TotalRU() (total float64, ok bool) {
	return r.totalRU, r.hasTotal
}

// HasTotal reports whether the result contains an authoritative total.
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
