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

// Coverage is a frozen, bounded summary of statement evidence. RequiredUnits
// determines candidate-total eligibility, while CollectedUnits is the superset
// producers were allowed to observe. Partial, unavailable, unsupported, and
// invalid masks retain per-unit diagnostic causes; only their intersections
// with RequiredUnits affect production eligibility.
type Coverage struct {
	RequiredUnits    UnitMask
	CollectedUnits   UnitMask
	PresentUnits     UnitMask
	PartialUnits     UnitMask
	UnavailableUnits UnitMask
	UnsupportedUnits UnitMask
	InvalidUnits     UnitMask
}

// CollectionState is an internal statement RU collection outcome.
type CollectionState uint8

const (
	// StateUnavailable means no authoritative result can be calculated because
	// a required source or the formula weights are unavailable.
	StateUnavailable CollectionState = iota
	// StatePartial means some usable evidence exists but required coverage is incomplete.
	StatePartial
	// StateComplete means all required evidence and weights produced a valid total.
	StateComplete
	// StateInvalid means an observation, invariant, or arithmetic operation was invalid.
	StateInvalid
)

// Reason is a bounded explanation for a non-complete collection outcome.
type Reason uint8

const (
	// ReasonNone is used by complete outcomes.
	ReasonNone Reason = iota
	// ReasonMissingEvidence means no usable required evidence was supplied.
	ReasonMissingEvidence
	// ReasonIncompleteEvidence means some required evidence was absent or partial.
	ReasonIncompleteEvidence
	// ReasonUnsupported means a required producer explicitly does not support the path.
	ReasonUnsupported
	// ReasonWeightsUnavailable means units may be available but weights were not configured.
	ReasonWeightsUnavailable
	// ReasonInvalidConfiguration means collector configuration or a transition was invalid.
	ReasonInvalidConfiguration
	// ReasonInvalidObservation means a unit kind or delta was invalid.
	ReasonInvalidObservation
	// ReasonArithmeticOverflow means checked addition or multiplication was not finite.
	ReasonArithmeticOverflow
)

// Outcome is the internal state and bounded reason of a frozen result.
type Outcome struct {
	State  CollectionState
	Reason Reason
}
