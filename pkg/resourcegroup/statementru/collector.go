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

import "sync"

// UnitRecorder is the value-recording seam used by unit producers.
type UnitRecorder interface {
	Add(UnitKind, float64) bool
}

// EvidenceRecorder is the coverage seam used only by the statement-level,
// per-unit coordinator after every contributor for the affected units has
// terminated. Individual contributors receive only UnitRecorder.
type EvidenceRecorder interface {
	MarkPresent(UnitMask) bool
	MarkPartial(UnitMask) bool
	MarkUnavailable(UnitMask) bool
	MarkUnsupported(UnitMask) bool
}

// Config contains statement-local collection configuration. RequiredUnits must
// be nonempty; it selects which units are applicable to this statement. Weights
// and observations outside that mask do not enter its authoritative total.
// NewCollector copies Weights immediately. A nil Weights pointer means that
// weights are not available; a non-nil all-zero vector is valid.
type Config struct {
	RequiredUnits UnitMask
	Weights       *Weights
	RetainDetails bool
}

// Collector accumulates one logical statement's typed RU units. Its methods
// are safe for concurrent producers. Callers must finish producers before
// Finalize when their observations are required for an authoritative result.
type Collector struct {
	mu sync.Mutex

	weights    Weights
	hasWeights bool

	values   UnitValues
	coverage Coverage

	invalidReason Reason

	retainDetails bool
	finalized     bool
	result        Result
}

// NewCollector creates a statement RU collector and freezes its configuration.
func NewCollector(config Config) *Collector {
	collector := &Collector{
		coverage: Coverage{
			RequiredUnits: config.RequiredUnits,
		},
		retainDetails: config.RetainDetails,
	}
	if config.RequiredUnits == 0 || !config.RequiredUnits.valid() {
		collector.invalidReason = ReasonInvalidConfiguration
	}
	if config.Weights != nil {
		collector.weights = *config.Weights
		collector.hasWeights = true
		for i, weight := range collector.weights {
			if config.RequiredUnits&UnitKind(i).Mask() == 0 {
				continue
			}
			if !validNumber(weight) {
				collector.invalidReason = ReasonInvalidConfiguration
				break
			}
		}
	}
	return collector
}

// Add adds one finite, nonnegative delta. It does not mark the corresponding
// unit present. It returns false for invalid or late observations.
func (c *Collector) Add(kind UnitKind, delta float64) bool {
	if c == nil {
		return false
	}
	if !kind.valid() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.finalized {
			return false
		}
		c.markInvalidLocked(ReasonInvalidObservation, 0)
		return false
	}
	if !validNumber(delta) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.finalized {
			return false
		}
		c.markInvalidLocked(ReasonInvalidObservation, kind.Mask())
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalized {
		return false
	}
	next, ok := checkedAdd(c.values[kind], delta)
	if !ok {
		c.markInvalidLocked(ReasonArithmeticOverflow, kind.Mask())
		return false
	}
	c.values[kind] = next
	return true
}

// MarkPresent marks units whose complete evidence is available. Presence is
// sticky and does not clear a previously recorded partial or unavailable state.
// An empty mask is an accepted no-op.
func (c *Collector) MarkPresent(units UnitMask) bool {
	return c.markEvidence(units, evidencePresent)
}

// MarkPartial marks units with usable but incomplete evidence. An empty mask
// is an accepted no-op.
func (c *Collector) MarkPartial(units UnitMask) bool {
	return c.markEvidence(units, evidencePartial)
}

// MarkUnavailable marks units whose producer supplied no usable evidence. An
// empty mask is an accepted no-op.
func (c *Collector) MarkUnavailable(units UnitMask) bool {
	return c.markEvidence(units, evidenceUnavailable)
}

// MarkUnsupported marks units whose producer does not support the required
// evidence. An empty mask is an accepted no-op.
func (c *Collector) MarkUnsupported(units UnitMask) bool {
	return c.markEvidence(units, evidenceUnsupported)
}

type evidenceMark uint8

const (
	evidencePresent evidenceMark = iota
	evidencePartial
	evidenceUnavailable
	evidenceUnsupported
)

func (c *Collector) markEvidence(units UnitMask, mark evidenceMark) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalized {
		return false
	}
	if !units.valid() {
		c.markInvalidLocked(ReasonInvalidConfiguration, 0)
		return false
	}
	if units == 0 {
		return true
	}
	switch mark {
	case evidencePresent:
		c.coverage.PresentUnits |= units
	case evidencePartial:
		c.coverage.PartialUnits |= units
	case evidenceUnavailable:
		c.coverage.UnavailableUnits |= units
	case evidenceUnsupported:
		c.coverage.UnavailableUnits |= units
		c.coverage.UnsupportedUnits |= units
	default:
		c.markInvalidLocked(ReasonInvalidConfiguration, 0)
		return false
	}
	return true
}

// Finalize validates coverage and arithmetic, then freezes one immutable result.
// Repeated calls return the same value.
func (c *Collector) Finalize() Result {
	if c == nil {
		return Result{
			initialized: true,
			outcome:     Outcome{State: StateUnavailable, Reason: ReasonMissingEvidence},
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.finalized {
		return c.result
	}
	c.result = c.finalizeLocked()
	c.finalized = true
	if !c.retainDetails {
		c.values = UnitValues{}
		c.coverage = Coverage{}
	}
	return c.result
}

func (c *Collector) finalizeLocked() Result {
	result := Result{
		initialized: true,
	}
	if c.invalidReason != ReasonNone {
		result.outcome = Outcome{State: StateInvalid, Reason: c.invalidReason}
		return c.retainResultDetails(result)
	}

	required := c.coverage.RequiredUnits
	presentRequired := c.coverage.PresentUnits & required
	missingRequired := required &^ c.coverage.PresentUnits
	partialRequired := c.coverage.PartialUnits & required
	unavailableRequired := c.coverage.UnavailableUnits & required
	incomplete := missingRequired != 0 || partialRequired != 0 || unavailableRequired != 0
	hasEvidence := presentRequired != 0 || partialRequired != 0
	if incomplete {
		if hasEvidence {
			result.outcome = Outcome{State: StatePartial, Reason: c.incompleteReason()}
		} else {
			result.outcome = Outcome{State: StateUnavailable, Reason: c.missingReason()}
		}
		return c.retainResultDetails(result)
	}
	if !c.hasWeights {
		result.outcome = Outcome{State: StateUnavailable, Reason: ReasonWeightsUnavailable}
		return c.retainResultDetails(result)
	}

	var total float64
	for i, value := range c.values {
		kind := UnitKind(i)
		if required&kind.Mask() == 0 {
			continue
		}
		weighted, ok := checkedMultiply(value, c.weights[i])
		if !ok {
			c.coverage.InvalidUnits |= kind.Mask()
			result.outcome = Outcome{State: StateInvalid, Reason: ReasonArithmeticOverflow}
			return c.retainResultDetails(result)
		}
		total, ok = checkedAdd(total, weighted)
		if !ok {
			c.coverage.InvalidUnits |= kind.Mask()
			result.outcome = Outcome{State: StateInvalid, Reason: ReasonArithmeticOverflow}
			return c.retainResultDetails(result)
		}
	}
	result.totalRU = total
	result.hasTotal = true
	result.outcome = Outcome{State: StateComplete, Reason: ReasonNone}
	return c.retainResultDetails(result)
}

func (c *Collector) incompleteReason() Reason {
	if c.coverage.UnsupportedUnits&c.coverage.RequiredUnits != 0 {
		return ReasonUnsupported
	}
	return ReasonIncompleteEvidence
}

func (c *Collector) missingReason() Reason {
	if c.coverage.UnsupportedUnits&c.coverage.RequiredUnits != 0 {
		return ReasonUnsupported
	}
	return ReasonMissingEvidence
}

func (c *Collector) markInvalidLocked(reason Reason, units UnitMask) {
	c.coverage.InvalidUnits |= units
	if (units == 0 || units&c.coverage.RequiredUnits != 0) && c.invalidReason == ReasonNone {
		c.invalidReason = reason
	}
}

func (c *Collector) retainResultDetails(result Result) Result {
	if c.retainDetails {
		result.details = &resultDetails{
			units:    c.values,
			coverage: c.coverage,
		}
	}
	return result
}
