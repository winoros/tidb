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

// UnitContributorRegistrar registers statement-owned contributors before
// their physical work starts. Producers must terminate every registered
// contributor before Statement.Finish; finalization does not wait for them.
type UnitContributorRegistrar interface {
	RegisterUnitContributor(UnitMask) UnitContributor
}

// UnitContributor is an exactly-once completion lease. Complete atomically
// submits a fixed vector. The other terminal methods retain only bounded
// coverage state and never submit a partial value. Every terminal method
// consumes the lease even when it returns false, so a rejected completion is
// terminal and cannot be retried.
type UnitContributor interface {
	Complete(UnitValues) bool
	Partial() bool
	Unavailable() bool
	Unsupported() bool
}

type contributorCoordinator struct {
	mu sync.Mutex

	collector   *Collector
	sealed      bool
	registered  [UnitCount]uint64
	completed   [UnitCount]uint64
	partial     UnitMask
	unavailable UnitMask
	unsupported UnitMask
}

type statementContributorRegistrar struct {
	statement *Statement
}

func (r *statementContributorRegistrar) RegisterUnitContributor(units UnitMask) UnitContributor {
	if r == nil || r.statement == nil {
		return nil
	}
	return r.statement.registerUnitContributor(units)
}

var sealedContributorCoordinator = &contributorCoordinator{sealed: true}

func (s *Statement) registerUnitContributor(units UnitMask) UnitContributor {
	for {
		coordinator := s.contributors.Load()
		if coordinator == sealedContributorCoordinator {
			return nil
		}
		if coordinator == nil {
			candidate := &contributorCoordinator{collector: s.collector}
			if !s.contributors.CompareAndSwap(nil, candidate) {
				continue
			}
			coordinator = candidate
		}
		return coordinator.register(units)
	}
}

func (s *Statement) sealUnitContributors() {
	coordinator := s.contributors.Swap(sealedContributorCoordinator)
	if coordinator != nil && coordinator != sealedContributorCoordinator {
		coordinator.seal()
	}
}

type unitContributor struct {
	coordinator *contributorCoordinator
	units       UnitMask
	done        bool
}

func (c *unitContributor) Complete(values UnitValues) bool {
	return c.finish(values, contributorComplete)
}

func (c *unitContributor) Partial() bool {
	return c.finish(UnitValues{}, contributorPartial)
}

func (c *unitContributor) Unavailable() bool {
	return c.finish(UnitValues{}, contributorUnavailable)
}

func (c *unitContributor) Unsupported() bool {
	return c.finish(UnitValues{}, contributorUnsupported)
}

type contributorTerminal uint8

const (
	contributorComplete contributorTerminal = iota
	contributorPartial
	contributorUnavailable
	contributorUnsupported
)

func (c *unitContributor) finish(values UnitValues, terminal contributorTerminal) bool {
	if c == nil || c.coordinator == nil {
		return false
	}
	coordinator := c.coordinator
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.sealed || c.done {
		return false
	}
	c.done = true

	switch terminal {
	case contributorComplete:
		if !vectorWithinMask(values, c.units) || !coordinator.collector.AcceptVector(values) {
			coordinator.partial |= c.units
			return false
		}
		forEachUnit(c.units, func(kind UnitKind) {
			coordinator.completed[kind]++
		})
	case contributorPartial:
		coordinator.partial |= c.units
	case contributorUnavailable:
		coordinator.unavailable |= c.units
	case contributorUnsupported:
		coordinator.unsupported |= c.units
	default:
		coordinator.partial |= c.units
		return false
	}
	return true
}

func (c *contributorCoordinator) register(units UnitMask) UnitContributor {
	if c == nil || units == 0 || !units.valid() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed {
		return nil
	}
	forEachUnit(units, func(kind UnitKind) {
		c.registered[kind]++
	})
	return &unitContributor{coordinator: c, units: units}
}

func (c *contributorCoordinator) seal() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed {
		return
	}
	c.sealed = true

	var present, partial, unavailable, unsupported UnitMask
	for kind := UnitKind(0); kind < UnitKind(UnitCount); kind++ {
		mask := kind.Mask()
		registered := c.registered[kind]
		if registered == 0 {
			continue
		}
		hasUnsupported := c.unsupported&mask != 0
		hasPartial := c.partial&mask != 0
		hasIncomplete := hasUnsupported || hasPartial || c.unavailable&mask != 0 || c.completed[kind] != registered
		switch {
		case !hasIncomplete:
			present |= mask
		case c.completed[kind] > 0 || hasPartial:
			partial |= mask
		default:
			unavailable |= mask
		}
		if hasUnsupported {
			unsupported |= mask
		}
	}
	c.collector.MarkPresent(present)
	c.collector.MarkPartial(partial)
	c.collector.MarkUnavailable(unavailable)
	c.collector.MarkUnsupported(unsupported)
}

func vectorWithinMask(values UnitValues, units UnitMask) bool {
	for i, value := range values {
		if units&UnitKind(i).Mask() == 0 && value != 0 {
			return false
		}
	}
	return true
}

func forEachUnit(units UnitMask, callback func(UnitKind)) {
	for kind := UnitKind(0); kind < UnitKind(UnitCount); kind++ {
		if units&kind.Mask() != 0 {
			callback(kind)
		}
	}
}
