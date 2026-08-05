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
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func TestCollectorAdd(t *testing.T) {
	weights := Weights{1, 2, 3, 4, 5, 6, 7, 8}
	collector := NewCollector(Config{
		RequiredUnits: AllUnits,
		Weights:       &weights,
		RetainDetails: true,
	})
	for kind := UnitKind(0); kind < UnitKind(UnitCount); kind++ {
		if !collector.Add(kind, float64(kind+1)) {
			t.Fatalf("unit %d was not accepted", kind)
		}
	}
	if !collector.MarkPresent(AllUnits) {
		t.Fatal("complete coverage was not accepted")
	}

	weights[CPUWork] = 1000
	result := collector.Finalize()
	if result.Outcome() != (Outcome{State: StateComplete, Reason: ReasonNone}) {
		t.Fatalf("unexpected outcome: %+v", result.Outcome())
	}
	total, ok := result.TotalRU()
	if !ok || total != 204 {
		t.Fatalf("unexpected total: %v, %v", total, ok)
	}
	units, ok := result.Units()
	if !ok {
		t.Fatal("calibration units were not retained")
	}
	for kind := UnitKind(0); kind < UnitKind(UnitCount); kind++ {
		if units[kind] != float64(kind+1) {
			t.Fatalf("unexpected unit %d: %v", kind, units[kind])
		}
	}

	if total, ok = result.TotalRU(); !ok || total != 204 {
		t.Fatalf("weights were not frozen: %v, %v", total, ok)
	}

}

func TestCollectorAcceptVectorAtomic(t *testing.T) {
	weights := Weights{CPUWork: 1, ScanBytes: 1}
	collector := NewCollector(Config{
		RequiredUnits: CPUWork.Mask() | ScanBytes.Mask(),
		Weights:       &weights,
		RetainDetails: true,
	})
	first := UnitValues{CPUWork: 2, ScanBytes: 3}
	if !collector.AcceptVector(first) {
		t.Fatal("valid vector was rejected")
	}
	invalid := UnitValues{CPUWork: 5, ScanBytes: math.NaN()}
	if collector.AcceptVector(invalid) {
		t.Fatal("invalid vector was accepted")
	}
	result := collector.Finalize()
	assertOutcome(t, result, StateInvalid, ReasonInvalidObservation)
	units, ok := result.Units()
	if !ok || units != first {
		t.Fatalf("rejected vector changed accepted values: %+v, %v", units, ok)
	}
	if collector.AcceptVector(UnitValues{CPUWork: 1}) {
		t.Fatal("late vector was accepted")
	}
}

func TestStatementUnitContributors(t *testing.T) {
	weights := Weights{CPUWork: 1}
	newStatement := func() *Statement {
		return NewStatement(Selection{
			Mode:          ModeCalibration,
			Applicable:    true,
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
		})
	}

	t.Run("all contributors complete", func(t *testing.T) {
		statement := newStatement()
		registrar := statement.UnitContributorRegistrar()
		if got := registrar.RequiredUnits(); got != CPUWork.Mask() {
			t.Fatalf("unexpected required units: %v", got)
		}
		first := registrar.RegisterUnitContributor(CPUWork.Mask())
		second := registrar.RegisterUnitContributor(CPUWork.Mask())
		if first == nil || second == nil {
			t.Fatal("contributors were not registered")
		}
		if !first.Complete(UnitValues{CPUWork: 2}) || !second.Complete(UnitValues{CPUWork: 3}) {
			t.Fatal("complete vectors were rejected")
		}
		if first.Unavailable() {
			t.Fatal("duplicate contributor completion was accepted")
		}
		finish, performed := statement.Finish(TerminalSuccess)
		if !performed {
			t.Fatal("statement did not finish")
		}
		assertOutcome(t, finish.Result, StateComplete, ReasonNone)
		total, ok := finish.Result.TotalRU()
		if !ok || total != 5 {
			t.Fatalf("unexpected contributor total: %v, %v", total, ok)
		}
		if registrar.RegisterUnitContributor(CPUWork.Mask()) != nil {
			t.Fatal("registration after seal succeeded")
		}
	})

	t.Run("concurrent contributors are accepted exactly once", func(t *testing.T) {
		statement := newStatement()
		registrar := statement.UnitContributorRegistrar()
		contributors := make([]UnitContributor, 64)
		for i := range contributors {
			contributors[i] = registrar.RegisterUnitContributor(CPUWork.Mask())
			if contributors[i] == nil {
				t.Fatal("contributor was not registered")
			}
		}
		var wait sync.WaitGroup
		wait.Add(len(contributors))
		for _, contributor := range contributors {
			go func() {
				defer wait.Done()
				if !contributor.Complete(UnitValues{CPUWork: 1}) {
					t.Error("concurrent vector was rejected")
				}
			}()
		}
		wait.Wait()
		finish, _ := statement.Finish(TerminalSuccess)
		total, ok := finish.Result.TotalRU()
		if !ok || total != float64(len(contributors)) {
			t.Fatalf("unexpected concurrent total: %v, %v", total, ok)
		}
	})

	t.Run("one incomplete owner makes the unit partial", func(t *testing.T) {
		statement := newStatement()
		registrar := statement.UnitContributorRegistrar()
		complete := registrar.RegisterUnitContributor(CPUWork.Mask())
		incomplete := registrar.RegisterUnitContributor(CPUWork.Mask())
		if !complete.Complete(UnitValues{CPUWork: 2}) || !incomplete.Unavailable() {
			t.Fatal("contributor terminal states were rejected")
		}
		finish, _ := statement.Finish(TerminalSuccess)
		assertOutcome(t, finish.Result, StatePartial, ReasonIncompleteEvidence)
		units, ok := finish.Result.Units()
		if !ok || units[CPUWork] != 2 {
			t.Fatalf("accepted contributor value was lost: %+v, %v", units, ok)
		}
	})

	t.Run("streaming and fixed-vector domains combine without hiding incompleteness", func(t *testing.T) {
		statement := newStatement()
		if !statement.UnitRecorder().Add(CPUWork, 2) || !statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()) {
			t.Fatal("streaming evidence was rejected")
		}
		complete := statement.UnitContributorRegistrar().RegisterUnitContributor(CPUWork.Mask())
		if !complete.Complete(UnitValues{CPUWork: 3}) {
			t.Fatal("fixed-vector evidence was rejected")
		}
		finish, _ := statement.Finish(TerminalSuccess)
		assertOutcome(t, finish.Result, StateComplete, ReasonNone)
		total, ok := finish.Result.TotalRU()
		if !ok || total != 5 {
			t.Fatalf("disjoint producer values were not combined: %v, %v", total, ok)
		}

		statement = newStatement()
		if !statement.UnitRecorder().Add(CPUWork, 2) || !statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()) {
			t.Fatal("streaming evidence was rejected")
		}
		incomplete := statement.UnitContributorRegistrar().RegisterUnitContributor(CPUWork.Mask())
		if !incomplete.Unavailable() {
			t.Fatal("incomplete fixed-vector evidence was rejected")
		}
		finish, _ = statement.Finish(TerminalSuccess)
		assertOutcome(t, finish.Result, StatePartial, ReasonIncompleteEvidence)
	})

	t.Run("unterminated owner fails closed", func(t *testing.T) {
		statement := newStatement()
		if statement.UnitContributorRegistrar().RegisterUnitContributor(CPUWork.Mask()) == nil {
			t.Fatal("contributor was not registered")
		}
		finish, _ := statement.Finish(TerminalSuccess)
		assertOutcome(t, finish.Result, StateUnavailable, ReasonMissingEvidence)
	})

	t.Run("unsupported optional unit is isolated", func(t *testing.T) {
		statement := newStatement()
		registrar := statement.UnitContributorRegistrar()
		cpu := registrar.RegisterUnitContributor(CPUWork.Mask())
		hash := registrar.RegisterUnitContributor(HashStateRows.Mask())
		if !cpu.Complete(UnitValues{CPUWork: 4}) || !hash.Unsupported() {
			t.Fatal("contributor terminal state was rejected")
		}
		finish, _ := statement.Finish(TerminalSuccess)
		assertOutcome(t, finish.Result, StateComplete, ReasonNone)
		coverage, ok := finish.Result.Coverage()
		if !ok || coverage.UnsupportedUnits != HashStateRows.Mask() {
			t.Fatalf("optional unsupported evidence was lost: %+v, %v", coverage, ok)
		}
	})

	t.Run("usable and unsupported owners retain partial evidence", func(t *testing.T) {
		for _, test := range []struct {
			name      string
			terminate func(UnitContributor) bool
			wantValue float64
		}{
			{
				name: "complete and unsupported",
				terminate: func(contributor UnitContributor) bool {
					return contributor.Complete(UnitValues{CPUWork: 3})
				},
				wantValue: 3,
			},
			{
				name: "partial and unsupported",
				terminate: func(contributor UnitContributor) bool {
					return contributor.Partial()
				},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				statement := newStatement()
				registrar := statement.UnitContributorRegistrar()
				usable := registrar.RegisterUnitContributor(CPUWork.Mask())
				unsupported := registrar.RegisterUnitContributor(CPUWork.Mask())
				if !test.terminate(usable) || !unsupported.Unsupported() {
					t.Fatal("contributor terminal state was rejected")
				}
				finish, _ := statement.Finish(TerminalSuccess)
				assertOutcome(t, finish.Result, StatePartial, ReasonUnsupported)
				units, ok := finish.Result.Units()
				if !ok || units[CPUWork] != test.wantValue {
					t.Fatalf("usable evidence was lost: %+v, %v", units, ok)
				}
				coverage, ok := finish.Result.Coverage()
				if !ok || coverage.PartialUnits != CPUWork.Mask() || coverage.UnsupportedUnits != CPUWork.Mask() {
					t.Fatalf("partial unsupported evidence was not retained: %+v, %v", coverage, ok)
				}
			})
		}
	})

	t.Run("vector cannot escape its lease mask", func(t *testing.T) {
		statement := newStatement()
		contributor := statement.UnitContributorRegistrar().RegisterUnitContributor(CPUWork.Mask())
		if contributor.Complete(UnitValues{CPUWork: 1, ScanBytes: 1}) {
			t.Fatal("out-of-mask vector was accepted")
		}
		if contributor.Complete(UnitValues{CPUWork: 1}) {
			t.Fatal("rejected terminal completion was retried")
		}
		finish, _ := statement.Finish(TerminalSuccess)
		assertOutcome(t, finish.Result, StatePartial, ReasonIncompleteEvidence)
	})
}

func TestCollectorCoverageOutcomes(t *testing.T) {
	weights := Weights{CPUWork: 1, ScanBytes: 1}
	required := CPUWork.Mask() | ScanBytes.Mask()

	t.Run("missing", func(t *testing.T) {
		result := NewCollector(Config{RequiredUnits: required, Weights: &weights}).Finalize()
		assertOutcome(t, result, StateUnavailable, ReasonMissingEvidence)
	})

	t.Run("partial", func(t *testing.T) {
		collector := NewCollector(Config{RequiredUnits: required, Weights: &weights})
		collector.MarkPresent(CPUWork.Mask())
		result := collector.Finalize()
		assertOutcome(t, result, StatePartial, ReasonIncompleteEvidence)
	})

	t.Run("unsupported", func(t *testing.T) {
		collector := NewCollector(Config{RequiredUnits: required, Weights: &weights})
		collector.MarkUnsupported(required)
		result := collector.Finalize()
		assertOutcome(t, result, StateUnavailable, ReasonUnsupported)
	})

	t.Run("explicitly unavailable", func(t *testing.T) {
		collector := NewCollector(Config{RequiredUnits: required, Weights: &weights, RetainDetails: true})
		collector.MarkUnavailable(required)
		result := collector.Finalize()
		assertOutcome(t, result, StateUnavailable, ReasonMissingEvidence)
		coverage, ok := result.Coverage()
		if !ok || coverage.UnavailableUnits != required || coverage.UnsupportedUnits != 0 {
			t.Fatalf("unavailable evidence was not retained: %+v, %v", coverage, ok)
		}
	})

	t.Run("partially unsupported", func(t *testing.T) {
		collector := NewCollector(Config{RequiredUnits: required, Weights: &weights, RetainDetails: true})
		collector.MarkPresent(CPUWork.Mask())
		collector.MarkUnsupported(ScanBytes.Mask())
		result := collector.Finalize()
		assertOutcome(t, result, StatePartial, ReasonUnsupported)
		coverage, ok := result.Coverage()
		if !ok || coverage.UnavailableUnits != ScanBytes.Mask() || coverage.UnsupportedUnits != ScanBytes.Mask() {
			t.Fatalf("unavailable units were not retained: %+v, %v", coverage, ok)
		}
	})

	t.Run("weights unavailable", func(t *testing.T) {
		collector := NewCollector(Config{RequiredUnits: required, RetainDetails: true})
		collector.MarkPresent(required)
		result := collector.Finalize()
		assertOutcome(t, result, StateUnavailable, ReasonWeightsUnavailable)
		if _, ok := result.Units(); !ok {
			t.Fatal("calibration details were not retained")
		}
	})

	t.Run("optional evidence does not change missing required outcome", func(t *testing.T) {
		optionalWeights := Weights{CPUWork: 1}
		collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &optionalWeights})
		collector.Add(NetworkBytes, 1)
		collector.MarkUnsupported(ScanBytes.Mask())
		assertOutcome(t, collector.Finalize(), StateUnavailable, ReasonMissingEvidence)
	})

	t.Run("added value does not imply evidence", func(t *testing.T) {
		cpuWeights := Weights{CPUWork: 1}
		collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &cpuWeights})
		collector.Add(CPUWork, 3)
		assertOutcome(t, collector.Finalize(), StateUnavailable, ReasonMissingEvidence)
	})

	t.Run("empty evidence masks are no-ops", func(t *testing.T) {
		cpuWeights := Weights{CPUWork: 1}
		collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &cpuWeights})
		if !collector.MarkPresent(0) || !collector.MarkPartial(0) ||
			!collector.MarkUnavailable(0) || !collector.MarkUnsupported(0) {
			t.Fatal("an empty evidence mask was rejected")
		}
		assertOutcome(t, collector.Finalize(), StateUnavailable, ReasonMissingEvidence)
	})

	t.Run("required mask configuration", func(t *testing.T) {
		assertOutcome(t, NewCollector(Config{}).Finalize(), StateInvalid, ReasonInvalidConfiguration)

		weightsWithNonApplicableUnit := Weights{CPUWork: 1, ScanBytes: 100}
		collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weightsWithNonApplicableUnit})
		collector.Add(CPUWork, 2)
		collector.Add(ScanBytes, 10)
		collector.MarkPresent(CPUWork.Mask())
		result := collector.Finalize()
		assertOutcome(t, result, StateComplete, ReasonNone)
		if total, ok := result.TotalRU(); !ok || total != 2 {
			t.Fatalf("non-applicable weighted unit changed total: %v, %v", total, ok)
		}

		weightsWithInvalidNonApplicableUnit := Weights{CPUWork: 1, ScanBytes: math.NaN()}
		collector = NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weightsWithInvalidNonApplicableUnit})
		collector.Add(CPUWork, 2)
		collector.MarkPresent(CPUWork.Mask())
		result = collector.Finalize()
		assertOutcome(t, result, StateComplete, ReasonNone)
		if total, ok := result.TotalRU(); !ok || total != 2 {
			t.Fatalf("invalid non-applicable weight changed total: %v, %v", total, ok)
		}

		zeroWeights := Weights{}
		collector = NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &zeroWeights})
		collector.MarkPresent(CPUWork.Mask())
		assertOutcome(t, collector.Finalize(), StateComplete, ReasonNone)
	})

	t.Run("optional diagnostics do not change authoritative result", func(t *testing.T) {
		cpuWeights := Weights{CPUWork: 2, NetworkBytes: 100}
		collector := NewCollector(Config{
			RequiredUnits: CPUWork.Mask(),
			Weights:       &cpuWeights,
			RetainDetails: true,
		})
		collector.Add(CPUWork, 3)
		collector.Add(NetworkBytes, 100)
		collector.MarkPresent(CPUWork.Mask())
		if collector.Add(NetworkBytes, -1) {
			t.Fatal("invalid optional observation was accepted")
		}
		collector.MarkPartial(HashStateRows.Mask())
		collector.MarkUnsupported(ScanBytes.Mask())

		result := collector.Finalize()
		assertOutcome(t, result, StateComplete, ReasonNone)
		if total, ok := result.TotalRU(); !ok || total != 6 {
			t.Fatalf("optional units changed total: %v, %v", total, ok)
		}
		coverage, ok := result.Coverage()
		if !ok || coverage.InvalidUnits != NetworkBytes.Mask() ||
			coverage.PartialUnits != HashStateRows.Mask() ||
			coverage.UnsupportedUnits != ScanBytes.Mask() {
			t.Fatalf("optional diagnostic causes were not retained: %+v, %v", coverage, ok)
		}
	})
}

func TestCollectorAuthoritativeZero(t *testing.T) {
	weights := Weights{1, 2, 3, 4, 5, 6, 7, 8}
	collector := NewCollector(Config{RequiredUnits: AllUnits, Weights: &weights})
	if !collector.MarkPresent(AllUnits) {
		t.Fatal("zero evidence was not accepted")
	}
	result := collector.Finalize()
	if !result.HasTotal() {
		t.Fatal("authoritative zero has no total")
	}
	total, ok := result.TotalRU()
	if !ok || total != 0 {
		t.Fatalf("unexpected authoritative zero: %v, %v", total, ok)
	}
	assertOutcome(t, result, StateComplete, ReasonNone)
	if _, ok := result.Units(); ok {
		t.Fatal("ResultOnly retained units")
	}
	if _, ok := result.Coverage(); ok {
		t.Fatal("ResultOnly retained coverage")
	}
	if result.details != nil ||
		collector.values != (UnitValues{}) || collector.coverage != (Coverage{}) {
		t.Fatal("ResultOnly retained raw details after finalization")
	}
}

func TestCollectorInvalidArithmetic(t *testing.T) {
	weights := Weights{CPUWork: 1}
	tests := []struct {
		name   string
		mutate func(*Collector) bool
		reason Reason
	}{
		{
			name: "invalid kind",
			mutate: func(collector *Collector) bool {
				return collector.Add(UnitKind(UnitCount), 1)
			},
			reason: ReasonInvalidObservation,
		},
		{
			name: "negative",
			mutate: func(collector *Collector) bool {
				return collector.Add(CPUWork, -1)
			},
			reason: ReasonInvalidObservation,
		},
		{
			name: "nan",
			mutate: func(collector *Collector) bool {
				return collector.Add(CPUWork, math.NaN())
			},
			reason: ReasonInvalidObservation,
		},
		{
			name: "infinity",
			mutate: func(collector *Collector) bool {
				return collector.Add(CPUWork, math.Inf(1))
			},
			reason: ReasonInvalidObservation,
		},
		{
			name: "addition overflow",
			mutate: func(collector *Collector) bool {
				if !collector.Add(CPUWork, math.MaxFloat64) {
					t.Fatal("initial maximum value was rejected")
				}
				return collector.Add(CPUWork, math.MaxFloat64)
			},
			reason: ReasonArithmeticOverflow,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weights})
			collector.MarkPresent(CPUWork.Mask())
			if tt.mutate(collector) {
				t.Fatal("invalid observation was accepted")
			}
			assertOutcome(t, collector.Finalize(), StateInvalid, tt.reason)
		})
	}

	t.Run("invalid weight", func(t *testing.T) {
		invalidWeights := Weights{CPUWork: math.NaN()}
		collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &invalidWeights})
		collector.MarkPresent(CPUWork.Mask())
		assertOutcome(t, collector.Finalize(), StateInvalid, ReasonInvalidConfiguration)
	})

	t.Run("invalid unit masks", func(t *testing.T) {
		invalidMask := UnitKind(UnitCount).Mask()
		collector := NewCollector(Config{RequiredUnits: invalidMask})
		assertOutcome(t, collector.Finalize(), StateInvalid, ReasonInvalidConfiguration)

		collector = NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weights})
		if collector.MarkPresent(invalidMask) {
			t.Fatal("invalid unit mask was accepted")
		}
		assertOutcome(t, collector.Finalize(), StateInvalid, ReasonInvalidConfiguration)
	})

	t.Run("multiplication overflow", func(t *testing.T) {
		overflowWeights := Weights{CPUWork: 3}
		collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &overflowWeights})
		collector.Add(CPUWork, math.MaxFloat64/2)
		collector.MarkPresent(CPUWork.Mask())
		assertOutcome(t, collector.Finalize(), StateInvalid, ReasonArithmeticOverflow)
	})

	t.Run("total addition overflow", func(t *testing.T) {
		overflowWeights := Weights{CPUWork: 0.75, ScanBytes: 0.75}
		required := CPUWork.Mask() | ScanBytes.Mask()
		collector := NewCollector(Config{
			RequiredUnits: required,
			Weights:       &overflowWeights,
			RetainDetails: true,
		})
		if !collector.Add(CPUWork, math.MaxFloat64) || !collector.Add(ScanBytes, math.MaxFloat64) {
			t.Fatal("finite unit values were rejected before finalization")
		}
		cpuWeighted, cpuOK := checkedMultiply(math.MaxFloat64, overflowWeights[CPUWork])
		scanWeighted, scanOK := checkedMultiply(math.MaxFloat64, overflowWeights[ScanBytes])
		if !cpuOK || !scanOK {
			t.Fatal("a finite weighted product overflowed before final addition")
		}
		if _, ok := checkedAdd(cpuWeighted, scanWeighted); ok {
			t.Fatal("test inputs do not overflow the final total addition")
		}
		collector.MarkPresent(required)
		result := collector.Finalize()
		assertOutcome(t, result, StateInvalid, ReasonArithmeticOverflow)
		coverage, ok := result.Coverage()
		if !ok || coverage.InvalidUnits != ScanBytes.Mask() {
			t.Fatalf("overflowing total unit was not retained: %+v, %v", coverage, ok)
		}
	})
}

func TestCollectorFinalization(t *testing.T) {
	weights := Weights{CPUWork: 2}
	collector := NewCollector(Config{
		RequiredUnits: CPUWork.Mask(),
		Weights:       &weights,
		RetainDetails: true,
	})
	collector.Add(CPUWork, 3)
	collector.MarkPresent(CPUWork.Mask())
	first := collector.Finalize()
	second := collector.Finalize()
	if first != second {
		t.Fatalf("finalize was not idempotent: %+v != %+v", first, second)
	}
	if collector.Add(CPUWork, 4) || collector.MarkPresent(ScanBytes.Mask()) ||
		collector.MarkPartial(CPUWork.Mask()) ||
		collector.MarkUnsupported(CPUWork.Mask()) {
		t.Fatal("a late mutation was accepted")
	}
	if result := collector.Finalize(); result != first {
		t.Fatal("late mutations changed the result")
	}

	units, ok := first.Units()
	if !ok {
		t.Fatal("units were not retained")
	}
	units[CPUWork] = 100
	if units[CPUWork] != 100 {
		t.Fatal("caller-owned unit copy was not mutable")
	}
	unitsAgain, _ := first.Units()
	if unitsAgain[CPUWork] != 3 {
		t.Fatal("result exposed mutable unit storage")
	}
	coverage, ok := first.Coverage()
	if !ok {
		t.Fatal("coverage was not retained")
	}
	coverage.PresentUnits = 0
	if coverage.PresentUnits != 0 {
		t.Fatal("caller-owned coverage copy was not mutable")
	}
	coverageAgain, _ := first.Coverage()
	if coverageAgain.PresentUnits != CPUWork.Mask() {
		t.Fatal("result exposed mutable coverage storage")
	}

	zeroResult := Result{}
	assertOutcome(t, zeroResult, StateUnavailable, ReasonMissingEvidence)
	assertOutcome(t, (*Collector)(nil).Finalize(), StateUnavailable, ReasonMissingEvidence)

	t.Run("concurrent add and finalize", func(t *testing.T) {
		const (
			runs       = 100
			adders     = 32
			finalizers = 4
		)
		for range runs {
			collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weights})
			collector.MarkPresent(CPUWork.Mask())
			start := make(chan struct{})
			results := make(chan Result, finalizers)
			var accepted atomic.Int64
			var wg sync.WaitGroup
			wg.Add(adders + finalizers)
			for range adders {
				go func() {
					defer wg.Done()
					<-start
					if collector.Add(CPUWork, 1) {
						accepted.Add(1)
					}
				}()
			}
			for range finalizers {
				go func() {
					defer wg.Done()
					<-start
					results <- collector.Finalize()
				}()
			}
			close(start)
			wg.Wait()
			close(results)

			var frozen Result
			for result := range results {
				if !frozen.initialized {
					frozen = result
				} else if result != frozen {
					t.Fatalf("concurrent finalizers disagreed: %+v != %+v", result, frozen)
				}
			}
			total, ok := frozen.TotalRU()
			if !ok || total != float64(accepted.Load())*weights[CPUWork] {
				t.Fatalf("accepted deltas and frozen total differ: %v, %v, accepted %d", total, ok, accepted.Load())
			}
		}
	})

	t.Run("concurrent coverage and finalize", func(t *testing.T) {
		for range 100 {
			collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weights})
			collector.MarkPresent(CPUWork.Mask())
			start := make(chan struct{})
			accepted := make(chan bool, 1)
			go func() {
				<-start
				accepted <- collector.MarkPartial(CPUWork.Mask())
			}()
			close(start)
			result := collector.Finalize()
			if <-accepted {
				assertOutcome(t, result, StatePartial, ReasonIncompleteEvidence)
			} else {
				assertOutcome(t, result, StateComplete, ReasonNone)
			}
		}
	})
}

func TestCollectorConcurrentAdd(t *testing.T) {
	const (
		workers    = 16
		addsPerRun = 1000
	)
	weights := Weights{JoinOutputRows: 1}
	collector := NewCollector(Config{RequiredUnits: JoinOutputRows.Mask(), Weights: &weights})

	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for range addsPerRun {
				if !collector.Add(JoinOutputRows, 1) {
					t.Error("concurrent observation was rejected")
					return
				}
			}
		}()
	}
	wg.Wait()
	collector.MarkPresent(JoinOutputRows.Mask())
	result := collector.Finalize()
	total, ok := result.TotalRU()
	if !ok || total != workers*addsPerRun {
		t.Fatalf("unexpected concurrent total: %v, %v", total, ok)
	}

	allocCollector := NewCollector(Config{RequiredUnits: JoinOutputRows.Mask(), Weights: &weights})
	allocations := testing.AllocsPerRun(1000, func() {
		if !allocCollector.Add(JoinOutputRows, 1) {
			t.Fatal("allocation test observation was rejected")
		}
	})
	if allocations != 0 {
		t.Fatalf("Add allocated: %v allocations/run", allocations)
	}
}

func assertOutcome(t *testing.T, result Result, state CollectionState, reason Reason) {
	t.Helper()
	if result.Outcome() != (Outcome{State: state, Reason: reason}) {
		t.Fatalf("unexpected outcome: got %+v, want {%v %v}", result.Outcome(), state, reason)
	}
	if state == StateComplete && !result.HasTotal() {
		t.Fatal("complete result has no total")
	}
	if state != StateComplete && result.HasTotal() {
		t.Fatal("non-complete result has a total")
	}
}
