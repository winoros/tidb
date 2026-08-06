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

import "testing"

var (
	benchmarkAccepted   bool
	benchmarkDiagnostic Diagnostic
	benchmarkResult     Result
	benchmarkFinish     FinishResult
	benchmarkStatement  *Statement
	benchmarkUnits      UnitValues
)

func BenchmarkCollectorAddUnit(b *testing.B) {
	weights := Weights{CPUWork: 1}
	collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weights})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkAccepted = collector.Add(CPUWork, 1)
	}
}

func BenchmarkCollectorAddUnitParallel(b *testing.B) {
	weights := Weights{CPUWork: 1}
	collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weights})
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = collector.Add(CPUWork, 1)
		}
	})
}

func BenchmarkCollectorAcceptVector(b *testing.B) {
	weights := Weights{CPUWork: 1, ScanBytes: 1, NetworkBytes: 1}
	collector := NewCollector(Config{
		RequiredUnits: CPUWork.Mask() | ScanBytes.Mask() | NetworkBytes.Mask(),
		Weights:       &weights,
	})
	values := UnitValues{CPUWork: 1, ScanBytes: 2, NetworkBytes: 3}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkAccepted = collector.AcceptVector(values)
	}
}

func BenchmarkStatementUnitContributorLifecycle(b *testing.B) {
	weights := Weights{CPUWork: 1}
	values := UnitValues{CPUWork: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		statement := NewStatement(Selection{
			Mode:          ModeResultOnly,
			Applicable:    true,
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
		})
		contributor := statement.UnitContributorRegistrar().RegisterUnitContributor(CPUWork.Mask())
		benchmarkAccepted = contributor.Complete(values)
		benchmarkFinish, _ = statement.Finish(TerminalSuccess)
	}
}

func BenchmarkStatementUnitContributorParallel(b *testing.B) {
	weights := Weights{CPUWork: 1}
	statement := NewStatement(Selection{
		Mode:          ModeResultOnly,
		Applicable:    true,
		RequiredUnits: CPUWork.Mask(),
		Weights:       &weights,
	})
	registrar := statement.UnitContributorRegistrar()
	values := UnitValues{CPUWork: 1}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			contributor := registrar.RegisterUnitContributor(CPUWork.Mask())
			_ = contributor.Complete(values)
		}
	})
	b.StopTimer()
	benchmarkFinish, _ = statement.Finish(TerminalSuccess)
}

func BenchmarkStatementLocalCPUWorkOwner(b *testing.B) {
	// This isolates the core owner and opaque producer tokens. Each address-taken
	// token may escape once in this fixture; BenchmarkStatementRUCPUWorkStatementSetup
	// in executor/internal/exec measures the complete StatementContext + hook
	// construction and lifecycle path.
	weights := Weights{CPUWork: 1}
	for _, test := range []struct {
		name      string
		producers int
	}{
		{name: "zero_producers", producers: 0},
		{name: "one_producer", producers: 1},
		{name: "32_producers", producers: 32},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				statement := NewStatement(Selection{
					Mode:          ModeResultOnly,
					Applicable:    true,
					RequiredUnits: CPUWork.Mask(),
					Weights:       &weights,
				})
				registrar := statement.LocalCPUWorkRegistrar()
				if !registrar.Activate() {
					b.Fatal("failed to activate local CPUWork owner")
				}
				for range test.producers {
					var producer LocalCPUWorkProducer
					if !registrar.RegisterLocalCPUWorkProducer(&producer) ||
						!producer.BeginGeneration() || !producer.CompleteGeneration() {
						b.Fatal("failed to complete local CPUWork producer")
					}
				}
				if !registrar.CompleteLocalCPUWorkInventory() {
					b.Fatal("failed to complete local CPUWork inventory")
				}
				var first bool
				benchmarkFinish, first = statement.Finish(TerminalSuccess)
				if !first || benchmarkFinish.Result.Outcome().State != StateComplete {
					b.Fatal("statement did not complete local CPUWork lifecycle")
				}
			}
		})
	}
}

func BenchmarkCollectorLifecycleFinalize(b *testing.B) {
	weights := Weights{CPUWork: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weights})
		collector.Add(CPUWork, 1)
		collector.MarkPresent(CPUWork.Mask())
		benchmarkResult = collector.Finalize()
	}
}

func BenchmarkCollectorIdempotentFinalize(b *testing.B) {
	weights := Weights{CPUWork: 1}
	collector := NewCollector(Config{RequiredUnits: CPUWork.Mask(), Weights: &weights})
	collector.Add(CPUWork, 1)
	collector.MarkPresent(CPUWork.Mask())
	collector.Finalize()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkResult = collector.Finalize()
	}
}

func BenchmarkCollectorCalibrationFinalize(b *testing.B) {
	weights := Weights{CPUWork: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		collector := NewCollector(Config{
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
			RetainDetails: true,
		})
		collector.Add(CPUWork, 1)
		collector.MarkPresent(CPUWork.Mask())
		benchmarkResult = collector.Finalize()
		benchmarkUnits, _ = benchmarkResult.Units()
	}
}

func BenchmarkStatementOffSelection(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkStatement = NewStatement(Selection{Mode: ModeOff})
	}
}

func BenchmarkStatementResultOnlyOwnerLifecycle(b *testing.B) {
	weights := Weights{CPUWork: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		statement := NewStatement(Selection{
			Mode:          ModeResultOnly,
			Applicable:    true,
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
		})
		statement.UnitRecorder().Add(CPUWork, 1)
		statement.EvidenceRecorder().MarkPresent(CPUWork.Mask())
		benchmarkFinish, _ = statement.Finish(TerminalSuccess)
	}
}

func BenchmarkStatementCalibrationDiagnosticLifecycle(b *testing.B) {
	weights := Weights{CPUWork: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		statement := NewStatement(Selection{
			Mode:           ModeCalibration,
			Applicable:     true,
			RequiredUnits:  CPUWork.Mask(),
			CollectedUnits: CPUWork.Mask() | NetworkBytes.Mask(),
			Weights:        &weights,
		})
		statement.UnitRecorder().Add(CPUWork, 1)
		statement.UnitRecorder().Add(NetworkBytes, 2)
		statement.EvidenceRecorder().MarkPresent(CPUWork.Mask())
		statement.EvidenceRecorder().MarkPresent(NetworkBytes.Mask())
		finish, _ := statement.Finish(TerminalSuccess)
		benchmarkDiagnostic, _ = finish.Diagnostic()
	}
}

func BenchmarkStatementDiagnosticProjection(b *testing.B) {
	weights := Weights{CPUWork: 1}
	statement := NewStatement(Selection{
		Mode:          ModeCalibration,
		Applicable:    true,
		RequiredUnits: CPUWork.Mask(),
		Weights:       &weights,
	})
	statement.UnitRecorder().Add(CPUWork, 1)
	statement.EvidenceRecorder().MarkPresent(CPUWork.Mask())
	finish, _ := statement.Finish(TerminalSuccess)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchmarkDiagnostic, _ = finish.Diagnostic()
	}
}
