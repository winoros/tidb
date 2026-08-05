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
	benchmarkAccepted  bool
	benchmarkResult    Result
	benchmarkFinish    FinishResult
	benchmarkStatement *Statement
	benchmarkUnits     UnitValues
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
