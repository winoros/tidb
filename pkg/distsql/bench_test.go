// Copyright 2021 PingCAP, Inc.
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

package distsql

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

var statementRUBenchmarkSink uint64
var rangeScanBenchmarkSink uint64

func BenchmarkCloudSummaryObserveResponseMicro(b *testing.B) {
	for _, responseCount := range []int{1, 16, 128} {
		for _, summaryCount := range []int{1, 5, 50, 500} {
			name := fmt.Sprintf("Responses=%d/Summaries=%d", responseCount, summaryCount)
			b.Run(name, func(b *testing.B) {
				plan := cloudSummaryPlan{steps: make([]cloudSummaryStep, summaryCount)}
				rows := make([]uint64, summaryCount)
				for i := range summaryCount {
					rows[i] = 1
					if i > 0 {
						plan.steps[i] = cloudSummaryStep{child: i - 1, multiplier: 1}
					}
				}
				response := cloudSummaryTestResponse(rows...)
				stats := completeSingleCopResponseTestStats()

				owner := &cloudSummaryOwner{plan: plan}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					owner.cpuWork = 0
					owner.responses = 0
					owner.failed = false
					owner.done = false
					for range responseCount {
						owner.observeResponse(response, stats)
					}
					statementRUBenchmarkSink = owner.cpuWork
				}
			})
		}
	}
}

func BenchmarkCloudSummaryPrepareOwnerLifecycle(b *testing.B) {
	dagData, err := cloudSummaryTestDAG().Marshal()
	if err != nil {
		b.Fatal(err)
	}
	dctx := newCloudSummaryTestDistSQLContext()
	scanWeights := statementru.Weights{statementru.ScanBytes: 1}

	b.Run("Off", func(b *testing.B) {
		dctx.StatementRUUnitContributors = nil
		b.ReportAllocs()
		for range b.N {
			request := &kv.Request{Tp: kv.ReqTypeDAG, Data: dagData, StoreType: kv.TiKV}
			if owner := prepareCloudSummaryOwner(dctx, request); owner != nil {
				b.Fatal("Off mode created a cloud-summary owner")
			}
		}
	})

	b.Run("configured_uncollected", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			statement := statementru.NewStatement(statementru.Selection{
				Mode:          statementru.ModeResultOnly,
				Applicable:    true,
				RequiredUnits: statementru.ScanBytes.Mask(),
				Weights:       &scanWeights,
			})
			dctx.StatementRUUnitContributors = statement.UnitContributorRegistrar()
			request := &kv.Request{Tp: kv.ReqTypeDAG, Data: dagData, StoreType: kv.TiKV}
			if owner := prepareCloudSummaryOwner(dctx, request); owner != nil {
				b.Fatal("uncollected CPU work created a cloud-summary owner")
			}
			statement.Finish(statementru.TerminalSuccess)
		}
	})

	b.Run("optional_collected", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			statement := statementru.NewStatement(statementru.Selection{
				Mode:           statementru.ModeResultOnly,
				Applicable:     true,
				RequiredUnits:  statementru.ScanBytes.Mask(),
				CollectedUnits: statementru.ScanBytes.Mask() | statementru.CPUWork.Mask(),
				Weights:        &scanWeights,
			})
			dctx.StatementRUUnitContributors = statement.UnitContributorRegistrar()
			request := &kv.Request{Tp: kv.ReqTypeDAG, Data: dagData, StoreType: kv.TiKV}
			owner := prepareCloudSummaryOwner(dctx, request)
			if owner == nil {
				b.Fatal("optional collected CPU work did not create a cloud-summary owner")
			}
			owner.abort()
			statement.Finish(statementru.TerminalSuccess)
		}
	})

	b.Run("Stage1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			statement := newCloudSummaryTestStatement()
			dctx.StatementRUUnitContributors = statement.UnitContributorRegistrar()
			request := &kv.Request{Tp: kv.ReqTypeDAG, Data: dagData, StoreType: kv.TiKV}
			owner := prepareCloudSummaryOwner(dctx, request)
			if owner == nil {
				b.Fatal("Stage1 did not create a cloud-summary owner")
			}
			owner.abort()
			statement.Finish(statementru.TerminalSuccess)
		}
	})
}

func BenchmarkRangeScanByteEstimateOwnerLifecycle(b *testing.B) {
	weights := statementru.Weights{statementru.ScanBytes: 1}
	for _, responseCount := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("Responses=%d", responseCount), func(b *testing.B) {
			stats := completeScanDetailV2TestStats(10, 2, 8)
			want := float64(40 * responseCount)
			b.ReportAllocs()
			for range b.N {
				statement := statementru.NewStatement(statementru.Selection{
					Mode:          statementru.ModeResultOnly,
					Applicable:    true,
					RequiredUnits: statementru.ScanBytes.Mask(),
					Weights:       &weights,
				})
				owner := &rangeScanByteEstimateOwner{
					contributor: statement.UnitContributorRegistrar().RegisterUnitContributor(statementru.ScanBytes.Mask()),
				}
				for range responseCount {
					owner.observeResponse(stats)
				}
				owner.completeEOF()
				finish, _ := statement.Finish(statementru.TerminalSuccess)
				total, ok := finish.Result.TotalRU()
				if !ok || total != want {
					b.Fatalf("unexpected scan total: %v, %v", total, ok)
				}
				rangeScanBenchmarkSink = uint64(total)
			}
		})
	}
}

func BenchmarkRangeScanByteEstimateObserveResponseMicro(b *testing.B) {
	stats := completeScanDetailV2TestStats(10, 2, 8)
	for _, responseCount := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("Responses=%d", responseCount), func(b *testing.B) {
			owner := &rangeScanByteEstimateOwner{}
			b.ReportAllocs()
			for range b.N {
				owner.totalKeys = 0
				owner.processedKeys = 0
				owner.processedKeysSize = 0
				owner.responses = 0
				owner.sawUsableDetail = false
				owner.incomplete = false
				owner.done = false
				for range responseCount {
					owner.observeResponse(stats)
				}
				rangeScanBenchmarkSink = owner.processedKeysSize
			}
		})
	}
}

func BenchmarkCloudSummarySelectToEOF(b *testing.B) {
	dagData, err := cloudSummaryTestDAG().Marshal()
	if err != nil {
		b.Fatal(err)
	}
	responseData, err := cloudSummaryTestResponse(10, 5, 4, 3).Marshal()
	if err != nil {
		b.Fatal(err)
	}
	stats := completeScanDetailV2TestStats(10, 2, 8)
	subset := &cloudSummaryTestSubset{data: responseData, stats: stats}
	dctx := newCloudSummaryTestDistSQLContext()
	client := &cloudSummaryTestClient{}
	dctx.Client = client
	chk := chunk.NewChunkWithCapacity(nil, 0)

	tests := []struct {
		name  string
		units statementru.UnitMask
		want  float64
	}{
		{name: "Off"},
		{name: "CPU", units: statementru.CPUWork.Mask(), want: 39},
		{name: "CPUAndScan", units: statementru.CPUWork.Mask() | statementru.ScanBytes.Mask(), want: 79},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				dctx.RuntimeStatsColl = execdetails.NewRuntimeStatsColl(dctx.RuntimeStatsColl)
				var statement *statementru.Statement
				if test.units != 0 {
					weights := statementru.Weights{
						statementru.CPUWork:   1,
						statementru.ScanBytes: 1,
					}
					statement = statementru.NewStatement(statementru.Selection{
						Mode:          statementru.ModeCalibration,
						Applicable:    true,
						RequiredUnits: test.units,
						Weights:       &weights,
					})
					dctx.StatementRUUnitContributors = statement.UnitContributorRegistrar()
				} else {
					dctx.StatementRUUnitContributors = nil
				}
				client.response = &cloudSummaryTestKVResponse{subsets: []kv.ResultSubset{subset}}
				request := &kv.Request{Tp: kv.ReqTypeDAG, Data: dagData, StoreType: kv.TiKV}
				result, err := SelectWithRuntimeStats(
					context.Background(), dctx, request, nil, []int{1, 2, 3, 4}, 4,
				)
				if err != nil {
					b.Fatal(err)
				}
				if err := result.Next(context.Background(), chk); err != nil {
					b.Fatal(err)
				}
				if err := result.Close(); err != nil {
					b.Fatal(err)
				}
				if statement != nil {
					finish, _ := statement.Finish(statementru.TerminalSuccess)
					total, ok := finish.Result.TotalRU()
					if !ok || total != test.want {
						b.Fatalf("unexpected statement RU total: %v, %v", total, ok)
					}
					statementRUBenchmarkSink = uint64(total)
				}
			}
			b.StopTimer()
			if !dctx.RuntimeStatsColl.ExistsCopStats(4) {
				b.Fatal("matched benchmark bypassed normal cop runtime-stat recording")
			}
		})
	}
}

func BenchmarkSelectResponseChunk_BigResponse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sctx := newMockSessionContext()
		sctx.GetSessionVars().InitChunkSize = 32
		sctx.GetSessionVars().MaxChunkSize = 1024
		selectResult, colTypes := createSelectNormalByBenchmarkTest(4000, 20000, sctx)
		chk := chunk.NewChunkWithCapacity(colTypes, 1024)
		b.StartTimer()
		for {
			err := selectResult.Next(context.TODO(), chk)
			if err != nil {
				panic(err)
			}
			if chk.NumRows() == 0 {
				break
			}
			chk.Reset()
		}
	}
}

func BenchmarkSelectResponseChunk_SmallResponse(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sctx := newMockSessionContext()
		sctx.GetSessionVars().InitChunkSize = 32
		sctx.GetSessionVars().MaxChunkSize = 1024
		selectResult, colTypes := createSelectNormalByBenchmarkTest(32, 3200, sctx)
		chk := chunk.NewChunkWithCapacity(colTypes, 1024)
		b.StartTimer()
		for {
			err := selectResult.Next(context.TODO(), chk)
			if err != nil {
				panic(err)
			}
			if chk.NumRows() == 0 {
				break
			}
			chk.Reset()
		}
	}
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkSelectResponseChunk_BigResponse,
		BenchmarkSelectResponseChunk_SmallResponse,
	)
}
