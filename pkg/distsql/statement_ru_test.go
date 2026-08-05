// Copyright 2026 PingCAP, Inc.
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
	"errors"
	"reflect"
	"testing"
	"time"

	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestFreezeCloudSummaryPlan(t *testing.T) {
	dag := cloudSummaryTestDAG()
	plan, status := freezeCloudSummaryPlan(dag)
	require.Equal(t, cloudSummaryPlanSupported, status)
	require.False(t, plan.hasHashAgg)
	require.Equal(t, []cloudSummaryStep{
		{},
		{child: 0, multiplier: 2},
		{child: 1, multiplier: 3},
		{child: 2, multiplier: 1},
	}, plan.steps)

	t.Run("RPN expression counts are frozen", func(t *testing.T) {
		rpn := true
		dag.IsRpnExpr = &rpn
		dag.Executors[1].Selection.RpnConditions = cloudSummaryTestRPNExprs(4)
		dag.Executors[2].Projection.RpnExprs = cloudSummaryTestRPNExprs(5)
		plan, status := freezeCloudSummaryPlan(dag)
		require.Equal(t, cloudSummaryPlanSupported, status)
		require.Equal(t, uint64(4), plan.steps[1].multiplier)
		require.Equal(t, uint64(5), plan.steps[2].multiplier)
	})

	t.Run("HashAgg CPU is linear but state is capability-gated", func(t *testing.T) {
		dag := cloudSummaryTestDAG()
		dag.Executors = dag.Executors[:2]
		dag.Executors[1] = &tipb.Executor{
			Tp: tipb.ExecType_TypeAggregation,
			Aggregation: &tipb.Aggregation{
				GroupBy: cloudSummaryTestExprs(2),
				AggFunc: cloudSummaryTestExprs(3),
			},
		}
		plan, status := freezeCloudSummaryPlan(dag)
		require.Equal(t, cloudSummaryPlanSupported, status)
		require.True(t, plan.hasHashAgg)
		require.Equal(t, uint64(5), plan.steps[1].multiplier)
	})

	t.Run("nonlinear and non-chain DAGs fail closed", func(t *testing.T) {
		dag := cloudSummaryTestDAG()
		dag.Executors[1] = &tipb.Executor{Tp: tipb.ExecType_TypeTopN, TopN: &tipb.TopN{}}
		_, status := freezeCloudSummaryPlan(dag)
		require.Equal(t, cloudSummaryPlanUnsupported, status)

		dag = cloudSummaryTestDAG()
		parent := uint32(3)
		dag.Executors[0].ParentIdx = &parent
		_, status = freezeCloudSummaryPlan(dag)
		require.Equal(t, cloudSummaryPlanUnavailable, status)

		dag = cloudSummaryTestDAG()
		dag.IntermediateOutputChannels = []*tipb.IntermediateOutputChannel{{ExecutorIdx: 0}}
		_, status = freezeCloudSummaryPlan(dag)
		require.Equal(t, cloudSummaryPlanUnavailable, status)
	})

	t.Run("nested executor children fail closed", func(t *testing.T) {
		for _, executor := range []*tipb.Executor{
			{Tp: tipb.ExecType_TypeSelection, Selection: &tipb.Selection{Child: &tipb.Executor{}}},
			{Tp: tipb.ExecType_TypeProjection, Projection: &tipb.Projection{Child: &tipb.Executor{}}},
			{Tp: tipb.ExecType_TypeLimit, Limit: &tipb.Limit{Child: &tipb.Executor{}}},
			{Tp: tipb.ExecType_TypeAggregation, Aggregation: &tipb.Aggregation{Child: &tipb.Executor{}}},
		} {
			dag := cloudSummaryTestDAG()
			dag.Executors = dag.Executors[:2]
			dag.Executors[1] = executor
			_, status := freezeCloudSummaryPlan(dag)
			require.Equal(t, cloudSummaryPlanUnsupported, status)
		}
	})
}

func TestCloudSummaryScanProtoCompatibility(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		fields []string
	}{
		{
			name:  "table scan",
			value: tipb.TableScan{},
			fields: []string{
				"TableId", "Columns", "Desc", "PrimaryColumnIds", "NextReadEngine", "Ranges",
				"PrimaryPrefixColumnIds", "KeepOrder", "IsFastScan", "PushedDownFilterConditions",
				"RuntimeFilterList", "MaxWaitTimeMs", "DeprecatedAnnQuery", "UsedColumnarIndexes",
			},
		},
		{
			name:  "partition table scan",
			value: tipb.PartitionTableScan{},
			fields: []string{
				"TableId", "Columns", "Desc", "PrimaryColumnIds", "PrimaryPrefixColumnIds", "PartitionIds",
				"IsFastScan", "PushedDownFilterConditions", "RuntimeFilterList", "MaxWaitTimeMs",
				"DeprecatedAnnQuery", "UsedColumnarIndexes",
			},
		},
		{
			name:  "index scan",
			value: tipb.IndexScan{},
			fields: []string{
				"TableId", "IndexId", "Columns", "Desc", "Unique", "PrimaryColumnIds",
				"FtsQueryInfo", "TiciVectorQueryInfo",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typeOf := reflect.TypeOf(test.value)
			fields := make([]string, typeOf.NumField())
			for i := range typeOf.NumField() {
				fields[i] = typeOf.Field(i).Name
			}
			require.Equal(t, test.fields, fields,
				"classify every new scan protobuf field before widening the Stage-1 strict subset")
		})
	}
}

func TestPrepareCloudSummaryOwner(t *testing.T) {
	statement := newCloudSummaryTestStatement()
	dctx := newCloudSummaryTestDistSQLContext()
	dctx.StatementRUUnitContributors = statement.UnitContributorRegistrar()
	dag := cloudSummaryTestDAG()
	data, err := dag.Marshal()
	require.NoError(t, err)
	request := &kv.Request{
		Tp:             kv.ReqTypeDAG,
		Data:           data,
		StoreType:      kv.TiKV,
		Cacheable:      true,
		StoreBatchSize: 16,
	}
	owner := prepareCloudSummaryOwner(dctx, request)
	require.NotNil(t, owner)
	require.False(t, request.Cacheable)
	require.Zero(t, request.StoreBatchSize)
	var updated tipb.DAGRequest
	require.NoError(t, updated.Unmarshal(request.Data))
	require.NotNil(t, updated.CollectExecutionSummaries)
	require.True(t, updated.GetCollectExecutionSummaries())

	owner.observeResponse(cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats())
	owner.completeEOF()
	finish, _ := statement.Finish(statementru.TerminalSuccess)
	require.Equal(t, statementru.Outcome{State: statementru.StateComplete}, finish.Result.Outcome())
	total, ok := finish.Result.TotalRU()
	require.True(t, ok)
	require.Equal(t, float64(39), total)
}

func TestSelectEnablesCloudSummaryCollection(t *testing.T) {
	statement := newCloudSummaryTestStatement()
	dctx := newCloudSummaryTestDistSQLContext()
	dctx.StatementRUUnitContributors = statement.UnitContributorRegistrar()
	client := &cloudSummaryTestClient{response: &cloudSummaryTestKVResponse{subsets: []kv.ResultSubset{
		newCloudSummaryTestSubset(t, cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats()),
	}}}
	dctx.Client = client
	dag := cloudSummaryTestDAG()
	data, err := dag.Marshal()
	require.NoError(t, err)
	request := &kv.Request{
		Tp:             kv.ReqTypeDAG,
		Data:           data,
		StoreType:      kv.TiKV,
		Cacheable:      true,
		StoreBatchSize: 8,
	}
	result, err := Select(context.Background(), dctx, request, nil)
	require.NoError(t, err)
	require.NotNil(t, client.option)
	require.True(t, client.option.EnableCollectExecutionInfo)
	require.False(t, request.Cacheable)
	require.Zero(t, request.StoreBatchSize)
	require.NoError(t, result.Next(context.Background(), chunk.NewChunkWithCapacity(nil, 0)))
	require.NoError(t, result.Close())
	finish, _ := statement.Finish(statementru.TerminalSuccess)
	total, ok := finish.Result.TotalRU()
	require.True(t, ok)
	require.Equal(t, float64(39), total)
}

func TestSelectRejectsUnmodeledCloudScanWork(t *testing.T) {
	tests := []struct {
		name       string
		executorTp tipb.ExecType
		configure  func(*tipb.Executor)
	}{
		{
			name:       "table pushed filter",
			executorTp: tipb.ExecType_TypeTableScan,
			configure: func(executor *tipb.Executor) {
				executor.TblScan.PushedDownFilterConditions = []*tipb.Expr{{}}
			},
		},
		{
			name:       "table runtime filter",
			executorTp: tipb.ExecType_TypeTableScan,
			configure: func(executor *tipb.Executor) {
				executor.TblScan.RuntimeFilterList = []*tipb.RuntimeFilter{{}}
			},
		},
		{
			name:       "table deprecated ANN",
			executorTp: tipb.ExecType_TypeTableScan,
			configure: func(executor *tipb.Executor) {
				executor.TblScan.DeprecatedAnnQuery = &tipb.ANNQueryInfo{}
			},
		},
		{
			name:       "table columnar index",
			executorTp: tipb.ExecType_TypeTableScan,
			configure: func(executor *tipb.Executor) {
				executor.TblScan.UsedColumnarIndexes = []*tipb.ColumnarIndexInfo{{}}
			},
		},
		{
			name:       "partition pushed filter",
			executorTp: tipb.ExecType_TypePartitionTableScan,
			configure: func(executor *tipb.Executor) {
				executor.PartitionTableScan.PushedDownFilterConditions = []*tipb.Expr{{}}
			},
		},
		{
			name:       "partition runtime filter",
			executorTp: tipb.ExecType_TypePartitionTableScan,
			configure: func(executor *tipb.Executor) {
				executor.PartitionTableScan.RuntimeFilterList = []*tipb.RuntimeFilter{{}}
			},
		},
		{
			name:       "partition deprecated ANN",
			executorTp: tipb.ExecType_TypePartitionTableScan,
			configure: func(executor *tipb.Executor) {
				executor.PartitionTableScan.DeprecatedAnnQuery = &tipb.ANNQueryInfo{}
			},
		},
		{
			name:       "partition columnar index",
			executorTp: tipb.ExecType_TypePartitionTableScan,
			configure: func(executor *tipb.Executor) {
				executor.PartitionTableScan.UsedColumnarIndexes = []*tipb.ColumnarIndexInfo{{}}
			},
		},
		{
			name:       "index FTS",
			executorTp: tipb.ExecType_TypeIndexScan,
			configure: func(executor *tipb.Executor) {
				executor.IdxScan.FtsQueryInfo = &tipb.FTSQueryInfo{}
			},
		},
		{
			name:       "index vector",
			executorTp: tipb.ExecType_TypeIndexScan,
			configure: func(executor *tipb.Executor) {
				executor.IdxScan.TiciVectorQueryInfo = &tipb.TiCIVectorQueryInfo{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement := newCloudSummaryTestStatement()
			dctx := newCloudSummaryTestDistSQLContext()
			dctx.StatementRUUnitContributors = statement.UnitContributorRegistrar()
			dctx.Client = &cloudSummaryTestClient{response: &cloudSummaryTestKVResponse{subsets: []kv.ResultSubset{
				newCloudSummaryTestSubset(t, cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats()),
			}}}
			dag := cloudSummaryTestDAG()
			switch test.executorTp {
			case tipb.ExecType_TypePartitionTableScan:
				dag.Executors[0] = &tipb.Executor{
					Tp:                 tipb.ExecType_TypePartitionTableScan,
					PartitionTableScan: &tipb.PartitionTableScan{},
				}
			case tipb.ExecType_TypeIndexScan:
				dag.Executors[0] = &tipb.Executor{
					Tp:      tipb.ExecType_TypeIndexScan,
					IdxScan: &tipb.IndexScan{},
				}
			}
			test.configure(dag.Executors[0])
			data, err := dag.Marshal()
			require.NoError(t, err)
			request := &kv.Request{Tp: kv.ReqTypeDAG, Data: data, StoreType: kv.TiKV}

			result, err := Select(context.Background(), dctx, request, nil)
			require.NoError(t, err)
			require.NoError(t, result.Next(context.Background(), chunk.NewChunkWithCapacity(nil, 0)))
			require.NoError(t, result.Close())
			finish, _ := statement.Finish(statementru.TerminalSuccess)
			require.False(t, finish.Result.HasTotal())
			require.Equal(t, statementru.Outcome{
				State:  statementru.StateUnavailable,
				Reason: statementru.ReasonUnsupported,
			}, finish.Result.Outcome())
		})
	}
}

func TestCloudSummaryOwnerCoverage(t *testing.T) {
	tests := []struct {
		name    string
		run     func(*cloudSummaryOwner)
		state   statementru.CollectionState
		reason  statementru.Reason
		cpuWork float64
	}{
		{
			name: "multiple complete responses",
			run: func(owner *cloudSummaryOwner) {
				owner.observeResponse(cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats())
				owner.observeResponse(cloudSummaryTestResponse(2, 2, 2, 2), completeCloudSummaryTestStats())
				owner.completeEOF()
			},
			state:   statementru.StateComplete,
			cpuWork: 51,
		},
		{
			name: "missing summary fields",
			run: func(owner *cloudSummaryOwner) {
				response := cloudSummaryTestResponse(10, 5, 4, 3)
				response.ExecutionSummaries[1].NumIterations = nil
				owner.observeResponse(response, completeCloudSummaryTestStats())
				owner.completeEOF()
			},
			state:  statementru.StateUnavailable,
			reason: statementru.ReasonMissingEvidence,
		},
		{
			name: "valid evidence followed by an invalid response",
			run: func(owner *cloudSummaryOwner) {
				owner.observeResponse(cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats())
				owner.observeResponse(&tipb.SelectResponse{}, completeCloudSummaryTestStats())
				owner.completeEOF()
			},
			state:  statementru.StatePartial,
			reason: statementru.ReasonIncompleteEvidence,
		},
		{
			name: "invalid response followed by valid evidence",
			run: func(owner *cloudSummaryOwner) {
				owner.observeResponse(&tipb.SelectResponse{}, completeCloudSummaryTestStats())
				owner.observeResponse(cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats())
				owner.completeEOF()
			},
			state:  statementru.StatePartial,
			reason: statementru.ReasonIncompleteEvidence,
		},
		{
			name: "retry RPC stats",
			run: func(owner *cloudSummaryOwner) {
				stats := completeCloudSummaryTestStats()
				stats.ReqStats.RecordRPCRuntimeStats(tikvrpc.CmdCop, time.Nanosecond)
				owner.observeResponse(cloudSummaryTestResponse(10, 5, 4, 3), stats)
				owner.completeEOF()
			},
			state:  statementru.StateUnavailable,
			reason: statementru.ReasonMissingEvidence,
		},
		{
			name: "cache hit",
			run: func(owner *cloudSummaryOwner) {
				stats := completeCloudSummaryTestStats()
				stats.CoprCacheHit = true
				owner.observeResponse(cloudSummaryTestResponse(10, 5, 4, 3), stats)
				owner.completeEOF()
			},
			state:  statementru.StateUnavailable,
			reason: statementru.ReasonMissingEvidence,
		},
		{
			name: "early close after evidence",
			run: func(owner *cloudSummaryOwner) {
				owner.observeResponse(cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats())
				owner.abort()
			},
			state:  statementru.StatePartial,
			reason: statementru.ReasonIncompleteEvidence,
		},
		{
			name: "zero response EOF is not authoritative empty",
			run: func(owner *cloudSummaryOwner) {
				owner.completeEOF()
			},
			state:  statementru.StateUnavailable,
			reason: statementru.ReasonMissingEvidence,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, owner := newCloudSummaryTestOwner()
			test.run(owner)
			finish, _ := statement.Finish(statementru.TerminalSuccess)
			require.Equal(t, test.state, finish.Result.Outcome().State)
			require.Equal(t, test.reason, finish.Result.Outcome().Reason)
			if test.state == statementru.StateComplete {
				total, ok := finish.Result.TotalRU()
				require.True(t, ok)
				require.Equal(t, test.cpuWork, total)
			} else {
				require.False(t, finish.Result.HasTotal())
			}
		})
	}
}

func TestSelectResultCloudSummaryLifecycle(t *testing.T) {
	t.Run("clean EOF commits once", func(t *testing.T) {
		statement, owner := newCloudSummaryTestOwner()
		response := &cloudSummaryTestKVResponse{subsets: []kv.ResultSubset{
			newCloudSummaryTestSubset(t, cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats()),
		}}
		result := newCloudSummaryTestSelectResult(response, owner)
		require.NoError(t, result.fetchResp(context.Background()))
		require.Nil(t, result.selectResp)
		require.NoError(t, result.Close())
		finish, _ := statement.Finish(statementru.TerminalSuccess)
		total, ok := finish.Result.TotalRU()
		require.True(t, ok)
		require.Equal(t, float64(39), total)
	})

	t.Run("error after data is partial", func(t *testing.T) {
		statement, owner := newCloudSummaryTestOwner()
		expected := errors.New("response failed")
		response := &cloudSummaryTestKVResponse{
			subsets: []kv.ResultSubset{newCloudSummaryTestSubset(t, cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats())},
			err:     expected,
		}
		result := newCloudSummaryTestSelectResult(response, owner)
		require.ErrorIs(t, result.fetchResp(context.Background()), expected)
		finish, _ := statement.Finish(statementru.TerminalSuccess)
		require.Equal(t, statementru.StatePartial, finish.Result.Outcome().State)
	})

	t.Run("IntoIter decode error after evidence is partial", func(t *testing.T) {
		statement, owner := newCloudSummaryTestOwner()
		selectResponse := cloudSummaryTestResponse(10, 5, 4, 3)
		selectResponse.EncodeType = tipb.EncodeType(255)
		selectResponse.Chunks = []tipb.Chunk{{RowsData: []byte{1}}}
		response := &cloudSummaryTestKVResponse{subsets: []kv.ResultSubset{
			newCloudSummaryTestSubset(t, selectResponse, completeCloudSummaryTestStats()),
		}}
		result := newCloudSummaryTestSelectResult(response, owner)
		iter, err := result.IntoIter(nil)
		require.NoError(t, err)
		_, err = iter.Next(context.Background())
		require.ErrorContains(t, err, "unsupported encode type")
		finish, _ := statement.Finish(statementru.TerminalSuccess)
		require.Equal(t, statementru.Outcome{
			State:  statementru.StatePartial,
			Reason: statementru.ReasonIncompleteEvidence,
		}, finish.Result.Outcome())
	})

	t.Run("Close and NextRaw before drain fail closed", func(t *testing.T) {
		for _, raw := range []bool{false, true} {
			statement, owner := newCloudSummaryTestOwner()
			response := &cloudSummaryTestKVResponse{subsets: []kv.ResultSubset{
				newCloudSummaryTestSubset(t, cloudSummaryTestResponse(10, 5, 4, 3), completeCloudSummaryTestStats()),
			}}
			result := newCloudSummaryTestSelectResult(response, owner)
			if raw {
				_, err := result.NextRaw(context.Background())
				require.NoError(t, err)
			} else {
				require.NoError(t, result.Close())
			}
			finish, _ := statement.Finish(statementru.TerminalSuccess)
			require.Equal(t, statementru.StateUnavailable, finish.Result.Outcome().State)
		}
	})
}

func cloudSummaryTestDAG() *tipb.DAGRequest {
	return &tipb.DAGRequest{Executors: []*tipb.Executor{
		{Tp: tipb.ExecType_TypeTableScan, TblScan: &tipb.TableScan{}},
		{Tp: tipb.ExecType_TypeSelection, Selection: &tipb.Selection{Conditions: cloudSummaryTestExprs(2)}},
		{Tp: tipb.ExecType_TypeProjection, Projection: &tipb.Projection{Exprs: cloudSummaryTestExprs(3)}},
		{Tp: tipb.ExecType_TypeLimit, Limit: &tipb.Limit{}},
	}}
}

func cloudSummaryTestExprs(count int) []*tipb.Expr {
	expressions := make([]*tipb.Expr, count)
	for i := range expressions {
		expressions[i] = &tipb.Expr{}
	}
	return expressions
}

func cloudSummaryTestRPNExprs(count int) []*tipb.RpnExpr {
	expressions := make([]*tipb.RpnExpr, count)
	for i := range expressions {
		expressions[i] = &tipb.RpnExpr{}
	}
	return expressions
}

func newCloudSummaryTestStatement() *statementru.Statement {
	weights := statementru.Weights{statementru.CPUWork: 1}
	return statementru.NewStatement(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	})
}

func newCloudSummaryTestOwner() (*statementru.Statement, *cloudSummaryOwner) {
	statement := newCloudSummaryTestStatement()
	contributor := statement.UnitContributorRegistrar().RegisterUnitContributor(statementru.CPUWork.Mask())
	plan, status := freezeCloudSummaryPlan(cloudSummaryTestDAG())
	if status != cloudSummaryPlanSupported {
		panic("test DAG is unsupported")
	}
	return statement, &cloudSummaryOwner{plan: plan, contributor: contributor}
}

func cloudSummaryTestResponse(rows ...uint64) *tipb.SelectResponse {
	summaries := make([]*tipb.ExecutorExecutionSummary, len(rows))
	for i, rowCount := range rows {
		processed := uint64(i + 1)
		iterations := uint64(1)
		rowsCopy := rowCount
		summaries[i] = &tipb.ExecutorExecutionSummary{
			TimeProcessedNs: &processed,
			NumProducedRows: &rowsCopy,
			NumIterations:   &iterations,
		}
	}
	return &tipb.SelectResponse{ExecutionSummaries: summaries}
}

func completeCloudSummaryTestStats() *copr.CopRuntimeStats {
	stats := &copr.CopRuntimeStats{ReqStats: tikv.NewRegionRequestRuntimeStats()}
	stats.ReqStats.RecordRPCRuntimeStats(tikvrpc.CmdCop, time.Nanosecond)
	return stats
}

func newCloudSummaryTestDistSQLContext() *distsqlctx.DistSQLContext {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx = stmtctx.NewStmtCtx()
	return ctx.GetDistSQLCtx()
}

type cloudSummaryTestSubset struct {
	data  []byte
	stats *copr.CopRuntimeStats
}

func newCloudSummaryTestSubset(t *testing.T, response *tipb.SelectResponse, stats *copr.CopRuntimeStats) *cloudSummaryTestSubset {
	data, err := response.Marshal()
	require.NoError(t, err)
	return &cloudSummaryTestSubset{data: data, stats: stats}
}

func (s *cloudSummaryTestSubset) GetData() []byte                           { return s.data }
func (*cloudSummaryTestSubset) GetStartKey() kv.Key                         { return nil }
func (s *cloudSummaryTestSubset) MemSize() int64                            { return int64(len(s.data)) }
func (*cloudSummaryTestSubset) RespTime() time.Duration                     { return 0 }
func (s *cloudSummaryTestSubset) GetCopRuntimeStats() *copr.CopRuntimeStats { return s.stats }

type cloudSummaryTestKVResponse struct {
	subsets []kv.ResultSubset
	err     error
	closed  bool
}

type cloudSummaryTestClient struct {
	kv.RequestTypeSupportedChecker
	response kv.Response
	option   *kv.ClientSendOption
}

func (c *cloudSummaryTestClient) Send(_ context.Context, _ *kv.Request, _ any, option *kv.ClientSendOption) kv.Response {
	c.option = option
	return c.response
}

func (r *cloudSummaryTestKVResponse) Next(context.Context) (kv.ResultSubset, error) {
	if len(r.subsets) != 0 {
		subset := r.subsets[0]
		r.subsets = r.subsets[1:]
		return subset, nil
	}
	if r.err != nil {
		err := r.err
		r.err = nil
		return nil, err
	}
	return nil, nil
}

func (r *cloudSummaryTestKVResponse) Close() error {
	r.closed = true
	return nil
}

func newCloudSummaryTestSelectResult(response kv.Response, owner *cloudSummaryOwner) *selectResult {
	return &selectResult{
		label:                   "dag",
		resp:                    response,
		ctx:                     newCloudSummaryTestDistSQLContext(),
		storeType:               kv.TiKV,
		statementRUCloudSummary: owner,
	}
}
