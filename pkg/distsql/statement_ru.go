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
	"math/bits"

	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikvrpc"
)

const maxExactStatementRUInteger = uint64(1 << 53)

type cloudSummaryPlanStatus uint8

const (
	cloudSummaryPlanSupported cloudSummaryPlanStatus = iota
	cloudSummaryPlanUnavailable
	cloudSummaryPlanUnsupported
)

type cloudSummaryStep struct {
	child      int
	multiplier uint64
}

type cloudSummaryPlan struct {
	steps      []cloudSummaryStep
	hasHashAgg bool
}

type cloudSummaryOwner struct {
	plan        cloudSummaryPlan
	contributor statementru.UnitContributor
	cpuWork     uint64
	responses   uint64
	failed      bool
	done        bool
}

type rangeScanByteEstimateOwner struct {
	contributor       statementru.UnitContributor
	totalKeys         uint64
	processedKeys     uint64
	processedKeysSize uint64
	responses         uint64
	sawUsableDetail   bool
	incomplete        bool
	done              bool
}

func markMPPStatementRUUnsupported(dctx *distsqlctx.DistSQLContext) {
	if dctx == nil || dctx.StatementRUUnitContributors == nil {
		return
	}
	units := dctx.StatementRUUnitContributors.CollectedUnits() &
		(statementru.CPUWork.Mask() | statementru.ScanBytes.Mask())
	// CPU work can be required while scan bytes is optional (or vice versa).
	// Register one lease per unit so the registrar never has to accept a mixed
	// required/optional lease merely to fail an unsupported MPP path closed.
	for _, unit := range []statementru.UnitMask{statementru.CPUWork.Mask(), statementru.ScanBytes.Mask()} {
		if units&unit == 0 {
			continue
		}
		contributor := dctx.StatementRUUnitContributors.RegisterUnitContributor(unit)
		if contributor != nil {
			contributor.Unsupported()
		}
	}
}

func prepareRangeScanByteEstimateOwner(dctx *distsqlctx.DistSQLContext, request *kv.Request) *rangeScanByteEstimateOwner {
	if dctx == nil || request == nil || dctx.StatementRUUnitContributors == nil || request.Tp != kv.ReqTypeDAG {
		return nil
	}
	if dctx.StatementRUUnitContributors.CollectedUnits()&statementru.ScanBytes.Mask() == 0 {
		return nil
	}
	contributor := dctx.StatementRUUnitContributors.RegisterUnitContributor(statementru.ScanBytes.Mask())
	if contributor == nil {
		return nil
	}
	if request.StoreType != kv.TiKV || request.BatchCop {
		contributor.Unsupported()
		return nil
	}
	// Cached and store-batched responses do not provide a one-to-one detail
	// record for every physical scan response. Disable both until the response
	// contract carries explicit per-response coverage.
	request.Cacheable = false
	request.StoreBatchSize = 0
	return &rangeScanByteEstimateOwner{contributor: contributor}
}

func prepareCloudSummaryOwner(dctx *distsqlctx.DistSQLContext, request *kv.Request) *cloudSummaryOwner {
	if dctx == nil || request == nil || dctx.StatementRUUnitContributors == nil || request.Tp != kv.ReqTypeDAG {
		return nil
	}
	collectedUnits := dctx.StatementRUUnitContributors.CollectedUnits()
	collectCPU := collectedUnits&statementru.CPUWork.Mask() != 0
	collectHashState := collectedUnits&statementru.HashStateRows.Mask() != 0
	if !collectCPU && !collectHashState {
		return nil
	}
	if request.StoreType != kv.TiKV || request.BatchCop {
		if collectCPU {
			finishImmediateCloudSummaryContributor(dctx.StatementRUUnitContributors, statementru.CPUWork.Mask(), cloudSummaryPlanUnsupported)
		}
		if collectHashState {
			finishImmediateCloudSummaryContributor(dctx.StatementRUUnitContributors, statementru.HashStateRows.Mask(), cloudSummaryPlanUnsupported)
		}
		return nil
	}

	var dag tipb.DAGRequest
	if err := dag.Unmarshal(request.Data); err != nil {
		if collectCPU {
			finishImmediateCloudSummaryContributor(dctx.StatementRUUnitContributors, statementru.CPUWork.Mask(), cloudSummaryPlanUnavailable)
		}
		if collectHashState {
			finishImmediateCloudSummaryContributor(dctx.StatementRUUnitContributors, statementru.HashStateRows.Mask(), cloudSummaryPlanUnavailable)
		}
		return nil
	}
	if collectHashState && dagHasHashAggregation(&dag) {
		finishImmediateCloudSummaryContributor(dctx.StatementRUUnitContributors, statementru.HashStateRows.Mask(), cloudSummaryPlanUnsupported)
	}
	if !collectCPU {
		return nil
	}
	plan, status := freezeCloudSummaryPlan(&dag)
	if status != cloudSummaryPlanSupported {
		finishImmediateCloudSummaryContributor(dctx.StatementRUUnitContributors, statementru.CPUWork.Mask(), status)
		return nil
	}

	contributor := dctx.StatementRUUnitContributors.RegisterUnitContributor(statementru.CPUWork.Mask())
	if contributor == nil {
		return nil
	}
	collect := true
	dag.CollectExecutionSummaries = &collect
	data, err := dag.Marshal()
	if err != nil {
		contributor.Unavailable()
		return nil
	}
	request.Data = data
	request.Cacheable = false
	request.StoreBatchSize = 0
	return &cloudSummaryOwner{plan: plan, contributor: contributor}
}

func dagHasHashAggregation(dag *tipb.DAGRequest) bool {
	if dag == nil {
		return false
	}
	for _, executor := range dag.Executors {
		if executor != nil && executor.GetTp() == tipb.ExecType_TypeAggregation {
			return true
		}
	}
	return false
}

func finishImmediateCloudSummaryContributor(
	registrar statementru.UnitContributorRegistrar,
	units statementru.UnitMask,
	status cloudSummaryPlanStatus,
) {
	contributor := registrar.RegisterUnitContributor(units)
	if contributor == nil {
		return
	}
	if status == cloudSummaryPlanUnsupported {
		contributor.Unsupported()
	} else {
		contributor.Unavailable()
	}
}

// The Stage-1 plan grammar is one list-based, implicit unary chain. Position 0
// is a metadata-only TableScan, PartitionTableScan, or IndexScan. Every later
// position consumes only its immediately preceding child and is one of:
//
//   - Selection: child rows * condition count
//   - Projection: child rows * expression count
//   - Limit: child rows
//   - StreamAgg or HashAgg: child rows * (group-by + aggregate count)
//
// Anything that performs additional scan-side work, uses a nonlinear formula,
// or has an explicit/non-chain topology is unsupported. The scan protobuf
// shape is locked by TestCloudSummaryScanProtoCompatibility so a TiPB field
// addition requires an explicit classification here before Stage 1 can accept
// it.
func freezeCloudSummaryPlan(dag *tipb.DAGRequest) (cloudSummaryPlan, cloudSummaryPlanStatus) {
	if dag == nil || dag.GetRootExecutor() != nil || len(dag.GetIntermediateOutputChannels()) != 0 || len(dag.Executors) == 0 {
		return cloudSummaryPlan{}, cloudSummaryPlanUnavailable
	}
	plan := cloudSummaryPlan{steps: make([]cloudSummaryStep, len(dag.Executors))}
	rpn := dag.GetIsRpnExpr()
	for i, executor := range dag.Executors {
		if executor == nil || !validCloudSummaryParent(executor, i, len(dag.Executors)) {
			return plan, cloudSummaryPlanUnavailable
		}
		if executor.GetTp() == tipb.ExecType_TypeAggregation {
			plan.hasHashAgg = true
		}
		if i == 0 {
			if !supportedCloudSummaryScan(executor) {
				return plan, cloudSummaryPlanUnsupported
			}
			continue
		}

		multiplier, status := cloudSummaryMultiplier(executor, rpn)
		if status != cloudSummaryPlanSupported {
			return plan, status
		}
		plan.steps[i] = cloudSummaryStep{child: i - 1, multiplier: multiplier}
	}
	return plan, cloudSummaryPlanSupported
}

func validCloudSummaryParent(executor *tipb.Executor, index, count int) bool {
	if index == count-1 {
		return executor.ParentIdx == nil
	}
	if executor.ParentIdx == nil {
		return true
	}
	return uint64(*executor.ParentIdx) == uint64(index+1)
}

func supportedCloudSummaryScan(executor *tipb.Executor) bool {
	switch executor.GetTp() {
	case tipb.ExecType_TypeTableScan:
		return executor.TblScan != nil &&
			len(executor.TblScan.PushedDownFilterConditions) == 0 &&
			len(executor.TblScan.RuntimeFilterList) == 0 &&
			executor.TblScan.DeprecatedAnnQuery == nil &&
			len(executor.TblScan.UsedColumnarIndexes) == 0
	case tipb.ExecType_TypeIndexScan:
		return executor.IdxScan != nil && executor.IdxScan.FtsQueryInfo == nil && executor.IdxScan.TiciVectorQueryInfo == nil
	case tipb.ExecType_TypePartitionTableScan:
		return executor.PartitionTableScan != nil &&
			len(executor.PartitionTableScan.PushedDownFilterConditions) == 0 &&
			len(executor.PartitionTableScan.RuntimeFilterList) == 0 &&
			executor.PartitionTableScan.DeprecatedAnnQuery == nil &&
			len(executor.PartitionTableScan.UsedColumnarIndexes) == 0
	default:
		return false
	}
}

func cloudSummaryMultiplier(executor *tipb.Executor, rpn bool) (uint64, cloudSummaryPlanStatus) {
	switch executor.GetTp() {
	case tipb.ExecType_TypeSelection:
		if executor.Selection == nil {
			return 0, cloudSummaryPlanUnavailable
		}
		if executor.Selection.Child != nil {
			return 0, cloudSummaryPlanUnsupported
		}
		if rpn {
			return uint64(len(executor.Selection.RpnConditions)), cloudSummaryPlanSupported
		}
		return uint64(len(executor.Selection.Conditions)), cloudSummaryPlanSupported
	case tipb.ExecType_TypeProjection:
		if executor.Projection == nil {
			return 0, cloudSummaryPlanUnavailable
		}
		if executor.Projection.Child != nil {
			return 0, cloudSummaryPlanUnsupported
		}
		if rpn {
			return uint64(len(executor.Projection.RpnExprs)), cloudSummaryPlanSupported
		}
		return uint64(len(executor.Projection.Exprs)), cloudSummaryPlanSupported
	case tipb.ExecType_TypeLimit:
		if executor.Limit == nil {
			return 0, cloudSummaryPlanUnavailable
		}
		if executor.Limit.Child != nil || len(executor.Limit.PartitionBy) != 0 || len(executor.Limit.TruncateKeyExpr) != 0 {
			return 0, cloudSummaryPlanUnsupported
		}
		return 1, cloudSummaryPlanSupported
	case tipb.ExecType_TypeAggregation, tipb.ExecType_TypeStreamAgg:
		if executor.Aggregation == nil {
			return 0, cloudSummaryPlanUnavailable
		}
		if executor.Aggregation.Child != nil {
			return 0, cloudSummaryPlanUnsupported
		}
		var groupBy, aggregateFunctions uint64
		if rpn {
			groupBy = uint64(len(executor.Aggregation.RpnGroupBy))
			aggregateFunctions = uint64(len(executor.Aggregation.RpnAggFunc))
		} else {
			groupBy = uint64(len(executor.Aggregation.GroupBy))
			aggregateFunctions = uint64(len(executor.Aggregation.AggFunc))
		}
		if groupBy > ^uint64(0)-aggregateFunctions {
			return 0, cloudSummaryPlanUnavailable
		}
		return groupBy + aggregateFunctions, cloudSummaryPlanSupported
	case tipb.ExecType_TypeTopN, tipb.ExecType_TypeSort:
		return 0, cloudSummaryPlanUnsupported
	default:
		return 0, cloudSummaryPlanUnsupported
	}
}

func (o *cloudSummaryOwner) observeResponse(response *tipb.SelectResponse, stats *copr.CopRuntimeStats) {
	if o == nil || o.done {
		return
	}
	if response == nil || !completeSingleCopResponseStats(stats) {
		o.failed = true
		return
	}
	summaries := response.GetExecutionSummaries()
	if len(summaries) != len(o.plan.steps) {
		o.failed = true
		return
	}
	for _, summary := range summaries {
		if summary == nil || summary.TimeProcessedNs == nil || summary.NumProducedRows == nil || summary.NumIterations == nil {
			o.failed = true
			return
		}
	}

	var responseWork uint64
	for i, step := range o.plan.steps {
		if i == 0 || step.multiplier == 0 {
			continue
		}
		work, ok := checkedCloudSummaryProduct(*summaries[step.child].NumProducedRows, step.multiplier)
		if !ok || responseWork > maxExactStatementRUInteger-work {
			o.failed = true
			return
		}
		responseWork += work
	}
	if o.responses == ^uint64(0) {
		o.failed = true
		return
	}
	// Once any response fails validation this owner can no longer submit a
	// complete vector. Continue validating later responses so coverage remains
	// independent of region response order: any later usable response makes the
	// owner partial rather than unavailable.
	if !o.failed {
		if o.cpuWork > maxExactStatementRUInteger-responseWork {
			o.failed = true
			return
		}
		o.cpuWork += responseWork
	}
	o.responses++
}

// completeSingleCopResponseStats is deliberately stricter than the execution
// path. The current execution summaries and scan details contain no stable
// attempt identity, so they are accepted only when client statistics prove
// exactly one CmdCop RPC, no cache reuse, no request error, and no retry or
// backoff evidence.
func completeSingleCopResponseStats(stats *copr.CopRuntimeStats) bool {
	if stats == nil || stats.CoprCacheHit || stats.ReqStats == nil ||
		stats.ReqStats.GetRPCStatsCount() != 1 || stats.ReqStats.GetCmdRPCCount(tikvrpc.CmdCop) != 1 ||
		len(stats.ReqStats.ErrStats) != 0 || stats.ReqStats.OtherErrCnt != 0 ||
		stats.BackoffTime != 0 || len(stats.BackoffSleep) != 0 || len(stats.BackoffTimes) != 0 {
		return false
	}
	return true
}

func checkedCloudSummaryProduct(rows, multiplier uint64) (uint64, bool) {
	if rows == 0 || multiplier == 0 {
		return 0, true
	}
	if rows > maxExactStatementRUInteger/multiplier {
		return 0, false
	}
	return rows * multiplier, true
}

func (o *rangeScanByteEstimateOwner) observeResponse(stats *copr.CopRuntimeStats) {
	if o == nil || o.done {
		return
	}
	if stats == nil || !stats.ScanDetailV2Present || stats.ScanDetail == nil {
		o.incomplete = true
		return
	}
	detail := stats.ScanDetail
	if detail.TotalKeys < 0 || detail.ProcessedKeys < 0 || detail.ProcessedKeysSize < 0 {
		o.incomplete = true
		return
	}
	o.sawUsableDetail = true
	// ScanDetailV2 defines processed versions as user key-value pairs and total
	// versions as the superset encountered by MVCC. A nonempty processed set
	// therefore needs both a nonzero byte size and no more entries than total.
	// Treat old or inconsistent zero-filled details as incomplete evidence.
	if (detail.TotalKeys == 0 && (detail.ProcessedKeys != 0 || detail.ProcessedKeysSize != 0)) ||
		(detail.ProcessedKeys == 0 && detail.ProcessedKeysSize != 0) ||
		detail.ProcessedKeys > detail.TotalKeys ||
		(detail.ProcessedKeys > 0 && detail.ProcessedKeysSize == 0) {
		o.incomplete = true
		return
	}
	if !completeSingleCopResponseStats(stats) {
		o.incomplete = true
		return
	}
	totalKeys := uint64(detail.TotalKeys)
	processedKeys := uint64(detail.ProcessedKeys)
	processedKeysSize := uint64(detail.ProcessedKeysSize)
	if totalKeys > maxExactStatementRUInteger || processedKeys > maxExactStatementRUInteger ||
		processedKeysSize > maxExactStatementRUInteger ||
		o.totalKeys > maxExactStatementRUInteger-totalKeys ||
		o.processedKeys > maxExactStatementRUInteger-processedKeys ||
		o.processedKeysSize > maxExactStatementRUInteger-processedKeysSize {
		o.incomplete = true
		return
	}
	if o.responses == ^uint64(0) {
		o.incomplete = true
		return
	}
	if !o.incomplete {
		o.totalKeys += totalKeys
		o.processedKeys += processedKeys
		o.processedKeysSize += processedKeysSize
	}
	o.responses++
}

func (o *rangeScanByteEstimateOwner) completeEOF() {
	if o == nil || o.done {
		return
	}
	o.done = true
	if o.incomplete || o.responses == 0 {
		if o.sawUsableDetail {
			o.contributor.Partial()
		} else {
			o.contributor.Unavailable()
		}
		return
	}
	// Merge the raw tuple across every physical response before division. The
	// estimate is intentionally owner-wide: summing per-response ratios would
	// make the result depend on region partitioning. When a direct total-size
	// field is propagated, response observation must explicitly select one
	// homogeneous source and this final estimate can then become an exact sum.
	var scanBytes float64
	if o.totalKeys != 0 && o.processedKeys != 0 {
		if !rangeScanEstimateWithinExactBoundary(o.processedKeysSize, o.totalKeys, o.processedKeys) {
			o.contributor.Partial()
			return
		}
		scanBytes = float64(o.processedKeysSize) / float64(o.processedKeys) * float64(o.totalKeys)
		if scanBytes > float64(maxExactStatementRUInteger) {
			o.contributor.Partial()
			return
		}
	}
	var values statementru.UnitValues
	values[statementru.ScanBytes] = scanBytes
	o.contributor.Complete(values)
}

func rangeScanEstimateWithinExactBoundary(processedKeysSize, totalKeys, processedKeys uint64) bool {
	estimateHigh, estimateLow := bits.Mul64(processedKeysSize, totalKeys)
	boundaryHigh, boundaryLow := bits.Mul64(maxExactStatementRUInteger, processedKeys)
	return estimateHigh < boundaryHigh ||
		(estimateHigh == boundaryHigh && estimateLow <= boundaryLow)
}

func (o *rangeScanByteEstimateOwner) abort() {
	if o == nil || o.done {
		return
	}
	o.done = true
	if o.sawUsableDetail {
		o.contributor.Partial()
	} else {
		o.contributor.Unavailable()
	}
}

func (o *cloudSummaryOwner) completeEOF() {
	if o == nil || o.done {
		return
	}
	o.done = true
	if o.failed || o.responses == 0 {
		if o.responses > 0 {
			o.contributor.Partial()
		} else {
			o.contributor.Unavailable()
		}
		return
	}
	var values statementru.UnitValues
	values[statementru.CPUWork] = float64(o.cpuWork)
	o.contributor.Complete(values)
}

func (o *cloudSummaryOwner) abort() {
	if o == nil || o.done {
		return
	}
	o.done = true
	if o.responses > 0 {
		o.contributor.Partial()
	} else {
		o.contributor.Unavailable()
	}
}
