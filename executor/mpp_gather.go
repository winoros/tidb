// Copyright 2020 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func useMPPExecution(ctx sessionctx.Context, tr *plannercore.PhysicalTableReader) bool {
	if !ctx.GetSessionVars().IsMPPAllowed() {
		return false
	}
	_, ok := tr.GetTablePlan().(*plannercore.PhysicalExchangeSender)
	return ok
}

// MPPGather dispatch MPP tasks and read data from root tasks.
type MPPGather struct {
	// following fields are construct needed
	baseExecutor
	is           infoschema.InfoSchema
	originalPlan plannercore.PhysicalPlan
	startTS      uint64

	mppReqs []*kv.MPPDispatchRequest

	respIter distsql.SelectResult

	memTracker *memory.Tracker
}

func (e *MPPGather) appendMPPDispatchReq(pf *plannercore.Fragment) error {
	dagReq, err := constructDAGReq(e.ctx, []plannercore.PhysicalPlan{pf.ExchangeSender}, kv.TiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for i := range pf.ExchangeSender.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	if !pf.IsRoot {
		dagReq.EncodeType = tipb.EncodeType_TypeCHBlock
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeChunk
	}
	for _, mppTask := range pf.ExchangeSender.Tasks {
		if mppTask.PartitionTableIDs != nil {
			err = updateExecutorTableID(context.Background(), dagReq.RootExecutor, true, mppTask.PartitionTableIDs)
		} else {
			err = updateExecutorTableID(context.Background(), dagReq.RootExecutor, true, []int64{mppTask.TableID})
		}
		if err != nil {
			return errors.Trace(err)
		}
		err = e.fixTaskForCTEStorageAndReader(dagReq.RootExecutor, mppTask.Meta)
		if err != nil {
			return err
		}
		pbData, err := dagReq.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		logutil.BgLogger().Info("Dispatch mpp task", zap.Uint64("timestamp", mppTask.StartTs),
			zap.Int64("ID", mppTask.ID), zap.String("address", mppTask.Meta.GetAddress()),
			zap.String("plan", plannercore.ToString(pf.ExchangeSender)))
		req := &kv.MPPDispatchRequest{
			Data:      pbData,
			Meta:      mppTask.Meta,
			ID:        mppTask.ID,
			IsRoot:    pf.IsRoot,
			Timeout:   10,
			SchemaVar: e.is.SchemaMetaVersion(),
			StartTs:   e.startTS,
			State:     kv.MppTaskReady,
		}
		e.mppReqs = append(e.mppReqs, req)
	}
	return nil
}

// fixTaskForCTEStorageAndReader fixes the upstream/downstream tasks for the producers and consumers.
// After we split the fragments. A CTE producer in the fragment will holds all the task address of the consumers.
// For example, the producer has two task on node_1 and node_2. As we know that each consumer also has two task on the same nodes(node_1 and node_2)
// We need to prune address of node_2 for producer's task on node_1 since we just want the producer task on the node_1 only send to the consumer tasks on the node_1.
// And the same for the task on the node_2.
// And the same for the consumer task. We need to prune the unnecessary task address of its producer tasks(i.e. the downstream tasks).
func (e *MPPGather) fixTaskForCTEStorageAndReader(exec *tipb.Executor, meta kv.MPPTaskMeta) error {
	children := make([]*tipb.Executor, 0, 2)
	switch exec.Tp {
	case tipb.ExecType_TypeTableScan, tipb.ExecType_TypePartitionTableScan, tipb.ExecType_TypeIndexScan:
	case tipb.ExecType_TypeSelection:
		children = append(children, exec.Selection.Child)
	case tipb.ExecType_TypeAggregation, tipb.ExecType_TypeStreamAgg:
		children = append(children, exec.Aggregation.Child)
	case tipb.ExecType_TypeTopN:
		children = append(children, exec.TopN.Child)
	case tipb.ExecType_TypeLimit:
		children = append(children, exec.Limit.Child)
	case tipb.ExecType_TypeExchangeSender:
		children = append(children, exec.ExchangeSender.Child)
		if len(exec.ExchangeSender.UpstreamCteTaskMeta) == 0 {
			break
		}
		actualUpStreamTasks := make([][]byte, 0, len(exec.ExchangeSender.UpstreamCteTaskMeta))
		actualTIDs := make([]int64, 0, len(exec.ExchangeSender.UpstreamCteTaskMeta))
		for _, tasksFromOneConsumer := range exec.ExchangeSender.UpstreamCteTaskMeta {
			for _, taskBytes := range tasksFromOneConsumer.EncodedTasks {
				taskMeta := &mpp.TaskMeta{}
				err := taskMeta.Unmarshal(taskBytes)
				if err != nil {
					return err
				}
				if taskMeta.Address != meta.GetAddress() {
					continue
				}
				actualUpStreamTasks = append(actualUpStreamTasks, taskBytes)
				actualTIDs = append(actualTIDs, taskMeta.TaskId)
			}
		}
		logutil.BgLogger().Warn("refine tunnel for cte producer task", zap.String("the final tunnel", fmt.Sprintf("up stream consumer tasks: %v", actualTIDs)))
		exec.ExchangeSender.EncodedTaskMeta = actualUpStreamTasks
	case tipb.ExecType_TypeExchangeReceiver:
		if len(exec.ExchangeReceiver.OriginalCtePrdocuerTaskMeta) == 0 {
			break
		}
		exec.ExchangeReceiver.EncodedTaskMeta = [][]byte{}
		actualTIDs := make([]int64, 0, 4)
		for _, taskBytes := range exec.ExchangeReceiver.OriginalCtePrdocuerTaskMeta {
			taskMeta := &mpp.TaskMeta{}
			err := taskMeta.Unmarshal(taskBytes)
			if err != nil {
				return err
			}
			if taskMeta.Address != meta.GetAddress() {
				continue
			}
			exec.ExchangeReceiver.EncodedTaskMeta = append(exec.ExchangeReceiver.EncodedTaskMeta, taskBytes)
			actualTIDs = append(actualTIDs, taskMeta.TaskId)
		}
		logutil.BgLogger().Warn("refine tunnel for cte consumer task", zap.String("the final tunnel", fmt.Sprintf("down stream producer task: %v", actualTIDs)))
	case tipb.ExecType_TypeJoin:
		children = append(children, exec.Join.Children...)
	case tipb.ExecType_TypeProjection:
		children = append(children, exec.Projection.Child)
	case tipb.ExecType_TypeWindow:
		children = append(children, exec.Window.Child)
	case tipb.ExecType_TypeSort:
		children = append(children, exec.Sort.Child)
	default:
		return errors.Errorf("unknown new tipb protocol %d", exec.Tp)
	}
	for _, child := range children {
		err := e.fixTaskForCTEStorageAndReader(child, meta)
		if err != nil {
			return err
		}
	}
	return nil
}

func collectPlanIDS(plan plannercore.PhysicalPlan, ids []int) []int {
	ids = append(ids, plan.ID())
	for _, child := range plan.Children() {
		ids = collectPlanIDS(child, ids)
	}
	return ids
}

// Open decides the task counts and locations and generate exchange operators for every plan fragment.
// Then dispatch tasks to tiflash stores. If any task fails, it would cancel the rest tasks.
func (e *MPPGather) Open(ctx context.Context) (err error) {
	// TODO: Move the construct tasks logic to planner, so we can see the explain results.
	sender := e.originalPlan.(*plannercore.PhysicalExchangeSender)
	planIDs := collectPlanIDS(e.originalPlan, nil)
	frags, err := plannercore.GenerateRootMPPTasks(e.ctx, e.startTS, sender, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	for _, frag := range frags {
		err = e.appendMPPDispatchReq(frag)
		if err != nil {
			return errors.Trace(err)
		}
	}
	failpoint.Inject("checkTotalMPPTasks", func(val failpoint.Value) {
		if val.(int) != len(e.mppReqs) {
			failpoint.Return(errors.Errorf("The number of tasks is not right, expect %d tasks but actually there are %d tasks", val.(int), len(e.mppReqs)))
		}
	})
	e.respIter, err = distsql.DispatchMPPTasks(ctx, e.ctx, e.mppReqs, e.retFieldTypes, planIDs, e.id, e.startTS, e.memTracker)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Next fills data into the chunk passed by its caller.
func (e *MPPGather) Next(ctx context.Context, chk *chunk.Chunk) error {
	err := e.respIter.Next(ctx, chk)
	return errors.Trace(err)
}

// Close and release the used resources.
func (e *MPPGather) Close() error {
	e.mppReqs = nil
	if e.respIter != nil {
		return e.respIter.Close()
	}
	return nil
}
