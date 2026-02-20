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

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/mvmerge"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
)

func (b *executorBuilder) buildMVDeltaMerge(v *plannercore.MVDeltaMerge) exec.Executor {
	if v.Source == nil {
		b.err = errors.New("MVDeltaMerge source is nil")
		return nil
	}

	sourceExec := b.build(v.Source)
	if b.err != nil {
		return nil
	}
	if sourceExec == nil {
		b.err = errors.New("MVDeltaMerge source executor is nil")
		return nil
	}
	sourceColTypes := sourceExec.RetFieldTypes()
	if v.MVColumnCount < 0 || v.MVColumnCount > len(sourceColTypes) {
		b.err = errors.Errorf(
			"MVDeltaMerge MVColumnCount %d out of source schema range [0,%d]",
			v.MVColumnCount,
			len(sourceColTypes),
		)
		return nil
	}
	deltaAggColStart := v.MVColumnCount
	deltaAggColCount := len(sourceColTypes) - v.MVColumnCount

	targetTbl, ok := b.is.TableByID(context.Background(), v.MVTableID)
	if !ok || targetTbl == nil {
		b.err = errors.Errorf("materialized view table %d not found in infoschema", v.MVTableID)
		return nil
	}
	targetTblInfo := targetTbl.Meta()
	if targetTblInfo == nil {
		b.err = errors.Errorf("materialized view table %d has nil metadata", v.MVTableID)
		return nil
	}

	aggMappings, err := buildMVMergeAggMappings(
		b.ctx.GetExprCtx(),
		v.AggInfos,
		v.MVColumnCount,
		len(sourceColTypes),
		sourceColTypes,
	)
	if err != nil {
		b.err = err
		return nil
	}

	handleCols, err := buildMVMergeTargetHandleCols(targetTblInfo)
	if err != nil {
		b.err = err
		return nil
	}
	oldColIDs, insertColIDs, err := buildMVMergeTargetWritableColIDs(targetTbl, v.MVColumnCount)
	if err != nil {
		b.err = err
		return nil
	}

	return &MVMergeAggExec{
		BaseExecutor:               exec.NewBaseExecutor(b.ctx, v.Schema(), v.ID(), sourceExec),
		AggMappings:                aggMappings,
		DeltaAggColStart:           deltaAggColStart,
		DeltaAggColCount:           deltaAggColCount,
		TargetTable:                targetTbl,
		TargetInfo:                 targetTblInfo,
		TargetHandleCols:           handleCols,
		TargetWritableOldColIDs:    oldColIDs,
		TargetWritableInsertColIDs: insertColIDs,
	}
}

func buildMVMergeAggMappings(
	exprCtx expression.BuildContext,
	aggInfos []mvmerge.AggInfo,
	mvColumnCount int,
	sourceColumnCount int,
	sourceColTypes []*types.FieldType,
) ([]MVMergeAggMapping, error) {
	orderedAggInfos, err := reorderMVAggInfosForExecutor(aggInfos)
	if err != nil {
		return nil, err
	}

	seenOutputCol := make(map[int]struct{}, len(orderedAggInfos))
	mappings := make([]MVMergeAggMapping, 0, len(orderedAggInfos))
	for _, aggInfo := range orderedAggInfos {
		if aggInfo.MVOffset < 0 || aggInfo.MVOffset >= sourceColumnCount {
			return nil, errors.Errorf(
				"aggregate mv offset %d out of source schema range [0,%d)",
				aggInfo.MVOffset,
				sourceColumnCount,
			)
		}
		if _, exists := seenOutputCol[aggInfo.MVOffset]; exists {
			return nil, errors.Errorf("duplicate aggregate output mv offset %d", aggInfo.MVOffset)
		}
		for _, dep := range aggInfo.Dependencies {
			if dep < 0 || dep >= sourceColumnCount {
				return nil, errors.Errorf(
					"aggregate dependency %d out of source schema range [0,%d)",
					dep,
					sourceColumnCount,
				)
			}
			// Dependencies in MV-column area must read values produced by previously merged aggregates.
			if dep < mvColumnCount {
				if _, ok := seenOutputCol[dep]; !ok {
					return nil, errors.Errorf(
						"aggregate mv offset %d depends on mv column %d before it is merged",
						aggInfo.MVOffset,
						dep,
					)
				}
			}
		}

		aggFuncDesc, err := buildMVMergeAggFuncDesc(exprCtx, aggInfo, sourceColTypes)
		if err != nil {
			return nil, err
		}
		mappings = append(mappings, MVMergeAggMapping{
			AggFunc:         aggFuncDesc,
			ColID:           []int{aggInfo.MVOffset},
			DependencyColID: append([]int(nil), aggInfo.Dependencies...),
		})
		seenOutputCol[aggInfo.MVOffset] = struct{}{}
	}
	return mappings, nil
}

func reorderMVAggInfosForExecutor(aggInfos []mvmerge.AggInfo) ([]mvmerge.AggInfo, error) {
	if len(aggInfos) == 0 {
		return nil, errors.New("MVDeltaMerge requires at least one aggregate")
	}

	ordered := make([]mvmerge.AggInfo, 0, len(aggInfos))
	countStarIdx := -1
	for i, aggInfo := range aggInfos {
		if aggInfo.Kind != mvmerge.AggCountStar {
			continue
		}
		if countStarIdx >= 0 {
			return nil, errors.New("MVDeltaMerge expects exactly one COUNT(*) aggregate")
		}
		countStarIdx = i
	}
	if countStarIdx < 0 {
		return nil, errors.New("MVDeltaMerge expects COUNT(*) aggregate")
	}
	ordered = append(ordered, aggInfos[countStarIdx])

	kindOrders := []mvmerge.AggKind{
		mvmerge.AggCount,
		mvmerge.AggSum,
		mvmerge.AggMin,
		mvmerge.AggMax,
	}
	for _, kind := range kindOrders {
		for i, aggInfo := range aggInfos {
			if i == countStarIdx || aggInfo.Kind != kind {
				continue
			}
			ordered = append(ordered, aggInfo)
		}
	}
	if len(ordered) != len(aggInfos) {
		return nil, errors.New("MVDeltaMerge contains unsupported aggregate kind")
	}
	return ordered, nil
}

func buildMVMergeAggFuncDesc(
	exprCtx expression.BuildContext,
	aggInfo mvmerge.AggInfo,
	sourceColTypes []*types.FieldType,
) (*aggregation.AggFuncDesc, error) {
	switch aggInfo.Kind {
	case mvmerge.AggCountStar:
		return aggregation.NewAggFuncDesc(exprCtx, ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	case mvmerge.AggCount:
		if aggInfo.MVOffset < 0 || aggInfo.MVOffset >= len(sourceColTypes) {
			return nil, errors.Errorf("COUNT mv offset %d out of range [0,%d)", aggInfo.MVOffset, len(sourceColTypes))
		}
		return aggregation.NewAggFuncDesc(exprCtx, ast.AggFuncCount, []expression.Expression{
			&expression.Column{Index: aggInfo.MVOffset, RetType: sourceColTypes[aggInfo.MVOffset]},
		}, false)
	case mvmerge.AggSum:
		if aggInfo.MVOffset < 0 || aggInfo.MVOffset >= len(sourceColTypes) {
			return nil, errors.Errorf("SUM mv offset %d out of range [0,%d)", aggInfo.MVOffset, len(sourceColTypes))
		}
		return aggregation.NewAggFuncDesc(exprCtx, ast.AggFuncSum, []expression.Expression{
			&expression.Column{Index: aggInfo.MVOffset, RetType: sourceColTypes[aggInfo.MVOffset]},
		}, false)
	case mvmerge.AggMin:
		if aggInfo.MVOffset < 0 || aggInfo.MVOffset >= len(sourceColTypes) {
			return nil, errors.Errorf("MIN mv offset %d out of range [0,%d)", aggInfo.MVOffset, len(sourceColTypes))
		}
		return aggregation.NewAggFuncDesc(exprCtx, ast.AggFuncMin, []expression.Expression{
			&expression.Column{Index: aggInfo.MVOffset, RetType: sourceColTypes[aggInfo.MVOffset]},
		}, false)
	case mvmerge.AggMax:
		if aggInfo.MVOffset < 0 || aggInfo.MVOffset >= len(sourceColTypes) {
			return nil, errors.Errorf("MAX mv offset %d out of range [0,%d)", aggInfo.MVOffset, len(sourceColTypes))
		}
		return aggregation.NewAggFuncDesc(exprCtx, ast.AggFuncMax, []expression.Expression{
			&expression.Column{Index: aggInfo.MVOffset, RetType: sourceColTypes[aggInfo.MVOffset]},
		}, false)
	default:
		return nil, errors.Errorf("unsupported aggregate kind %v in MVDeltaMerge", aggInfo.Kind)
	}
}

func buildMVMergeTargetHandleCols(tblInfo *model.TableInfo) (plannerutil.HandleCols, error) {
	if tblInfo == nil {
		return nil, errors.New("MVDeltaMerge target table metadata is nil")
	}

	if tblInfo.PKIsHandle {
		pkCol := tblInfo.GetPkColInfo()
		if pkCol == nil {
			return nil, errors.Errorf("MV table %s declares PKIsHandle but has no PK column", tblInfo.Name.O)
		}
		intCol := &expression.Column{
			Index:   pkCol.Offset,
			ID:      pkCol.ID,
			RetType: &pkCol.FieldType,
		}
		return plannerutil.NewIntHandleCols(intCol), nil
	}

	if tblInfo.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		if pkIdx == nil {
			return nil, errors.Errorf("MV table %s uses common handle but has no primary index", tblInfo.Name.O)
		}
		tableColumns := make([]*expression.Column, len(tblInfo.Columns))
		for i, col := range tblInfo.Columns {
			tableColumns[i] = &expression.Column{
				Index:   col.Offset,
				ID:      col.ID,
				RetType: &col.FieldType,
			}
		}
		return plannerutil.NewCommonHandleCols(tblInfo, pkIdx, tableColumns), nil
	}

	return nil, errors.Errorf("MV table %s requires primary key for FAST refresh merge", tblInfo.Name.O)
}

func buildMVMergeTargetWritableColIDs(tbl table.Table, mvColumnCount int) ([]int, []int, error) {
	if tbl == nil {
		return nil, nil, errors.New("MVDeltaMerge target table is nil")
	}
	writableCols := tbl.WritableCols()
	oldColIDs := make([]int, len(writableCols))
	insertColIDs := make([]int, len(writableCols))
	for i, col := range writableCols {
		if col.Offset < 0 || col.Offset >= mvColumnCount {
			return nil, nil, errors.Errorf(
				"MV writable column %s offset %d out of MV source range [0,%d)",
				col.Name.O,
				col.Offset,
				mvColumnCount,
			)
		}
		oldColIDs[i] = col.Offset
		insertColIDs[i] = col.Offset
	}
	return oldColIDs, insertColIDs, nil
}
