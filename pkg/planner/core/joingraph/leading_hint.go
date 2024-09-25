// Copyright 2024 PingCAP, Inc.
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

package joingraph

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/hint"
)

func (b *builder) dealWithLeadingHint(sctx sessionctx.Context) {
	leadingHintInfo, hasDiffLeadingHint := hint.CheckAndGenerateLeadingHint(b.leadingHintInfo)
	if hasDiffLeadingHint {
		sctx.GetSessionVars().StmtCtx.SetHintWarning(
			"We can only use one leading hint at most, when multiple leading hints are used, all leading hints will be invalid",
		)
	}
	if leadingHintInfo != nil && len(leadingHintInfo.LeadingJoinOrder) > 0 {

	}
}

// extractTableAlias returns table alias of the base.LogicalPlan's columns.
// It will return nil when there are multiple table alias, because the alias is only used to check if
// the base.LogicalPlan Match some optimizer hints, and hints are not expected to take effect in this case.
func extractTableAlias(p base.Plan, parentOffset int) *hint.HintedTable {
	if len(p.OutputNames()) > 0 && p.OutputNames()[0].TblName.L != "" {
		firstName := p.OutputNames()[0]
		for _, name := range p.OutputNames() {
			if name.TblName.L != firstName.TblName.L ||
				(name.DBName.L != "" && firstName.DBName.L != "" && name.DBName.L != firstName.DBName.L) { // DBName can be nil, see #46160
				return nil
			}
		}
		qbOffset := p.QueryBlockOffset()
		var blockAsNames []ast.HintTable
		if p := p.SCtx().GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
			blockAsNames = *p
		}
		// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
		if qbOffset != parentOffset && blockAsNames != nil && blockAsNames[qbOffset].TableName.L != "" {
			qbOffset = parentOffset
		}
		dbName := firstName.DBName
		if dbName.L == "" {
			dbName = model.NewCIStr(p.SCtx().GetSessionVars().CurrentDB)
		}
		return &hint.HintedTable{DBName: dbName, TblName: firstName.TblName, SelectOffset: qbOffset}
	}
	return nil
}

func (b *builder) buildJoinByLeadingHint(sctx sessionctx.Context, leading hint.PlanHints) {
	var queryBlockNames []ast.HintTable
	leadingJoinNodes := make([]*joinNode, 0, len(leading.LeadingJoinOrder))
	if p := sctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
		queryBlockNames = *p
	}
	coveredByLeading := bitset.New(uint(len(b.vertexes)))
	for _, hintTbl := range leading.LeadingJoinOrder {
		match := false
		for i, leaf := range b.vertexes {
			if coveredByLeading.Test(uint(i)) {
				continue
			}
			tableAlias := extractTableAlias(leaf, leaf.QueryBlockOffset())
			if tableAlias == nil {
				continue
			}
			if (hintTbl.DBName.L == tableAlias.DBName.L || hintTbl.DBName.L == "*") && hintTbl.TblName.L == tableAlias.TblName.L && hintTbl.SelectOffset == tableAlias.SelectOffset {
				match = true
				leadingJoinNodes = append(leadingJoinNodes, &joinNode{p: leaf, vertexes: *bitset.New(uint(i)).Set(uint(i))})
				coveredByLeading.Set(uint(i))
				break
			}
		}
		if match {
			continue
		}

		// consider query block alias: select /*+ leading(t1, t2) */ * from (select ...) t1, t2 ...
		groupIdx := -1
		for i, leaf := range b.vertexes {
			if coveredByLeading.Test(uint(i)) {
				continue
			}
			blockOffset := leaf.QueryBlockOffset()
			if blockOffset > 1 && blockOffset < len(queryBlockNames) {
				blockName := queryBlockNames[blockOffset]
				if hintTbl.DBName.L == blockName.DBName.L && hintTbl.TblName.L == blockName.TableName.L {
					// this can happen when multiple join groups are from the same block, for example:
					//   select /*+ leading(tx) */ * from (select * from t1, t2 ...) tx, ...
					// `tx` is split to 2 join groups `t1` and `t2`, and they have the same block offset.
					// TODO: currently we skip this case for simplification, we can support it in the future.
					if groupIdx != -1 {
						groupIdx = -1
						break
					}
					groupIdx = i
				}
			}
		}
		if groupIdx == -1 {
			continue
		}
		leadingJoinNodes = append(leadingJoinNodes, &joinNode{p: b.vertexes[groupIdx], vertexes: *bitset.New(uint(groupIdx)).Set(uint(groupIdx))})
		coveredByLeading.Set(uint(groupIdx))
	}
	for len(leadingJoinNodes) >= 2 {
		join := b.CheckAndMakeJoin(leadingJoinNodes[0], leadingJoinNodes[1])
		if join == nil {
			leadingJoinNodes[0].p.SCtx().GetSessionVars().StmtCtx.SetHintWarning(
				"leading hint is inapplicable, check if the leading hint table is valid")
			return
		}
	}
}
