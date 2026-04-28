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

package logicalop

import (
	"context"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/cardinality"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/planner/util/utilfuncp"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/size"
)

// LogicalCTE is for CTE.
type LogicalCTE struct {
	LogicalSchemaProducer

	Cte       *CTEClass
	CteAsName ast.CIStr
	CteName   ast.CIStr
	SeedStat  *property.StatsInfo

	OnlyUsedAsStorage bool
}

// Init only assigns type and context.
func (p LogicalCTE) Init(ctx base.PlanContext, offset int) *LogicalCTE {
	p.BaseLogicalPlan = NewBaseLogicalPlan(ctx, plancodec.TypeCTE, &p, offset)
	return &p
}

// CTEClass holds the information and plan for a CTE. Most of the fields in this struct are the same as cteInfo.
// But the cteInfo is used when building the plan, and CTEClass is used also for building the executor.
type CTEClass struct {
	// The union between seed part and recursive part is DISTINCT or DISTINCT ALL.
	IsDistinct bool
	// SeedPartLogicalPlan and RecursivePartLogicalPlan are the logical plans for the seed part and recursive part of this CTE.
	SeedPartLogicalPlan base.LogicalPlan
	// RecursivePartLogicalPlan is nil if this CTE is not a recursive CTE.
	RecursivePartLogicalPlan base.LogicalPlan
	// SeedPartPhysicalPlan and RecursivePartPhysicalPlan are the physical plans for the seed part and recursive part of this CTE.
	SeedPartPhysicalPlan      base.PhysicalPlan
	RecursivePartPhysicalPlan base.PhysicalPlan
	// storageID for this CTE.
	IDForStorage int
	// OptFlag is the OptFlag for the whole CTE.
	OptFlag   uint64
	HasLimit  bool
	LimitBeg  uint64
	LimitEnd  uint64
	IsInApply bool
	// PushDownPredicates may be push-downed by different references.
	PushDownPredicates []expression.Expression
	ColumnMap          map[string]*expression.Column
	IsOuterMostCTE     bool
	UseSequence        bool
}

const emptyCTEClassSize = int64(unsafe.Sizeof(CTEClass{}))

// MemoryUsage return the memory usage of CTEClass
func (cc *CTEClass) MemoryUsage() (sum int64) {
	if cc == nil {
		return
	}

	sum = emptyCTEClassSize
	if cc.SeedPartPhysicalPlan != nil {
		sum += cc.SeedPartPhysicalPlan.MemoryUsage()
	}
	if cc.RecursivePartPhysicalPlan != nil {
		sum += cc.RecursivePartPhysicalPlan.MemoryUsage()
	}

	for _, expr := range cc.PushDownPredicates {
		sum += expr.MemoryUsage()
	}
	for key, val := range cc.ColumnMap {
		sum += size.SizeOfString + int64(len(key)) + size.SizeOfPointer + val.MemoryUsage()
	}
	return
}

// *************************** start implementation of logicalPlan interface ***************************

// HashCode inherits the BaseLogicalPlan.<0th> implementation.

// PredicatePushDown implements base.LogicalPlan.<1st> interface.
func (p *LogicalCTE) PredicatePushDown(predicates []expression.Expression) ([]expression.Expression, base.LogicalPlan, error) {
	if p.OnlyUsedAsStorage {
		return p.predicatePushDownStorage(predicates)
	}
	if p.Cte.RecursivePartLogicalPlan != nil {
		return predicates, p.Self(), nil
	}
	if !p.Cte.UseSequence && !p.Cte.IsOuterMostCTE {
		return predicates, p.Self(), nil
	}
	pushedPredicates := make([]expression.Expression, len(predicates))
	copy(pushedPredicates, predicates)
	// The filter might change the correlated status of the cte.
	// We forbid the push down that makes the change for now.
	// Will support it later.
	if !p.Cte.IsInApply {
		for i := len(pushedPredicates) - 1; i >= 0; i-- {
			if len(expression.ExtractCorColumns(pushedPredicates[i])) == 0 {
				continue
			}
			pushedPredicates = append(pushedPredicates[0:i], pushedPredicates[i+1:]...)
		}
	}
	if len(pushedPredicates) == 0 {
		p.Cte.PushDownPredicates = append(p.Cte.PushDownPredicates, expression.NewOne())
		return predicates, p.Self(), nil
	}
	newPred := make([]expression.Expression, 0, len(predicates))
	for i := range pushedPredicates {
		newPred = append(newPred, pushedPredicates[i].Clone())
		newPred[i] = ruleutil.ResolveExprAndReplace(newPred[i], p.Cte.ColumnMap)
	}
	p.Cte.PushDownPredicates = append(p.Cte.PushDownPredicates, expression.ComposeCNFCondition(p.SCtx().GetExprCtx(), newPred...))
	return predicates, p.Self(), nil
}

func (p *LogicalCTE) predicatePushDownStorage(predicates []expression.Expression) ([]expression.Expression, base.LogicalPlan, error) {
	if p.Cte.RecursivePartLogicalPlan == nil && len(p.Cte.PushDownPredicates) > 0 {
		newCond := expression.ComposeDNFCondition(p.SCtx().GetExprCtx(), p.Cte.PushDownPredicates...)
		seed := p.Cte.SeedPartLogicalPlan
		if p.ChildLen() > 0 {
			seed = p.Children()[0]
		}
		newSel := LogicalSelection{Conditions: []expression.Expression{newCond}}.Init(p.SCtx(), seed.QueryBlockOffset())
		newSel.SetChildren(seed)
		if p.ChildLen() > 0 {
			p.SetChild(0, newSel)
		} else {
			p.SetChildren(newSel)
		}
		p.Cte.PushDownPredicates = p.Cte.PushDownPredicates[:0]
		p.Cte.OptFlag = ruleutil.SetPredicatePushDownFlag(p.Cte.OptFlag)
	}
	for i, child := range p.Children() {
		_, newChild, err := child.PredicatePushDown(nil)
		if err != nil {
			return nil, p.Self(), err
		}
		p.SetChild(i, newChild)
	}
	return predicates, p.Self(), nil
}

// PruneColumns implements the base.LogicalPlan.<2nd> interface.
// LogicalCTE just does an empty function call. Its logical optimization is an individual phase.
func (p *LogicalCTE) PruneColumns(parentUsedCols []*expression.Column) (base.LogicalPlan, error) {
	if p.OnlyUsedAsStorage && p.Cte.RecursivePartLogicalPlan == nil && p.ChildLen() > 0 {
		child, err := p.Children()[0].PruneColumns(p.storageSeedUsedColumns(parentUsedCols))
		if err != nil {
			return nil, err
		}
		p.SetChild(0, child)
	}
	return p, nil
}

func (p *LogicalCTE) storageSeedUsedColumns(parentUsedCols []*expression.Column) []*expression.Column {
	seedUsedCols := make([]*expression.Column, 0, len(parentUsedCols))
	for _, col := range parentUsedCols {
		if seedCol, ok := p.Cte.ColumnMap[string(col.HashCode())]; ok {
			seedUsedCols = append(seedUsedCols, seedCol)
			continue
		}
		if idx := p.Schema().ColumnIndex(col); idx >= 0 && idx < p.Children()[0].Schema().Len() {
			seedUsedCols = append(seedUsedCols, p.Children()[0].Schema().Columns[idx])
			continue
		}
		seedUsedCols = append(seedUsedCols, col)
	}
	return seedUsedCols
}

// BuildKeyInfo inherits the BaseLogicalPlan.<4th> implementation.

// PushDownTopN implements the base.LogicalPlan.<5th> interface.
func (p *LogicalCTE) PushDownTopN(topNLogicalPlan base.LogicalPlan) base.LogicalPlan {
	var topN *LogicalTopN
	if topNLogicalPlan != nil {
		topN = topNLogicalPlan.(*LogicalTopN)
	}
	if topN != nil {
		return topN.AttachChild(p)
	}
	return p
}

// DeriveTopN inherits BaseLogicalPlan.LogicalPlan.<6th> implementation.

// PredicateSimplification inherits BaseLogicalPlan.LogicalPlan.<7th> implementation.

// ConstantPropagation inherits BaseLogicalPlan.LogicalPlan.<8th> implementation.

// PullUpConstantPredicates inherits BaseLogicalPlan.LogicalPlan.<9th> implementation.

// RecursiveDeriveStats implements BaseLogicalPlan.LogicalPlan.<10th> interface.
func (p *LogicalCTE) RecursiveDeriveStats(colGroups [][]*expression.Column) (*property.StatsInfo, bool, error) {
	if !p.OnlyUsedAsStorage || p.Cte.RecursivePartLogicalPlan == nil || p.ChildLen() < 2 {
		return p.BaseLogicalPlan.RecursiveDeriveStats(colGroups)
	}
	cumColGroups := p.ExtractColGroups(colGroups)
	seedStats, seedReload, err := p.Children()[0].RecursiveDeriveStats(cumColGroups)
	if err != nil {
		return nil, false, err
	}
	if p.SeedStat != nil {
		*p.SeedStat = *seedStats
	}
	recurStats, recurReload, err := p.Children()[1].RecursiveDeriveStats(cumColGroups)
	if err != nil {
		return nil, false, err
	}
	childStats := []*property.StatsInfo{seedStats, recurStats}
	childSchemas := []*expression.Schema{p.Children()[0].Schema(), p.Children()[1].Schema()}
	return p.DeriveStats(childStats, p.Schema(), childSchemas, []bool{seedReload, recurReload})
}

// DeriveStats implements the base.LogicalPlan.<11th> interface.
func (p *LogicalCTE) DeriveStats(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchemas []*expression.Schema, reloads []bool) (*property.StatsInfo, bool, error) {
	var reload bool
	for _, one := range reloads {
		reload = reload || one
	}
	if !reload && p.StatsInfo() != nil {
		return p.StatsInfo(), false, nil
	}

	if p.Cte.UseSequence {
		return p.deriveStatsFromSequence(childStats, selfSchema, childSchemas)
	}

	var err error
	if p.Cte.SeedPartPhysicalPlan == nil {
		// Build push-downed predicates.
		if len(p.Cte.PushDownPredicates) > 0 {
			newCond := expression.ComposeDNFCondition(p.SCtx().GetExprCtx(), p.Cte.PushDownPredicates...)
			newSel := LogicalSelection{Conditions: []expression.Expression{newCond}}.Init(p.SCtx(), p.Cte.SeedPartLogicalPlan.QueryBlockOffset())
			newSel.SetChildren(p.Cte.SeedPartLogicalPlan)
			p.Cte.SeedPartLogicalPlan = newSel
			p.Cte.OptFlag = ruleutil.SetPredicatePushDownFlag(p.Cte.OptFlag)
		}
		p.Cte.SeedPartLogicalPlan, p.Cte.SeedPartPhysicalPlan, _, err = utilfuncp.DoOptimize(context.TODO(), p.SCtx(), p.Cte.OptFlag, p.Cte.SeedPartLogicalPlan)
		if err != nil {
			return nil, false, err
		}
	}
	if p.OnlyUsedAsStorage {
		p.SetChildren(p.Cte.SeedPartLogicalPlan)
	}
	resStat := p.Cte.SeedPartPhysicalPlan.StatsInfo()
	// Changing the pointer so that SeedStat in LogicalCTETable can get the new stat.
	*p.SeedStat = *resStat
	p.SetStats(&property.StatsInfo{
		RowCount: resStat.RowCount,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	for i, col := range selfSchema.Columns {
		p.StatsInfo().ColNDVs[col.UniqueID] += resStat.ColNDVs[p.Cte.SeedPartLogicalPlan.Schema().Columns[i].UniqueID]
	}
	if p.Cte.RecursivePartLogicalPlan != nil {
		if p.Cte.RecursivePartPhysicalPlan == nil {
			// TODO: parallel apply inside a recursive CTE body produces incorrect results
			// (grandchildren are silently dropped) because the CTE iteration model shares
			// mutable state (the working-table buffer) across goroutines, causing rows from
			// deeper recursion levels to be lost.  Disable parallel apply for the recursive
			// body until the executor is fixed to handle this safely.
			// See: TestLateralHierarchyParallelApply (flat query verifies concurrency > 1
			// for non-recursive LATERAL; recursive correctness is tracked separately).
			vars := p.SCtx().GetSessionVars()
			savedParallelApply := vars.EnableParallelApply
			vars.EnableParallelApply = false
			defer func() { vars.EnableParallelApply = savedParallelApply }()
			p.Cte.RecursivePartLogicalPlan, p.Cte.RecursivePartPhysicalPlan, _, err = utilfuncp.DoOptimize(context.TODO(), p.SCtx(), p.Cte.OptFlag, p.Cte.RecursivePartLogicalPlan)
			if err != nil {
				return nil, false, err
			}
		}
		recurStat := p.Cte.RecursivePartLogicalPlan.StatsInfo()
		for i, col := range selfSchema.Columns {
			p.StatsInfo().ColNDVs[col.UniqueID] += recurStat.ColNDVs[p.Cte.RecursivePartLogicalPlan.Schema().Columns[i].UniqueID]
		}
		if p.Cte.IsDistinct {
			p.StatsInfo().RowCount, _ = cardinality.EstimateColsNDVWithMatchedLen(
				p.SCtx(), p.Schema().Columns, p.Schema(), p.StatsInfo())
		} else {
			p.StatsInfo().RowCount += recurStat.RowCount
		}
	}
	return p.StatsInfo(), true, nil
}

func (p *LogicalCTE) deriveStatsFromSequence(childStats []*property.StatsInfo, selfSchema *expression.Schema, childSchemas []*expression.Schema) (*property.StatsInfo, bool, error) {
	seedStats := p.Cte.SeedPartLogicalPlan.StatsInfo()
	seedSchema := p.Cte.SeedPartLogicalPlan.Schema()
	if len(childStats) > 0 && childStats[0] != nil {
		seedStats = childStats[0]
	}
	if len(childSchemas) > 0 && childSchemas[0] != nil {
		seedSchema = childSchemas[0]
	}
	if seedStats == nil {
		var err error
		seedStats, _, err = p.Cte.SeedPartLogicalPlan.RecursiveDeriveStats(nil)
		if err != nil {
			return nil, false, err
		}
	}
	p.SetStats(&property.StatsInfo{
		RowCount: seedStats.RowCount,
		ColNDVs:  make(map[int64]float64, selfSchema.Len()),
	})
	if p.SeedStat != nil {
		*p.SeedStat = *seedStats
	}
	for i, col := range selfSchema.Columns {
		p.StatsInfo().ColNDVs[col.UniqueID] += seedStats.ColNDVs[seedSchema.Columns[i].UniqueID]
	}
	if p.Cte.RecursivePartLogicalPlan != nil {
		recurStats := p.Cte.RecursivePartLogicalPlan.StatsInfo()
		recurSchema := p.Cte.RecursivePartLogicalPlan.Schema()
		if len(childStats) > 1 && childStats[1] != nil {
			recurStats = childStats[1]
		}
		if len(childSchemas) > 1 && childSchemas[1] != nil {
			recurSchema = childSchemas[1]
		}
		if recurStats == nil {
			var err error
			recurStats, _, err = p.Cte.RecursivePartLogicalPlan.RecursiveDeriveStats(nil)
			if err != nil {
				return nil, false, err
			}
		}
		for i, col := range selfSchema.Columns {
			p.StatsInfo().ColNDVs[col.UniqueID] += recurStats.ColNDVs[recurSchema.Columns[i].UniqueID]
		}
		if p.Cte.IsDistinct {
			p.StatsInfo().RowCount, _ = cardinality.EstimateColsNDVWithMatchedLen(
				p.SCtx(), p.Schema().Columns, p.Schema(), p.StatsInfo())
		} else {
			p.StatsInfo().RowCount += recurStats.RowCount
		}
	}
	return p.StatsInfo(), true, nil
}

// ExtractColGroups inherits BaseLogicalPlan.LogicalPlan.<12th> implementation.

// PreparePossibleProperties implements base.LogicalPlan.<13th> interface.
func (p *LogicalCTE) PreparePossibleProperties(_ *expression.Schema, childrenProperties ...*base.PossiblePropertiesInfo) *base.PossiblePropertiesInfo {
	if len(childrenProperties) > 0 {
		hasTiFlash := false
		hasValidChild := false
		for _, child := range childrenProperties {
			if child == nil {
				continue
			}
			if !hasValidChild {
				hasTiFlash = child.HasTiFlash
				hasValidChild = true
				continue
			}
			hasTiFlash = hasTiFlash && child.HasTiFlash
		}
		if hasValidChild {
			p.hasTiFlash = hasTiFlash
			return &base.PossiblePropertiesInfo{HasTiFlash: p.hasTiFlash}
		}
	}

	hasTiFlash := false
	if p.Cte != nil && p.Cte.SeedPartLogicalPlan != nil {
		hasTiFlash = GetHasTiFlash(p.Cte.SeedPartLogicalPlan)
	}
	p.hasTiFlash = hasTiFlash
	return &base.PossiblePropertiesInfo{HasTiFlash: p.hasTiFlash}
}

// ExtractCorrelatedCols implements the base.LogicalPlan.<15th> interface.
func (p *LogicalCTE) ExtractCorrelatedCols() []*expression.CorrelatedColumn {
	corCols := coreusage.ExtractCorrelatedCols4LogicalPlan(p.Cte.SeedPartLogicalPlan)
	if p.Cte.RecursivePartLogicalPlan != nil {
		corCols = append(corCols, coreusage.ExtractCorrelatedCols4LogicalPlan(p.Cte.RecursivePartLogicalPlan)...)
	}
	return corCols
}

// MaxOneRow inherits BaseLogicalPlan.LogicalPlan.<16th> implementation.

// Children inherits BaseLogicalPlan.LogicalPlan.<17th> implementation.

// SetChildren implements BaseLogicalPlan.LogicalPlan.<18th> interface.
func (p *LogicalCTE) SetChildren(children ...base.LogicalPlan) {
	p.BaseLogicalPlan.SetChildren(children...)
	p.syncStorageChildrenToCTEClass(children...)
}

// SetChild implements BaseLogicalPlan.LogicalPlan.<19th> interface.
func (p *LogicalCTE) SetChild(i int, child base.LogicalPlan) {
	p.BaseLogicalPlan.SetChild(i, child)
	if !p.OnlyUsedAsStorage {
		return
	}
	if i == 0 {
		p.syncSeedChild(child)
	} else if i == 1 {
		p.syncRecursiveChild(child)
	}
}

func (p *LogicalCTE) syncStorageChildrenToCTEClass(children ...base.LogicalPlan) {
	if !p.OnlyUsedAsStorage || p.Cte == nil {
		return
	}
	if len(children) > 0 {
		p.syncSeedChild(children[0])
	}
	if len(children) > 1 {
		p.syncRecursiveChild(children[1])
	}
}

func (p *LogicalCTE) syncSeedChild(child base.LogicalPlan) {
	if p.Cte.SeedPartLogicalPlan != child {
		p.Cte.SeedPartLogicalPlan = child
		p.Cte.SeedPartPhysicalPlan = nil
	}
}

func (p *LogicalCTE) syncRecursiveChild(child base.LogicalPlan) {
	if p.Cte.RecursivePartLogicalPlan != child {
		p.Cte.RecursivePartLogicalPlan = child
		p.Cte.RecursivePartPhysicalPlan = nil
	}
}

// RollBackTaskMap inherits BaseLogicalPlan.LogicalPlan.<20th> implementation.

// CanPushToCop inherits BaseLogicalPlan.LogicalPlan.<21st> implementation.

// ExtractFD inherits BaseLogicalPlan.LogicalPlan.<22nd> implementation.

// GetBaseLogicalPlan inherits BaseLogicalPlan.LogicalPlan.<23rd> implementation.

// ConvertOuterToInnerJoin inherits BaseLogicalPlan.LogicalPlan.<24th> implementation.

// *************************** end implementation of logicalPlan interface ***************************
