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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intset"
)

type builder struct {
	vertexes []base.LogicalPlan
	ops      []op
	// We split the operators to two pars: innerOps and nonInnerOps.
	// It's because that the proper place of a inner join's filter is where we add a outer join.
	// e.g. t1 inner join t2 on t1.a = t2.a left join t3 on t1.b=t3.b where func(t3.c)
	//      The func(t3.c) is not null rejective so the outer join remains.
	//      And since func(t3.c) is not a join key, we will put it into the innerJoinFreeEdges.
	//      If the best join reorder is t1left join t3 inner join t2.
	//      Then we'll find that the func(t3.c) should be added to the place where we add the left join.
	//      But it cannot directly be added to it. We need to construct Selection(func(t3.c))->LeftJoin.
	//      So for convenience, we split the operators to innerOps and nonInnerOps.
	innerOps    *intset.FastIntSet
	nonInnerOps *intset.FastIntSet
	vertexIdx   uint
	opIdx       uint

	leadingHintInfo       []*hint.PlanHints
	joinMethodHintiNfo    map[int]*joinMethodHint
	nodesCoveredByLeading *intset.FastIntSet

	innerJoinFreeEdges []specialFreeEdge

	// The two fields is used for MakeJoin
	innerOpsBuffer        *intset.FastIntSet
	freeEdgeBuffer        *intset.FastIntSet
	freeEdgeCoveredBuffer bitset.BitSet
}

type joinMethodHint struct {
	preferredJoinMethod uint
	joinMethodHintInfo  *hint.PlanHints
}

type op struct {
	joinType      logicalop.JoinType
	selfJoinKeys  []*expression.ScalarFunction
	extraFilters  []expression.Expression
	leftVertexes  bitset.BitSet
	rightVertexes bitset.BitSet
	leftOps       bitset.BitSet
	rightOps      bitset.BitSet

	// selfCovered stores the vertexes used in the join key and filters.
	// It's used to check that this filter should already be used.
	// Because we can not know which join key/filter is used when doing reorder by DP.
	selfCovered bitset.BitSet

	ses bitset.BitSet
	tes bitset.BitSet

	conflictRules []conflictRule
}

// specialFreeEdge stores the other conditions.
// We don't build cartesian product yet. So we don't treat those filters as edges.
// Once a inner join is built, we check that whether there's some can be added to the new inner join.
type specialFreeEdge struct {
	covered bitset.BitSet
	filters expression.Expression
}

type conflictRule struct {
	from bitset.BitSet
	to   bitset.BitSet
}

type joinNode struct {
	p        base.LogicalPlan
	cumCost  float64
	vertexes bitset.BitSet
}

func (b *builder) Init(join base.LogicalPlan) {
	vertexCap, opCap, leadingHintCap, freeEdgeCap := b.quickCheckCap(join)
	b.vertexes = make([]base.LogicalPlan, 0, vertexCap)
	b.ops = make([]op, 0, opCap)
	b.leadingHintInfo = make([]*hint.PlanHints, 0, leadingHintCap)
	b.innerJoinFreeEdges = make([]specialFreeEdge, 0, freeEdgeCap)
}

func (b *builder) quickCheckCap(p base.LogicalPlan) (vCap, oCap, leadingCap, freeEdgeCap int) {
	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		if b.JoinIsNotReorderable(x) {
			return 1, 0, 0, 0
		}
		vCapL, oCapL, leadingCapL, freeEdgeCapL := b.quickCheckCap(x.Children()[0])
		vCapR, oCapR, leadingCapR, freeEdgeCapR := b.quickCheckCap(x.Children()[1])
		vCap = vCapL + vCapR
		oCap = oCapL + oCapR
		leadingCap = leadingCapL + leadingCapR
		freeEdgeCap = freeEdgeCapL + freeEdgeCapR

		if x.PreferJoinOrder {
			leadingCap++
		}
		if x.JoinType == logicalop.InnerJoin {
			freeEdgeCap += len(x.OtherConditions)
			oCap += len(x.EqualConditions)
		}
		return
	default:
		return 1, 0, 0, 0
	}
}

func (b *builder) JoinIsNotReorderable(p *logicalop.LogicalJoin) bool {
	// If the join has set the preferred join type and the advanced join hint is disabled, we will not consider the join order.
	if p.PreferJoinType > uint(0) && !p.SCtx().GetSessionVars().EnableAdvancedJoinHint {
		return true
	}
	// If the join is a straight join, we will not consider the join order.
	if p.StraightJoin {
		return true
	}
	// If the join has no equal condition, we will not consider the join order.
	if len(p.EqualConditions) == 0 {
		return true
	}
	return false
}

func (b *builder) ExtractGraph(p base.LogicalPlan) (bitset.BitSet, bitset.BitSet) {
	switch join := p.(type) {
	case *logicalop.LogicalJoin:
		if join.PreferJoinOrder {
			b.leadingHintInfo = append(b.leadingHintInfo, join.HintInfo)
		}
		if b.JoinIsNotReorderable(join) {
			b.vertexes = append(b.vertexes, p)
			b.vertexIdx++
			return *bitset.New(b.vertexIdx).Set(b.vertexIdx - 1), *bitset.New(0)
		}

		leftVertexes, leftOps := b.ExtractGraph(join.Children()[0])
		rightVertexes, rightOps := b.ExtractGraph(join.Children()[1])
		b.newOPEdge(join, leftVertexes, rightVertexes, leftOps, rightOps)

		if join.SCtx().GetSessionVars().EnableAdvancedJoinHint && join.PreferJoinOrder {
			if join.LeftPreferJoinType > 0 {
				b.joinMethodHintiNfo[join.Children()[0].ID()] = &joinMethodHint{
					join.LeftPreferJoinType,
					join.HintInfo,
				}
			}
			if join.RightPreferJoinType > 0 {
				b.joinMethodHintiNfo[join.Children()[1].ID()] = &joinMethodHint{
					join.RightPreferJoinType,
					join.HintInfo,
				}
			}
		}

		return *leftVertexes.Union(&rightVertexes), *leftOps.Union(&rightOps).Set(b.opIdx - 1)
	default:
		b.vertexes = append(b.vertexes, p)
		b.vertexIdx++
		return *bitset.New(b.vertexIdx).Set(b.vertexIdx - 1), *bitset.New(0)
	}
}

func (b *builder) newOPEdge(join *logicalop.LogicalJoin, leftVertexes, rightVertexes bitset.BitSet, leftOps, rightOps bitset.BitSet) {
	if join.JoinType == logicalop.InnerJoin {
		for i := range join.EqualConditions {
			o := op{
				joinType:      join.JoinType,
				selfJoinKeys:  join.EqualConditions[i : i+1],
				leftVertexes:  leftVertexes,
				rightVertexes: rightVertexes,
				leftOps:       leftOps,
				rightOps:      rightOps,
			}
			o.selfCovered = *bitset.New(uint(len(b.vertexes)))
			col1 := join.EqualConditions[i].GetArgs()[0].(*expression.Column)
			col2 := join.EqualConditions[i].GetArgs()[1].(*expression.Column)
			matchedCount := 0
			for i, v := range b.vertexes {
				if v.Schema().Contains(col1) || v.Schema().Contains(col2) {
					o.selfCovered.Set(uint(i))
					matchedCount++
					if matchedCount == 2 {
						break
					}
					continue
				}
			}
			o.calcSES(b)
			o.calcTESAndConflictRules(b)
			b.ops = append(b.ops, o)
			b.opIdx++
		}
		for _, filter := range join.OtherConditions {
			cols := expression.ExtractColumns(filter)
			dominateSet := bitset.New(uint(len(b.vertexes)))
			for i, v := range b.vertexes {
				for _, col := range cols {
					if v.Schema().Contains(col) {
						dominateSet.Set(uint(i))
						break
					}
				}
			}
			b.innerJoinFreeEdges = append(b.innerJoinFreeEdges, specialFreeEdge{
				covered: *dominateSet,
				filters: filter,
			})
		}
		return
	}
	extraFilters := make([]expression.Expression, 0, len(join.LeftConditions)+len(join.RightConditions)+len(join.OtherConditions))
	o := op{
		joinType:      join.JoinType,
		selfJoinKeys:  join.EqualConditions,
		extraFilters:  extraFilters,
		leftVertexes:  leftVertexes,
		rightVertexes: rightVertexes,
		leftOps:       leftOps,
		rightOps:      rightOps,
	}
	if o.joinType == logicalop.RightOuterJoin {
		o.joinType = logicalop.LeftOuterJoin
		o.leftVertexes, o.rightVertexes = o.rightVertexes, o.leftVertexes
		o.leftOps, o.rightOps = o.rightOps, o.leftOps
	}
	o.calcSES(b)
	o.calcTESAndConflictRules(b)
	b.ops = append(b.ops, o)
	b.opIdx++
}

func (o *op) calcSES(b *builder) {
	for _, key := range o.selfJoinKeys {
		cols := expression.ExtractColumns(key)
		for _, col := range cols {
			for i, v := range b.vertexes {
				if v.Schema().Contains(col) {
					o.ses.Set(uint(i))
				}
			}
		}
	}
	for _, filter := range o.extraFilters {
		cols := expression.ExtractColumns(filter)
		for _, col := range cols {
			for i, v := range b.vertexes {
				if v.Schema().Contains(col) {
					o.ses.Set(uint(i))
				}
			}
		}
	}
}

// calcTESAndConflictRules follows the chapter 5.4 Approach CD-C of the paper
func (o *op) calcTESAndConflictRules(b *builder) {
	// Clone it so we can do in-place modifications later.
	o.tes = *o.ses.Clone()

	// The paper has chapter 6.2 Cross Products and Degenerate Predicates.
	// We don't have to handle it here because we don't handle cross products here.

	for leftOpIdx, found := o.leftOps.NextSet(0); found; leftOpIdx, found = o.leftOps.NextSet(leftOpIdx + 1) {
		leftOp := b.ops[leftOpIdx]
		if !checkAssociate(&leftOp, o) {
			cf := conflictRule{
				from: leftOp.rightVertexes,
				to:   *leftOp.leftVertexes.Intersection(&leftOp.ses),
			}
			if cf.to.Count() == 0 {
				cf.to = leftOp.leftVertexes
			}
			o.addConflictRule(cf)
		}
		if !checkLeftAssoc(&leftOp, o) {
			cf := conflictRule{
				from: leftOp.leftVertexes,
				to:   *leftOp.rightVertexes.Intersection(&leftOp.ses),
			}
			if cf.to.Count() == 0 {
				cf.to = leftOp.rightVertexes
			}
			o.addConflictRule(cf)
		}
	}
	for rightOpIdx, found := o.rightOps.NextSet(0); found; rightOpIdx, found = o.rightOps.NextSet(rightOpIdx + 1) {
		rightOp := b.ops[rightOpIdx]
		if !checkAssociate(o, &rightOp) {
			cf := conflictRule{
				from: rightOp.leftVertexes,
				to:   *rightOp.rightVertexes.Intersection(&rightOp.ses),
			}
			if cf.to.Count() == 0 {
				cf.to = rightOp.rightVertexes
			}
			o.addConflictRule(cf)
		}
		if !checkRightAssoc(o, &rightOp) {
			cf := conflictRule{
				from: rightOp.rightVertexes,
				to:   *rightOp.leftVertexes.Intersection(&rightOp.ses),
			}
			if cf.to.Count() == 0 {
				cf.to = rightOp.leftVertexes
			}
			o.addConflictRule(cf)
		}
	}
}

// addConflictRule follows the chapter 5.5 Rule Simplification of the paper.
func (o *op) addConflictRule(cf conflictRule) {
	if cf.from.Intersection(&o.tes).Count() > 0 {
		o.tes.InPlaceUnion(&cf.to)
		return
	}
	if o.tes.IsSuperSet(&cf.to) {
		return
	}
	o.conflictRules = append(o.conflictRules, cf)
}

// isApplicable follows the applicable APPLICABLE_B/C in the paper.
func (o *op) isApplicable(left, right *bitset.BitSet) bool {
	if !left.IsSuperSet(o.tes.Intersection(&o.leftVertexes)) || !right.IsSuperSet(o.tes.Intersection(&o.rightVertexes)) {
		return false
	}
	unioned := left.Union(right)
	for _, cf := range o.conflictRules {
		if cf.from.Intersection(unioned).Count() > 0 && !unioned.IsSuperSet(&cf.to) {
			return false
		}
	}
	return true
}

func (o *op) isFromOneSide(node *bitset.BitSet) bool {
	return node.IsSuperSet(o.tes.Intersection(&o.leftVertexes)) || node.IsSuperSet(o.tes.Intersection(&o.rightVertexes))
}

func (b *builder) CheckAndMakeJoin(left, right *joinNode) *logicalop.LogicalJoin {
	b.innerOpsBuffer.Clear()
	b.freeEdgeBuffer.Clear()
	for i, ok := b.innerOps.Next(0); ok; i, ok = b.innerOps.Next(i + 1) {
		// If the edge is applicable and it's not a already used one.
		if b.ops[i].isApplicable(&left.vertexes, &right.vertexes) && !b.ops[i].isFromOneSide(&left.vertexes) && !b.ops[i].isFromOneSide(&right.vertexes) {
			b.innerOpsBuffer.Insert(i)
		}
	}
	b.freeEdgeCoveredBuffer.ClearAll()
	b.freeEdgeCoveredBuffer.InPlaceUnion(&left.vertexes)
	b.freeEdgeCoveredBuffer.InPlaceUnion(&right.vertexes)
	for i, edge := range b.innerJoinFreeEdges {
		// If the free other conditions is applicable and it's not a already used one.
		if b.freeEdgeCoveredBuffer.IsSuperSet(&edge.covered) && !edge.covered.IsStrictSuperSet(&left.vertexes) && !edge.covered.IsStrictSuperSet(&right.vertexes) {
			b.freeEdgeBuffer.Insert(i)
		}
	}
	for i, ok := b.nonInnerOps.Next(0); ok; i, ok = b.nonInnerOps.Next(i + 1) {
		if b.ops[i].isApplicable(&left.vertexes, &right.vertexes) && !b.ops[i].isFromOneSide(&left.vertexes) && !b.ops[i].isFromOneSide(&right.vertexes) {
			innerFilters := make([]expression.Expression, 0, b.innerOpsBuffer.Len()+b.freeEdgeBuffer.Len())
			for j, ok := b.innerOpsBuffer.Next(0); ok; j, ok = b.innerOpsBuffer.Next(j + 1) {
				innerFilters = append(innerFilters, b.ops[j].selfJoinKeys[0])
			}
			for j, ok := b.freeEdgeBuffer.Next(0); ok; j, ok = b.freeEdgeBuffer.Next(j + 1) {
				innerFilters = append(innerFilters, b.innerJoinFreeEdges[j].filters)
			}
			return b.newLeftXXJoin(b.ops[i].joinType, left.p, right.p, b.ops[i].selfJoinKeys, b.ops[i].extraFilters, innerFilters)
		}
	}
	// If no applicable join is found, we will return nil.
	if b.innerOpsBuffer.Len() == 0 {
		return nil
	}
	joinKeys := make([]*expression.ScalarFunction, 0, b.innerOpsBuffer.Len())
	otherFilters := make([]expression.Expression, 0, b.freeEdgeBuffer.Len())
	for i, ok := b.innerOpsBuffer.Next(0); ok; i, ok = b.innerOpsBuffer.Next(i + 1) {
		joinKeys = append(joinKeys, b.ops[i].selfJoinKeys[0])
	}
	for i, ok := b.freeEdgeBuffer.Next(0); ok; i, ok = b.freeEdgeBuffer.Next(i + 1) {
		otherFilters = append(otherFilters, b.innerJoinFreeEdges[i].filters)
	}
	return b.newInnerJoin(left.p, right.p, joinKeys, otherFilters)
}

func (b *builder) newLeftXXJoin(
	joinTp logicalop.JoinType,
	left, right base.LogicalPlan,
	joinKeys []*expression.ScalarFunction,
	otherConditions []expression.Expression,
	extraFilters []expression.Expression,
) *logicalop.LogicalJoin {
	return nil
}

func (b *builder) newInnerJoin(
	left, right base.LogicalPlan,
	joinKeys []*expression.ScalarFunction,
	otherConditions []expression.Expression,
) *logicalop.LogicalJoin {
	return nil
}

// InnerJoin, LeftJoin, RightJoin, SemiJoin, AntiSemiJoin, LeftSemiJoin, LeftAntiSemiJoin
// maps to: InnerJoin, SemiJoin, AntiSemiJoin, LeftJoin
var joinTypeIdx = []int{0, 3, -1, 1, 2, 1, 2}

// associate(o1, o2) is the common associative rule we known in math.
//   - The original join order is (t1 o1 t2) o2 t3 where o1 and o2 are join operators like INNER JOIN.
//   - If the order t1 o1 (t2 o2 t3) is a valid order, associate(o1, o2) will return true.
//   - NOTE that the prerequisite is that the join condition is valid and will not cause cartesian product.
//
// Apparently, it could be true only when o1 is INNER JOIN.
var associate = [][]bool{
	{true, true, true, true},
	{false, false, false, false},
	{false, false, false, false},
	{false, false, false, false},
}

// leftAssociate(o1, o2) is designed for the following case:
//   - The original join order is (t1 o1 t2) o2 t3 where o1 and o2 are join operators like INNER JOIN.
//   - If the order (t1 o1 t3) o2 t2 is a valid order, leftAssociate(o1, o2) will return true.
//   - NOTE that the prerequisite is that the join condition is valid and will not cause cartesian product.
//
// It's trival when o1 is INNER JOIN.
// Suppose that o1 is outer join. t1 left join t3 left join t2 could be valid if the initial one is t1 left join t2 left join t3.
// And t1 inner join t3 left join t2 also a valid order if the initial one is t1 left join t2 inner join t3.
// Let's consider the case that t1 left join t2 semi join t3.
//   - Does t1 semi join t3 left join t2 a valid order?
//   - It's YES because the semi join only reduces the rows of t1.
//   - And if there's null rejective condition on t2. We'll fall back to INNER JOIN case.
//
// That o1 is (ANTI) SEMI JOIN is similar to the case that o1 is LEFT JOIN.
var leftAssociate = [][]bool{
	{true, true, true, true},
	{true, true, true, true},
	{true, true, true, true},
	{true, true, true, true},
}

// rightAssociate(o1, o2) is designed for the following case:
//   - The original join order is t1 o1 (t2 o2 t3) where o1 and o2 are join operators like INNER JOIN.
//   - If the order t2 o2 (t1 o1 t3) is a valid order, rightAssociate(o1, o2) will return true.
//   - NOTE that the prerequisite is that the join condition is valid and will not cause cartesian product.
//
// We can see that only rightAssociate(INNER JOIN, INNER JOIN) is true and that case is trival.
// Let's have a look at other cases.
// If we have t1 inner join (t2 left join t3), the order t2 left join (t1 inner join t3) is appraently a invalid one.
// And therefore for the case rightAssociate(INNER JOIN, SEMI JOIN)
//
// Then let's look at the case that o1 is left join.
// If we have t1 left join (t2 inner join t3), then rightAssociate()'s result is t2 inner join (t1 left join t3), which is also a invalid one.
// And for the case that t1 left join (t2 left join t3), the order t2 left join (t1 left join t3) is also invalid.
// Because the t1 will not suppliment NULL values while it will need to supplement NULLs in the rightAssociate()'s result
//
// That o1 is (ANTI) SEMI JOIN is similar to the case that o1 is LEFT JOIN.
var rightAssociate = [][]bool{
	{true, false, false, false},
	{false, false, false, false},
	{false, false, false, false},
	{false, false, false, false},
}

// Some notes about the paper's conflict detection rules:
// Why the paper's conflict dedection doesn't consider the commutative rule and they still says that CD-C is complete?
// I tried some addidtion proof to see that whether the commutative rule is not necessary.
// After we introduce the left-associate and right-associate. Let's see that whether we can enumerate all the orders by the three rules.
// Initially, we have (t1 o1 t2) o2 t3.
// By associate rule, we can have t1 o1 (t2 o2 t3).
// By leftAssociate rule, we can have (t1 o1 t3) o2 t2.
// At this step, we have already had a routine to put any of the t1, t2 or t3 to the uppest level.
// Then we are going to check whether we can continue do the transformation to pull up them.
// Suppose that there's the fourth table t4 and we use t12, t23, t13 to represent the join result of t1 and t2, t2 and t3, t1 and t3.
// We have the following cases:
//   - (t12 o2 t3) o3 t4 => This is the initial case. Whether t3 can be pulled up again depends on the leftAssociate(o2, o3), which is always true.
//   - (t1 o1 t23) o3 t4 => It's the result after associate(o1, o2). So o1 is inner join/cross join. We can use associate(o1, o3) to pull up t1.
//   - (t13 o2 t2) o3 t4 => It's the result after leftAssociate(o1, o2). So t2 is an abitrary one. Whether t2 can be pulled up depends on the leftAssociate(o2, o3), which is always true.
// So by given the initial relation (t1 o1 t2) o2 t3, we can alway pull up any of them to the uppest level to continue the later join reorder.
// Hence the paper's conflict detection only considers associate/leftAssociate/rightAssociate rules.

func getJoinTypeIdx(j logicalop.JoinType) int {
	return joinTypeIdx[j]
}

func checkAssociate(o1, o2 *op) bool {
	return associate[getJoinTypeIdx(o1.joinType)][getJoinTypeIdx(o2.joinType)]
}

func checkLeftAssoc(o1, o2 *op) bool {
	return leftAssociate[getJoinTypeIdx(o1.joinType)][getJoinTypeIdx(o2.joinType)]
}

func checkRightAssoc(o1, o2 *op) bool {
	return rightAssociate[getJoinTypeIdx(o1.joinType)][getJoinTypeIdx(o2.joinType)]
}
