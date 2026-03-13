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

package util

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/funcdep"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// ExtractNotNullFromConds extracts not-null columns from conditions.
// It uses the set-based (per-variable) null-rejection algorithm: for each
// CNF conjunct, it finds columns whose individual NULL would make the
// conjunct non-TRUE. This is more precise than the previous evaluate-with-null
// approach for expressions like COALESCE(a, b) > 5, which no longer
// incorrectly marks both a and b as NOT NULL.
func ExtractNotNullFromConds(conditions []expression.Expression, p base.LogicalPlan) intset.FastIntSet {
	// CNF: each conjunct must be TRUE independently, so any column that is
	// null-rejected by ANY conjunct is known to be NOT NULL.
	notnullColsUniqueIDs := intset.NewFastIntSet()
	for _, condition := range conditions {
		cols := NullRejectedCols(p.Schema(), condition)
		notnullColsUniqueIDs = notnullColsUniqueIDs.Union(cols)
	}
	return notnullColsUniqueIDs
}

// ExtractConstantCols extracts constant columns from conditions.
func ExtractConstantCols(conditions []expression.Expression, sctx base.PlanContext,
	fds *funcdep.FDSet) intset.FastIntSet {
	// extract constant cols
	// eg: where a=1 and b is null and (1+c)=5.
	// TODO: Some columns can only be determined to be constant from multiple constraints (e.g. x <= 1 AND x >= 1)
	var (
		constObjs      []expression.Expression
		constUniqueIDs = intset.NewFastIntSet()
	)
	constObjs = expression.ExtractConstantEqColumnsOrScalar(sctx.GetExprCtx(), constObjs, conditions)
	for _, constObj := range constObjs {
		switch x := constObj.(type) {
		case *expression.Column:
			constUniqueIDs.Insert(int(x.UniqueID))
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				constUniqueIDs.Insert(uniqueID)
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				constUniqueIDs.Insert(scalarUniqueID)
			}
		}
	}
	return constUniqueIDs
}

// ExtractEquivalenceCols extracts equivalence columns from conditions.
func ExtractEquivalenceCols(conditions []expression.Expression, sctx base.PlanContext,
	fds *funcdep.FDSet) [][]intset.FastIntSet {
	var equivObjsPair [][]expression.Expression
	equivObjsPair = expression.ExtractEquivalenceColumns(equivObjsPair, conditions)
	equivUniqueIDs := make([][]intset.FastIntSet, 0, len(equivObjsPair))
	for _, equivObjPair := range equivObjsPair {
		// lhs of equivalence.
		var (
			lhsUniqueID int
			rhsUniqueID int
		)
		switch x := equivObjPair[0].(type) {
		case *expression.Column:
			lhsUniqueID = int(x.UniqueID)
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				lhsUniqueID = uniqueID
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				lhsUniqueID = scalarUniqueID
			}
		}
		// rhs of equivalence.
		switch x := equivObjPair[1].(type) {
		case *expression.Column:
			rhsUniqueID = int(x.UniqueID)
		case *expression.ScalarFunction:
			hashCode := string(x.HashCode())
			if uniqueID, ok := fds.IsHashCodeRegistered(hashCode); ok {
				rhsUniqueID = uniqueID
			} else {
				scalarUniqueID := int(sctx.GetSessionVars().AllocPlanColumnID())
				fds.RegisterUniqueID(string(x.HashCode()), scalarUniqueID)
				rhsUniqueID = scalarUniqueID
			}
		}
		equivUniqueIDs = append(equivUniqueIDs, []intset.FastIntSet{intset.NewFastIntSet(
			lhsUniqueID), intset.NewFastIntSet(rhsUniqueID)})
	}
	return equivUniqueIDs
}
