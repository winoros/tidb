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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
)

// IsNullRejected checks whether the predicate is null-rejected with respect
// to the inner schema. It uses a two-phase approach:
//
//  1. Structural 3VL analysis (boolean-based / joint algorithm): recursively
//     classifies functions by their null-propagation properties. This handles
//     AND, OR, NOT, IN, and all null-preserving functions correctly, including
//     cases the old evaluator missed (e.g. issue #49616).
//
//  2. Evaluation fallback: for predicates containing null-hiding functions
//     (COALESCE, IFNULL, etc.) where the structural analysis is conservative,
//     falls back to EvaluateExprWithNull which can constant-fold the specific
//     argument values.
//
// The skipPlanCacheCheck parameter is kept for API compatibility.
func IsNullRejected(ctx base.PlanContext, innerSchema *expression.Schema,
	predicate expression.Expression, skipPlanCacheCheck bool) bool {
	targetIDs := schemaColIDs(innerSchema)
	if isNonTrue(predicate, targetIDs) {
		return true
	}
	// Fallback: evaluation-based analysis for null-hiding functions
	// (e.g., coalesce(b1, 2) > 2 where b1 is an inner column).
	return isNullRejectedByEval(ctx, innerSchema, predicate, skipPlanCacheCheck)
}

// isNullRejectedByEval uses the evaluation approach: replace all inner
// schema columns with NULL and check if the result is FALSE or NULL.
// This handles null-hiding functions like COALESCE that the structural
// analysis treats conservatively.
func isNullRejectedByEval(ctx base.PlanContext, innerSchema *expression.Schema,
	predicate expression.Expression, skipPlanCacheCheck bool) bool {
	// The expression must reference at least one inner-schema column;
	// otherwise it's trivially not null-rejected by the inner table.
	if !expression.ExprReferenceSchema(predicate, innerSchema) {
		return false
	}
	exprCtx := ctx.GetNullRejectCheckExprCtx()
	result, err := expression.EvaluateExprWithNull(exprCtx, innerSchema, predicate, skipPlanCacheCheck)
	if err != nil {
		return false
	}
	x, ok := result.(*expression.Constant)
	if !ok {
		return false
	}
	if x.Value.IsNull() {
		return true
	}
	sc := ctx.GetSessionVars().StmtCtx
	if isTrue, err := x.Value.ToBool(sc.TypeCtxOrDefault()); err == nil && isTrue == 0 {
		return true
	}
	return false
}

// ResetNotNullFlag resets the not null flag of [start, end] columns in the schema.
func ResetNotNullFlag(schema *expression.Schema, start, end int) {
	for i := start; i < end; i++ {
		col := *schema.Columns[i]
		newFieldType := *col.RetType
		newFieldType.DelFlag(mysql.NotNullFlag)
		col.RetType = &newFieldType
		schema.Columns[i] = &col
	}
}
