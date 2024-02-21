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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
)

// EvalAstExprWithPlanCtx evaluates ast expression with plan context.
// Different with expression.EvalSimpleAst, it uses planner context and is more powerful to build
// some special expressions like subquery, window function, etc.
// If you only want to evaluate simple expressions, use `expression.EvalSimpleAst` instead.
var EvalAstExprWithPlanCtx func(ctx sessionctx.Context, expr ast.ExprNode) (types.Datum, error)

// RewriteAstExprWithPlanCtx rewrites ast expression directly.
// Different with expression.BuildSimpleExpr, it uses planner context and is more powerful to build
// some special expressions like subquery, window function, etc.
// If you only want to build simple expressions, use `expression.BuildSimpleExpr` instead.
var RewriteAstExprWithPlanCtx func(ctx sessionctx.Context, expr ast.ExprNode,
	schema *expression.Schema, names types.NameSlice, allowCastArray bool) (expression.Expression, error)
