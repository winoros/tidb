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

package util

import (
	"slices"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestIsNullRejectedStaticProof(t *testing.T) {
	ctx, cols, consts := buildNullRejectTestContext()

	tests := []struct {
		name     string
		schema   *expression.Schema
		expr     expression.Expression
		rejected bool
	}{
		{
			name:     "not_abs_inner",
			schema:   expression.NewSchema(cols.innerIntA),
			expr:     expression.NewFunctionInternal(ctx, ast.UnaryNot, consts.tinyType, expression.NewFunctionInternal(ctx, ast.Abs, consts.intType, cols.innerIntA)),
			rejected: true,
		},
		{
			name:   "not_and_false",
			schema: expression.NewSchema(cols.innerIntA),
			expr: expression.NewFunctionInternal(ctx, ast.UnaryNot, consts.tinyType,
				expression.NewFunctionInternal(ctx, ast.LogicAnd, consts.tinyType, cols.innerIntA, consts.zero)),
			rejected: false,
		},
		{
			name:   "or_and_issue_49616_shape",
			schema: expression.NewSchema(cols.innerIntA),
			expr: expression.NewFunctionInternal(ctx, ast.LogicOr, consts.tinyType,
				expression.NewFunctionInternal(ctx, ast.GT, consts.tinyType, cols.innerIntA, consts.zero),
				expression.NewFunctionInternal(ctx, ast.LogicAnd, consts.tinyType,
					expression.NewFunctionInternal(ctx, ast.EQ, consts.tinyType, cols.innerIntA, consts.zero),
					expression.NewFunctionInternal(ctx, ast.GT, consts.tinyType, cols.outerInt, consts.zero),
				),
			),
			rejected: true,
		},
		{
			name:   "multi_column_or",
			schema: expression.NewSchema(cols.innerIntA, cols.innerIntB),
			expr: expression.NewFunctionInternal(ctx, ast.LogicOr, consts.tinyType,
				expression.NewFunctionInternal(ctx, ast.LT, consts.tinyType, cols.innerIntA, consts.one),
				expression.NewFunctionInternal(ctx, ast.LT, consts.tinyType, cols.innerIntB, consts.one),
			),
			rejected: true,
		},
		{
			name:     "coalesce_hides_null",
			schema:   expression.NewSchema(cols.innerIntA),
			expr:     expression.NewFunctionInternal(ctx, ast.Coalesce, consts.intType, cols.innerIntA, consts.one),
			rejected: false,
		},
		{
			name:     "coalesce_all_target_args_null",
			schema:   expression.NewSchema(cols.innerIntA, cols.innerIntB),
			expr:     expression.NewFunctionInternal(ctx, ast.Coalesce, consts.intType, cols.innerIntA, cols.innerIntB),
			rejected: true,
		},
		{
			name:     "null_eq_single_target_arg",
			schema:   expression.NewSchema(cols.innerIntA),
			expr:     expression.NewFunctionInternal(ctx, ast.NullEQ, consts.tinyType, cols.innerIntA, consts.one),
			rejected: true,
		},
		{
			name:     "null_eq_both_target_args",
			schema:   expression.NewSchema(cols.innerIntA, cols.innerIntB),
			expr:     expression.NewFunctionInternal(ctx, ast.NullEQ, consts.tinyType, cols.innerIntA, cols.innerIntB),
			rejected: false,
		},
		{
			name:     "in_all_target_list_args",
			schema:   expression.NewSchema(cols.innerIntA, cols.innerIntB),
			expr:     expression.NewFunctionInternal(ctx, ast.In, consts.tinyType, consts.one, cols.innerIntA, cols.innerIntB),
			rejected: true,
		},
		{
			name:     "in_partial_target_list_args",
			schema:   expression.NewSchema(cols.innerIntA),
			expr:     expression.NewFunctionInternal(ctx, ast.In, consts.tinyType, consts.one, cols.outerInt, cols.innerIntA),
			rejected: false,
		},
		{
			name:   "is_not_null_over_null_preserving_func",
			schema: expression.NewSchema(cols.innerIntA),
			expr: expression.NewFunctionInternal(ctx, ast.UnaryNot, consts.tinyType,
				expression.NewFunctionInternal(ctx, ast.IsNull, consts.tinyType, expression.NewFunctionInternal(ctx, ast.Abs, consts.intType, cols.innerIntA))),
			rejected: true,
		},
	}

	for _, tt := range tests {
		require.Equalf(t, tt.rejected, IsNullRejected(ctx, tt.schema, tt.expr, true), "case=%s", tt.name)
	}
}

func TestNullRejectUnaryAnyNullPreservingFuncs(t *testing.T) {
	ctx, cols, consts := buildNullRejectTestContext()
	require.Equal(t, []string{ast.Abs, ast.BitNeg, ast.Cast, ast.UnaryMinus}, sortedNullRejectFuncNames(nullRejectUnaryAnyNullPreservingFuncs))

	for _, funcName := range sortedNullRejectFuncNames(nullRejectUnaryAnyNullPreservingFuncs) {
		expr := expression.NewFunctionInternal(ctx, funcName, consts.intType, cols.innerIntA)
		require.Truef(t, IsNullRejected(ctx, expression.NewSchema(cols.innerIntA), expr, true), "func=%s", funcName)
	}
}

func TestNullRejectBinaryAnyNullPreservingFuncs(t *testing.T) {
	ctx, cols, consts := buildNullRejectTestContext()
	require.Equal(t,
		[]string{ast.And, ast.Or, ast.Xor, ast.Div, ast.EQ, ast.GE, ast.GT, ast.IntDiv, ast.LE, ast.LeftShift, ast.LT, ast.Minus, ast.Mod, ast.Mul, ast.NE, ast.Plus, ast.Regexp, ast.RightShift, ast.Strcmp},
		sortedNullRejectFuncNames(nullRejectBinaryAnyNullPreservingFuncs),
	)

	for _, funcName := range sortedNullRejectFuncNames(nullRejectBinaryAnyNullPreservingFuncs) {
		expr := buildBinaryNullRejectExpr(ctx, cols, consts, funcName)
		require.Truef(t, IsNullRejected(ctx, expression.NewSchema(cols.innerIntA, cols.innerStr), expr, true), "func=%s", funcName)
	}
}

func TestNullRejectTernaryAnyNullPreservingFuncs(t *testing.T) {
	ctx, cols, consts := buildNullRejectTestContext()
	require.Equal(t, []string{ast.Ilike, ast.Like}, sortedNullRejectFuncNames(nullRejectTernaryAnyNullPreservingFuncs))

	for _, funcName := range sortedNullRejectFuncNames(nullRejectTernaryAnyNullPreservingFuncs) {
		expr := expression.NewFunctionInternal(ctx, funcName, consts.tinyType, cols.innerStr, consts.pattern, consts.escape)
		require.Truef(t, IsNullRejected(ctx, expression.NewSchema(cols.innerStr), expr, true), "func=%s", funcName)
	}
}

func TestNullRejectAllArgsNullPreservingFuncs(t *testing.T) {
	ctx, cols, consts := buildNullRejectTestContext()
	require.Equal(t, []string{ast.Coalesce, ast.Ifnull}, sortedNullRejectFuncNames(nullRejectAllArgsNullPreservingFuncs))

	for _, funcName := range sortedNullRejectFuncNames(nullRejectAllArgsNullPreservingFuncs) {
		expr := expression.NewFunctionInternal(ctx, funcName, consts.intType, cols.innerIntA, cols.innerIntB)
		require.Truef(t, IsNullRejected(ctx, expression.NewSchema(cols.innerIntA, cols.innerIntB), expr, true), "func=%s", funcName)
	}
}

type nullRejectTestColumns struct {
	innerIntA *expression.Column
	innerIntB *expression.Column
	outerInt  *expression.Column
	innerStr  *expression.Column
}

type nullRejectTestConstants struct {
	intType  *types.FieldType
	tinyType *types.FieldType
	zero     *expression.Constant
	one      *expression.Constant
	pattern  *expression.Constant
	escape   *expression.Constant
}

func buildNullRejectTestContext() (*mock.Context, nullRejectTestColumns, nullRejectTestConstants) {
	ctx := mock.NewContext()
	intType := types.NewFieldType(mysql.TypeLonglong)
	tinyType := types.NewFieldType(mysql.TypeTiny)
	stringType := types.NewFieldType(mysql.TypeVarString)
	charset, collate := types.DefaultCharsetForType(mysql.TypeVarString)
	stringType.SetCharset(charset)
	stringType.SetCollate(collate)

	return ctx,
		nullRejectTestColumns{
			innerIntA: &expression.Column{UniqueID: 1, Index: 0, RetType: intType},
			innerIntB: &expression.Column{UniqueID: 2, Index: 1, RetType: intType},
			outerInt:  &expression.Column{UniqueID: 3, Index: 2, RetType: intType},
			innerStr:  &expression.Column{UniqueID: 4, Index: 3, RetType: stringType},
		},
		nullRejectTestConstants{
			intType:  intType,
			tinyType: tinyType,
			zero:     &expression.Constant{Value: types.NewIntDatum(0), RetType: intType},
			one:      &expression.Constant{Value: types.NewIntDatum(1), RetType: intType},
			pattern:  &expression.Constant{Value: types.NewStringDatum("x%"), RetType: stringType},
			escape:   &expression.Constant{Value: types.NewIntDatum('\\'), RetType: intType},
		}
}

func buildBinaryNullRejectExpr(
	ctx *mock.Context,
	cols nullRejectTestColumns,
	consts nullRejectTestConstants,
	funcName string,
) expression.Expression {
	switch funcName {
	case ast.Regexp, ast.Strcmp:
		retType := consts.intType
		if funcName == ast.Regexp {
			retType = consts.tinyType
		}
		return expression.NewFunctionInternal(ctx, funcName, retType, cols.innerStr, consts.pattern)
	case ast.EQ, ast.NE, ast.LT, ast.LE, ast.GT, ast.GE:
		return expression.NewFunctionInternal(ctx, funcName, consts.tinyType, cols.innerIntA, consts.one)
	default:
		return expression.NewFunctionInternal(ctx, funcName, consts.intType, cols.innerIntA, consts.one)
	}
}

func sortedNullRejectFuncNames(funcs map[string]struct{}) []string {
	names := make([]string, 0, len(funcs))
	for name := range funcs {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
