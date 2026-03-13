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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// helpers for building test expressions

func col(id int) *expression.Column {
	return &expression.Column{
		UniqueID: int64(id),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
}

func constInt(val int64) *expression.Constant {
	return &expression.Constant{
		Value:   types.NewIntDatum(val),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
}

func newFunc(funcName string, args ...expression.Expression) expression.Expression {
	ctx := mock.NewContext()
	return expression.NewFunctionInternal(
		ctx.GetExprCtx(), funcName,
		types.NewFieldType(mysql.TypeLonglong), args...)
}

func schemaOf(cols ...*expression.Column) *expression.Schema {
	return expression.NewSchema(cols...)
}

// ─────────────────────────────────────────────────────
// Tests for set-based algorithm: NullRejectedCols
// ─────────────────────────────────────────────────────

func TestNullRejectedCols_Column(t *testing.T) {
	schema := schemaOf(col(1), col(2))

	// Column in schema → {col}
	got := NullRejectedCols(schema, col(1))
	require.True(t, got.Has(1))

	// Column not in schema → {}
	got = NullRejectedCols(schema, col(99))
	require.True(t, got.IsEmpty())
}

func TestNullRejectedCols_Constant(t *testing.T) {
	schema := schemaOf(col(1))
	got := NullRejectedCols(schema, constInt(42))
	require.True(t, got.IsEmpty())
}

func TestNullRejectedCols_Comparison(t *testing.T) {
	a, b := col(1), col(2)
	schema := schemaOf(a, b)

	// a > 5: NonTrue = {a} (null-preserving, MustNull(a) = {a})
	expr := newFunc(ast.GT, col(1), constInt(5))
	got := NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1), got)

	// a = b: NonTrue = {a, b}
	expr = newFunc(ast.EQ, col(1), col(2))
	got = NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1, 2), got)

	// a + b > 5: null-preserving chain, NonTrue = {a, b}
	plus := newFunc(ast.Plus, col(1), col(2))
	expr = newFunc(ast.GT, plus, constInt(5))
	got = NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1, 2), got)
}

func TestNullRejectedCols_AND(t *testing.T) {
	a, b := col(1), col(2)
	schema := schemaOf(a, b)

	// a > 1 AND b > 2: NonTrue = {a} ∪ {b} = {a, b}
	expr := newFunc(ast.LogicAnd,
		newFunc(ast.GT, col(1), constInt(1)),
		newFunc(ast.GT, col(2), constInt(2)))
	got := NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1, 2), got)
}

func TestNullRejectedCols_OR(t *testing.T) {
	a, b := col(1), col(2)
	schema := schemaOf(a, b)

	// a > 1 OR b > 2: NonTrue(set) = {a} ∩ {b} = {}
	expr := newFunc(ast.LogicOr,
		newFunc(ast.GT, col(1), constInt(1)),
		newFunc(ast.GT, col(2), constInt(2)))
	got := NullRejectedCols(schema, expr)
	require.True(t, got.IsEmpty())

	// a > 1 OR a > 2: NonTrue(set) = {a} ∩ {a} = {a}
	expr = newFunc(ast.LogicOr,
		newFunc(ast.GT, col(1), constInt(1)),
		newFunc(ast.GT, col(1), constInt(2)))
	got = NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1), got)
}

func TestNullRejectedCols_NOT(t *testing.T) {
	a := col(1)
	schema := schemaOf(a)

	// NOT (a > 1): NonTrue = MustNull(a > 1) = {a}
	expr := newFunc(ast.UnaryNot, newFunc(ast.GT, col(1), constInt(1)))
	got := NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1), got)

	// NOT (a IS NULL) = IS NOT NULL(a): NonTrue = NonFalse(IS NULL(a)) = {a}
	// Because IS NULL(NULL) = TRUE ∈ {T,U}, so NOT(TRUE) = FALSE ∈ {F,U}.
	expr = newFunc(ast.UnaryNot, newFunc(ast.IsNull, col(1)))
	got = NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1), got)
}

func TestNullRejectedCols_NullHiding(t *testing.T) {
	a, b := col(1), col(2)
	schema := schemaOf(a, b)

	// COALESCE(a, b) > 5: NonTrue = {} (COALESCE is null-hiding)
	coalesce := newFunc(ast.Coalesce, col(1), col(2))
	expr := newFunc(ast.GT, coalesce, constInt(5))
	got := NullRejectedCols(schema, expr)
	require.True(t, got.IsEmpty())

	// a <=> b: NonTrue = {} (NullEQ is null-hiding)
	expr = newFunc(ast.NullEQ, col(1), col(2))
	got = NullRejectedCols(schema, expr)
	require.True(t, got.IsEmpty())
}

func TestNullRejectedCols_IsNull(t *testing.T) {
	a := col(1)
	schema := schemaOf(a)

	// a IS NOT NULL → IsTruthWithoutNull: NonTrue = MustNull(a) = {a}
	expr := newFunc(ast.IsTruthWithoutNull, col(1))
	got := NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1), got)

	// a IS NULL: NonTrue = {}
	expr = newFunc(ast.IsNull, col(1))
	got = NullRejectedCols(schema, expr)
	require.True(t, got.IsEmpty())
}

func TestNullRejectedCols_IN(t *testing.T) {
	a := col(1)
	schema := schemaOf(a)

	// a IN (1, 2, 3): each branch a=const is null-preserving, MustNull = {a}.
	// OR intersection: {a} for all branches → {a}.
	expr := newFunc(ast.In, col(1), constInt(1), constInt(2), constInt(3))
	got := NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1), got)
}

func TestNullRejectedCols_IN_WithCols(t *testing.T) {
	a, b, c := col(1), col(2), col(3)
	schema := schemaOf(a, b, c)

	// 1 IN (a, b): branches are 1=a and 1=b.
	// MustNull(1=a) = {a}, MustNull(1=b) = {b}.
	// OR intersection of NonTrue: {a} ∩ {b} = {} (set-based loses info here).
	expr := newFunc(ast.In, constInt(1), col(1), col(2))
	got := NullRejectedCols(schema, expr)
	require.True(t, got.IsEmpty())
}

func TestNullRejectedCols_ComplexANDOR(t *testing.T) {
	a, b := col(1), col(2)
	schema := schemaOf(a, b)

	// NOT (a > 1 AND FALSE):
	// NonFalse(a > 1 AND FALSE) = NonFalse(a>1) ∩ NonFalse(FALSE) = {a} ∩ {} = {}
	// NonTrue(NOT ...) = NonFalse(...) = {}
	// So NonTrue = {}
	expr := newFunc(ast.UnaryNot,
		newFunc(ast.LogicAnd,
			newFunc(ast.GT, col(1), constInt(1)),
			constInt(0)))
	got := NullRejectedCols(schema, expr)
	require.True(t, got.IsEmpty())
}

// ─────────────────────────────────────────────────────
// Tests for boolean-based algorithm (via isNonTrue/isMustNull)
// These test the internal functions used by IsNullRejected.
// ─────────────────────────────────────────────────────

func TestIsNonTrue_Column(t *testing.T) {
	target := intset.NewFastIntSet(1, 2)
	require.True(t, isNonTrue(col(1), target))
	require.True(t, isNonTrue(col(2), target))
	require.False(t, isNonTrue(col(99), target))
}

func TestIsNonTrue_Constant(t *testing.T) {
	target := intset.NewFastIntSet(1)
	require.False(t, isNonTrue(constInt(5), target))
}

func TestIsNonTrue_AND(t *testing.T) {
	target := intset.NewFastIntSet(1, 2)

	// a > 1 AND b > 2: isNonTrue = isNonTrue(a>1) || isNonTrue(b>2)
	// = isMustNull(a>1) || isMustNull(b>2) = true || true = true
	expr := newFunc(ast.LogicAnd,
		newFunc(ast.GT, col(1), constInt(1)),
		newFunc(ast.GT, col(2), constInt(2)))
	require.True(t, isNonTrue(expr, target))
}

func TestIsNonTrue_OR(t *testing.T) {
	target := intset.NewFastIntSet(1, 2)

	// a > 1 OR b > 2: both sides must be non-TRUE.
	// isNonTrue(a>1, {1,2}) = isMustNull(a>1) = has(1) = true
	// isNonTrue(b>2, {1,2}) = isMustNull(b>2) = has(2) = true
	// → true (boolean alg correctly handles different columns in OR branches)
	expr := newFunc(ast.LogicOr,
		newFunc(ast.GT, col(1), constInt(1)),
		newFunc(ast.GT, col(2), constInt(2)))
	require.True(t, isNonTrue(expr, target))

	// Only one column in target: a > 1 OR b > 2 with target = {1}
	// isNonTrue(b>2, {1}) = false → false
	target1 := intset.NewFastIntSet(1)
	require.False(t, isNonTrue(expr, target1))
}

func TestIsNonTrue_NOT(t *testing.T) {
	target := intset.NewFastIntSet(1)

	// NOT (a > 1): isNonTrue = isMustNull(a > 1) = true
	expr := newFunc(ast.UnaryNot, newFunc(ast.GT, col(1), constInt(1)))
	require.True(t, isNonTrue(expr, target))

	// NOT (a IS NULL) = IS NOT NULL(a): isNonFalse(IS NULL(a)) = isMustNull(a) = true
	expr = newFunc(ast.UnaryNot, newFunc(ast.IsNull, col(1)))
	require.True(t, isNonTrue(expr, target))
}

func TestIsNonTrue_IN(t *testing.T) {
	target := intset.NewFastIntSet(1, 2)

	// a IN (1, 2): for each branch a=const, isMustNull(a) || isMustNull(const)
	// = true || false = true for all branches → true
	expr := newFunc(ast.In, col(1), constInt(1), constInt(2))
	require.True(t, isNonTrue(expr, target))

	// 1 IN (a, b): for branch 1=a, isMustNull(1) || isMustNull(a) = false || true = true
	// for branch 1=b, isMustNull(1) || isMustNull(b) = false || true = true
	// → true (boolean alg handles this!)
	expr = newFunc(ast.In, constInt(1), col(1), col(2))
	require.True(t, isNonTrue(expr, target))

	// 1 IN (a, b) with target = {1} only:
	// branch 1=a: isMustNull(1,{1}) || isMustNull(a,{1}) = false || true = true
	// branch 1=b: isMustNull(1,{1}) || isMustNull(b,{1}) = false || false = false
	// → false
	target1 := intset.NewFastIntSet(1)
	require.False(t, isNonTrue(expr, target1))
}

func TestIsNonTrue_NullHiding(t *testing.T) {
	target := intset.NewFastIntSet(1, 2)

	// COALESCE(a, b) > 5: COALESCE is hiding, so isMustNull(COALESCE(...)) = false.
	// GT is preserving, so isNonTrue = isMustNull(COALESCE(a,b)) || isMustNull(5)
	// = false || false = false
	coalesce := newFunc(ast.Coalesce, col(1), col(2))
	expr := newFunc(ast.GT, coalesce, constInt(5))
	require.False(t, isNonTrue(expr, target))
}

func TestIsNonTrue_IsNull(t *testing.T) {
	target := intset.NewFastIntSet(1)

	// a IS NULL: accepts NULL → not null-rejecting
	expr := newFunc(ast.IsNull, col(1))
	require.False(t, isNonTrue(expr, target))

	// NOT(a IS NULL) = IS NOT NULL: null-rejecting!
	// isNonTrue(NOT(IS NULL(a))) = isNonFalse(IS NULL(a)) = isMustNull(a) = true
	expr = newFunc(ast.UnaryNot, newFunc(ast.IsNull, col(1)))
	require.True(t, isNonTrue(expr, target))
}

func TestIsNonTrue_Issue49616(t *testing.T) {
	// The pattern from issue #49616:
	// null-rejected OR (null-rejected AND something)
	// e.g., a > 1 OR (a > 2 AND b IS NULL)
	target := intset.NewFastIntSet(1) // only col(1) is inner

	// a > 1 OR (a > 2 AND b IS NULL):
	// isNonTrue(a > 1, {1}) = true (a is in target)
	// isNonTrue(a > 2 AND b IS NULL, {1}):
	//   = isNonTrue(a > 2, {1}) || isNonTrue(b IS NULL, {1})
	//   = true || false = true
	// → true AND true = true
	expr := newFunc(ast.LogicOr,
		newFunc(ast.GT, col(1), constInt(1)),
		newFunc(ast.LogicAnd,
			newFunc(ast.GT, col(1), constInt(2)),
			newFunc(ast.IsNull, col(2))))
	require.True(t, isNonTrue(expr, target))
}

func TestIsNonTrue_ORWithDifferentInnerCols(t *testing.T) {
	// t0.b0 IN (t11.b1, t11.c1): equivalent to b0=b1 OR b0=c1
	// With target = {b1, c1} (inner schema):
	// isNonTrue(b0=b1, {b1,c1}) = isMustNull(b0,{b1,c1}) || isMustNull(b1,{b1,c1})
	//   = false || true = true
	// isNonTrue(b0=c1, {b1,c1}) = isMustNull(b0,{b1,c1}) || isMustNull(c1,{b1,c1})
	//   = false || true = true
	// → true
	target := intset.NewFastIntSet(2, 3) // b1=2, c1=3
	expr := newFunc(ast.In, col(1), col(2), col(3)) // b0 IN (b1, c1)
	require.True(t, isNonTrue(expr, target))
}

func TestIsMustNull_AND(t *testing.T) {
	target := intset.NewFastIntSet(1, 2)

	// AND(a>1, b>2): both sides must be must-null.
	// isMustNull(a>1) = true, isMustNull(b>2) = true → true
	expr := newFunc(ast.LogicAnd,
		newFunc(ast.GT, col(1), constInt(1)),
		newFunc(ast.GT, col(2), constInt(2)))
	require.True(t, isMustNull(expr, target))

	// AND(a>1, FALSE): isMustNull(FALSE) = false → false
	// (AND(NULL, FALSE) = FALSE, not NULL)
	expr = newFunc(ast.LogicAnd,
		newFunc(ast.GT, col(1), constInt(1)),
		constInt(0))
	require.False(t, isMustNull(expr, target))
}

func TestIsMustNull_OR(t *testing.T) {
	target := intset.NewFastIntSet(1, 2)

	// OR(a>1, b>2): both sides must be must-null.
	// isMustNull(a>1) = true, isMustNull(b>2) = true → true
	expr := newFunc(ast.LogicOr,
		newFunc(ast.GT, col(1), constInt(1)),
		newFunc(ast.GT, col(2), constInt(2)))
	require.True(t, isMustNull(expr, target))

	// OR(a>1, TRUE): isMustNull(TRUE) = false → false
	// (OR(NULL, TRUE) = TRUE, not NULL)
	expr = newFunc(ast.LogicOr,
		newFunc(ast.GT, col(1), constInt(1)),
		constInt(1))
	require.False(t, isMustNull(expr, target))
}

func TestIsMustNull_ScalarTests(t *testing.T) {
	target := intset.NewFastIntSet(1)

	// IS NULL, IS TRUE, IS FALSE: always return boolean, never NULL
	require.False(t, isMustNull(newFunc(ast.IsNull, col(1)), target))
	require.False(t, isMustNull(newFunc(ast.IsTruthWithNull, col(1)), target))
	require.False(t, isMustNull(newFunc(ast.IsTruthWithoutNull, col(1)), target))
	require.False(t, isMustNull(newFunc(ast.IsFalsity, col(1)), target))
}

func TestNullRejectedCols_Cast(t *testing.T) {
	schema := schemaOf(col(1))

	// CAST(a AS INT) > 5: Cast is null-preserving
	castExpr := newFunc(ast.Cast, col(1))
	expr := newFunc(ast.GT, castExpr, constInt(5))
	got := NullRejectedCols(schema, expr)
	require.Equal(t, intset.NewFastIntSet(1), got)
}
