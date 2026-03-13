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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/types"
)

// The builtins below are the only ones whose NULL propagation we currently use
// in the null-reject proof.
//
// The unary/binary/ternary any-null-preserving buckets are the strong case:
// once any analyzed argument is proven NULL, the result is NULL regardless of
// how the remaining arguments evaluate. `mustNull` and `analyzeConstant` may
// therefore stop as soon as they find one NULL child.
//
// The all-args-null-preserving bucket is weaker: only the fact that every
// argument is NULL forces the result to NULL. A subset of NULL arguments may be
// hidden by non-NULL inputs, as with `COALESCE` and `IFNULL`, so the proof must
// wait until all arguments are known NULL before concluding NULL.
//
// Any builtin not listed here is treated conservatively as NULL-hiding until
// its 3VL contract is reviewed and covered by tests.
var nullRejectUnaryAnyNullPreservingFuncs = map[string]struct{}{
	ast.Abs:        {},
	ast.BitNeg:     {},
	ast.Cast:       {},
	ast.UnaryMinus: {},
}

var nullRejectBinaryAnyNullPreservingFuncs = map[string]struct{}{
	ast.And:        {},
	ast.Div:        {},
	ast.EQ:         {},
	ast.GE:         {},
	ast.GT:         {},
	ast.IntDiv:     {},
	ast.LE:         {},
	ast.LeftShift:  {},
	ast.LT:         {},
	ast.Minus:      {},
	ast.Mod:        {},
	ast.Mul:        {},
	ast.NE:         {},
	ast.Or:         {},
	ast.Plus:       {},
	ast.Regexp:     {},
	ast.RightShift: {},
	ast.Strcmp:     {},
	ast.Xor:        {},
}

var nullRejectTernaryAnyNullPreservingFuncs = map[string]struct{}{
	ast.Like:  {},
	ast.Ilike: {},
}

var nullRejectAllArgsNullPreservingFuncs = map[string]struct{}{
	ast.Coalesce: {},
	ast.Ifnull:   {},
}

type nullRejectTruthSet uint8

const (
	nullRejectTruthFalse nullRejectTruthSet = 1 << iota
	nullRejectTruthTrue
	nullRejectTruthNull
)

// nullRejectAnalyzer reasons over one target schema: all columns in `schema`
// are assumed to become NULL together, while every other input remains unknown.
//
// The analyzer never substitutes concrete NULL values into the expression tree.
// Instead, it tracks a conservative 3VL truth set for predicates and a
// must-NULL proof for scalar expressions. This keeps `NOT`, `AND`/`OR`, and
// NULL-hiding builtins sound without the ad-hoc rewrites used before.
type nullRejectAnalyzer struct {
	ctx    base.PlanContext
	schema *expression.Schema
}

// allConstants checks whether `expr` can be folded without depending on
// runtime parameters or non-deterministic state.
func allConstants(ctx expression.BuildContext, expr expression.Expression) bool {
	if expression.MaybeOverOptimized4PlanCache(ctx, expr) {
		return false
	}
	switch v := expr.(type) {
	case *expression.ScalarFunction:
		for _, arg := range v.GetArgs() {
			if !allConstants(ctx, arg) {
				return false
			}
		}
		return true
	case *expression.Constant:
		return true
	}
	return false
}

func foldNullRejectConstant(ctx base.PlanContext, expr expression.Expression) (*expression.Constant, bool) {
	if !allConstants(ctx.GetExprCtx(), expr) {
		return nil, false
	}
	folded := expression.FoldConstant(ctx.GetExprCtx(), expr)
	c, ok := folded.(*expression.Constant)
	if !ok || c.ParamMarker != nil || c.DeferredExpr != nil {
		return nil, false
	}
	return c, true
}

func newNullRejectNullConstant() *expression.Constant {
	return &expression.Constant{
		Value:   types.Datum{},
		RetType: types.NewFieldType(mysql.TypeNull),
	}
}

func isNullRejectAnyNullPreservingFunc(funcName string) bool {
	if _, ok := nullRejectUnaryAnyNullPreservingFuncs[funcName]; ok {
		return true
	}
	if _, ok := nullRejectBinaryAnyNullPreservingFuncs[funcName]; ok {
		return true
	}
	_, ok := nullRejectTernaryAnyNullPreservingFuncs[funcName]
	return ok
}

func truthSetFromConstant(ctx base.PlanContext, c *expression.Constant) nullRejectTruthSet {
	if c.Value.IsNull() {
		return nullRejectTruthNull
	}
	isTrue, err := c.Value.ToBool(ctx.GetSessionVars().StmtCtx.TypeCtxOrDefault())
	if err != nil {
		return nullRejectTruthFalse | nullRejectTruthTrue
	}
	if isTrue == 0 {
		return nullRejectTruthFalse
	}
	return nullRejectTruthTrue
}

func (s nullRejectTruthSet) has(flag nullRejectTruthSet) bool {
	return s&flag != 0
}

func combineBinaryTruthSet(lhs, rhs nullRejectTruthSet, combine func(nullRejectTruthSet, nullRejectTruthSet) nullRejectTruthSet) nullRejectTruthSet {
	var ret nullRejectTruthSet
	for _, lv := range []nullRejectTruthSet{nullRejectTruthFalse, nullRejectTruthTrue, nullRejectTruthNull} {
		if !lhs.has(lv) {
			continue
		}
		for _, rv := range []nullRejectTruthSet{nullRejectTruthFalse, nullRejectTruthTrue, nullRejectTruthNull} {
			if !rhs.has(rv) {
				continue
			}
			ret |= combine(lv, rv)
		}
	}
	return ret
}

func andTruthValue(lhs, rhs nullRejectTruthSet) nullRejectTruthSet {
	switch lhs {
	case nullRejectTruthFalse:
		return nullRejectTruthFalse
	case nullRejectTruthTrue:
		return rhs
	default:
		if rhs == nullRejectTruthFalse {
			return nullRejectTruthFalse
		}
		return nullRejectTruthNull
	}
}

func orTruthValue(lhs, rhs nullRejectTruthSet) nullRejectTruthSet {
	switch lhs {
	case nullRejectTruthTrue:
		return nullRejectTruthTrue
	case nullRejectTruthFalse:
		return rhs
	default:
		if rhs == nullRejectTruthTrue {
			return nullRejectTruthTrue
		}
		return nullRejectTruthNull
	}
}

func xorTruthValue(lhs, rhs nullRejectTruthSet) nullRejectTruthSet {
	if lhs == nullRejectTruthNull || rhs == nullRejectTruthNull {
		return nullRejectTruthNull
	}
	if lhs == rhs {
		return nullRejectTruthFalse
	}
	return nullRejectTruthTrue
}

func notTruthValue(v nullRejectTruthSet) nullRejectTruthSet {
	switch v {
	case nullRejectTruthFalse:
		return nullRejectTruthTrue
	case nullRejectTruthTrue:
		return nullRejectTruthFalse
	default:
		return nullRejectTruthNull
	}
}

func applyUnaryTruthSet(arg nullRejectTruthSet, apply func(nullRejectTruthSet) nullRejectTruthSet) nullRejectTruthSet {
	var ret nullRejectTruthSet
	for _, v := range []nullRejectTruthSet{nullRejectTruthFalse, nullRejectTruthTrue, nullRejectTruthNull} {
		if !arg.has(v) {
			continue
		}
		ret |= apply(v)
	}
	return ret
}

func (a nullRejectAnalyzer) truthifyScalar(expr expression.Expression) nullRejectTruthSet {
	if c, ok := a.analyzeConstant(expr); ok {
		return truthSetFromConstant(a.ctx, c)
	}
	if a.mustNull(expr) {
		return nullRejectTruthNull
	}
	return nullRejectTruthFalse | nullRejectTruthTrue
}

func (a nullRejectAnalyzer) analyzeConstant(expr expression.Expression) (*expression.Constant, bool) {
	if c, ok := foldNullRejectConstant(a.ctx, expr); ok {
		return c, true
	}
	switch x := expr.(type) {
	case *expression.Column:
		if a.schema.Contains(x) {
			return newNullRejectNullConstant(), true
		}
		return nil, false
	case *expression.ScalarFunction:
		args := make([]expression.Expression, len(x.GetArgs()))
		allConst := true
		hasNullConst := false
		allListArgsNull := x.FuncName.L == ast.In && len(x.GetArgs()) > 1
		for i, arg := range x.GetArgs() {
			c, ok := a.analyzeConstant(arg)
			if !ok {
				allConst = false
				if i > 0 && x.FuncName.L == ast.In {
					allListArgsNull = false
				}
				continue
			}
			args[i] = c
			if c.Value.IsNull() {
				hasNullConst = true
			}
			if i > 0 && x.FuncName.L == ast.In && !c.Value.IsNull() {
				allListArgsNull = false
			}
		}
		if allConst {
			exact := expression.NewFunctionInternal(a.ctx.GetExprCtx(), x.FuncName.L, x.RetType.Clone(), args...)
			return foldNullRejectConstant(a.ctx, exact)
		}
		if x.FuncName.L == ast.In {
			// `NULL IN (...)` is always NULL in SQL 3VL, even if the list contains
			// more NULLs or values from non-target inputs. Once the left operand is
			// proven NULL, later reasoning does not need to inspect the list.
			if leftConst, ok := a.analyzeConstant(x.GetArgs()[0]); ok && leftConst.Value.IsNull() {
				return newNullRejectNullConstant(), true
			}
			// `expr IN (NULL, ..., NULL)` is also always NULL: every membership test
			// becomes `expr = NULL`, which can never yield TRUE, and the presence of
			// at least one NULL comparison keeps the final result at NULL instead of
			// FALSE. This shortcut is sound even when `expr` itself stays unknown.
			if allListArgsNull {
				return newNullRejectNullConstant(), true
			}
		}
		// For any-null-preserving builtins, one exact NULL child is enough to pin
		// the whole result to NULL. We use this only for builtins whose contract
		// was explicitly reviewed in the registry above.
		if hasNullConst && isNullRejectAnyNullPreservingFunc(x.FuncName.L) {
			return newNullRejectNullConstant(), true
		}
		return nil, false
	default:
		return nil, false
	}
}

func (a nullRejectAnalyzer) mustNull(expr expression.Expression) bool {
	if c, ok := a.analyzeConstant(expr); ok {
		return c.Value.IsNull()
	}
	switch x := expr.(type) {
	case *expression.Column:
		return a.schema.Contains(x)
	case *expression.ScalarFunction:
		switch x.FuncName.L {
		case ast.LogicAnd, ast.LogicOr, ast.LogicXor, ast.UnaryNot, ast.IsNull, ast.IsTruthWithNull, ast.IsTruthWithoutNull, ast.IsFalsity:
			return a.analyzeBool(expr) == nullRejectTruthNull
		default:
			// The builtin registry is split by the strongest sound proof we can use.
			// Any-null-preserving builtins expose NULL as soon as one child must be
			// NULL, while all-args-null-preserving builtins may still hide isolated
			// NULLs behind other inputs and only become must-NULL after every child
			// is forced to NULL.
			if isNullRejectAnyNullPreservingFunc(x.FuncName.L) {
				for _, arg := range x.GetArgs() {
					if a.mustNull(arg) {
						return true
					}
				}
				return false
			}
			if _, ok := nullRejectAllArgsNullPreservingFuncs[x.FuncName.L]; ok {
				for _, arg := range x.GetArgs() {
					if !a.mustNull(arg) {
						return false
					}
				}
				return len(x.GetArgs()) > 0
			}
			return false
		}
	default:
		return false
	}
}

func (a nullRejectAnalyzer) analyzeBool(expr expression.Expression) nullRejectTruthSet {
	if c, ok := a.analyzeConstant(expr); ok {
		return truthSetFromConstant(a.ctx, c)
	}
	sf, ok := expr.(*expression.ScalarFunction)
	if !ok {
		return a.truthifyScalar(expr)
	}
	switch sf.FuncName.L {
	case ast.LogicAnd:
		return combineBinaryTruthSet(a.analyzeBool(sf.GetArgs()[0]), a.analyzeBool(sf.GetArgs()[1]), andTruthValue)
	case ast.LogicOr:
		return combineBinaryTruthSet(a.analyzeBool(sf.GetArgs()[0]), a.analyzeBool(sf.GetArgs()[1]), orTruthValue)
	case ast.LogicXor:
		return combineBinaryTruthSet(a.analyzeBool(sf.GetArgs()[0]), a.analyzeBool(sf.GetArgs()[1]), xorTruthValue)
	case ast.UnaryNot:
		return applyUnaryTruthSet(a.analyzeBool(sf.GetArgs()[0]), notTruthValue)
	case ast.IsNull:
		// `IS NULL` is a total predicate. If the child must be NULL, the result is
		// exactly TRUE; otherwise we keep both TRUE and FALSE because non-target
		// inputs may still decide whether the child becomes NULL.
		if a.mustNull(sf.GetArgs()[0]) {
			return nullRejectTruthTrue
		}
		return nullRejectTruthFalse | nullRejectTruthTrue
	case ast.IsTruthWithNull:
		// `... IS TRUE` with `keepNull` preserves UNKNOWN: TRUE stays TRUE, FALSE
		// stays FALSE, and NULL remains NULL. This mapping mirrors the builtin's
		// runtime contract, so the proof can transform the child's truth set
		// pointwise without losing soundness.
		return applyUnaryTruthSet(a.analyzeBool(sf.GetArgs()[0]), func(v nullRejectTruthSet) nullRejectTruthSet {
			switch v {
			case nullRejectTruthTrue:
				return nullRejectTruthTrue
			case nullRejectTruthFalse:
				return nullRejectTruthFalse
			default:
				return nullRejectTruthNull
			}
		})
	case ast.IsTruthWithoutNull:
		// `... IS TRUE` without `keepNull` collapses both FALSE and UNKNOWN to
		// FALSE. The proof must preserve that distinction because this branch is
		// what lets `NOT ABS(inner_col)` become null-rejected while `IS TRUE`
		// itself still stays two-valued.
		return applyUnaryTruthSet(a.analyzeBool(sf.GetArgs()[0]), func(v nullRejectTruthSet) nullRejectTruthSet {
			if v == nullRejectTruthTrue {
				return nullRejectTruthTrue
			}
			return nullRejectTruthFalse
		})
	case ast.IsFalsity:
		// `... IS FALSE` is also two-valued here: only a definite FALSE child maps
		// to TRUE, while TRUE and UNKNOWN both map to FALSE.
		return applyUnaryTruthSet(a.analyzeBool(sf.GetArgs()[0]), func(v nullRejectTruthSet) nullRejectTruthSet {
			if v == nullRejectTruthFalse {
				return nullRejectTruthTrue
			}
			return nullRejectTruthFalse
		})
	case ast.NullEQ, ast.In:
		// This is the proof's fallback top element for operators whose exact
		// result depends on correlations we intentionally do not model after the
		// earlier constant/must-NULL shortcuts fail. Returning the full truth set
		// stays sound because null-reject only relies on proving TRUE impossible;
		// the extra UNKNOWN state is conservative precision loss, not a claim that
		// `<=>` itself can evaluate to NULL at runtime.
		return nullRejectTruthFalse | nullRejectTruthTrue | nullRejectTruthNull
	default:
		return a.truthifyScalar(expr)
	}
}

// IsNullRejected checks whether a predicate can never evaluate to TRUE after
// every column in `innerSchema` becomes NULL.
//
// The static proof is intentionally conservative for builtins whose NULL
// contract is not listed above. That may miss some optimizations, but it keeps
// null-reject reasoning sound across `NOT`, DNF/CNF combinations, and
// NULL-hiding functions.
func IsNullRejected(ctx base.PlanContext, innerSchema *expression.Schema, predicate expression.Expression,
	skipPlanCacheCheck bool) bool {
	// The static proof never evaluates parameterized expressions, so keeping the
	// legacy flag only preserves the public API shape used by existing callers.
	_ = skipPlanCacheCheck
	analyzer := nullRejectAnalyzer{ctx: ctx, schema: innerSchema}
	return !analyzer.analyzeBool(predicate).has(nullRejectTruthTrue)
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
