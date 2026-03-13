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
	"github.com/pingcap/tidb/pkg/util/intset"
)

// ──────────────────────────────────────────────────────────────────────
// Set-based algorithm: per-variable null-rejection analysis.
//
// Three proof modes in three-valued logic (3VL):
//   - NonTrue(E):  col-IDs where x=NULL ⇒ E ∈ {FALSE, UNKNOWN}
//   - NonFalse(E): col-IDs where x=NULL ⇒ E ∈ {TRUE, UNKNOWN}
//   - MustNull(E): col-IDs where x=NULL ⇒ E = UNKNOWN (NULL)
//
// NonFalse is needed for NOT: NonTrue(NOT A) = NonFalse(A), because
// NOT(A) ∈ {F,U} ↔ A ∈ {T,U}.
//
// Used by ExtractNotNullFromConds for functional dependency analysis.
// ──────────────────────────────────────────────────────────────────────

// NullRejectedCols returns the set of column UniqueIDs from schema for
// which the predicate is null-rejected under per-variable analysis.
// A column c is returned when: setting c = NULL alone (other columns
// free) guarantees E ∈ {FALSE, UNKNOWN}.
func NullRejectedCols(schema *expression.Schema, predicate expression.Expression) intset.FastIntSet {
	colIDs := schemaColIDs(schema)
	nt, _, _ := nonTrueFalseMustNull(predicate, colIDs)
	return nt
}

// nonTrueFalseMustNull recursively computes three column-ID sets:
//   - NonTrue(E):  x=NULL ⇒ E ∈ {FALSE, NULL}
//   - NonFalse(E): x=NULL ⇒ E ∈ {TRUE, NULL}
//   - MustNull(E): x=NULL ⇒ E = NULL
func nonTrueFalseMustNull(expr expression.Expression, colIDs intset.FastIntSet) (nonTrue, nonFalse, mustNull intset.FastIntSet) {
	switch x := expr.(type) {
	case *expression.Column:
		id := int(x.UniqueID)
		if colIDs.Has(id) {
			// Column in schema: x=NULL makes the column value NULL.
			// NULL is both non-TRUE and non-FALSE.
			s := intset.NewFastIntSet(id)
			return s, s, s
		}
		return
	case *expression.Constant:
		return // empty sets
	case *expression.ScalarFunction:
		return nonTrueFalseMustNullScalar(x, colIDs)
	default:
		return // conservative: empty
	}
}

func nonTrueFalseMustNullScalar(sf *expression.ScalarFunction, colIDs intset.FastIntSet) (nonTrue, nonFalse, mustNull intset.FastIntSet) {
	args := sf.GetArgs()
	switch sf.FuncName.L {

	// --- Logical AND ---
	// AND is non-TRUE if either side is non-TRUE.
	// AND is non-FALSE if either side is non-FALSE... NO: AND(F,T)=F, AND(T,F)=F.
	// Actually: AND is FALSE when either side is FALSE, so AND is non-FALSE when
	// BOTH sides are non-FALSE: NonFalse(AND) = NonFalse(A) ∩ NonFalse(B).
	// MustNull: AND(NULL,NULL)=NULL, AND(NULL,TRUE)=NULL, AND(NULL,FALSE)=FALSE.
	// MustNull(AND) = MustNull(A) ∩ MustNull(B) (both must be NULL to avoid FALSE).
	case ast.LogicAnd:
		nt0, nf0, mn0 := nonTrueFalseMustNull(args[0], colIDs)
		nt1, nf1, mn1 := nonTrueFalseMustNull(args[1], colIDs)
		nonTrue = nt0.Union(nt1)
		nonFalse = nf0.Intersection(nf1)
		mustNull = mn0.Intersection(mn1)
		return

	// --- Logical OR ---
	// OR is non-TRUE when BOTH sides are non-TRUE.
	// OR is non-FALSE when either side is non-FALSE (if one side is TRUE, OR=TRUE; but
	// that would only help nonFalse if we could detect TRUE, so conservatively:
	// OR is non-FALSE when either side guarantees non-FALSE).
	// Actually: OR(F,F)=F, OR(F,NULL)=NULL, OR(NULL,F)=NULL. OR is FALSE only when
	// both are FALSE. So non-FALSE of OR: either side non-FALSE suffices.
	// NonFalse(OR) = NonFalse(A) ∪ NonFalse(B).
	// MustNull: OR(NULL,NULL)=NULL, OR(NULL,TRUE)=TRUE, OR(NULL,FALSE)=NULL.
	// MustNull(OR) = MustNull(A) ∩ MustNull(B).
	case ast.LogicOr:
		nt0, nf0, mn0 := nonTrueFalseMustNull(args[0], colIDs)
		nt1, nf1, mn1 := nonTrueFalseMustNull(args[1], colIDs)
		nonTrue = nt0.Intersection(nt1)
		nonFalse = nf0.Union(nf1)
		mustNull = mn0.Intersection(mn1)
		return

	// --- NOT ---
	// NOT flips TRUE↔FALSE, leaves NULL unchanged.
	// NonTrue(NOT A) = NonFalse(A)
	// NonFalse(NOT A) = NonTrue(A)
	// MustNull(NOT A) = MustNull(A)
	case ast.UnaryNot:
		nt, nf, mn := nonTrueFalseMustNull(args[0], colIDs)
		nonTrue = nf    // NOT is non-TRUE when A is non-FALSE
		nonFalse = nt   // NOT is non-FALSE when A is non-TRUE
		mustNull = mn
		return

	// --- IN ---
	// a IN (b1,...,bn) = a=b1 OR ... OR a=bn
	// Each a=bi is null-preserving (EQ). For null-preserving:
	//   nonTrue = nonFalse = mustNull = ∪ mustNull(argi).
	// For OR of these:
	//   NonTrue = ∩ nonTrue(a=bi) = ∩ (mustNull(a) ∪ mustNull(bi))
	//   NonFalse = ∪ nonFalse(a=bi) = ∪ (mustNull(a) ∪ mustNull(bi))
	//            = mustNull(a) ∪ (∪ mustNull(bi))
	//   MustNull = ∩ mustNull(a=bi) = ∩ (mustNull(a) ∪ mustNull(bi))
	case ast.In:
		if len(args) < 2 {
			return
		}
		_, _, mnLHS := nonTrueFalseMustNull(args[0], colIDs)

		var mnArgIntersection intset.FastIntSet
		var mnArgUnion intset.FastIntSet
		first := true
		for i := 1; i < len(args); i++ {
			_, _, mnArg := nonTrueFalseMustNull(args[i], colIDs)
			if first {
				mnArgIntersection = mnArg
				mnArgUnion = mnArg
				first = false
			} else {
				mnArgIntersection = mnArgIntersection.Intersection(mnArg)
				mnArgUnion = mnArgUnion.Union(mnArg)
			}
		}
		combined := mnLHS.Union(mnArgIntersection)
		nonTrue = combined
		mustNull = combined
		nonFalse = mnLHS.Union(mnArgUnion)
		return

	// --- IS NULL ---
	// IS NULL(x): x=NULL → TRUE. Never returns NULL.
	// NonTrue = {} (IS NULL(NULL) = TRUE, which is not non-TRUE)
	// NonFalse = MustNull(x) (IS NULL(NULL) = TRUE ∈ {T,U})
	// MustNull = {} (never returns NULL)
	case ast.IsNull:
		_, _, mn := nonTrueFalseMustNull(args[0], colIDs)
		nonFalse = mn
		return

	// --- IS TRUE (IsTruthWithNull) ---
	// IS TRUE: NULL→FALSE, TRUE→TRUE, FALSE→FALSE. Never returns NULL.
	// NonTrue = MustNull(arg) (NULL input → FALSE which is non-TRUE)
	// NonFalse = {} conservatively (we'd need to know when arg is TRUE)
	// MustNull = {}
	case ast.IsTruthWithNull:
		_, _, mn := nonTrueFalseMustNull(args[0], colIDs)
		nonTrue = mn
		return

	// --- IsTruthWithoutNull (used for IS NOT NULL in some contexts) ---
	// Returns TRUE if arg is non-NULL and truthy, FALSE otherwise. Never NULL.
	// NULL input → FALSE.
	// NonTrue = MustNull(arg)
	// NonFalse = {} conservatively
	case ast.IsTruthWithoutNull:
		_, _, mn := nonTrueFalseMustNull(args[0], colIDs)
		nonTrue = mn
		return

	// --- IS FALSE (IsFalsity) ---
	// IS FALSE: NULL→FALSE, TRUE→FALSE, FALSE→TRUE. Never returns NULL.
	// NonTrue = MustNull(arg) (NULL → FALSE is non-TRUE)
	// NonFalse = MustNull(arg) (NULL → FALSE is... FALSE, which is non-FALSE? No, FALSE IS non-FALSE? No.)
	// Wait: NonFalse means "result ∈ {T,U}". IS FALSE(NULL) = FALSE, so not non-FALSE.
	// NonFalse = {} conservatively.
	case ast.IsFalsity:
		_, _, mn := nonTrueFalseMustNull(args[0], colIDs)
		nonTrue = mn
		return

	default:
		return nonTrueFalseMustNullByPropagation(sf, colIDs)
	}
}

func nonTrueFalseMustNullByPropagation(sf *expression.ScalarFunction, colIDs intset.FastIntSet) (nonTrue, nonFalse, mustNull intset.FastIntSet) {
	prop := expression.GetNullPropagation(sf.FuncName.L)
	switch prop {
	case expression.NullPropPreserving:
		// Any NULL arg → NULL output.
		// MustNull = ∪ MustNull(argi)
		// For null-preserving: NonTrue = NonFalse = MustNull (NULL is both non-TRUE and non-FALSE).
		args := sf.GetArgs()
		for _, arg := range args {
			_, _, mn := nonTrueFalseMustNull(arg, colIDs)
			mustNull = mustNull.Union(mn)
		}
		nonTrue = mustNull
		nonFalse = mustNull
		return

	default:
		return // conservative: empty
	}
}

// ──────────────────────────────────────────────────────────────────────
// Boolean-based algorithm: joint null-rejection analysis.
// Given a target set S (all columns of the inner schema), returns true
// when "all columns in S simultaneously NULL" ⇒ E is non-TRUE.
// Used for outer join elimination.
// ──────────────────────────────────────────────────────────────────────

// isNonTrue returns true when: setting every column in targetIDs to NULL
// simultaneously makes expr evaluate to non-TRUE (FALSE or UNKNOWN).
func isNonTrue(expr expression.Expression, targetIDs intset.FastIntSet) bool {
	switch x := expr.(type) {
	case *expression.Column:
		return targetIDs.Has(int(x.UniqueID))
	case *expression.Constant:
		return false
	case *expression.ScalarFunction:
		return isNonTrueScalar(x, targetIDs)
	default:
		return false
	}
}

func isNonTrueScalar(sf *expression.ScalarFunction, targetIDs intset.FastIntSet) bool {
	args := sf.GetArgs()
	switch sf.FuncName.L {
	case ast.LogicAnd:
		return isNonTrue(args[0], targetIDs) || isNonTrue(args[1], targetIDs)

	case ast.LogicOr:
		return isNonTrue(args[0], targetIDs) && isNonTrue(args[1], targetIDs)

	case ast.UnaryNot:
		// NOT(A) is non-TRUE when A is non-FALSE (A ∈ {T, U}).
		return isNonFalse(args[0], targetIDs)

	case ast.In:
		// a IN (b1,...,bn) = a=b1 OR ... OR a=bn
		// OR is non-TRUE iff all branches are non-TRUE.
		if len(args) < 2 {
			return false
		}
		for i := 1; i < len(args); i++ {
			// Each a=bi is null-preserving. isNonTrue = isMustNull.
			if !(isMustNull(args[0], targetIDs) || isMustNull(args[i], targetIDs)) {
				return false
			}
		}
		return true

	case ast.IsNull:
		// IS NULL accepts NULL (returns TRUE), not null-rejecting.
		return false

	case ast.IsTruthWithNull, ast.IsTruthWithoutNull, ast.IsFalsity:
		// NULL input → FALSE output (non-TRUE).
		return isMustNull(args[0], targetIDs)

	default:
		return isNonTrueByPropagation(sf, targetIDs)
	}
}

func isNonTrueByPropagation(sf *expression.ScalarFunction, targetIDs intset.FastIntSet) bool {
	prop := expression.GetNullPropagation(sf.FuncName.L)
	switch prop {
	case expression.NullPropPreserving:
		return isMustNullScalar(sf, targetIDs)
	default:
		return false
	}
}

// isNonFalse returns true when: setting every column in targetIDs to NULL
// simultaneously makes expr evaluate to non-FALSE (TRUE or UNKNOWN).
func isNonFalse(expr expression.Expression, targetIDs intset.FastIntSet) bool {
	switch x := expr.(type) {
	case *expression.Column:
		// Column = NULL, which is UNKNOWN, which is non-FALSE.
		return targetIDs.Has(int(x.UniqueID))
	case *expression.Constant:
		return false
	case *expression.ScalarFunction:
		return isNonFalseScalar(x, targetIDs)
	default:
		return false
	}
}

func isNonFalseScalar(sf *expression.ScalarFunction, targetIDs intset.FastIntSet) bool {
	args := sf.GetArgs()
	switch sf.FuncName.L {
	case ast.LogicAnd:
		// AND is non-FALSE when both sides are non-FALSE.
		return isNonFalse(args[0], targetIDs) && isNonFalse(args[1], targetIDs)

	case ast.LogicOr:
		// OR is non-FALSE when either side is non-FALSE.
		return isNonFalse(args[0], targetIDs) || isNonFalse(args[1], targetIDs)

	case ast.UnaryNot:
		// NOT(A) is non-FALSE when A is non-TRUE.
		return isNonTrue(args[0], targetIDs)

	case ast.In:
		// a IN (b1,...,bn) = a=b1 OR ... OR a=bn
		// OR is non-FALSE when any branch is non-FALSE.
		if len(args) < 2 {
			return false
		}
		for i := 1; i < len(args); i++ {
			if isMustNull(args[0], targetIDs) || isMustNull(args[i], targetIDs) {
				return true
			}
		}
		return false

	case ast.IsNull:
		// IS NULL(x): x=NULL → TRUE, which is non-FALSE.
		return isMustNull(args[0], targetIDs)

	case ast.IsTruthWithNull, ast.IsTruthWithoutNull, ast.IsFalsity:
		// NULL input → FALSE output, which is NOT non-FALSE.
		return false

	default:
		return isNonFalseByPropagation(sf, targetIDs)
	}
}

func isNonFalseByPropagation(sf *expression.ScalarFunction, targetIDs intset.FastIntSet) bool {
	prop := expression.GetNullPropagation(sf.FuncName.L)
	switch prop {
	case expression.NullPropPreserving:
		// NULL-preserving: any arg must-null → output NULL → non-FALSE.
		return isMustNullScalar(sf, targetIDs)
	default:
		return false
	}
}

// isMustNull returns true when: setting every column in targetIDs to NULL
// simultaneously makes expr evaluate to NULL.
func isMustNull(expr expression.Expression, targetIDs intset.FastIntSet) bool {
	switch x := expr.(type) {
	case *expression.Column:
		return targetIDs.Has(int(x.UniqueID))
	case *expression.Constant:
		return false
	case *expression.ScalarFunction:
		return isMustNullScalar(x, targetIDs)
	default:
		return false
	}
}

func isMustNullScalar(sf *expression.ScalarFunction, targetIDs intset.FastIntSet) bool {
	args := sf.GetArgs()
	switch sf.FuncName.L {
	case ast.LogicAnd:
		return isMustNull(args[0], targetIDs) && isMustNull(args[1], targetIDs)

	case ast.LogicOr:
		return isMustNull(args[0], targetIDs) && isMustNull(args[1], targetIDs)

	case ast.UnaryNot:
		return isMustNull(args[0], targetIDs)

	case ast.In:
		if len(args) < 2 {
			return false
		}
		for i := 1; i < len(args); i++ {
			if !(isMustNull(args[0], targetIDs) || isMustNull(args[i], targetIDs)) {
				return false
			}
		}
		return true

	case ast.IsNull, ast.IsTruthWithNull, ast.IsTruthWithoutNull, ast.IsFalsity:
		return false

	default:
		return isMustNullByPropagation(sf, targetIDs)
	}
}

func isMustNullByPropagation(sf *expression.ScalarFunction, targetIDs intset.FastIntSet) bool {
	prop := expression.GetNullPropagation(sf.FuncName.L)
	switch prop {
	case expression.NullPropPreserving:
		for _, arg := range sf.GetArgs() {
			if isMustNull(arg, targetIDs) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// ──────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────

// schemaColIDs builds a FastIntSet of column UniqueIDs from a schema.
func schemaColIDs(schema *expression.Schema) intset.FastIntSet {
	var s intset.FastIntSet
	for _, col := range schema.Columns {
		s.Insert(int(col.UniqueID))
	}
	return s
}
