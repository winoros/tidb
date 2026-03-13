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

package expression

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

// TestAllBuiltinFunctionsClassified ensures every function in the funcs registry
// plus all specially-constructed ScalarFunction names have an entry in
// nullPropagationMap. When a new builtin is added, this test fails and
// forces the developer to classify the new function.
func TestAllBuiltinFunctionsClassified(t *testing.T) {
	// All functions registered in the funcs map.
	for name := range funcs {
		_, ok := nullPropagationMap[name]
		require.True(t, ok, "builtin function %q (in funcs map) is not classified "+
			"in nullPropagationMap; add it to pkg/expression/null_propagation.go", name)
	}

	// Functions that bypass the funcs map but can appear as ScalarFunction.FuncName.
	specialNames := []string{
		ast.Cast,
		ast.GetVar,
		ast.Values,
		InternalFuncFromBinary,
		InternalFuncToBinary,
	}
	for _, name := range specialNames {
		_, ok := nullPropagationMap[name]
		require.True(t, ok, "special function %q is not classified "+
			"in nullPropagationMap; add it to pkg/expression/null_propagation.go", name)
	}
}

func TestGetNullPropagation(t *testing.T) {
	require.Equal(t, NullPropSpecial, GetNullPropagation(ast.LogicAnd))
	require.Equal(t, NullPropSpecial, GetNullPropagation(ast.LogicOr))
	require.Equal(t, NullPropSpecial, GetNullPropagation(ast.In))
	require.Equal(t, NullPropSpecial, GetNullPropagation(ast.IsNull))
	require.Equal(t, NullPropPreserving, GetNullPropagation(ast.EQ))
	require.Equal(t, NullPropPreserving, GetNullPropagation(ast.Plus))
	require.Equal(t, NullPropHiding, GetNullPropagation(ast.Coalesce))
	require.Equal(t, NullPropHiding, GetNullPropagation(ast.NullEQ))
	// Unknown function is conservatively treated as Hiding.
	require.Equal(t, NullPropHiding, GetNullPropagation("unknown_func_xyz"))
}
