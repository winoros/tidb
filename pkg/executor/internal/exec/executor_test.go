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

package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type mockNextIOAccExecutor struct {
	BaseExecutorV2
}

func newMockNextIOAccExecutor(children ...Executor) *mockNextIOAccExecutor {
	ctx := mock.NewContext()
	return &mockNextIOAccExecutor{
		BaseExecutorV2: NewBaseExecutorV2(ctx.GetSessionVars(), nil, 0, children...),
	}
}

func TestNextIOAccAddInputCountsRowsWithZeroCols(t *testing.T) {
	t.Run("add input counts rows with zero cols", func(t *testing.T) {
		acc := &nextIOAcc{}

		acc.addInput(3, 0)

		require.Equal(t, int64(3), acc.inRows)
		require.Equal(t, int64(0), acc.inCells)
	})

	t.Run("base executor reuses local accumulator state", func(t *testing.T) {
		exec := newMockNextIOAccExecutor()

		first := getReusableNextIOAcc(exec)
		first.addInput(4, 2)

		second := getReusableNextIOAcc(exec)
		require.Same(t, first, second)
		require.Equal(t, int64(0), second.inRows)
		require.Equal(t, int64(0), second.inCells)

		allocs := testing.AllocsPerRun(1000, func() {
			acc := getReusableNextIOAcc(exec)
			acc.addInput(1, 1)
		})
		require.Less(t, allocs, 1.)
	})

	t.Run("only executors with children need local accumulator", func(t *testing.T) {
		parentAcc := &nextIOAcc{}

		require.False(t, needNextIOAcc(true, nil, 0))
		require.True(t, needNextIOAcc(true, nil, 1))
		require.False(t, needNextIOAcc(false, parentAcc, 0))
		require.True(t, needNextIOAcc(false, parentAcc, 1))
	})
}

func TestRUV2ExecutorMetricByTypeIncludesConcreteExecutorTypes(t *testing.T) {
	cases := map[string]ruv2ExecutorMetric{
		"*aggregate.HashAggExec":        {level: 2, label: "HashAggExec", useCells: false},
		"*aggregate.StreamAggExec":      {level: 3, label: "StreamAggExec", useCells: false},
		"*executor.BatchPointGetExec":   {level: 1, label: "BatchPointGetExec", useCells: true},
		"*executor.ExpandExec":          {level: 2, label: "ExpandExec", useCells: false},
		"*executor.IndexLookUpExecutor": {level: 2, label: "IndexLookUpExecutor", useCells: false},
		"*executor.IndexReaderExecutor": {level: 2, label: "IndexReaderExecutor", useCells: false},
		"*executor.LimitExec":           {level: 1, label: "LimitExec", useCells: true},
		"*executor.MemTableReaderExec":  {level: 2, label: "MemTableReaderExec", useCells: false},
		"*executor.PointGetExecutor":    {level: 1, label: "PointGetExecutor", useCells: true},
		"*executor.ProjectionExec":      {level: 2, label: "ProjectionExec", useCells: true},
		"*executor.SelectLockExec":      {level: 2, label: "SelectLockExec", useCells: true},
		"*executor.SelectionExec":       {level: 2, label: "SelectionExec", useCells: false},
		"*executor.TableDualExec":       {level: 2, label: "TableDualExec", useCells: false},
		"*executor.TableReaderExecutor": {level: 2, label: "TableReaderExecutor", useCells: false},
		"*executor.UnionScanExec":       {level: 2, label: "UnionScanExec", useCells: false},
		"*join.HashJoinV1Exec":          {level: 2, label: "HashJoinV1Exec", useCells: false},
		"*join.HashJoinV2Exec":          {level: 2, label: "HashJoinV2Exec", useCells: false},
		"*join.IndexLookUpJoin":         {level: 2, label: "IndexLookUpJoin", useCells: true},
		"*join.IndexLookUpMergeJoin":    {level: 2, label: "IndexLookUpMergeJoin", useCells: true},
		"*join.IndexNestedLoopHashJoin": {level: 2, label: "IndexNestedLoopHashJoin", useCells: true},
		"*join.MergeJoinExec":           {level: 2, label: "MergeJoinExec", useCells: false},
		"*sortexec.SortExec":            {level: 3, label: "SortExec", useCells: true},
		"*sortexec.TopNExec":            {level: 2, label: "TopNExec", useCells: true},
		"*windows.OrderedWindowExec":    {level: 2, label: "WindowExec", useCells: false},
		"*windows.PipelinedWindowExec":  {level: 2, label: "WindowExec", useCells: false},
		"*windows.WindowExec":           {level: 2, label: "WindowExec", useCells: false},
	}

	for typ, expected := range cases {
		actual, ok := ruv2ExecutorMetricByType(typ)
		require.True(t, ok, typ)
		require.Equal(t, expected, actual)
	}

	for _, staleType := range []string{
		"*executor.HashJoinExec",
		"*executor.IndexLookUpJoin",
		"*executor.SortExec",
		"*executor.WindowExec",
	} {
		_, ok := ruv2ExecutorMetricByType(staleType)
		require.False(t, ok, staleType)
	}
}

type mockStatementRUExecutor struct {
	BaseExecutorV2
	rows       int
	returnErr  error
	panicValue any
	childReq   *chunk.Chunk
}

type statementRUUnsupportedExecutor struct {
	Executor
}

func newMockStatementRUExecutor(ctx sessionctx.Context, rows int, children ...Executor) *mockStatementRUExecutor {
	executor := &mockStatementRUExecutor{
		BaseExecutorV2: NewBaseExecutorV2(ctx.GetSessionVars(), nil, 0, children...),
		rows:           rows,
	}
	if len(children) > 0 {
		executor.childReq = children[0].NewChunk()
	}
	return executor
}

func (e *mockStatementRUExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if len(e.AllChildren()) > 0 {
		child := e.Children(0)
		if err := Next(ctx, child, e.childReq); err != nil {
			return err
		}
		e.RecordStatementRUCPUWork(e.childReq.NumRows())
	}
	if e.panicValue != nil {
		panic(e.panicValue)
	}
	if e.returnErr != nil {
		return e.returnErr
	}
	req.SetNumVirtualRows(e.rows)
	return nil
}

func configureStatementRUCPUWorkForTest(t testing.TB, ctx sessionctx.Context, executor Executor, multiplier int) *statementru.Statement {
	weights := statementru.Weights{statementru.CPUWork: 1}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))
	require.True(t, ConfigureStatementRUCPUWork(executor, sc, multiplier))
	return sc.TakeStatementRUForExecution()
}

func statementRUCPUWorkUnits(t testing.TB, statement *statementru.Statement, terminal statementru.TerminalStatus) float64 {
	require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := statement.Finish(terminal)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	return units[statementru.CPUWork]
}

func TestStatementRUCPUWorkHookAccounting(t *testing.T) {
	t.Run("aggregated rows retain int64 width", func(t *testing.T) {
		ctx := mock.NewContext()
		executor := newMockStatementRUExecutor(ctx, 0)
		statement := configureStatementRUCPUWorkForTest(t, ctx, executor, 2)
		rows := int64(1) << 40

		executor.RecordStatementRUCPUWork64(rows)

		require.Equal(t, float64(rows*2), statementRUCPUWorkUnits(t, statement, statementru.TerminalSuccess))
	})

	t.Run("multiple chunks use frozen direct-child formula", func(t *testing.T) {
		ctx := mock.NewContext()
		child := newMockStatementRUExecutor(ctx, 4)
		parent := newMockStatementRUExecutor(ctx, 2, child)
		statement := configureStatementRUCPUWorkForTest(t, ctx, parent, 3)

		req := parent.NewChunk()
		require.NoError(t, Next(context.Background(), parent, req))
		require.NoError(t, Next(context.Background(), parent, req))

		require.Equal(t, float64(24), statementRUCPUWorkUnits(t, statement, statementru.TerminalSuccess))
	})

	t.Run("nested descendant rows do not leak into parent input", func(t *testing.T) {
		ctx := mock.NewContext()
		grandchild := newMockStatementRUExecutor(ctx, 7)
		child := newMockStatementRUExecutor(ctx, 3, grandchild)
		parent := newMockStatementRUExecutor(ctx, 1, child)
		statement := configureStatementRUCPUWorkForTest(t, ctx, parent, 2)

		require.NoError(t, Next(context.Background(), parent, parent.NewChunk()))

		require.Equal(t, float64(6), statementRUCPUWorkUnits(t, statement, statementru.TerminalSuccess))
	})

	t.Run("successful child work survives parent error and panic", func(t *testing.T) {
		for _, test := range []struct {
			name       string
			returnErr  error
			panicValue any
		}{
			{name: "error", returnErr: errors.New("parent failed")},
			{name: "panic", panicValue: "parent panicked"},
		} {
			t.Run(test.name, func(t *testing.T) {
				ctx := mock.NewContext()
				child := newMockStatementRUExecutor(ctx, 5)
				parent := newMockStatementRUExecutor(ctx, 0, child)
				parent.returnErr = test.returnErr
				parent.panicValue = test.panicValue
				statement := configureStatementRUCPUWorkForTest(t, ctx, parent, 2)

				require.Error(t, Next(context.Background(), parent, parent.NewChunk()))

				require.Equal(t, float64(10), statementRUCPUWorkUnits(t, statement, statementru.TerminalError))
			})
		}
	})

	t.Run("errored child chunk does not become parent input", func(t *testing.T) {
		ctx := mock.NewContext()
		child := newMockStatementRUExecutor(ctx, 5)
		child.returnErr = errors.New("child failed")
		parent := newMockStatementRUExecutor(ctx, 0, child)
		statement := configureStatementRUCPUWorkForTest(t, ctx, parent, 2)

		require.Error(t, Next(context.Background(), parent, parent.NewChunk()))

		require.Equal(t, float64(0), statementRUCPUWorkUnits(t, statement, statementru.TerminalError))
	})
}

func TestStatementRUCPUWorkHookConfiguration(t *testing.T) {
	t.Run("off and zero formulas install no hook", func(t *testing.T) {
		ctx := mock.NewContext()
		executor := newMockStatementRUExecutor(ctx, 0, newMockStatementRUExecutor(ctx, 1))

		require.True(t, ConfigureStatementRUCPUWork(executor, ctx.GetSessionVars().StmtCtx, 1))
		require.Nil(t, executor.statementRUHook)
		require.False(t, ConfigureStatementRUCPUWork(&statementRUUnsupportedExecutor{}, ctx.GetSessionVars().StmtCtx, 1))

		weights := statementru.Weights{statementru.CPUWork: 1}
		require.True(t, ctx.GetSessionVars().StmtCtx.ConfigureStatementRU(statementru.Selection{
			Mode:          statementru.ModeCalibration,
			Applicable:    true,
			RequiredUnits: statementru.CPUWork.Mask(),
			Weights:       &weights,
		}))
		require.True(t, ConfigureStatementRUCPUWork(executor, ctx.GetSessionVars().StmtCtx, 0))
		require.Nil(t, executor.statementRUHook)
		require.False(t, ConfigureStatementRUCPUWork(executor, ctx.GetSessionVars().StmtCtx, -1))
	})

	t.Run("enabled formula is installed once", func(t *testing.T) {
		ctx := mock.NewContext()
		executor := newMockStatementRUExecutor(ctx, 0, newMockStatementRUExecutor(ctx, 1))
		_ = configureStatementRUCPUWorkForTest(t, ctx, executor, 1)
		installed := executor.statementRUHook
		require.NotNil(t, installed)

		require.False(t, ConfigureStatementRUCPUWork(executor, ctx.GetSessionVars().StmtCtx, 2))
		require.Same(t, installed, executor.statementRUHook)
	})

	t.Run("terminal-only executor keeps recorder with zero CPU multiplier", func(t *testing.T) {
		ctx := mock.NewContext()
		executor := newMockStatementRUExecutor(ctx, 0)
		weights := statementru.Weights{statementru.HashStateRows: 1}
		require.True(t, ctx.GetSessionVars().StmtCtx.ConfigureStatementRU(statementru.Selection{
			Mode:          statementru.ModeCalibration,
			Applicable:    true,
			RequiredUnits: statementru.HashStateRows.Mask(),
			Weights:       &weights,
		}))
		require.True(t, ConfigureStatementRUExecutor(executor, ctx.GetSessionVars().StmtCtx, StatementRUExecutorConfig{
			AdditionalUnits: statementru.HashStateRows.Mask(),
		}))
		require.True(t, executor.StatementRUEnabled())
		executor.RecordStatementRUUnit(statementru.HashStateRows, 7)

		statement := ctx.GetSessionVars().StmtCtx.TakeStatementRUForExecution()
		require.NotNil(t, statement)
		require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.HashStateRows.Mask()))
		result, first := statement.Finish(statementru.TerminalSuccess)
		require.True(t, first)
		units, ok := result.Result.Units()
		require.True(t, ok)
		require.Equal(t, float64(7), units[statementru.HashStateRows])
	})

	t.Run("fully disjoint producer installs no hook", func(t *testing.T) {
		ctx := mock.NewContext()
		executor := newMockStatementRUExecutor(ctx, 0)
		weights := statementru.Weights{statementru.NetworkBytes: 1}
		require.True(t, ctx.GetSessionVars().StmtCtx.ConfigureStatementRU(statementru.Selection{
			Mode:          statementru.ModeCalibration,
			Applicable:    true,
			RequiredUnits: statementru.NetworkBytes.Mask(),
			Weights:       &weights,
		}))

		require.True(t, ConfigureStatementRUExecutor(executor, ctx.GetSessionVars().StmtCtx, StatementRUExecutorConfig{
			CPUWorkMultiplier: 3,
			AdditionalUnits:   statementru.HashStateRows.Mask(),
		}))
		require.False(t, executor.StatementRUEnabled())
	})

	t.Run("CPU multiplier is disabled when only an additional unit intersects", func(t *testing.T) {
		ctx := mock.NewContext()
		executor := newMockStatementRUExecutor(ctx, 0)
		weights := statementru.Weights{statementru.NetworkBytes: 1}
		sc := ctx.GetSessionVars().StmtCtx
		require.True(t, sc.ConfigureStatementRU(statementru.Selection{
			Mode:           statementru.ModeCalibration,
			Applicable:     true,
			RequiredUnits:  statementru.NetworkBytes.Mask(),
			CollectedUnits: statementru.NetworkBytes.Mask() | statementru.HashStateRows.Mask(),
			Weights:        &weights,
		}))
		require.True(t, ConfigureStatementRUExecutor(executor, sc, StatementRUExecutorConfig{
			CPUWorkMultiplier: 3,
			AdditionalUnits:   statementru.HashStateRows.Mask(),
		}))
		require.True(t, executor.StatementRUEnabled())
		require.Zero(t, executor.statementRUHook.multiplier)

		executor.RecordStatementRUCPUWork(11)
		executor.RecordStatementRUUnit(statementru.HashStateRows, 7)
		statement := sc.TakeStatementRUForExecution()
		require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.NetworkBytes.Mask()|statementru.HashStateRows.Mask()))
		finish, first := statement.Finish(statementru.TerminalSuccess)
		require.True(t, first)
		units, ok := finish.Result.Units()
		require.True(t, ok)
		require.Zero(t, units[statementru.CPUWork])
		require.Equal(t, float64(7), units[statementru.HashStateRows])
	})

	t.Run("installed hook accepts only its frozen producer mask", func(t *testing.T) {
		ctx := mock.NewContext()
		executor := newMockStatementRUExecutor(ctx, 0)
		weights := statementru.Weights{statementru.CPUWork: 1}
		sc := ctx.GetSessionVars().StmtCtx
		require.True(t, sc.ConfigureStatementRU(statementru.Selection{
			Mode:           statementru.ModeCalibration,
			Applicable:     true,
			RequiredUnits:  statementru.CPUWork.Mask(),
			CollectedUnits: statementru.CPUWork.Mask() | statementru.HashStateRows.Mask() | statementru.JoinOutputRows.Mask(),
			Weights:        &weights,
		}))
		require.True(t, ConfigureStatementRUExecutor(executor, sc, StatementRUExecutorConfig{
			CPUWorkMultiplier: 1,
			AdditionalUnits:   statementru.HashStateRows.Mask(),
		}))

		executor.RecordStatementRUCPUWork(2)
		executor.RecordStatementRUUnit(statementru.HashStateRows, 7)
		executor.RecordStatementRUUnit(statementru.JoinOutputRows, 11)
		statement := sc.TakeStatementRUForExecution()
		require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
		finish, first := statement.Finish(statementru.TerminalSuccess)
		require.True(t, first)
		require.Equal(t, statementru.Outcome{State: statementru.StateComplete}, finish.Result.Outcome())
		units, ok := finish.Result.Units()
		require.True(t, ok)
		require.Equal(t, float64(2), units[statementru.CPUWork])
		require.Equal(t, float64(7), units[statementru.HashStateRows])
		require.Zero(t, units[statementru.JoinOutputRows])
	})

	t.Run("invalid additional unit mask is rejected", func(t *testing.T) {
		ctx := mock.NewContext()
		executor := newMockStatementRUExecutor(ctx, 0)
		invalid := statementru.UnitMask(1 << statementru.UnitCount)

		require.False(t, ConfigureStatementRUExecutor(executor, ctx.GetSessionVars().StmtCtx, StatementRUExecutorConfig{
			AdditionalUnits: invalid,
		}))
		require.False(t, executor.StatementRUEnabled())
	})

	t.Run("derived base does not inherit formula", func(t *testing.T) {
		ctx := mock.NewContext()
		child := newMockStatementRUExecutor(ctx, 1)
		executor := newMockStatementRUExecutor(ctx, 0, child)
		_ = configureStatementRUCPUWorkForTest(t, ctx, executor, 1)
		require.NotNil(t, executor.statementRUHook)

		derived := executor.BuildNewBaseExecutorV2(nil, nil, 1, child)
		require.Nil(t, derived.statementRUHook)
	})
}

func TestInvalidateStatementRUUnit(t *testing.T) {
	ctx := mock.NewContext()
	executor := newMockStatementRUExecutor(ctx, 0)
	weights := statementru.Weights{statementru.CPUWork: 1}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeResultOnly,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))
	require.True(t, ConfigureStatementRUExecutor(executor, sc, StatementRUExecutorConfig{
		AdditionalUnits: statementru.CPUWork.Mask(),
	}))
	statement := sc.TakeStatementRUForExecution()
	require.NotNil(t, statement)

	executor.InvalidateStatementRUUnit(statementru.CPUWork)
	require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.False(t, finish.Result.HasTotal())
	require.Equal(t, statementru.Outcome{
		State:  statementru.StateInvalid,
		Reason: statementru.ReasonInvalidObservation,
	}, finish.Result.Outcome())
}

func BenchmarkStatementRUCPUWorkHook(b *testing.B) {
	for _, mode := range []string{"disabled", "collected_disjoint", "enabled"} {
		b.Run(mode, func(b *testing.B) {
			ctx := mock.NewContext()
			child := newMockStatementRUExecutor(ctx, 1)
			parent := newMockStatementRUExecutor(ctx, 1, child)
			var statement *statementru.Statement
			switch mode {
			case "collected_disjoint":
				weights := statementru.Weights{statementru.NetworkBytes: 1}
				sc := ctx.GetSessionVars().StmtCtx
				if !sc.ConfigureStatementRU(statementru.Selection{
					Mode:          statementru.ModeCalibration,
					Applicable:    true,
					RequiredUnits: statementru.NetworkBytes.Mask(),
					Weights:       &weights,
				}) || !ConfigureStatementRUCPUWork(parent, sc, 1) {
					b.Fatal("statement RU disjoint configuration rejected")
				}
				if parent.StatementRUEnabled() {
					b.Fatal("disjoint collection installed a CPU hook")
				}
			case "enabled":
				statement = configureStatementRUCPUWorkForTest(b, ctx, parent, 1)
			}
			req := parent.NewChunk()
			goCtx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := Next(goCtx, parent, req); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if statement != nil {
				require.Equal(b, float64(b.N), statementRUCPUWorkUnits(b, statement, statementru.TerminalSuccess))
			}
		})
	}
}
