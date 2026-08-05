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

package sortexec

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

var errStatementRUChild = errors.New("statement RU child error")

type statementRUErrorDataSource struct {
	*testutil.MockDataSource
	nextCalls int
}

func (s *statementRUErrorDataSource) Next(ctx context.Context, req *chunk.Chunk) error {
	if s.nextCalls == 1 {
		s.nextCalls++
		return errStatementRUChild
	}
	s.nextCalls++
	return s.MockDataSource.Next(ctx, req)
}

func newStatementRUDataSource(rows int) (*mock.Context, *testutil.MockDataSource) {
	return newStatementRUDataSourceWithChunkSize(rows, 2)
}

func newStatementRUDataSourceWithChunkSize(rows, chunkSize int) (*mock.Context, *testutil.MockDataSource) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = chunkSize
	ctx.GetSessionVars().MaxChunkSize = chunkSize
	ctx.GetSessionVars().ExecutorConcurrency = 2
	ctx.GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(memory.LabelForSQLText, -1)
	ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(ctx.GetSessionVars().MemTracker)

	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(column)
	dataSource := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		Ctx:        ctx,
		DataSchema: schema,
		Rows:       rows,
		GenDataFunc: func(row int, _ *types.FieldType) any {
			return int64(row)
		},
	})
	dataSource.PrepareChunks()
	return ctx, dataSource
}

func newStatementRUSortExec(ctx sessionctx.Context, child exec.Executor) *SortExec {
	column := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)}
	schema := expression.NewSchema(column)
	return &SortExec{
		BaseExecutor: exec.NewBaseExecutor(ctx, schema, 1, child),
		ByItems: []*plannerutil.ByItems{
			{Expr: column},
		},
		ExecSchema: schema,
	}
}

func newStatementRUTopNExec(ctx sessionctx.Context, child exec.Executor, offset, count uint64) *TopNExec {
	return &TopNExec{
		SortExec:    *newStatementRUSortExec(ctx, child),
		Limit:       &physicalop.PhysicalLimit{Offset: offset, Count: count},
		Concurrency: 2,
	}
}

func configureStatementRUNonlinearForTest(t testing.TB, ctx sessionctx.Context, executor exec.Executor) *statementru.Statement {
	weights := statementru.Weights{statementru.CPUWork: 1}
	sc := ctx.GetSessionVars().StmtCtx
	require.True(t, sc.ConfigureStatementRU(statementru.Selection{
		Mode:          statementru.ModeCalibration,
		Applicable:    true,
		RequiredUnits: statementru.CPUWork.Mask(),
		Weights:       &weights,
	}))
	require.True(t, exec.ConfigureStatementRUExecutor(executor, sc, exec.StatementRUExecutorConfig{NeedsUnitRecorder: true}))
	return sc.TakeStatementRUForExecution()
}

func finishStatementRUNonlinearForTest(t testing.TB, statement *statementru.Statement, terminal statementru.TerminalStatus) float64 {
	require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := statement.Finish(terminal)
	require.True(t, first)
	units, ok := finish.Result.Units()
	require.True(t, ok)
	return units[statementru.CPUWork]
}

func finishStatementRUNonlinearInvalidForTest(t testing.TB, statement *statementru.Statement) {
	require.True(t, statement.EvidenceRecorder().MarkPresent(statementru.CPUWork.Mask()))
	finish, first := statement.Finish(statementru.TerminalSuccess)
	require.True(t, first)
	require.False(t, finish.Result.HasTotal())
	require.Equal(t, statementru.Outcome{
		State:  statementru.StateInvalid,
		Reason: statementru.ReasonInvalidObservation,
	}, finish.Result.Outcome())
	coverage, ok := finish.Result.Coverage()
	require.True(t, ok)
	require.Equal(t, statementru.CPUWork.Mask(), coverage.InvalidUnits)
}

func drainStatementRUExecutor(executor exec.Executor) error {
	ctx := context.Background()
	if err := exec.Open(ctx, executor); err != nil {
		return err
	}
	defer exec.Close(executor)

	req := exec.NewFirstChunk(executor)
	for {
		if err := exec.Next(ctx, executor, req); err != nil {
			return err
		}
		if req.NumRows() == 0 {
			return nil
		}
	}
}

func closeStatementRUExecutorEarly(t testing.TB, executor exec.Executor) {
	ctx := context.Background()
	require.NoError(t, exec.Open(ctx, executor))
	req := exec.NewFirstChunk(executor)
	require.NoError(t, exec.Next(ctx, executor, req))
	require.NotZero(t, req.NumRows())
	require.NoError(t, exec.Close(executor))
}

func TestStatementRUNonlinearSortAndTopN(t *testing.T) {
	t.Run("Sort", func(t *testing.T) {
		for _, rows := range []int{0, 1, 5} {
			t.Run(fmt.Sprintf("%d rows", rows), func(t *testing.T) {
				ctx, dataSource := newStatementRUDataSource(rows)
				executor := newStatementRUSortExec(ctx, dataSource)
				statement := configureStatementRUNonlinearForTest(t, ctx, executor)

				require.NoError(t, drainStatementRUExecutor(executor))
				expected := float64(rows) * math.Log2(float64(max(rows, 2)))
				require.InDelta(t, expected, finishStatementRUNonlinearForTest(t, statement, statementru.TerminalSuccess), 1e-9)
			})
		}
	})

	t.Run("TopN", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			rows   int
			offset uint64
			count  uint64
			want   float64
		}{
			{name: "zero count", rows: 5, offset: 3, count: 0, want: 0},
			{name: "one row", rows: 1, count: 2, want: 1},
			{name: "bounded heap", rows: 5, offset: 1, count: 2, want: 5 * math.Log2(3)},
		} {
			t.Run(test.name, func(t *testing.T) {
				ctx, dataSource := newStatementRUDataSource(test.rows)
				executor := newStatementRUTopNExec(ctx, dataSource, test.offset, test.count)
				statement := configureStatementRUNonlinearForTest(t, ctx, executor)

				require.NoError(t, drainStatementRUExecutor(executor))
				require.InDelta(t, test.want, finishStatementRUNonlinearForTest(t, statement, statementru.TerminalSuccess), 1e-9)
				if test.count == 0 {
					require.Zero(t, dataSource.ChunkPtr)
				}
			})
		}
	})

	t.Run("checked TopN limit", func(t *testing.T) {
		totalLimit, ok := checkedStatementRUTopNTotalLimit(^uint64(0)-1, 1)
		require.True(t, ok)
		require.Equal(t, ^uint64(0), totalLimit)

		_, ok = checkedStatementRUTopNTotalLimit(^uint64(0)-1, 2)
		require.False(t, ok)

		ctx, dataSource := newStatementRUDataSource(5)
		executor := newStatementRUTopNExec(ctx, dataSource, ^uint64(0)-1, 2)
		statement := configureStatementRUNonlinearForTest(t, ctx, executor)
		require.True(t, statement.UnitRecorder().Add(statementru.CPUWork, 7))

		require.NoError(t, exec.Open(context.Background(), executor))
		require.False(t, executor.statementRU.inputValid)
		require.True(t, executor.statementRU.inputInvalid)
		executor.statementRU.inputRows = 5
		executor.statementRU.inputComplete = true
		executor.recordStatementRUTopNCPUWork(executor.statementRU)
		require.NoError(t, exec.Close(executor))
		finishStatementRUNonlinearInvalidForTest(t, statement)
	})

	t.Run("Sort row count overflow invalidates at clean terminal", func(t *testing.T) {
		ctx, dataSource := newStatementRUDataSource(0)
		executor := newStatementRUSortExec(ctx, dataSource)
		statement := configureStatementRUNonlinearForTest(t, ctx, executor)
		require.NoError(t, exec.Open(context.Background(), executor))
		executor.statementRU.inputRows = ^uint64(0)
		executor.statementRU.addInputRows(1)
		require.False(t, executor.statementRU.inputValid)
		require.True(t, executor.statementRU.inputInvalid)
		executor.statementRU.inputComplete = true
		executor.recordStatementRUSortCPUWork(executor.statementRU)
		require.NoError(t, exec.Close(executor))
		finishStatementRUNonlinearInvalidForTest(t, statement)
	})

	t.Run("concurrent close suppresses channel-close terminal", func(t *testing.T) {
		t.Run("Sort", func(t *testing.T) {
			ctx, dataSource := newStatementRUDataSource(0)
			executor := newStatementRUSortExec(ctx, dataSource)
			statement := configureStatementRUNonlinearForTest(t, ctx, executor)
			require.NoError(t, exec.Open(context.Background(), executor))

			executor.statementRU.inputValid = false
			executor.statementRU.inputInvalid = true
			executor.statementRU.inputComplete = true
			executor.fetched.Store(true)
			go func() {
				<-executor.finishCh
				close(executor.Parallel.chunkChannel)
				close(executor.Parallel.resultChannel)
			}()

			nextDone := make(chan error, 1)
			go func() {
				nextDone <- exec.Next(context.Background(), executor, exec.NewFirstChunk(executor))
			}()
			select {
			case err := <-nextDone:
				require.Failf(t, "Next returned before Close", "error: %v", err)
			default:
			}

			require.NoError(t, exec.Close(executor))
			require.NoError(t, <-nextDone)
			require.Zero(t, finishStatementRUNonlinearForTest(t, statement, statementru.TerminalSuccess))
		})

		t.Run("TopN", func(t *testing.T) {
			ctx, dataSource := newStatementRUDataSource(0)
			executor := newStatementRUTopNExec(ctx, dataSource, 0, 5)
			statement := configureStatementRUNonlinearForTest(t, ctx, executor)
			require.NoError(t, exec.Open(context.Background(), executor))

			executor.statementRU.inputValid = false
			executor.statementRU.inputInvalid = true
			executor.statementRU.inputComplete = true
			executor.fetched.Store(true)
			go func() {
				<-executor.finishCh
				close(executor.resultChannel)
			}()

			nextDone := make(chan error, 1)
			go func() {
				nextDone <- exec.Next(context.Background(), executor, exec.NewFirstChunk(executor))
			}()
			select {
			case err := <-nextDone:
				require.Failf(t, "Next returned before Close", "error: %v", err)
			default:
			}

			require.NoError(t, exec.Close(executor))
			require.NoError(t, <-nextDone)
			require.Zero(t, finishStatementRUNonlinearForTest(t, statement, statementru.TerminalSuccess))
		})
	})

	t.Run("reopen isolates statement RU generation", func(t *testing.T) {
		t.Run("Sort", func(t *testing.T) {
			ctx, dataSource := newStatementRUDataSource(0)
			executor := newStatementRUSortExec(ctx, dataSource)
			statement := configureStatementRUNonlinearForTest(t, ctx, executor)

			require.NoError(t, exec.Open(context.Background(), executor))
			oldRuntime := executor.statementRU
			require.NoError(t, exec.Close(executor))
			oldRuntime.inputRows = 99
			oldRuntime.inputComplete = true

			require.NoError(t, exec.Open(context.Background(), executor))
			currentRuntime := executor.statementRU
			require.NotSame(t, oldRuntime, currentRuntime)
			executor.recordStatementRUSortCPUWork(oldRuntime)
			currentRuntime.inputRows = 5
			currentRuntime.inputComplete = true
			executor.recordStatementRUSortCPUWork(currentRuntime)
			require.NoError(t, exec.Close(executor))

			require.InDelta(t, 5*math.Log2(5), finishStatementRUNonlinearForTest(t, statement, statementru.TerminalSuccess), 1e-9)
		})

		t.Run("TopN", func(t *testing.T) {
			ctx, dataSource := newStatementRUDataSource(0)
			executor := newStatementRUTopNExec(ctx, dataSource, 0, 3)
			statement := configureStatementRUNonlinearForTest(t, ctx, executor)

			require.NoError(t, exec.Open(context.Background(), executor))
			oldRuntime := executor.statementRU
			require.NoError(t, exec.Close(executor))
			oldRuntime.inputRows = 99
			oldRuntime.inputComplete = true

			require.NoError(t, exec.Open(context.Background(), executor))
			currentRuntime := executor.statementRU
			require.NotSame(t, oldRuntime, currentRuntime)
			executor.recordStatementRUTopNCPUWork(oldRuntime)
			currentRuntime.inputRows = 5
			currentRuntime.inputComplete = true
			executor.recordStatementRUTopNCPUWork(currentRuntime)
			require.NoError(t, exec.Close(executor))

			require.InDelta(t, 5*math.Log2(3), finishStatementRUNonlinearForTest(t, statement, statementru.TerminalSuccess), 1e-9)
		})
	})

	t.Run("early close does not publish", func(t *testing.T) {
		for _, build := range []struct {
			name string
			new  func(sessionctx.Context, exec.Executor) exec.Executor
		}{
			{name: "Sort", new: func(ctx sessionctx.Context, child exec.Executor) exec.Executor {
				return newStatementRUSortExec(ctx, child)
			}},
			{name: "TopN", new: func(ctx sessionctx.Context, child exec.Executor) exec.Executor {
				return newStatementRUTopNExec(ctx, child, 0, 5)
			}},
		} {
			t.Run(build.name, func(t *testing.T) {
				ctx, dataSource := newStatementRUDataSource(5)
				executor := build.new(ctx, dataSource)
				statement := configureStatementRUNonlinearForTest(t, ctx, executor)

				closeStatementRUExecutorEarly(t, executor)
				require.Zero(t, finishStatementRUNonlinearForTest(t, statement, statementru.TerminalSuccess))
			})
		}
	})

	t.Run("child error does not publish", func(t *testing.T) {
		for _, build := range []struct {
			name string
			new  func(sessionctx.Context, exec.Executor) exec.Executor
		}{
			{name: "Sort", new: func(ctx sessionctx.Context, child exec.Executor) exec.Executor {
				return newStatementRUSortExec(ctx, child)
			}},
			{name: "TopN", new: func(ctx sessionctx.Context, child exec.Executor) exec.Executor {
				return newStatementRUTopNExec(ctx, child, 0, 5)
			}},
		} {
			t.Run(build.name, func(t *testing.T) {
				ctx, dataSource := newStatementRUDataSource(5)
				child := &statementRUErrorDataSource{MockDataSource: dataSource}
				executor := build.new(ctx, child)
				statement := configureStatementRUNonlinearForTest(t, ctx, executor)

				require.ErrorIs(t, drainStatementRUExecutor(executor), errStatementRUChild)
				require.Zero(t, finishStatementRUNonlinearForTest(t, statement, statementru.TerminalError))
			})
		}
	})
}

func BenchmarkStatementRUNonlinearAccounting(b *testing.B) {
	const rows = 8192
	for _, test := range []struct {
		name string
		new  func(sessionctx.Context, exec.Executor) exec.Executor
	}{
		{name: "Sort", new: func(ctx sessionctx.Context, child exec.Executor) exec.Executor {
			return newStatementRUSortExec(ctx, child)
		}},
		{name: "TopN", new: func(ctx sessionctx.Context, child exec.Executor) exec.Executor {
			return newStatementRUTopNExec(ctx, child, 0, 128)
		}},
	} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/enabled=%t", test.name, enabled), func(b *testing.B) {
				ctx, dataSource := newStatementRUDataSourceWithChunkSize(rows, 1024)
				executor := test.new(ctx, dataSource)
				var statement *statementru.Statement
				if enabled {
					statement = configureStatementRUNonlinearForTest(b, ctx, executor)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					dataSource.PrepareChunks()
					b.StartTimer()
					if err := drainStatementRUExecutor(executor); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()

				if statement != nil {
					require.Positive(b, finishStatementRUNonlinearForTest(b, statement, statementru.TerminalSuccess))
				}
			})
		}
	}
}
