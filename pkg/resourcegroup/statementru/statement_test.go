// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statementru

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordingStatementRUReporter struct {
	mu      sync.Mutex
	reports []Report
}

type reentrantStatementRUReporter struct {
	statement      *Statement
	reentrant      FinishResult
	reentrantFirst bool
}

type panickingStatementRUReporter struct {
	calls int
}

func (r *reentrantStatementRUReporter) ReportStatementRU(Report) {
	r.reentrant, r.reentrantFirst = r.statement.Finish(TerminalSuccess)
}

func (r *panickingStatementRUReporter) ReportStatementRU(Report) {
	r.calls++
	panic("reporter panic")
}

func (r *recordingStatementRUReporter) ReportStatementRU(report Report) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, report)
}

func (r *recordingStatementRUReporter) all() []Report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Report(nil), r.reports...)
}

func TestStatementSelectionAndFinish(t *testing.T) {
	weights := Weights{CPUWork: 2}
	require.Nil(t, NewStatement(Selection{}))
	require.Nil(t, NewStatement(Selection{Mode: ModeResultOnly}))
	require.Nil(t, NewStatement(Selection{Mode: Mode(255), Applicable: true}))
	t.Run("inapplicable selection is off and silent", func(t *testing.T) {
		reporter := &recordingStatementRUReporter{}
		require.Nil(t, NewStatement(Selection{
			Mode:       ModeResultOnly,
			Applicable: false,
			Reporter:   reporter,
		}))
		require.Empty(t, reporter.all())
	})

	t.Run("result only reports complete total including zero", func(t *testing.T) {
		for _, delta := range []float64{3, 0} {
			reporter := &recordingStatementRUReporter{}
			statement := NewStatement(Selection{
				Mode:          ModeResultOnly,
				Applicable:    true,
				RequiredUnits: CPUWork.Mask(),
				Weights:       &weights,
				Reporter:      reporter,
			})
			require.NotNil(t, statement)
			require.Equal(t, ModeResultOnly, statement.Mode())
			require.True(t, statement.UnitRecorder().Add(CPUWork, delta))
			require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, TerminalSuccess, finish.Terminal)
			require.True(t, finish.ReportSelected)
			total, ok := finish.Result.TotalRU()
			require.True(t, ok)
			require.Equal(t, delta*2, total)
			require.Equal(t, []Report{{TotalRU: delta * 2}}, reporter.all())
			_, retained := finish.Result.Units()
			require.False(t, retained)

			again, first := statement.Finish(TerminalError)
			require.False(t, first)
			require.Equal(t, finish, again)
			require.Len(t, reporter.all(), 1)
		}
	})

	t.Run("calibration retains details without production reporting", func(t *testing.T) {
		reporter := &recordingStatementRUReporter{}
		statement := NewStatement(Selection{
			Mode:          ModeCalibration,
			Applicable:    true,
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
			Reporter:      reporter,
		})
		require.True(t, statement.UnitRecorder().Add(CPUWork, 4))
		require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
		finish, first := statement.Finish(TerminalSuccess)
		require.True(t, first)
		require.False(t, finish.ReportSelected)
		units, retained := finish.Result.Units()
		require.True(t, retained)
		require.Equal(t, float64(4), units[CPUWork])
		require.Empty(t, reporter.all())
	})

	t.Run("producer capabilities cannot finalize or cross-cast", func(t *testing.T) {
		statement := NewStatement(Selection{
			Mode:          ModeResultOnly,
			Applicable:    true,
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
		})
		_, unitCanCoordinateEvidence := statement.UnitRecorder().(EvidenceRecorder)
		require.False(t, unitCanCoordinateEvidence)
		_, unitCanFinalize := statement.UnitRecorder().(interface{ Finalize() Result })
		require.False(t, unitCanFinalize)
		_, evidenceCanAdd := statement.EvidenceRecorder().(UnitRecorder)
		require.False(t, evidenceCanAdd)
		_, evidenceCanFinalize := statement.EvidenceRecorder().(interface{ Finalize() Result })
		require.False(t, evidenceCanFinalize)
	})

	t.Run("reporter may inspect frozen finish without reentrant deadlock", func(t *testing.T) {
		reporter := &reentrantStatementRUReporter{}
		statement := NewStatement(Selection{
			Mode:          ModeResultOnly,
			Applicable:    true,
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
			Reporter:      reporter,
		})
		reporter.statement = statement
		require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
		done := make(chan FinishResult, 1)
		go func() {
			finish, _ := statement.Finish(TerminalSuccess)
			done <- finish
		}()
		select {
		case finish := <-done:
			require.True(t, finish.ReportSelected)
			require.Equal(t, finish, reporter.reentrant)
			require.False(t, reporter.reentrantFirst)
		case <-time.After(time.Second):
			t.Fatal("reentrant reporter deadlocked")
		}
	})

	t.Run("reporter panic does not escape statement finish", func(t *testing.T) {
		reporter := &panickingStatementRUReporter{}
		statement := NewStatement(Selection{
			Mode:          ModeResultOnly,
			Applicable:    true,
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
			Reporter:      reporter,
		})
		require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
		finish, first := statement.Finish(TerminalSuccess)
		require.True(t, first)
		require.True(t, finish.ReportSelected)
		require.Equal(t, 1, reporter.calls)
		again, first := statement.Finish(TerminalSuccess)
		require.False(t, first)
		require.Equal(t, finish, again)
		require.Equal(t, 1, reporter.calls)
	})
}

func TestStatementFinishReportingGate(t *testing.T) {
	weights := Weights{CPUWork: 1}
	tests := []struct {
		name     string
		prepare  func(*Statement)
		terminal TerminalStatus
		state    CollectionState
		reason   Reason
	}{
		{
			name: "partial",
			prepare: func(s *Statement) {
				require.True(t, s.EvidenceRecorder().MarkPartial(CPUWork.Mask()))
			},
			terminal: TerminalSuccess,
			state:    StatePartial,
			reason:   ReasonIncompleteEvidence,
		},
		{
			name:     "unavailable",
			prepare:  func(*Statement) {},
			terminal: TerminalSuccess,
			state:    StateUnavailable,
			reason:   ReasonMissingEvidence,
		},
		{
			name: "invalid",
			prepare: func(s *Statement) {
				require.False(t, s.UnitRecorder().Add(CPUWork, -1))
			},
			terminal: TerminalSuccess,
			state:    StateInvalid,
			reason:   ReasonInvalidObservation,
		},
		{
			name: "unsupported evidence",
			prepare: func(s *Statement) {
				require.True(t, s.EvidenceRecorder().MarkUnsupported(CPUWork.Mask()))
			},
			terminal: TerminalSuccess,
			state:    StateUnavailable,
			reason:   ReasonUnsupported,
		},
		{
			name: "statement error",
			prepare: func(s *Statement) {
				require.True(t, s.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
			},
			terminal: TerminalError,
			state:    StateComplete,
			reason:   ReasonNone,
		},
		{
			name: "canceled",
			prepare: func(s *Statement) {
				require.True(t, s.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
			},
			terminal: TerminalCanceled,
			state:    StateComplete,
			reason:   ReasonNone,
		},
		{
			name: "deadline exceeded",
			prepare: func(s *Statement) {
				require.True(t, s.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
			},
			terminal: TerminalCanceled,
			state:    StateComplete,
			reason:   ReasonNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reporter := &recordingStatementRUReporter{}
			statement := NewStatement(Selection{
				Mode:          ModeResultOnly,
				Applicable:    true,
				RequiredUnits: CPUWork.Mask(),
				Weights:       &weights,
				Reporter:      reporter,
			})
			tt.prepare(statement)
			finish, first := statement.Finish(tt.terminal)
			require.True(t, first)
			require.Equal(t, tt.terminal, finish.Terminal)
			require.Equal(t, tt.state, finish.Result.Outcome().State)
			require.Equal(t, tt.reason, finish.Result.Outcome().Reason)
			require.False(t, finish.ReportSelected)
			require.Empty(t, reporter.all())
		})
	}
}

func TestStatementConcurrentFinishReportsOnce(t *testing.T) {
	weights := Weights{CPUWork: 1}
	reporter := &recordingStatementRUReporter{}
	statement := NewStatement(Selection{
		Mode:          ModeResultOnly,
		Applicable:    true,
		RequiredUnits: CPUWork.Mask(),
		Weights:       &weights,
		Reporter:      reporter,
	})
	require.True(t, statement.UnitRecorder().Add(CPUWork, 1))
	require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))

	const goroutines = 32
	type finishCall struct {
		finish FinishResult
		first  bool
	}
	calls := make(chan finishCall, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			finish, first := statement.Finish(TerminalSuccess)
			calls <- finishCall{finish: finish, first: first}
		}()
	}
	wg.Wait()
	close(calls)
	firstCount := 0
	for call := range calls {
		require.Equal(t, TerminalSuccess, call.finish.Terminal)
		if call.first {
			firstCount++
		}
	}
	require.Equal(t, 1, firstCount)
	require.Equal(t, []Report{{TotalRU: 1}}, reporter.all())
}
