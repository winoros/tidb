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

	t.Run("invalid enabled selection retains only its terminal result", func(t *testing.T) {
		invalidMask := UnitKind(UnitCount).Mask()
		invalidWeights := Weights{CPUWork: -1}
		for _, selection := range []Selection{
			{Mode: ModeResultOnly, Applicable: true},
			{
				Mode:           ModeResultOnly,
				Applicable:     true,
				RequiredUnits:  CPUWork.Mask(),
				CollectedUnits: ScanBytes.Mask(),
			},
			{
				Mode:           ModeCalibration,
				Applicable:     true,
				RequiredUnits:  CPUWork.Mask(),
				CollectedUnits: CPUWork.Mask() | invalidMask,
			},
			{
				Mode:          ModeResultOnly,
				Applicable:    true,
				RequiredUnits: CPUWork.Mask(),
				Weights:       &invalidWeights,
			},
		} {
			statement := NewStatement(selection)
			require.NotNil(t, statement)
			require.Nil(t, statement.UnitRecorder())
			require.Nil(t, statement.EvidenceRecorder())
			require.Nil(t, statement.UnitContributorRegistrar())
			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StateInvalid, Reason: ReasonInvalidConfiguration}, finish.Result.Outcome())
			require.False(t, finish.ReportSelected)
		}
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
			require.Equal(t, CPUWork.Mask(), statement.UnitRecorder().CollectedUnits())
			require.Equal(t, CPUWork.Mask(), statement.UnitContributorRegistrar().RequiredUnits())
			require.Equal(t, CPUWork.Mask(), statement.UnitContributorRegistrar().CollectedUnits())
			require.True(t, statement.UnitRecorder().Add(CPUWork, delta))
			require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, TerminalSuccess, finish.Terminal)
			require.True(t, finish.ReportSelected)
			_, hasDiagnostic := finish.Diagnostic()
			require.False(t, hasDiagnostic)
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

	t.Run("calibration exposes candidate details without production reporting", func(t *testing.T) {
		for _, delta := range []float64{4, 0} {
			reporter := &recordingStatementRUReporter{}
			statement := NewStatement(Selection{
				Mode:           ModeCalibration,
				Applicable:     true,
				RequiredUnits:  CPUWork.Mask(),
				CollectedUnits: CPUWork.Mask() | NetworkBytes.Mask(),
				Weights:        &weights,
				Reporter:       reporter,
			})
			require.True(t, statement.UnitRecorder().Add(CPUWork, delta))
			require.True(t, statement.UnitRecorder().Add(NetworkBytes, 5))
			require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
			require.True(t, statement.EvidenceRecorder().MarkUnavailable(NetworkBytes.Mask()))
			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.False(t, finish.ReportSelected)

			diagnostic, ok := finish.Diagnostic()
			require.True(t, ok)
			require.Equal(t, TerminalSuccess, diagnostic.Terminal())
			require.Equal(t, Outcome{State: StateComplete, Reason: ReasonNone}, diagnostic.Outcome())
			require.Equal(t, delta, diagnostic.Units()[CPUWork])
			require.Equal(t, float64(5), diagnostic.Units()[NetworkBytes])
			require.Equal(t, CPUWork.Mask(), diagnostic.Coverage().RequiredUnits)
			require.Equal(t, CPUWork.Mask()|NetworkBytes.Mask(), diagnostic.Coverage().CollectedUnits)
			candidate, present := diagnostic.CandidateTotalRU()
			require.True(t, present)
			require.Equal(t, delta*2, candidate)

			units := diagnostic.Units()
			units[CPUWork] = 99
			coverage := diagnostic.Coverage()
			coverage.RequiredUnits = AllUnits
			coverage.CollectedUnits = 0
			require.Equal(t, float64(99), units[CPUWork])
			require.Equal(t, AllUnits, coverage.RequiredUnits)
			require.Zero(t, coverage.CollectedUnits)
			require.Equal(t, delta, diagnostic.Units()[CPUWork])
			require.Equal(t, CPUWork.Mask(), diagnostic.Coverage().RequiredUnits)
			require.Equal(t, CPUWork.Mask()|NetworkBytes.Mask(), diagnostic.Coverage().CollectedUnits)
			require.False(t, statement.UnitRecorder().Add(CPUWork, 1))
			require.Empty(t, reporter.all())
		}
	})

	t.Run("calibration diagnostic preserves unavailable and incomplete outcomes", func(t *testing.T) {
		tests := []struct {
			name    string
			weights *Weights
			prepare func(*testing.T, *Statement)
			outcome Outcome
		}{
			{
				name:    "weights unavailable",
				prepare: func(t *testing.T, s *Statement) { require.True(t, s.EvidenceRecorder().MarkPresent(CPUWork.Mask())) },
				outcome: Outcome{State: StateUnavailable, Reason: ReasonWeightsUnavailable},
			},
			{
				name:    "partial",
				weights: &weights,
				prepare: func(t *testing.T, s *Statement) { require.True(t, s.EvidenceRecorder().MarkPartial(CPUWork.Mask())) },
				outcome: Outcome{State: StatePartial, Reason: ReasonIncompleteEvidence},
			},
			{
				name:    "unavailable",
				weights: &weights,
				prepare: func(*testing.T, *Statement) {},
				outcome: Outcome{State: StateUnavailable, Reason: ReasonMissingEvidence},
			},
			{
				name:    "unsupported",
				weights: &weights,
				prepare: func(t *testing.T, s *Statement) {
					require.True(t, s.EvidenceRecorder().MarkUnsupported(CPUWork.Mask()))
				},
				outcome: Outcome{State: StateUnavailable, Reason: ReasonUnsupported},
			},
			{
				name:    "invalid",
				weights: &weights,
				prepare: func(t *testing.T, s *Statement) { require.False(t, s.UnitRecorder().Add(CPUWork, -1)) },
				outcome: Outcome{State: StateInvalid, Reason: ReasonInvalidObservation},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				statement := NewStatement(Selection{
					Mode:          ModeCalibration,
					Applicable:    true,
					RequiredUnits: CPUWork.Mask(),
					Weights:       tt.weights,
				})
				tt.prepare(t, statement)
				finish, first := statement.Finish(TerminalSuccess)
				require.True(t, first)
				diagnostic, ok := finish.Diagnostic()
				require.True(t, ok)
				require.Equal(t, tt.outcome, diagnostic.Outcome())
				_, present := diagnostic.CandidateTotalRU()
				require.False(t, present)
			})
		}
	})

	t.Run("diagnostic terminal is orthogonal to a complete candidate", func(t *testing.T) {
		for _, terminal := range []TerminalStatus{TerminalError, TerminalCanceled} {
			statement := NewStatement(Selection{
				Mode:          ModeCalibration,
				Applicable:    true,
				RequiredUnits: CPUWork.Mask(),
				Weights:       &weights,
			})
			require.True(t, statement.UnitRecorder().Add(CPUWork, 3))
			require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
			finish, first := statement.Finish(terminal)
			require.True(t, first)
			require.False(t, finish.ReportSelected)
			diagnostic, ok := finish.Diagnostic()
			require.True(t, ok)
			require.Equal(t, terminal, diagnostic.Terminal())
			candidate, present := diagnostic.CandidateTotalRU()
			require.True(t, present)
			require.Equal(t, float64(6), candidate)
		}
	})

	t.Run("explain uses the same frozen diagnostic view", func(t *testing.T) {
		reporter := &recordingStatementRUReporter{}
		statement := NewStatement(Selection{
			Mode:          ModeExplain,
			Applicable:    true,
			RequiredUnits: CPUWork.Mask(),
			Weights:       &weights,
			Reporter:      reporter,
		})
		require.True(t, statement.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
		finish, first := statement.Finish(TerminalSuccess)
		require.True(t, first)
		require.False(t, finish.ReportSelected)
		require.Empty(t, reporter.all())
		diagnostic, ok := finish.Diagnostic()
		require.True(t, ok)
		candidate, present := diagnostic.CandidateTotalRU()
		require.True(t, present)
		require.Zero(t, candidate)
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

	t.Run("local streaming CPU evidence combines with remote contributors", func(t *testing.T) {
		newStatement := func(t *testing.T) *Statement {
			statement := NewStatement(Selection{
				Mode:          ModeCalibration,
				Applicable:    true,
				RequiredUnits: CPUWork.Mask(),
				Weights:       &weights,
			})
			require.True(t, statement.LocalCPUWorkRegistrar().Activate())
			return statement
		}
		completeRemote := func(t *testing.T, statement *Statement, value float64) {
			remote := statement.UnitContributorRegistrar().RegisterUnitContributor(CPUWork.Mask())
			require.NotNil(t, remote)
			var values UnitValues
			values[CPUWork] = value
			require.True(t, remote.Complete(values))
		}
		completeInventory := func(t *testing.T, statement *Statement) {
			require.True(t, statement.LocalCPUWorkRegistrar().CompleteLocalCPUWorkInventory())
		}

		t.Run("complete local and remote domains are additive", func(t *testing.T) {
			statement := newStatement(t)
			var producer LocalCPUWorkProducer
			require.True(t, statement.LocalCPUWorkRegistrar().RegisterLocalCPUWorkProducer(&producer))
			require.True(t, producer.BeginGeneration())
			require.True(t, statement.UnitRecorder().Add(CPUWork, 2))
			require.True(t, producer.CompleteGeneration())
			completeInventory(t, statement)
			completeRemote(t, statement, 3)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StateComplete}, finish.Result.Outcome())
			total, ok := finish.Result.TotalRU()
			require.True(t, ok)
			require.Equal(t, float64(10), total)
		})

		t.Run("inventoried empty local domain is authoritative zero", func(t *testing.T) {
			statement := newStatement(t)
			completeInventory(t, statement)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StateComplete}, finish.Result.Outcome())
			total, ok := finish.Result.TotalRU()
			require.True(t, ok)
			require.Zero(t, total)
		})

		t.Run("retry inventory pass may extend after completion", func(t *testing.T) {
			statement := newStatement(t)
			completeInventory(t, statement)
			var producer LocalCPUWorkProducer
			require.True(t, statement.LocalCPUWorkRegistrar().RegisterLocalCPUWorkProducer(&producer))
			require.True(t, producer.BeginGeneration())
			require.True(t, statement.UnitRecorder().Add(CPUWork, 2))
			require.True(t, producer.RecordObservation())
			require.True(t, producer.CompleteGeneration())
			completeInventory(t, statement)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StateComplete}, finish.Result.Outcome())
			units, ok := finish.Result.Units()
			require.True(t, ok)
			require.Equal(t, float64(2), units[CPUWork])
		})

		t.Run("producer added after inventory checkpoint remains fail closed", func(t *testing.T) {
			statement := newStatement(t)
			completeInventory(t, statement)
			var producer LocalCPUWorkProducer
			require.True(t, statement.LocalCPUWorkRegistrar().RegisterLocalCPUWorkProducer(&producer))

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.False(t, finish.Result.HasTotal())
			require.Equal(t, Outcome{State: StateUnavailable, Reason: ReasonMissingEvidence}, finish.Result.Outcome())
		})

		t.Run("late observation is rejected after finish", func(t *testing.T) {
			statement := newStatement(t)
			registrar := statement.LocalCPUWorkRegistrar()
			require.True(t, registrar.RecordLocalCPUWorkObservation())
			completeInventory(t, statement)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StateComplete}, finish.Result.Outcome())
			require.False(t, registrar.RecordLocalCPUWorkObservation())
		})

		t.Run("activation alone does not authorize empty local zero", func(t *testing.T) {
			statement := newStatement(t)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StateUnavailable, Reason: ReasonMissingEvidence}, finish.Result.Outcome())
		})

		t.Run("remote present cannot hide uncompleted empty inventory", func(t *testing.T) {
			reporter := &recordingStatementRUReporter{}
			statement := NewStatement(Selection{
				Mode:          ModeResultOnly,
				Applicable:    true,
				RequiredUnits: CPUWork.Mask(),
				Weights:       &weights,
				Reporter:      reporter,
			})
			require.True(t, statement.LocalCPUWorkRegistrar().Activate())
			completeRemote(t, statement, 3)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.False(t, finish.Result.HasTotal())
			require.False(t, finish.ReportSelected)
			require.Empty(t, reporter.all())
			require.Equal(t, Outcome{State: StatePartial, Reason: ReasonIncompleteEvidence}, finish.Result.Outcome())
		})

		t.Run("remote present cannot hide local unsupported", func(t *testing.T) {
			statement := newStatement(t)
			require.True(t, statement.LocalCPUWorkRegistrar().MarkLocalCPUWorkUnsupported())
			completeRemote(t, statement, 3)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StatePartial, Reason: ReasonUnsupported}, finish.Result.Outcome())
			coverage, ok := finish.Result.Coverage()
			require.True(t, ok)
			require.Equal(t, CPUWork.Mask(), coverage.PresentUnits)
			require.Equal(t, CPUWork.Mask(), coverage.UnavailableUnits)
			require.Equal(t, CPUWork.Mask(), coverage.UnsupportedUnits)
		})

		t.Run("remote present cannot hide active local generation", func(t *testing.T) {
			statement := newStatement(t)
			var producer LocalCPUWorkProducer
			require.True(t, statement.LocalCPUWorkRegistrar().RegisterLocalCPUWorkProducer(&producer))
			require.True(t, producer.BeginGeneration())
			completeInventory(t, statement)
			completeRemote(t, statement, 3)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StatePartial, Reason: ReasonIncompleteEvidence}, finish.Result.Outcome())
			coverage, ok := finish.Result.Coverage()
			require.True(t, ok)
			require.Equal(t, CPUWork.Mask(), coverage.PresentUnits)
			require.Equal(t, CPUWork.Mask(), coverage.UnavailableUnits)
		})

		t.Run("late next error downgrades completed close", func(t *testing.T) {
			statement := newStatement(t)
			var producer LocalCPUWorkProducer
			require.True(t, statement.LocalCPUWorkRegistrar().RegisterLocalCPUWorkProducer(&producer))
			require.True(t, producer.BeginGeneration())
			require.True(t, statement.UnitRecorder().Add(CPUWork, 2))
			require.True(t, producer.CompleteGeneration())
			require.False(t, producer.AbortGeneration(true))
			completeInventory(t, statement)

			finish, first := statement.Finish(TerminalSuccess)
			require.True(t, first)
			require.Equal(t, Outcome{State: StatePartial, Reason: ReasonIncompleteEvidence}, finish.Result.Outcome())
		})

		t.Run("begin and seal race preserves authority ordering", func(t *testing.T) {
			for range 100 {
				statement := newStatement(t)
				var producer LocalCPUWorkProducer
				require.True(t, statement.LocalCPUWorkRegistrar().RegisterLocalCPUWorkProducer(&producer))
				completeInventory(t, statement)

				start := make(chan struct{})
				begun := make(chan bool, 1)
				finished := make(chan FinishResult, 1)
				go func() {
					<-start
					begun <- producer.BeginGeneration()
				}()
				go func() {
					<-start
					finish, _ := statement.Finish(TerminalSuccess)
					finished <- finish
				}()
				close(start)

				<-begun
				finish := <-finished
				require.False(t, finish.Result.HasTotal())
				require.Equal(t, StateUnavailable, finish.Result.Outcome().State)
			}
		})
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

	calibration := NewStatement(Selection{
		Mode:          ModeCalibration,
		Applicable:    true,
		RequiredUnits: CPUWork.Mask(),
		Weights:       &weights,
	})
	require.True(t, calibration.UnitRecorder().Add(CPUWork, 2))
	require.True(t, calibration.EvidenceRecorder().MarkPresent(CPUWork.Mask()))
	type diagnosticCall struct {
		diagnostic Diagnostic
		ok         bool
		first      bool
	}
	diagnostics := make(chan diagnosticCall, goroutines)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			finish, first := calibration.Finish(TerminalSuccess)
			diagnostic, ok := finish.Diagnostic()
			diagnostics <- diagnosticCall{diagnostic: diagnostic, ok: ok, first: first}
		}()
	}
	wg.Wait()
	close(diagnostics)
	firstCount = 0
	var expected Diagnostic
	haveExpected := false
	for call := range diagnostics {
		require.True(t, call.ok)
		if !haveExpected {
			expected = call.diagnostic
			haveExpected = true
		}
		require.Equal(t, expected, call.diagnostic)
		if call.first {
			firstCount++
		}
	}
	require.True(t, haveExpected)
	require.Equal(t, 1, firstCount)
}
