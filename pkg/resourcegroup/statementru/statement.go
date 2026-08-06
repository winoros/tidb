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
	"sync/atomic"
)

// Mode controls how much statement RU state is collected and retained.
type Mode uint8

const (
	// ModeOff performs no collection and allocates no statement owner.
	ModeOff Mode = iota
	// ModeResultOnly calculates an authoritative total without retaining units.
	ModeResultOnly
	// ModeCalibration retains bounded unit and coverage details for calibration.
	ModeCalibration
	// ModeExplain retains bounded details for a later operator-attribution layer.
	ModeExplain
)

func (m Mode) valid() bool {
	return m <= ModeExplain
}

// Report is the complete production payload. A zero TotalRU is an
// authoritative zero, not missing evidence.
type Report struct {
	TotalRU float64
}

// Reporter receives one complete production result. Statement
// identity is supplied by the reporter's surrounding transport context. A
// reporter panic is contained because reporting must not interrupt statement
// completion bookkeeping.
type Reporter interface {
	ReportStatementRU(Report)
}

// Selection is the statement-local decision frozen before execution. Applicable
// distinguishes lifecycle eligibility from evidence support: an inapplicable
// statement selects true Off, while an applicable statement whose selected unit
// producer is unsupported uses EvidenceRecorder.MarkUnsupported. RequiredUnits
// is the nonempty candidate-total mask. CollectedUnits may be its superset for
// optional diagnostics; zero defaults to RequiredUnits.
type Selection struct {
	Mode           Mode
	Applicable     bool
	RequiredUnits  UnitMask
	CollectedUnits UnitMask
	Weights        *Weights
	Reporter       Reporter
}

// TerminalStatus is the bounded final logical-statement status.
type TerminalStatus uint8

const (
	// TerminalSuccess means the logical statement completed without an error.
	TerminalSuccess TerminalStatus = iota
	// TerminalError means the logical statement failed for a reason other than cancellation.
	TerminalError
	// TerminalCanceled means the statement was canceled or exceeded its deadline.
	TerminalCanceled
)

// FinishResult is the immutable collection result plus its terminal reporting
// decision. ReportSelected means this result passed the production reporting
// gate. It does not describe callback completion or downstream delivery.
type FinishResult struct {
	Result         Result
	Terminal       TerminalStatus
	ReportSelected bool
}

// Statement owns one logical statement's collector and exactly-once finish.
// Transparent retries keep using the same Statement.
type Statement struct {
	mode                 Mode
	requiredUnits        UnitMask
	collectedUnits       UnitMask
	producerCapabilities bool
	collector            *Collector
	reporter             Reporter
	unit                 statementUnitRecorder
	evidence             statementEvidenceRecorder
	contributors         atomic.Pointer[contributorCoordinator]
	registrar            statementContributorRegistrar
	localCPUWork         atomic.Pointer[statementLocalCPUWorkOwner]

	finishOnce sync.Once
	finish     FinishResult
}

// LocalCPUWorkProducer is an opaque lifecycle token embedded by one
// synchronous executor participating in TiDB's statement-local streaming
// CPUWork domain. Generations cover executor reuse: Open begins one generation
// and a successful Close completes it. Any incomplete generation keeps the
// whole local domain fail-closed at Statement.Finish. A registered token must
// not be copied.
type LocalCPUWorkProducer struct {
	noCopy localCPUWorkNoCopy
	owner  *statementLocalCPUWorkOwner
	begun  bool
	active bool
}

type localCPUWorkNoCopy struct{}

func (*localCPUWorkNoCopy) Lock()   {}
func (*localCPUWorkNoCopy) Unlock() {}

// LocalCPUWorkRegistrar exposes only the local streaming-domain lifecycle.
// Activate starts the inventory but never makes an empty inventory
// authoritative by itself. The top-level execution builder must explicitly
// complete one inventory pass after every local producer in that pass has
// either registered or marked the domain unsupported. A later retry build may
// extend the inventory with more registered producers; those producers are
// tracked immediately and keep the domain fail-closed until their generations
// complete. Fixed-vector producers, including remote CPU owners, use
// UnitContributorRegistrar instead.
type LocalCPUWorkRegistrar interface {
	Activate() bool
	CompleteLocalCPUWorkInventory() bool
	RegisterLocalCPUWorkProducer(*LocalCPUWorkProducer) bool
	RecordLocalCPUWorkObservation() bool
	MarkLocalCPUWorkUnsupported() bool
}

func (r *statementContributorRegistrar) Activate() bool {
	if r == nil || r.statement == nil {
		return false
	}
	return r.statement.localCPUWorkOwner() != nil
}

func (r *statementContributorRegistrar) RegisterLocalCPUWorkProducer(producer *LocalCPUWorkProducer) bool {
	if r == nil || r.statement == nil {
		return false
	}
	owner := r.statement.localCPUWorkOwner()
	if owner == nil {
		return false
	}
	return owner.register(producer)
}

func (r *statementContributorRegistrar) CompleteLocalCPUWorkInventory() bool {
	if r == nil || r.statement == nil {
		return false
	}
	owner := r.statement.localCPUWorkOwner()
	return owner != nil && owner.completeInventory()
}

func (r *statementContributorRegistrar) RecordLocalCPUWorkObservation() bool {
	if r == nil || r.statement == nil {
		return false
	}
	owner := r.statement.localCPUWorkOwner()
	return owner != nil && owner.recordObservation()
}

func (r *statementContributorRegistrar) MarkLocalCPUWorkUnsupported() bool {
	if r == nil || r.statement == nil {
		return false
	}
	owner := r.statement.localCPUWorkOwner()
	return owner != nil && owner.markUnsupported()
}

type statementLocalCPUWorkOwner struct {
	mu sync.Mutex

	collector             *Collector
	registeredProducers   uint64
	begunProducers        uint64
	activeProducers       uint64
	hasObservation        atomic.Bool
	sealedObservations    atomic.Bool
	hasCompleted          bool
	inventoryCheckpointed bool
	unsupported           bool
	incomplete            bool
	sealed                bool
}

var sealedLocalCPUWorkOwner = &statementLocalCPUWorkOwner{sealed: true}

func (s *Statement) localCPUWorkOwner() *statementLocalCPUWorkOwner {
	for {
		owner := s.localCPUWork.Load()
		if owner == sealedLocalCPUWorkOwner {
			return nil
		}
		if owner != nil {
			return owner
		}
		candidate := &statementLocalCPUWorkOwner{collector: s.collector}
		if s.localCPUWork.CompareAndSwap(nil, candidate) {
			return candidate
		}
	}
}

func (s *Statement) sealLocalCPUWork() {
	owner := s.localCPUWork.Swap(sealedLocalCPUWorkOwner)
	if owner != nil && owner != sealedLocalCPUWorkOwner {
		owner.seal()
	}
}

func (o *statementLocalCPUWorkOwner) register(producer *LocalCPUWorkProducer) bool {
	if o == nil || o.collector == nil || producer == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed || producer.owner != nil {
		return false
	}
	if o.registeredProducers == ^uint64(0) {
		o.incomplete = true
		return false
	}
	producer.owner = o
	o.registeredProducers++
	return true
}

func (o *statementLocalCPUWorkOwner) completeInventory() bool {
	if o == nil || o.collector == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed {
		return false
	}
	o.inventoryCheckpointed = true
	return true
}

func (o *statementLocalCPUWorkOwner) recordObservation() bool {
	if o == nil || o.collector == nil {
		return false
	}
	if o.sealedObservations.Load() {
		return false
	}
	if o.hasObservation.Load() {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed {
		return false
	}
	o.hasObservation.Store(true)
	return true
}

func (o *statementLocalCPUWorkOwner) markUnsupported() bool {
	if o == nil || o.collector == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sealed {
		return false
	}
	o.unsupported = true
	return true
}

// BeginGeneration starts one executor Open generation.
func (p *LocalCPUWorkProducer) BeginGeneration() bool {
	if p == nil || p.owner == nil {
		return false
	}
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	if p.owner.sealed {
		return false
	}
	if p.active {
		p.owner.incomplete = true
		return false
	}
	if !p.begun {
		p.begun = true
		p.owner.begunProducers++
	}
	p.active = true
	p.owner.activeProducers++
	return true
}

// RecordObservation marks one accepted CPUWork value for this producer. The
// sticky statement-level bit lets an incomplete generation retain partial
// diagnostic evidence. Only the first observation takes the owner lock.
func (p *LocalCPUWorkProducer) RecordObservation() bool {
	if p == nil || p.owner == nil {
		return false
	}
	return p.owner.recordObservation()
}

// CompleteGeneration completes the active generation after successful Close.
func (p *LocalCPUWorkProducer) CompleteGeneration() bool {
	return p.finishGeneration(false, false)
}

// AbortGeneration terminates an incomplete generation. hasUsableValue reports
// whether the generation already streamed a valid CPUWork observation.
func (p *LocalCPUWorkProducer) AbortGeneration(hasUsableValue bool) bool {
	return p.finishGeneration(true, hasUsableValue)
}

func (p *LocalCPUWorkProducer) finishGeneration(incomplete, hasUsableValue bool) bool {
	if p == nil || p.owner == nil {
		return false
	}
	p.owner.mu.Lock()
	defer p.owner.mu.Unlock()
	if p.owner.sealed {
		return false
	}
	if !p.active {
		if incomplete {
			// Close may complete before an in-flight Next publishes its error.
			// Statement finish waits for that outer execution boundary, so the
			// late abort must still downgrade the local domain.
			p.owner.incomplete = true
			if hasUsableValue {
				p.owner.hasObservation.Store(true)
			}
		}
		return false
	}
	if p.owner.activeProducers == 0 {
		p.owner.incomplete = true
		return false
	}
	p.active = false
	p.owner.activeProducers--
	if incomplete {
		p.owner.incomplete = true
		if hasUsableValue {
			p.owner.hasObservation.Store(true)
		}
		return true
	}
	p.owner.hasCompleted = true
	return true
}

func (o *statementLocalCPUWorkOwner) seal() {
	if o == nil || o.collector == nil {
		return
	}
	o.mu.Lock()
	if o.sealed {
		o.mu.Unlock()
		return
	}
	o.sealed = true
	o.sealedObservations.Store(true)
	unsupported := o.unsupported
	registered := o.registeredProducers
	inventoryCheckpointed := o.inventoryCheckpointed
	// Empty local CPUWork is authoritative only after the top-level execution
	// builder records a successful inventory checkpoint. This keeps selection
	// alone and interrupted construction fail-closed while allowing a genuinely
	// empty domain to contribute zero and later retry builds to extend it.
	allComplete := inventoryCheckpointed && registered == o.begunProducers &&
		!unsupported && !o.incomplete && o.activeProducers == 0
	hasUsable := o.hasObservation.Load() || o.hasCompleted
	o.mu.Unlock()

	unit := CPUWork.Mask()
	switch {
	case allComplete:
		o.collector.MarkPresent(unit)
	case hasUsable:
		o.collector.MarkPartial(unit)
	default:
		// This owner exists only after explicit activation. Missing inventory
		// completion is itself unavailable local evidence, including when a
		// remote contributor for the same unit is complete.
		o.collector.MarkUnavailable(unit)
	}
	if unsupported {
		o.collector.MarkUnsupported(unit)
	}
}

// The concrete capability adapters deliberately expose only their respective
// interfaces. Returning Collector directly would let producers type-assert to
// Finalize or to the other recorder interface.
type statementUnitRecorder struct {
	statement *Statement
}

func (r *statementUnitRecorder) CollectedUnits() UnitMask {
	if r == nil || r.statement == nil {
		return 0
	}
	return r.statement.collectedUnits
}

func (r *statementUnitRecorder) Add(kind UnitKind, delta float64) bool {
	if r == nil || r.statement == nil {
		return false
	}
	return r.statement.collector.Add(kind, delta)
}

type statementEvidenceRecorder struct {
	collector *Collector
}

func (r *statementEvidenceRecorder) MarkPresent(units UnitMask) bool {
	return r.collector.MarkPresent(units)
}

func (r *statementEvidenceRecorder) MarkPartial(units UnitMask) bool {
	return r.collector.MarkPartial(units)
}

func (r *statementEvidenceRecorder) MarkUnavailable(units UnitMask) bool {
	return r.collector.MarkUnavailable(units)
}

func (r *statementEvidenceRecorder) MarkUnsupported(units UnitMask) bool {
	return r.collector.MarkUnsupported(units)
}

// NewStatement translates a frozen selection into a generic statement owner.
// It does not activate a TiDB-local CPUWork domain merely because CPUWork is
// collected; the production StatementContext adapter activates that physical
// domain explicitly, while remote-only and package-local users may omit it.
// Off, inapplicable, and invalid modes return nil without allocating. A valid
// enabled mode with invalid unit configuration returns an owner whose Finish
// result is InvalidConfiguration, but exposes no producer capabilities.
func NewStatement(selection Selection) *Statement {
	if selection.Mode == ModeOff || !selection.Mode.valid() || !selection.Applicable {
		return nil
	}
	required, collected, _ := normalizeUnitSelection(selection.RequiredUnits, selection.CollectedUnits)
	retainDetails := selection.Mode == ModeCalibration || selection.Mode == ModeExplain
	collector := NewCollector(Config{
		RequiredUnits:  selection.RequiredUnits,
		CollectedUnits: selection.CollectedUnits,
		Weights:        selection.Weights,
		RetainDetails:  retainDetails,
	})
	statement := &Statement{
		mode:                 selection.Mode,
		requiredUnits:        required,
		collectedUnits:       collected,
		producerCapabilities: collector.configurationValid,
		collector:            collector,
		reporter:             selection.Reporter,
	}
	statement.unit.statement = statement
	statement.evidence.collector = collector
	statement.registrar.statement = statement
	return statement
}

// Mode returns the frozen collection mode. A nil Statement is Off.
func (s *Statement) Mode() Mode {
	if s == nil {
		return ModeOff
	}
	return s.mode
}

// UnitRecorder returns the narrow value-recording seam for unit producers.
func (s *Statement) UnitRecorder() UnitRecorder {
	if s == nil || !s.producerCapabilities {
		return nil
	}
	return &s.unit
}

// EvidenceRecorder returns the statement-level coverage-coordination seam.
func (s *Statement) EvidenceRecorder() EvidenceRecorder {
	if s == nil || !s.producerCapabilities {
		return nil
	}
	return &s.evidence
}

// UnitContributorRegistrar returns the statement-owned contributor lifecycle
// capability. It does not expose finalization or direct evidence mutation.
func (s *Statement) UnitContributorRegistrar() UnitContributorRegistrar {
	if s == nil || !s.producerCapabilities {
		return nil
	}
	return &s.registrar
}

// LocalCPUWorkRegistrar returns the statement-level owner for TiDB's local
// streaming CPUWork domain. It is absent when CPUWork is not collected.
func (s *Statement) LocalCPUWorkRegistrar() LocalCPUWorkRegistrar {
	if s == nil || !s.producerCapabilities || s.collectedUnits&CPUWork.Mask() == 0 {
		return nil
	}
	return &s.registrar
}

// Finish freezes the result and applies the production reporting gate exactly
// once. The caller translates its final error into a bounded terminal status.
// The bool is true only for the caller that performed finalization.
func (s *Statement) Finish(terminal TerminalStatus) (FinishResult, bool) {
	if s == nil {
		return FinishResult{}, false
	}
	finished := false
	s.finishOnce.Do(func() {
		finished = true
		if s.collectedUnits&CPUWork.Mask() != 0 {
			s.sealLocalCPUWork()
		}
		s.sealUnitContributors()
		s.finish.Result = s.collector.Finalize()
		s.finish.Terminal = terminal
		if s.finish.Terminal != TerminalSuccess || s.mode != ModeResultOnly || s.reporter == nil {
			return
		}
		if !s.finish.Result.HasTotal() {
			return
		}
		s.finish.ReportSelected = true
	})
	if finished && s.finish.ReportSelected {
		total, _ := s.finish.Result.TotalRU()
		// The result and report decision are already frozen. Keeping the callback
		// outside sync.Once allows a reporter to inspect Finish without deadlock.
		callStatementRUReporter(s.reporter, Report{TotalRU: total})
	}
	return s.finish, finished
}

func callStatementRUReporter(reporter Reporter, report Report) {
	defer func() {
		// A reporting sink must not suppress slow log, statement summary, and
		// the remaining logical-statement completion work.
		_ = recover()
	}()
	reporter.ReportStatementRU(report)
}
