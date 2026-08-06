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

package executor

import (
	"context"
	"math/bits"
	"sync"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

const maxExactPointReadRUInteger = uint64(1 << 53)

// statementRUPointReadOwner converts one point executor's completed client-go
// evidence into one scan-bytes contribution. It also keeps Close from
// detaching the stats owner while a Next call is still using it.
type statementRUPointReadOwner struct {
	stats *txnsnapshot.SnapshotRuntimeStats
	scan  statementru.UnitContributor

	mu                sync.Mutex
	activeNext        int
	closing           bool
	done              bool
	readComplete      bool
	failed            bool
	localBypass       bool
	snapshotContended bool
	quiesced          chan struct{}
}

// statementRUPointReadAttachment owns all point-read state installed on a
// snapshot. Runtime stats may share the same client-go collector, but the RU
// owner and snapshot option always have one lifecycle.
type statementRUPointReadAttachment struct {
	stats        *txnsnapshot.SnapshotRuntimeStats
	owner        *statementRUPointReadOwner
	registry     *statementRUPointReadRegistry
	snapshot     kv.Snapshot
	installed    bool
	registryPrev *statementRUPointReadAttachment
	registryNext *statementRUPointReadAttachment
}

// statementRUPointReadRegistry coordinates point executors within one logical
// read-only statement. It deliberately does not infer client snapshot identity
// from TiDB wrappers: any overlap is failed closed, and snapshot options are
// kept attached until the last overlapping point executor closes.
type statementRUPointReadRegistry struct {
	sync.Mutex
	head           *statementRUPointReadAttachment
	firstSnapshot  kv.Snapshot
	otherSnapshots []kv.Snapshot
}

func prepareStatementRUPointRead(
	sc *stmtctx.StatementContext,
	stats *txnsnapshot.SnapshotRuntimeStats,
	collectScanBytes bool,
) statementRUPointReadAttachment {
	var registrar statementru.UnitContributorRegistrar
	if sc != nil {
		registrar = sc.StatementRUUnitContributorRegistrar()
	}
	if stats == nil && registrar == nil {
		return statementRUPointReadAttachment{}
	}

	if registrar == nil {
		return statementRUPointReadAttachment{stats: stats}
	}
	if registrar.CollectedUnits()&statementru.ScanBytes.Mask() == 0 {
		return statementRUPointReadAttachment{stats: stats}
	}
	scan := registrar.RegisterUnitContributor(statementru.ScanBytes.Mask())
	if scan == nil {
		return statementRUPointReadAttachment{stats: stats}
	}
	if !collectScanBytes {
		scan.Unsupported()
		return statementRUPointReadAttachment{stats: stats}
	}
	if stats == nil {
		stats = &txnsnapshot.SnapshotRuntimeStats{}
	}
	stats.EnablePointReadStats()
	registryValue, _ := sc.GetOrEvaluateStmtCache(
		stmtctx.StmtPointReadRURegistryCacheKey,
		func() (any, error) { return &statementRUPointReadRegistry{}, nil },
	)
	return statementRUPointReadAttachment{
		stats:    stats,
		registry: registryValue.(*statementRUPointReadRegistry),
		owner: &statementRUPointReadOwner{
			stats: stats,
			scan:  scan,
		},
	}
}

func (a *statementRUPointReadAttachment) install(snapshot kv.Snapshot) {
	if a == nil || a.stats == nil || snapshot == nil || a.installed {
		return
	}
	if a.registry == nil {
		snapshot.SetOption(kv.CollectRuntimeStats, a.stats)
		a.snapshot = snapshot
		a.installed = true
		return
	}

	a.registry.Lock()
	if a.registry.head != nil {
		for attachment := a.registry.head; attachment != nil; attachment = attachment.registryNext {
			attachment.owner.markSnapshotContended()
		}
		a.owner.markSnapshotContended()
	}
	a.registryNext = a.registry.head
	if a.registry.head != nil {
		a.registry.head.registryPrev = a
	}
	a.registry.head = a
	if a.registry.firstSnapshot == nil {
		a.registry.firstSnapshot = snapshot
	} else {
		a.registry.otherSnapshots = append(a.registry.otherSnapshots, snapshot)
	}
	snapshot.SetOption(kv.CollectRuntimeStats, a.stats)
	a.snapshot = snapshot
	a.installed = true
	a.registry.Unlock()
}

func (a *statementRUPointReadAttachment) reset() {
	if a == nil {
		return
	}
	a.close()
	*a = statementRUPointReadAttachment{}
}

func (a *statementRUPointReadAttachment) close() {
	if a == nil {
		return
	}
	a.owner.quiesce()
	a.detach()
	a.owner.finishRecorded()
}

func (a *statementRUPointReadAttachment) detach() {
	if !a.installed || a.stats == nil || a.snapshot == nil {
		return
	}
	if a.registry == nil {
		a.snapshot.SetOption(kv.CollectRuntimeStats, nil)
		a.snapshot = nil
		a.installed = false
		return
	}

	a.registry.Lock()
	if a.registryPrev == nil {
		a.registry.head = a.registryNext
	} else {
		a.registryPrev.registryNext = a.registryNext
	}
	if a.registryNext != nil {
		a.registryNext.registryPrev = a.registryPrev
	}
	if a.registry.head == nil {
		a.registry.firstSnapshot.SetOption(kv.CollectRuntimeStats, nil)
		for _, snapshot := range a.registry.otherSnapshots {
			snapshot.SetOption(kv.CollectRuntimeStats, nil)
		}
		a.registry.firstSnapshot = nil
		a.registry.otherSnapshots = nil
	} else {
		// All overlapping owners are already failed closed. Keep one live
		// collector installed through every known wrapper until the last Close.
		for attachment := a.registry.head; attachment != nil; attachment = attachment.registryNext {
			attachment.snapshot.SetOption(kv.CollectRuntimeStats, attachment.stats)
		}
		a.snapshot.SetOption(kv.CollectRuntimeStats, a.registry.head.stats)
	}
	a.registry.Unlock()
	a.snapshot = nil
	a.installed = false
	a.registryPrev = nil
	a.registryNext = nil
}

func (a *statementRUPointReadAttachment) beginNext() bool {
	return a == nil || a.owner.beginNext()
}

func (a *statementRUPointReadAttachment) finishCall(success, completesReadPhase bool) {
	if a != nil {
		a.owner.finishCall(success, completesReadPhase)
	}
}

func (a *statementRUPointReadAttachment) markLocalBypass() {
	if a != nil {
		a.owner.markLocalBypass()
	}
}

// statementRUPointReadBatchBuffer records values returned at the transaction
// mem-buffer boundary before the client-go union getter removes those keys
// from the snapshot request. Pipelined DML may make this conservative, which
// can suppress a report but cannot publish a partial snapshot value.
type statementRUPointReadBatchBuffer struct {
	kv.MemBuffer
	attachment *statementRUPointReadAttachment
}

func (b *statementRUPointReadBatchBuffer) Get(
	ctx context.Context,
	key kv.Key,
	options ...kv.GetOption,
) (kv.ValueEntry, error) {
	value, err := b.MemBuffer.Get(ctx, key, options...)
	if err == nil {
		b.attachment.markLocalBypass()
	}
	return value, err
}

func (b *statementRUPointReadBatchBuffer) BatchGet(
	ctx context.Context,
	keys [][]byte,
	options ...kv.BatchGetOption,
) (map[string]kv.ValueEntry, error) {
	values, err := b.MemBuffer.BatchGet(ctx, keys, options...)
	if len(values) != 0 {
		b.attachment.markLocalBypass()
	}
	return values, err
}

func (o *statementRUPointReadOwner) beginNext() bool {
	if o == nil {
		return true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closing {
		return false
	}
	o.activeNext++
	return true
}

func (o *statementRUPointReadOwner) markLocalBypass() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.localBypass = true
	o.mu.Unlock()
}

func (o *statementRUPointReadOwner) markSnapshotContended() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.snapshotContended = true
	o.mu.Unlock()
}

// finishCall releases one active Next call and records its terminal evidence.
// Submission is deferred to Close so a concurrently opened executor sharing
// the same transaction snapshot can still invalidate attribution.
func (o *statementRUPointReadOwner) finishCall(success, completesReadPhase bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	if !success {
		o.failed = true
	} else if completesReadPhase {
		o.readComplete = true
	}
	o.activeNext--
	if o.activeNext == 0 && o.quiesced != nil {
		close(o.quiesced)
		o.quiesced = nil
	}
	o.mu.Unlock()
}

func (o *statementRUPointReadOwner) quiesce() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.closing = true
	if o.activeNext == 0 {
		o.mu.Unlock()
		return
	}
	if o.quiesced == nil {
		o.quiesced = make(chan struct{})
	}
	quiesced := o.quiesced
	o.mu.Unlock()
	<-quiesced
}

func (o *statementRUPointReadOwner) close() {
	if o == nil {
		return
	}
	o.quiesce()
	o.finishRecorded()
}

func (o *statementRUPointReadOwner) finishRecorded() {
	if o == nil {
		return
	}
	o.mu.Lock()
	success := o.readComplete && !o.failed
	o.mu.Unlock()
	o.finish(success)
}

func (o *statementRUPointReadOwner) finish(success bool) {
	if o == nil {
		return
	}
	o.mu.Lock()
	if o.done {
		o.mu.Unlock()
		return
	}
	o.done = true
	o.mu.Unlock()
	o.submit(success)
}

func (o *statementRUPointReadOwner) submit(success bool) {
	stats := o.stats.GetPointReadStats()
	o.mu.Lock()
	incompleteAttribution := o.localBypass || o.snapshotContended
	o.mu.Unlock()
	submitPointReadScan(o.scan, stats, success, incompleteAttribution)
}

func submitPointReadScan(
	scan statementru.UnitContributor,
	stats txnsnapshot.PointReadRuntimeStats,
	success bool,
	incompleteAttribution bool,
) {
	if !success {
		if pointReadHasUsableScanEvidence(stats) {
			scan.Partial()
		} else {
			scan.Unavailable()
		}
		return
	}
	if incompleteAttribution {
		if pointReadHasUsableScanEvidence(stats) {
			scan.Partial()
		} else {
			scan.Unavailable()
		}
		return
	}

	bytes, complete, usable := completePointReadScanBytes(stats)
	if !complete {
		if usable {
			scan.Partial()
		} else {
			scan.Unavailable()
		}
		return
	}
	var values statementru.UnitValues
	values[statementru.ScanBytes] = float64(bytes)
	scan.Complete(values)
}

func pointReadHasUsableScanEvidence(stats txnsnapshot.PointReadRuntimeStats) bool {
	return stats.Get.CompletedResponses != 0 || stats.Get.ScanDetailResponses != 0 ||
		stats.BatchGet.CompletedResponses != 0 || stats.BatchGet.ScanDetailResponses != 0 ||
		stats.BufferBatchGet.CompletedResponses != 0 || stats.BufferBatchGet.ScanDetailResponses != 0
}

func pointReadOperationObserved(operation txnsnapshot.PointReadOperationStats) bool {
	return operation.StartedOperations != 0 || operation.CompletedOperations != 0 ||
		operation.RPCAttempts != 0 || operation.CompletedResponses != 0 ||
		operation.ScanDetailResponses != 0 || operation.IntegrityInvalid || operation.Overflowed ||
		operation.ScanDetail.TotalVersions != 0 || operation.ScanDetail.TotalVersionsSize != 0 ||
		operation.ScanDetail.ProcessedVersions != 0 || operation.ScanDetail.ProcessedVersionsSize != 0 ||
		operation.ScanDetail.FallbackTotalVersions != 0 ||
		operation.ScanDetail.FallbackProcessedVersions != 0 ||
		operation.ScanDetail.FallbackProcessedVersionsSize != 0
}

func completePointReadScanBytes(stats txnsnapshot.PointReadRuntimeStats) (float64, bool, bool) {
	// Buffer-tier and unknown operations are not owned by point executors. Do
	// not accept otherwise complete Get or BatchGet slots in their presence.
	if stats.HasUnclassifiedOperations() || pointReadOperationObserved(stats.BufferBatchGet) {
		return 0, false, pointReadHasUsableScanEvidence(stats)
	}
	var total float64
	observed := false
	usable := false
	for _, operation := range []txnsnapshot.PointReadOperationStats{stats.Get, stats.BatchGet} {
		if !pointReadOperationObserved(operation) {
			continue
		}
		observed = true
		usable = usable || operation.CompletedResponses != 0 || operation.ScanDetailResponses != 0
		if !operation.ScanDetailComplete() {
			return 0, false, usable
		}
		scanBytes, valid := completePointReadOperationScanBytes(operation.ScanDetail)
		if !valid {
			return 0, false, true
		}
		if scanBytes > float64(maxExactPointReadRUInteger)-total {
			return 0, false, true
		}
		total += scanBytes
	}
	// TiDB-local buffers and cache-table wrappers can bypass the client-go
	// operation boundary. Without explicit local-hit provenance they are not a
	// proof of authoritative zero.
	if !observed {
		return 0, false, false
	}
	return total, true, usable
}

func completePointReadOperationScanBytes(detail txnsnapshot.PointReadScanDetail) (float64, bool) {
	fields := [...]uint64{
		detail.TotalVersions,
		detail.TotalVersionsSize,
		detail.ProcessedVersions,
		detail.ProcessedVersionsSize,
		detail.FallbackTotalVersions,
		detail.FallbackProcessedVersions,
		detail.FallbackProcessedVersionsSize,
	}
	for _, field := range fields {
		if field > maxExactPointReadRUInteger {
			return 0, false
		}
	}
	if detail.FallbackTotalVersions > detail.TotalVersions ||
		detail.FallbackProcessedVersions > detail.ProcessedVersions ||
		detail.FallbackProcessedVersionsSize > detail.ProcessedVersionsSize {
		return 0, false
	}

	directTotalVersions := detail.TotalVersions - detail.FallbackTotalVersions
	directProcessedVersions := detail.ProcessedVersions - detail.FallbackProcessedVersions
	directProcessedVersionsSize := detail.ProcessedVersionsSize - detail.FallbackProcessedVersionsSize
	if pointReadScanTupleInvalid(
		directTotalVersions,
		directProcessedVersions,
		directProcessedVersionsSize,
	) || pointReadScanTupleInvalid(
		detail.FallbackTotalVersions,
		detail.FallbackProcessedVersions,
		detail.FallbackProcessedVersionsSize,
	) || (directTotalVersions == 0) != (detail.TotalVersionsSize == 0) ||
		detail.TotalVersionsSize < directProcessedVersionsSize {
		return 0, false
	}

	fallbackBytes := float64(0)
	if detail.FallbackProcessedVersions != 0 {
		if !pointReadEstimateWithinExactBoundary(
			detail.FallbackProcessedVersionsSize,
			detail.FallbackTotalVersions,
			detail.FallbackProcessedVersions,
		) {
			return 0, false
		}
		fallbackBytes = float64(detail.FallbackProcessedVersionsSize) /
			float64(detail.FallbackProcessedVersions) * float64(detail.FallbackTotalVersions)
	}
	if fallbackBytes > float64(maxExactPointReadRUInteger-detail.TotalVersionsSize) {
		return 0, false
	}
	return float64(detail.TotalVersionsSize) + fallbackBytes, true
}

func pointReadScanTupleInvalid(totalVersions, processedVersions, processedVersionsSize uint64) bool {
	return processedVersions > totalVersions ||
		(totalVersions == 0 && (processedVersions != 0 || processedVersionsSize != 0)) ||
		(processedVersions == 0 && processedVersionsSize != 0) ||
		(processedVersions != 0 && processedVersionsSize == 0)
}

func pointReadEstimateWithinExactBoundary(processedSize, totalVersions, processedVersions uint64) bool {
	estimateHigh, estimateLow := bits.Mul64(processedSize, totalVersions)
	boundaryHigh, boundaryLow := bits.Mul64(maxExactPointReadRUInteger, processedVersions)
	return estimateHigh < boundaryHigh ||
		(estimateHigh == boundaryHigh && estimateLow <= boundaryLow)
}
