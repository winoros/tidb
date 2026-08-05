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

package join

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/resourcegroup/statementru"
)

// statementRUJoinRuntime belongs to one Open/Close generation. Async workers
// capture its pointer when they start, so a late worker from an old generation
// cannot write into the counters installed by a later Open. Execution failure
// suppresses terminal publication, while arithmetic invalidity is propagated
// only if this generation subsequently reaches a successful terminal boundary.
type statementRUJoinRuntime struct {
	buildRows       atomic.Int64
	probeRows       atomic.Int64
	hashStateRows   atomic.Uint64
	executionFailed atomic.Bool
	invalidUnits    atomic.Uint64
	committed       atomic.Bool
}

func newStatementRUJoinRuntime(enabled bool) *statementRUJoinRuntime {
	if !enabled {
		return nil
	}
	return &statementRUJoinRuntime{}
}

func (r *statementRUJoinRuntime) markFailed() {
	if r != nil {
		r.executionFailed.Store(true)
	}
}

func (r *statementRUJoinRuntime) markArithmeticInvalid(kind statementru.UnitKind) {
	if r == nil {
		return
	}
	mask := uint64(kind.Mask())
	for {
		current := r.invalidUnits.Load()
		if current&mask != 0 || r.invalidUnits.CompareAndSwap(current, current|mask) {
			return
		}
	}
}

func (r *statementRUJoinRuntime) markHashStateArithmeticInvalid() {
	r.markArithmeticInvalid(statementru.HashStateRows)
}

func (r *statementRUJoinRuntime) isArithmeticInvalid(kind statementru.UnitKind) bool {
	return r != nil && r.invalidUnits.Load()&uint64(kind.Mask()) != 0
}

func (r *statementRUJoinRuntime) addLocalRows(total *int64, rows int) {
	if r == nil || rows <= 0 || r.executionFailed.Load() || r.isArithmeticInvalid(statementru.CPUWork) {
		return
	}
	delta := int64(rows)
	if *total < 0 || math.MaxInt64-*total < delta {
		r.markArithmeticInvalid(statementru.CPUWork)
		return
	}
	*total += delta
}

func (r *statementRUJoinRuntime) addRows(dst *atomic.Int64, delta int64) {
	if r == nil || delta <= 0 || r.executionFailed.Load() || r.isArithmeticInvalid(statementru.CPUWork) {
		return
	}
	for {
		current := dst.Load()
		if current < 0 || math.MaxInt64-current < delta {
			r.markArithmeticInvalid(statementru.CPUWork)
			return
		}
		if dst.CompareAndSwap(current, current+delta) {
			return
		}
	}
}

func (r *statementRUJoinRuntime) addBuildRows(rows int64) {
	if r != nil {
		r.addRows(&r.buildRows, rows)
	}
}

func (r *statementRUJoinRuntime) addProbeRows(rows int64) {
	if r != nil {
		r.addRows(&r.probeRows, rows)
	}
}

// mergeBuildRowsAndDone keeps the worker-local merge on the completed side of
// the terminal WaitGroup barrier. Callers must invoke it exactly once for each
// successful Add on wg.
func (r *statementRUJoinRuntime) mergeBuildRowsAndDone(rows int64, wg *sync.WaitGroup) {
	r.addBuildRows(rows)
	wg.Done()
}

// mergeProbeRowsAndDone is the probe-side counterpart of
// mergeBuildRowsAndDone.
func (r *statementRUJoinRuntime) mergeProbeRowsAndDone(rows int64, wg *sync.WaitGroup) {
	r.addProbeRows(rows)
	wg.Done()
}

func (r *statementRUJoinRuntime) setHashStateRows(rows uint64) {
	if r != nil && !r.executionFailed.Load() && !r.isArithmeticInvalid(statementru.HashStateRows) {
		r.hashStateRows.Store(rows)
	}
}

func (r *statementRUJoinRuntime) addHashStateRows(delta uint64) {
	if r == nil || r.executionFailed.Load() || r.isArithmeticInvalid(statementru.HashStateRows) {
		return
	}
	for {
		current := r.hashStateRows.Load()
		if math.MaxUint64-current < delta {
			r.markArithmeticInvalid(statementru.HashStateRows)
			return
		}
		if r.hashStateRows.CompareAndSwap(current, current+delta) {
			return
		}
	}
}

func (r *statementRUJoinRuntime) recordTerminal(executor *exec.BaseExecutor, includeHashState bool) {
	if r == nil || r.executionFailed.Load() || !r.committed.CompareAndSwap(false, true) {
		return
	}
	buildRows, probeRows := r.buildRows.Load(), r.probeRows.Load()
	if buildRows < 0 || probeRows < 0 || math.MaxInt64-buildRows < probeRows {
		r.markArithmeticInvalid(statementru.CPUWork)
	}
	cpuInvalid := r.isArithmeticInvalid(statementru.CPUWork)
	hashStateInvalid := r.isArithmeticInvalid(statementru.HashStateRows)
	if cpuInvalid {
		executor.InvalidateStatementRUUnit(statementru.CPUWork)
	} else {
		executor.RecordStatementRUCPUWork64(buildRows + probeRows)
	}
	if includeHashState {
		if hashStateInvalid {
			executor.InvalidateStatementRUUnit(statementru.HashStateRows)
		} else {
			executor.RecordStatementRUUnit(statementru.HashStateRows, float64(r.hashStateRows.Load()))
		}
	}
}

func recordStatementRUJoinOutput(executor *exec.BaseExecutor, rows int) {
	if rows <= 0 {
		return
	}
	executor.RecordStatementRUUnit(statementru.JoinOutputRows, float64(rows))
}

func statementRUHashStateRowsV1(container *hashRowContainer) (uint64, bool) {
	if container == nil {
		return 0, true
	}
	rows := container.Len()
	if container.hashNANullBucket == nil {
		return rows, true
	}
	nullRows := uint64(len(container.hashNANullBucket.entries))
	if math.MaxUint64-rows < nullRows {
		return 0, false
	}
	return rows + nullRows, true
}

func statementRUHashStateRowsV2(hashTableContext *hashTableContext) (uint64, bool) {
	if hashTableContext == nil || hashTableContext.hashTable == nil {
		return 0, true
	}
	var rows uint64
	for _, table := range hashTableContext.hashTable.tables {
		if table == nil || table.rowData == nil {
			continue
		}
		validRows := table.rowData.validKeyCount()
		if math.MaxUint64-rows < validRows {
			return 0, false
		}
		rows += validRows
	}
	return rows, true
}
