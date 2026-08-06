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

import "math"

// UnitKind identifies one input of the statement RU formula.
type UnitKind uint8

const (
	// CPUWork is the dimensionless work produced by frozen executor formulas,
	// not measured CPU time. Transparent retries retain work already observed.
	CPUWork UnitKind = iota
	// ScanBytes is the per-owner total MVCC-key byte size. A producer may use
	// the documented processed-key average proxy, but exact and estimated
	// values for the same owner are mutually exclusive.
	ScanBytes
	// NetworkBytes is the encoded protobuf response-body size attributable to
	// the statement, counted once for each accepted physical response. It does
	// not include compression, transport framing, or TLS overhead; unsupported
	// storage commands make the unit incomplete instead of changing its meaning.
	NetworkBytes
	// HashStateRows is successful HashJoin build admission count or the exact
	// completed HashAgg logical group count.
	HashStateRows
	// JoinOutputRows is the number of rows in successfully returned join chunks.
	JoinOutputRows
	// WriteKeys is the number of completed keys in statement-owned commit details.
	WriteKeys
	// WriteBytes is the number of completed bytes in statement-owned commit details.
	WriteBytes
	// FrontendCompileBytes is the byte length of the original SQL or prepared
	// template when this execution compiles. A known plan-cache hit is present zero.
	FrontendCompileBytes

	unitKindCount
)

// UnitCount is the number of typed inputs in the statement RU formula.
const UnitCount = int(unitKindCount)

// UnitMask identifies a bounded set of UnitKind values.
type UnitMask uint16

// AllUnits contains every valid UnitKind.
const AllUnits UnitMask = (1 << UnitCount) - 1

// Mask returns the bit corresponding to k. An invalid kind returns an invalid
// nonzero mask so evidence APIs reject it instead of treating it as an empty no-op.
func (k UnitKind) Mask() UnitMask {
	if !k.valid() {
		return 1 << UnitCount
	}
	return 1 << k
}

func (k UnitKind) valid() bool {
	return k < unitKindCount
}

func (m UnitMask) valid() bool {
	return m&^AllUnits == 0
}

// UnitValues is the fixed-size vector of raw statement RU inputs.
type UnitValues [UnitCount]float64

// Weights is the fixed-size vector used to calculate a statement RU total.
type Weights [UnitCount]float64

func validNumber(v float64) bool {
	return v >= 0 && !math.IsNaN(v) && !math.IsInf(v, 0)
}

func checkedAdd(left, right float64) (float64, bool) {
	result := left + right
	return result, validNumber(result)
}

func checkedMultiply(left, right float64) (float64, bool) {
	result := left * right
	return result, validNumber(result)
}
