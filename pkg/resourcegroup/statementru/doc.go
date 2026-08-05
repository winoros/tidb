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

// Package statementru provides the dependency-free collection core for a
// statement Resource Unit result.
//
// A Collector accepts nonnegative finite unit deltas from concurrent
// producers. Unit values and evidence coverage are separate: adding a zero or
// nonzero delta does not prove that all required evidence for that unit is
// present or partial. Hot-path contributors use UnitRecorder. Only the
// statement-level, per-unit coordinator uses EvidenceRecorder, after all
// contributors for an affected unit have terminated. This preserves the
// distinction between an authoritative zero and missing evidence.
//
// Finalize takes one consistent snapshot and is idempotent. Mutations after
// finalization are rejected and cannot change the frozen Result. Result
// accessors return values rather than references to collector-owned state.
package statementru
