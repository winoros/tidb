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
// present or partial. Streaming hot-path producers use UnitRecorder and their
// statement-level coordinator uses EvidenceRecorder. Fixed-vector producers
// use UnitContributor leases instead. These mechanisms may share a unit only
// for disjoint physical work; their sticky evidence is combined, so an
// incomplete domain downgrades an otherwise present unit. Producers must
// terminate every registered contributor before Statement.Finish; Finish does
// not wait, and an unterminated contributor fails closed as missing evidence.
// This preserves the distinction between an authoritative zero and missing
// evidence.
// UnitContributorRegistrar provides an exactly-once lease for producers whose
// completeness is known only after several physical owners finish. Complete
// leases submit one atomic fixed vector; Statement.Finish seals registration
// and derives their evidence before freezing the collector. A rejected
// terminal call still consumes its lease and cannot be retried.
//
// Finalize takes one consistent snapshot and is idempotent. Mutations after
// finalization are rejected and cannot change the frozen Result. Result
// accessors return values rather than references to collector-owned state.
// Calibration and Explain may project that result into a Diagnostic containing
// bounded raw evidence and an explicitly non-authoritative candidate total.
package statementru
