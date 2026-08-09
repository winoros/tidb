# Statement RU Terminal Flat-Plan Walk Framework

- Author(s): [winoros](https://github.com/winoros)
- Discussion PR: Not created
- Tracking Issue: Not created
- Prototype: [pingcap/tidb#69977](https://github.com/pingcap/tidb/pull/69977)

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
  * [Goals](#goals)
  * [Non-Goals](#non-goals)
  * [Terminology](#terminology)
* [Detailed Design](#detailed-design)
  * [First-PR Boundary](#first-pr-boundary)
  * [Two Terminal Facts](#two-terminal-facts)
  * [Statement-Local Owner](#statement-local-owner)
  * [Final-Outcome Handoff](#final-outcome-handoff)
  * [Flat-Plan Construction](#flat-plan-construction)
  * [Framework-Owned Walk](#framework-owned-walk)
  * [Eligibility and Exactly-Once Semantics](#eligibility-and-exactly-once-semantics)
  * [Reporting Boundary](#reporting-boundary)
  * [Retries, Scalar Subqueries, and Cursors](#retries-scalar-subqueries-and-cursors)
  * [Performance Contract](#performance-contract)
  * [Stacked Rollout](#stacked-rollout)
* [Test Design](#test-design)
  * [Functional Tests](#functional-tests)
  * [Scenario Tests](#scenario-tests)
  * [Compatibility Tests](#compatibility-tests)
  * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This proposal introduces the first implementation layer for a new statement RU path. The layer is a default-dark framework that records the final session outcome, reaches the executor terminal, constructs a fresh flat view of the executor-terminal plan, and walks every tree in that flat view through a synchronous internal callback.

The first layer deliberately does not define RU units, weights, formulas, totals, completeness states, or a production reporter. It establishes only the lifecycle and traversal seam required by later calculation work. Existing RU behavior and all existing consumers remain unchanged.

The prototype in [PR #69977](https://github.com/pingcap/tidb/pull/69977) demonstrates that terminal flat-plan traversal can support statement and operator-oriented RU calculation. This proposal adopts the location and traversal idea, not the prototype's calculation or output structures.

## Motivation or Background

The new RU work eventually needs one authoritative RU value for Top RU, raw units for calibration, and total plus per-operator RU for `EXPLAIN ANALYZE`. The demo computes preview RU near `ExecStmt.FinishExecuteStmt` by flattening a plan and looking up runtime evidence by plan ID.

That location is useful but it is not, by itself, the complete statement-success boundary. On a result-set path, `finishStmt` can return a final commit or statement-limit error to the client before `recordSet.Close` enters `FinishExecuteStmt`, and that final error is not currently included in the `FinishExecuteStmt` argument. Optimistic transaction replay can also execute a historical statement again without re-entering `FinishExecuteStmt` for that replay.

The clean design therefore separates two facts:

1. the final session outcome that decides whether an eventual RU report is allowed; and
2. the executor-terminal plan walk that supplies a calculation candidate.

The first stacked PR establishes both handoff points but emits no RU. It is safe to merge dark and gives the following calculation PR a real lifecycle instead of a test-only approximation.

### Goals

This proposal has the following goals:

1. Copy the final `finishStmt` outcome into a statement-local owner before result-set close loses that error.
2. Run the plan-walk hook in `ExecStmt.FinishExecuteStmt` after current terminal evidence finalizers and before slow log, statement summary, and Top RU consumers.
3. Make the disabled path a nil-owner branch: no outcome recording, flattening, walk, callback, or hook allocation.
4. Make the framework, rather than a future calculator, own traversal of `Main`, every CTE tree, and every scalar-subquery tree returned by `FlattenPhysicalPlan`.
5. Consume every terminal classification inside one `sync.Once`, including skips and failures, so a later repeated call cannot turn a failed statement into a successful walk.
6. Keep live plan objects inside a synchronous callback and prevent them from becoming an asynchronous reporting payload.
7. Benchmark statement construction, disabled terminal hooks, and enabled fresh-flatten-plus-walk separately before introducing a production selector.

### Non-Goals

The first implementation layer does not:

- define RU units, coefficients, weights, formulas, totals, or model selection;
- define a generic collection result, coverage object, reason taxonomy, or production report DTO;
- add a public system variable, configuration switch, metric, log field, slow-log field, statement-summary field, Top RU field, or EXPLAIN output;
- call or extend `resourcegroup.ConsumptionReporter` or `ReportRUV2Consumption`;
- read runtime statistics or interpret missing runtime statistics as zero;
- retain `FlatOperator`, plan pointers, flat slices, or runtime statistics asynchronously;
- solve replay accounting for statements re-executed during a later optimistic transaction commit;
- support cursor RU before cursor EOF, CLOSE, RESET, and disconnect share a logical terminal;
- change existing RUv1, RUv2, resource-group admission, token deduction, or billing behavior;
- adopt the demo's unit calculation, metrics, or formatted output; or
- make a flat-plan cache change a prerequisite for the first real ResultOnly benchmark.

### Terminology

- **Final session outcome**: the result of `finishStmt`, including auto-commit, commit, and statement-limit errors known before a result set is closed.
- **Executor terminal**: `ExecStmt.FinishExecuteStmt`, where the current executor plan and accumulated execution details are still available.
- **Executor-terminal plan**: `ExecStmt.Plan` observed at that executor terminal. It is not claimed to include a future replay of a statement from transaction history.
- **Flat plan**: `plannercore.FlatPhysicalPlan`, which contains `Main`, `CTEs`, and `ScalarSubQueries`.
- **Walk owner**: a nullable statement-local object containing the final-outcome handoff, one `sync.Once`, and one synchronous visit function.
- **Production report**: a later complete authoritative RU value sent to a real consumer. A flat-plan visit is not a production report.

## Detailed Design

### First-PR Boundary

The first PR is a concrete lifecycle and walk helper, not a generic RU framework library. It adds only what is required to prove the path:

- a nullable RU-specific walk owner on `ExecStmt`;
- a small final-outcome handoff from session finalization to that owner;
- one framework-owned `walkStatementRUFlatPlan` helper;
- real `FinishExecuteStmt` wiring;
- package-local visit injection for tests;
- focused tests and microbenchmarks; and
- generated Bazel metadata.

It does not introduce event/result/reason structs, retry counters, runtime-stat handles, a generic collector interface, or a reporter interface. Those fields are added only with the concrete calculation or consumer that needs them.

Conceptually, the only private shape needed by this layer is:

```go
type statementRUPlanWalkOwner struct {
	finishOnce   sync.Once
	finalOutcome atomic.Uint32 // unknown, success, or failure
	visit        statementRUPlanVisitFunc
}

type statementRUPlanVisitFunc func(
	treeKind statementRUPlanTreeKind,
	treeIndex int,
	operatorIndex int,
	operator *plannercore.FlatOperator,
)
```

This is illustrative, not a compatibility promise. A private function type is enough because the framework owns the walk and later calculation can close over its own accumulator. The callback returns no generic result in the first PR.

### Two Terminal Facts

For statements without a result set, session execution calls `finishStmt` and then calls `FinishExecuteStmt` with the resulting error. The two terminal facts are already adjacent.

For statements with a result set, `execStmtResult.Finish` calls `finishStmt`, but `recordSet.Close` later calls `FinishExecuteStmt` with executor-close errors only. A commit or `finishStmt` error can therefore be returned to the client while `FinishExecuteStmt` sees a nil error.

The first PR records the final session outcome in the walk owner when `finishStmt` completes. For result-set statements this happens inside the existing `execStmtResult.Finish` once block, before `RecordSet.Close`. For non-result-set and file-transfer paths the equivalent handoff occurs before their direct `FinishExecuteStmt` call.

The cross-package entry is one narrow RU-specific method on `ExecStmt`, conceptually `RecordStatementRUFinalOutcome(success bool)`. It is exported only because `pkg/session` already owns `finishStmt` and depends on `pkg/executor`. It must not become a generic completion interface, global registration point, or API for unrelated consumers, and it is a no-op when the owner is nil.

The owner stores only an atomic three-state value: unknown, success, or failure. Recording uses compare-and-swap from unknown, so the first record wins and conflicting repeats cannot reverse the outcome. The atomic publication supplies the ordering between session outcome recording and terminal read. Production control flow must still record before calling `FinishExecuteStmt`; an unknown outcome fails closed.

The executor-terminal error is a second independent input. A walk is allowed only when the recorded session outcome is success and the error passed to `FinishExecuteStmt` is nil. A later ResultOnly layer may not report based on either signal alone.

### Statement-Local Owner

The owner pointer is nil unless an internal RU walk was selected before execution. The first PR installs no production selector, so normal production statements take only nil checks in outcome handoff and executor terminal code.

The owner is allocated only on the enabled test path. Adding one pointer to `ExecStmt` still changes object size, so benchmarks record statement-construction `B/op`; the contract is zero additional allocation in the nil-owner hooks, not zero total memory impact.

All non-nil classifications occur inside `finishOnce.Do`. Nil owner may return before `sync.Once`, but final session failure, executor-terminal error, restricted SQL, cursor, missing outcome, nil plan, empty flat plan, and callback panic all consume the once. The first owner-terminal call always wins.

The visit function runs synchronously on the terminal goroutine after executor and plan mutation have stopped. The framework does not claim that plan or runtime objects are safe to read concurrently with execution or rebuild.

### Final-Outcome Handoff

The first PR must cover every production call site that reaches `FinishExecuteStmt`:

```text
session finish result
    |
    +-- no result set -------- record final outcome --> FinishExecuteStmt
    +-- result set Finish ---- record final outcome --> RecordSet.Close --> FinishExecuteStmt
    +-- file transfer -------- record final outcome --------------------> FinishExecuteStmt
```

For the result-set path, a regression test uses the existing `finishStmtError` failpoint or an equivalent commit-error injection and proves that the visit function is not called even though executor close itself succeeds. Focused owner tests also prove unknown, success, and failure publication, first-record-wins behavior, and the conjunction of recorded outcome with the executor-terminal error.

The final outcome is the SQL/session outcome, not a network write acknowledgement. A client write failure after successful `finishStmt` does not retroactively change whether the database work completed. Cancellation or execution failure that reaches `finishStmt` remains an error and suppresses the walk.

This handoff does not make `FinishExecuteStmt` a universal command terminal. It only supplies the missing success fact for the supported statement paths.

### Flat-Plan Construction

After the owner has consumed a known successful eligible outcome, it constructs a fresh flat view:

```go
flat := plannercore.FlattenPhysicalPlan(a.Plan, false)
```

The first PR must not call the current `getFlatPlan` cache. `StatementContext.SetPlan` and `SetFlatPlan` are independent, `ResetForRetry` does not clear the flat plan, and `getFlatPlan` returns any non-nil cached value without checking that it belongs to the current plan. A plan rebuild can therefore leave stale `Origin` pointers.

Fresh flatten makes `Main` and CTE traversal derive from the current `ExecStmt.Plan`. Scalar subqueries are a weaker boundary: `FlattenPhysicalPlan` reads them from the session's mutable `MapScalarSubQ` registry rather than from the plan object. The first PR must test real scalar-subquery SQL, prepared EXECUTE, and rebuild paths. Until those tests establish ownership, the design claims only that the framework walks every scalar tree returned by the terminal flatten, not that plan identity alone proves every scalar tree belongs to the same generation.

The call uses `buildSideFirst=false`. Later ResultOnly calculation must be independent of DFS child order. EXPLAIN may retain its display-oriented order separately.

Flat-plan caching is an optional optimization after a real ResultOnly same-revision benchmark. If fresh flatten is material, the narrow optimization is an RU-local binding of the effective plan and its flat snapshot. A repository-wide redesign of plan digest, encoded plan, binary plan, hint, and flat-plan cache coherence is outside this RU stack.

### Framework-Owned Walk

`walkStatementRUFlatPlan` owns the complete traversal. A calculator never receives the whole `FlatPhysicalPlan` and cannot accidentally omit a tree.

The helper visits, in a documented internal order:

1. `Main` with tree index zero;
2. each entry in `CTEs` with its CTE slice index; and
3. each entry in `ScalarSubQueries` with its scalar slice index.

Within each tree it visits every non-nil flat entry with a non-nil `Origin` exactly once. It passes tree kind, tree index, operator index, and the current `FlatOperator` to the synchronous function. It must not call `FlatPlanTree.GetSelectPlan`, because that helper intentionally removes DML prefixes and foreign-key suffixes.

Tree kind and index distinguish occurrences in different trees for the duration of this walk. They are not a durable operator identity. Plan ID and Explain ID may repeat across trees and after rebuild; operator index is only a position in the current slice. A later exported operator result needs a separately reviewed identity, and multi-plan replay additionally needs an epoch or source-owned accounting.

The first PR tests the occurrence multiset, not a formula result or consumer-visible order. Formula completeness and deterministic error aggregation are defined only when a real calculator exists.

### Eligibility and Exactly-Once Semantics

The owner applies this policy inside one `finishOnce`:

| First owner-terminal call | First-PR behavior |
| --- | --- |
| Owner is nil | Return before `sync.Once`; no outcome state, flatten, or callback |
| Final outcome is unknown | Consume once; no walk |
| Final outcome is failure, or executor-terminal error is non-nil | Consume once; no walk |
| Restricted/internal SQL | Consume once; no walk |
| Cursor | Consume once; no walk |
| Nil plan or nil/empty flat plan | Consume once; no walk |
| Known-success eligible statement | Fresh flatten and walk all returned trees once |
| Visit panic | Recover inside the owner and consume once; do not block existing terminal work |

Tests must cover state transitions, not only isolated cases: terminal error then success, restricted or cursor then success, nil plan then non-nil plan, panic then retry, and success then terminal error. They also cover conflicting final-outcome records. In every case the first outcome record and the first non-nil-owner terminal call each win at their own boundary.

Because layer 1 has no production collector, a recovered callback panic cannot occur in normal production. Before a real calculator is installed, the calculation PR must add a bounded aggregate failure signal; it must not log SQL or per-operator data.

### Reporting Boundary

Layer 1 sends no RU value anywhere. Its visit callback is an internal synchronous seam, not a report.

A later ResultOnly layer may report only after both facts are frozen:

1. the final session outcome is successful; and
2. the calculation is complete and authoritative for the supported execution path.

The production payload and reporter are deliberately not designed in this PR. When they are introduced, an authoritative zero is a valid value. Failed, cancelled, incomplete, unavailable, or invalid calculations make no production call; detailed status remains confined to calibration, EXPLAIN, or bounded aggregate diagnostics.

Existing RUv2 reporters cannot be reused for a new numeric space. Existing slow log, statement summary, Top RU, resource-group consumption, and RUv2 finalization remain unchanged in layer 1.

### Retries, Scalar Subqueries, and Cursors

Pessimistic statement retry and plan rebuild can happen before the statement reaches its final session outcome. Fresh flatten observes the executor-terminal plan after that retry, subject to the scalar-registry caveat above.

Optimistic transaction replay is different. A write statement may enter transaction history, complete its original `FinishExecuteStmt`, and later be rebuilt and executed again during COMMIT without another `FinishExecuteStmt` call for the replay. Layer 1 therefore does not call its walk the final plan of that transaction replay and does not claim work completeness across it. A production ResultOnly selector must exclude replay-capable history paths until source-owned accumulation or an explicit replay epoch joins the original statement result.

The first PR adds a negative lifecycle test showing that optimistic replay does not produce a second walk. A retry-plan replacement test must state whether it covers pessimistic in-statement retry or optimistic replay; the two are not interchangeable.

Cursor statements are excluded. Eager and lazy detached cursors do not currently share one terminal: executor work can continue during FETCH, and detached cursor cleanup can bypass the normal `FinishExecuteStmt` path. Tests cover eager and lazy cases separately. Cursor RU remains disabled until EOF, CLOSE, RESET, and disconnect are unified.

### Performance Contract

The disabled path consists of nil-owner checks in final-outcome handoff and executor terminal code. It performs no owner allocation, flatten, walk, or callback. The `ExecStmt` pointer field can still affect object size and is measured.

The enabled first-layer cost is:

```text
owner/outcome bookkeeping + fresh flatten(executor-terminal plan) + one all-tree walk + callback cost
```

The first PR measures only this framework. It excludes demo formulas, runtime-stat instrumentation, Prometheus labels, Info logging, statement summary, Top RU, and EXPLAIN rendering.

Before any production selector is enabled, the calculation PR must run a paired same-binary end-to-end benchmark that includes execution-time instrumentation, final outcome handoff, terminal calculation, and reporting. Only that evidence determines whether fresh flatten needs a cache.

### Stacked Rollout

The implementation is split as follows:

1. **Terminal flat-plan walk**: the final-outcome handoff, nullable owner, framework-owned walk, fresh flatten, exactly-once semantics, panic isolation, tests, and microbenchmarks described here.
2. **First ResultOnly calculation**: a concrete order-independent calculator, explicit supported-path completeness, immutable statement result, bounded aggregate failure signal, and the first real total-only consumer. Replay-capable and cursor paths remain excluded.
3. **Additional evidence**: source-owned nonlinear executor state, remote/storage aggregates, write/commit evidence, and explicit coverage contracts.
4. **Calibration and EXPLAIN**: bounded raw-unit/coverage retention and stable per-operator materialization from the same calculation.

A plan-bound flat cache is an optional performance PR placed only where same-revision end-to-end evidence justifies it. It is not a prerequisite between layers 1 and 2.

## Test Design

### Functional Tests

Layer 1 uses two test levels:

- direct helper tests construct flat plans and prove that Main, multiple CTE trees, and multiple scalar-subquery trees are all walked once, including non-physical DML or synthetic entries and duplicate Plan/Explain IDs; and
- terminal tests inject a visit function into an `ExecStmt` and prove the real final-outcome handoff and `FinishExecuteStmt` wiring.

The exactly-once tests cover repeated sequential `FinishExecuteStmt` calls and concurrent calls to the private owner-terminal method. They do not assume that all of `FinishExecuteStmt` is concurrency-safe or read plan objects concurrently with execution.

### Scenario Tests

Required scenarios are:

- `finishStmtError` or commit failure on a result-set path: zero visits;
- execution error, cancellation, deadline, restricted SQL, and cursor: zero visits;
- all first-call-wins transitions listed in the eligibility section;
- stale `StmtCtx.flatPlan` plus a new `ExecStmt.Plan`: the walk uses fresh flatten;
- real scalar-subquery SQL and prepared EXECUTE: every terminal-returned scalar tree is visited;
- pessimistic retry with plan replacement: the walk sees the executor-terminal plan;
- optimistic transaction replay: no second walk and no claim that the original walk represents replay work;
- PointGet or EXECUTE effective-plan replacement: the walk uses `ExecStmt.Plan` rather than an old flat cache;
- eager cursor and lazy detached cursor: both excluded; and
- visit panic: existing terminal bookkeeping continues and later calls do not walk.

### Compatibility Tests

Layer 1 changes no SQL syntax, system variable, protocol, storage API, PD/TiKV/TiFlash interface, RU formula, or external report. Existing RUv1/RUv2, resource-group consumption, Top RU, slow log, statement summary, and EXPLAIN output must remain behaviorally unchanged when the owner is nil.

Adding Go files, imports, or top-level tests requires `make bazel_prepare`; generated Bazel metadata belongs in the implementation PR.

### Benchmark Tests

The first PR adds short, serial microbenchmarks with `ReportAllocs`:

1. `ExecStmt` or relevant statement setup before and after the owner pointer, to record object/setup `B/op`;
2. nil-owner final-outcome and executor-terminal hooks;
3. enabled fresh flatten plus no-op/counting walk for approximately 1, 11, 50, and 200 operators; and
4. flatten and walk sub-benchmarks reported separately.

The nil-owner hooks have a hard target of zero additional allocations. Time comparisons use the same binary and fixtures and do not run benchmark jobs concurrently. Enabled timings are characterization, not a production promotion threshold, because no real calculator or runtime instrumentation exists yet.

## Impacts & Risks

The intended impact is a small, testable lifecycle seam that does not predefine calculation or transport APIs. It also makes final session outcome explicit before a later report is authorized.

The main risks are:

- missing a `FinishExecuteStmt` call site in the final-outcome handoff;
- performing eligibility checks outside `sync.Once` and allowing a later call to reverse a skip;
- assuming fresh `ExecStmt.Plan` also owns the mutable scalar-subquery registry;
- treating the original walk as final after optimistic transaction replay;
- allowing live flat-plan pointers to escape the synchronous callback;
- silently enabling eager cursor while missing lazy cursor work; and
- adding a flat cache before evidence shows it is needed.

The tests and dark default address these risks for layer 1. Replay, cursor, scalar ownership, and complete calculation remain explicit production gates rather than implicit zero values.

## Investigation & Alternatives

### Use only `FinishExecuteStmt(err)` as final success

This is rejected. On a result-set path, `finishStmt` can return a commit or statement-limit error that is not present in the later `FinishExecuteStmt` argument. The small final-outcome handoff is required before production reporting can be correct.

### Give the whole flat plan to a generic collector

This is rejected. It lets each calculator accidentally omit CTE or scalar-subquery trees and makes the first PR define an unused event/result abstraction. The framework owns the walk and invokes one private function per occurrence.

### Add a generic internal result and reason enum now

This is rejected. Layer 1 has no real calculator or diagnostic consumer. The first ResultOnly PR should introduce only the immutable result and bounded failure signal it actually needs.

### Directly reuse the existing statement flat-plan cache

This is rejected for layer 1 because the cache is not bound to the current plan across retry/rebuild. Fresh flatten is the correctness baseline.

### Make a global plan-cache generation PR the second RU layer

This is rejected. It expands RU work into repository-wide cache coherence before a real same-revision benchmark proves the need. An RU-local snapshot binding remains an optional later optimization.

### Report zero or a partial/unavailable status until formulas land

This is rejected. Zero can be authoritative. Layer 1 emits nothing, and a later production reporter is called only for a complete value. Calibration and EXPLAIN can expose richer internal state separately.

### Collect every unit only at its producer

Producer-side accumulation is likely necessary for replay, asynchronous, or nonlinear evidence, but it does not replace the terminal ownership and reporting barrier. Later layers may combine source-owned scalars with terminal flat-plan attribution.

## Unresolved Questions

1. Which real rebuild paths preserve the scalar-subquery registry for the executor-terminal plan, and which must disable scalar attribution?
2. Which first concrete units can be complete from terminal plan/runtime evidence, and which require source-owned accumulation?
3. What is the first real production consumer and therefore the minimal immutable result and reporter contract?
4. How should replay-capable transaction history and cursor lifecycle join one statement RU result?
5. At what measured plan size or workload does an RU-local plan-bound flat cache become worthwhile?
