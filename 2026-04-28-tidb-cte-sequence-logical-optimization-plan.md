# TiDB CTE Sequence Logical Optimization Implementation Plan

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at the TiDB repository root. This standalone `.md` file intentionally omits outer fenced code blocks.


## Purpose / Big Picture

TiDB has introduced `LogicalSequence` / `PhysicalSequence` to represent CTE producers before the main query. The current planner still treats materialized CTE bodies as mostly separate optimization islands: reader-side predicate pushdown records predicates into `CTEClass.PushDownPredicates`, and `LogicalCTE.DeriveStats` later calls `DoOptimize` on the CTE body independently. This leaves logical rules such as predicate pushdown and column pruning incomplete for CTEs, and it makes CTE dependencies harder to reason about.

After this work, materialized CTE producers should be visible to the normal logical optimization pipeline through `LogicalSequence`. Non-recursive CTEs should be able to consume predicates collected from their readers and push them into the producer body during the same logical optimization pass. Recursive CTEs should also be represented in the logical `Sequence` shape, but external predicates from readers must not be pushed into the recursive fixpoint definition unless a later proof shows that the rewrite is semantics-preserving. Recursive CTEs remain blocked from MPP shared CTE physical execution; that is a physical planning limitation, not a reason to exclude them from the logical shape.

The observable outcome is:

- `WITH` queries using materialized non-recursive CTEs produce optimized logical plans where producer bodies have had safe predicates pushed into them.
- `WITH RECURSIVE` queries can still form the logical `Sequence` producer/main-query shape, while preserving recursive CTE semantics.
- MPP shared CTE plans remain available only for supported non-recursive CTEs.


## Progress

- [x] (2026-04-28 Asia/Shanghai) Initial implementation plan written under `~/dev/notes`.
- [x] (2026-04-28 Asia/Shanghai) Reviewed by an independent agent and updated to address the review findings.
- [x] (2026-04-29 Asia/Shanghai) Established regression coverage for logical `Sequence` construction without the MPP shared CTE switch, recursive CTE sequence shape, mixed `WITH RECURSIVE` clauses, non-recursive CTE predicate pushdown, and recursive predicate safety.
- [x] (2026-04-29 Asia/Shanghai) Refactored `tryToBuildSequence` so logical `Sequence` construction is not gated by MPP and does not reject recursive CTEs.
- [x] (2026-04-29 Asia/Shanghai) Taught storage `LogicalCTE` nodes to expose and synchronize producer children.
- [x] (2026-04-29 Asia/Shanghai) Moved non-recursive reader predicate consumption from `DeriveStats` into the logical `Sequence` optimization flow.
- [x] (2026-04-29 Asia/Shanghai) Preserved recursive CTE semantics by not pushing reader predicates into seed or recursive members.
- [x] (2026-04-29 Asia/Shanghai) Adjusted stats and physicalization so sequence producer children feed CTE readers without the old independent `DoOptimize` path.
- [x] (2026-04-29 Asia/Shanghai) Kept recursive CTEs on root physical execution and blocked MPP shared CTE alternatives.
- [x] (2026-04-29 Asia/Shanghai) Added regression tests and ran WIP validation.
- [x] (2026-04-29 Asia/Shanghai) Ran Ready validation before claiming completion or PR readiness.


## Surprises & Discoveries

- Observation: `tryToBuildSequence` currently returns the input plan when `tidb_opt_enable_mpp_shared_cte_execution` is off.
  Evidence: `pkg/planner/core/logical_plan_builder.go`, function `(*PlanBuilder).tryToBuildSequence`.

- Observation: `tryToBuildSequence` currently rejects any CTE from a `WITH RECURSIVE` clause because `cteInfo.nonRecursive` is initialized from the clause-level `!w.IsRecursive`, not from whether the individual CTE actually has a recursive member.
  Evidence: `pkg/planner/core/logical_plan_builder.go`, functions `buildWith` and `tryToBuildSequence`.

- Observation: `LogicalSequence.PredicatePushDown` and `LogicalSequence.PruneColumns` currently only recurse into the last child, the main query.
  Evidence: `pkg/planner/core/operator/logicalop/logical_sequence.go`.

- Observation: `LogicalCTE.PredicatePushDown` currently returns immediately for recursive CTEs. This is conservative and should remain the default for reader-origin predicates, because pushing an outer filter into the recursive definition can change the fixpoint.
  Evidence: `pkg/planner/core/operator/logicalop/logical_cte.go`.

- Observation: `PhysicalSequence.Attach2Task` currently returns the last task when any child is not MPP. For root execution, this means producer child physicalization must have side effects on `CTEClass` before the main query reader is physicalized, or the final plan will not retain the producer.
  Evidence: `pkg/planner/core/task.go`, function `attach2Task4PhysicalSequence`.

- Observation: `EnableMPPSharedCTEExecution` currently takes effect only in `tryToBuildSequence`; `ExhaustPhysicalPlans4LogicalSequence` does not check it before generating MPP choices.
  Evidence: `pkg/planner/core/logical_plan_builder.go`, function `tryToBuildSequence`, and `pkg/planner/core/operator/physicalop/physical_sequence.go`, function `ExhaustPhysicalPlans4LogicalSequence`.

- Observation: Dependent CTE predicate flow is blocked by the old `IsOuterMostCTE` guard. `LogicalCTE.PredicatePushDown` returns when `!Cte.IsOuterMostCTE`, and `RecheckCTE` marks CTEs referenced by another CTE as not outermost.
  Evidence: `pkg/planner/core/operator/logicalop/logical_cte.go`, function `PredicatePushDown`, and `pkg/planner/core/recheck_cte.go`, function `findCTEs`.

- Observation: Recursive CTE stats cannot use the default multi-child derive order if seed and recursive member become ordinary storage children. The default derives every child before the parent can update `SeedStat`, while `LogicalCTETable` reads `SeedStat` during its own derive.
  Evidence: `pkg/planner/core/operator/logicalop/base_logical_plan.go`, function `RecursiveDeriveStats`, and `pkg/planner/core/operator/logicalop/logical_cte_table.go`, function `DeriveStats`.

- Observation: The executor CTE test package lives at `pkg/executor/test/cte` and uses failpoints, so recursive semantics tests there must use the failpoint wrapper.
  Evidence: `pkg/executor/test/cte/cte_test.go`.

- Observation: A CTE producer that already contains `LogicalApply` cannot safely enter the new sequence-managed producer path yet. In an executor regression, the sequence path let projection/column pruning disturb the physical Apply outer-row schema and caused a `NestedLoopApplyExec` panic. Such producers now remain on the existing independent optimization path.
  Evidence: `pkg/planner/core/logical_plan_builder.go`, function `containsLogicalApply`, and `pkg/executor/test/cte/cte_test.go`, `TestCTEShareCorColumn`.

- Observation: Rule rewrites that mutate storage CTE producer children must call `LogicalCTE.SetChild` / `SetChildren` instead of only rewriting the returned child slice, otherwise `CTEClass.SeedPartLogicalPlan` / `RecursivePartLogicalPlan` can keep stale pointers.
  Evidence: `pkg/planner/core/operator/logicalop/logical_cte.go`, `SetChild`, and `pkg/planner/core/rule/rule_partition_processor.go`.


## Decision Log

- Decision: Logical `Sequence` construction must not be gated by `EnableMPPSharedCTEExecution`.
  Rationale: The session variable controls a physical shared-CTE execution strategy. It should not suppress logical producer visibility.
  Date/Author: 2026-04-28 / Codex

- Decision: After removing the logical gate, the `EnableMPPSharedCTEExecution` check must move to MPP physical choice generation.
  Rationale: Turning the session variable off must still prevent MPP shared CTE execution. Moving the gate preserves user-visible behavior while allowing logical optimization to see producer nodes.
  Date/Author: 2026-04-28 / Codex

- Decision: Recursive CTEs should be allowed into the logical `Sequence` shape, but reader predicates must not be pushed into recursive definitions by default.
  Rationale: Recursive CTE physical MPP support is limited, but logical rule traversal and dependency representation are still valuable. However, outer predicates can change recursive fixpoint generation if moved into seed or recursive members.
  Date/Author: 2026-04-28 / Codex

- Decision: Implement this in milestones and keep non-recursive predicate pushdown separate from recursive support.
  Rationale: Non-recursive materialized CTE predicate pushdown is the first behavior improvement. Recursive CTE support in logical `Sequence` is primarily a shape and traversal change until a sound predicate proof exists.
  Date/Author: 2026-04-28 / Codex

- Decision: Ready validation must include `make lint` if code changes are made, and `make bazel_prepare` is required if the implementation adds a new top-level Go test function in an existing `*_test.go` file or otherwise meets the repository's Bazel prepare triggers.
  Rationale: This follows root `AGENTS.md` and `.agents/skills/tidb-verify-profile` / `.agents/skills/tidb-bazel-prepare-gate`.
  Date/Author: 2026-04-28 / Codex

- Decision: Keep producer plans containing `LogicalApply` off the sequence-managed path for now.
  Rationale: The current sequence traversal exposes producer internals to ordinary projection and column pruning rules. That is desirable for ordinary non-recursive producers but is not yet proven safe for Apply subtrees that depend on an enclosing outer-row schema.
  Date/Author: 2026-04-29 / Codex

- Decision: For rule audit, enable ordinary traversal only for non-recursive storage CTEs where the rule is compatible with normal child rewrites. Continue skipping CTE readers, recursive storage CTEs, and correlate/decorrelate rewrites.
  Rationale: Recursive CTEs need fixpoint-specific handling, and correlate/decorrelate can introduce or reshape Apply under a producer, which is the known unsafe case.
  Date/Author: 2026-04-29 / Codex


## Outcomes & Retrospective

Implementation now uses `LogicalSequence` as the logical optimization boundary for materialized CTE producers. Non-recursive storage CTEs expose their seed plan as a child, consume reader-collected predicates during sequence predicate pushdown, and derive stats from the optimized child instead of independently calling `DoOptimize`. Recursive storage CTEs expose seed and recursive children, update seed stats before deriving the recursive member, and keep reader-origin predicates outside the recursive fixpoint.

Physical planning now keeps recursive CTE storage on root tasks and blocks MPP shared CTE alternatives when any producer is recursive or when `tidb_opt_enable_mpp_shared_cte_execution` is disabled. MPP shared CTE golden output was re-recorded for the expected new predicate pushdown shape.

Remaining intentional limitation: CTE producers containing `LogicalApply` use the old independent optimization path until sequence-managed producer optimization can preserve Apply outer-row schema invariants.


## Context and Orientation

The relevant planner files are under `pkg/planner/core` and `pkg/planner/core/operator`.

`CTE` means common table expression, a named subquery introduced by `WITH`. A materialized CTE is planned as a reusable producer plus one or more readers. A recursive CTE has a seed member and a recursive member; the recursive member reads rows produced by earlier iterations until a fixpoint is reached.

`LogicalSequence` is a logical operator whose earlier children are CTE producers and whose last child is the main query. Its documented dependency invariant is that later producer children may depend on earlier producer children, but earlier producers cannot depend on later ones.

Current key paths:

- `pkg/planner/core/logical_plan_builder.go`
  - `buildWith` creates `cteInfo` entries.
  - `buildCte` builds seed and recursive logical plans.
  - `tryBuildCTE` builds `LogicalCTE` readers, or `LogicalCTETable` references inside recursive members.
  - `tryToBuildSequence` currently decides whether to wrap current-layer CTE producers plus the main query in `LogicalSequence`.

- `pkg/planner/core/operator/logicalop/logical_cte.go`
  - `LogicalCTE` represents CTE readers and, with `OnlyUsedAsStorage`, storage/producers.
  - `CTEClass` stores shared CTE state, including seed and recursive logical/physical plans.
  - `PredicatePushDown` records reader predicates for non-recursive CTEs.
  - `DeriveStats` currently injects recorded predicates and independently calls `DoOptimize` on CTE body plans.

- `pkg/planner/core/operator/logicalop/logical_sequence.go`
  - `LogicalSequence` currently forwards predicate pushdown and column pruning only to the main query child.

- `pkg/planner/core/operator/physicalop/physical_cte.go`
  - `PhysicalCTE` is used by CTE readers.
  - `PhysicalCTEStorage` is used as the producer in shared CTE paths.
  - `findBestTask4LogicalCTE` has an old no-child reader path and a child-based path for storage nodes.

- `pkg/planner/core/operator/physicalop/physical_cte_table.go`
  - `LogicalCTETable` inside recursive members currently physicalizes to a root `PhysicalCTETable`, which means recursive CTEs cannot be fully MPP.

- `pkg/planner/core/operator/physicalop/physical_sequence.go`
  - `ExhaustPhysicalPlans4LogicalSequence` builds root and MPP choices. After this change, it must also own the `EnableMPPSharedCTEExecution` gate for MPP shared CTE choices. Recursive CTEs must not enable the MPP shared CTE choice.

- `pkg/planner/core/recheck_cte.go`
  - `RecheckCTE` marks whether a CTE is referenced only by the main query. It is described as temporary until `Sequence` is fully used for CTE optimization.


## Plan of Work

Milestone 1 establishes logical `Sequence` shape independently from MPP. Edit `(*PlanBuilder).tryToBuildSequence` so it removes only CTEs that are inline or have no `CTEClass`. Do not return early only because the session variable `EnableMPPSharedCTEExecution` is false. Move that session-variable gate to the physical MPP choice generation in `ExhaustPhysicalPlans4LogicalSequence`. Do not reject recursive CTEs. Use actual CTE structure, such as `cte.cteClass.RecursivePartLogicalPlan != nil`, when recursive-specific behavior is needed; do not rely on the clause-level `cteInfo.nonRecursive` as a per-CTE truth.

The storage `LogicalCTE` created inside `tryToBuildSequence` must expose the producer body as children. For a non-recursive CTE, set one child: `CTEClass.SeedPartLogicalPlan`. For a recursive CTE, set two children: seed and recursive member. Add a minimal flag to `CTEClass`, for example `UseSequence bool`, if reader-side stats and physicalization need to distinguish the new sequence-managed path from the old independent path. Keep the flag planner-internal and do not expose it to executor APIs.

Milestone 2 synchronizes storage children with `CTEClass`. Add `SetChildren` and `SetChild` methods on `*logicalop.LogicalCTE`. When `OnlyUsedAsStorage` is true, these methods must write child 0 back to `Cte.SeedPartLogicalPlan` and child 1, if present, back to `Cte.RecursivePartLogicalPlan`. When a logical child is changed, clear the corresponding stale physical plan field: `SeedPartPhysicalPlan` or `RecursivePartPhysicalPlan`. This keeps rule rewrites on producer children visible to later stats and physical planning.

Milestone 3 makes predicate pushdown sequence-aware for non-recursive CTEs. Change `LogicalSequence.PredicatePushDown` to first push predicates into the main query child. This lets CTE reader nodes collect safe non-recursive predicates into `CTEClass.PushDownPredicates`. Then process producer children from right to left, because later CTE producers may depend on earlier CTE producers and predicates can flow from a later producer to an earlier one through its readers.

Update non-storage `LogicalCTE.PredicatePushDown` so the sequence-managed path does not reuse the old `IsOuterMostCTE` gate. In the old independent-optimization path, keep the existing `IsOuterMostCTE` behavior. In the sequence-managed path, allow non-recursive CTE readers inside later producers to collect predicates for earlier producers, subject to the existing correlated-column safety filtering. Recursive CTE readers must still refuse reader-origin predicate collection, because pushing those predicates into the fixpoint is not sound by default.

For storage `LogicalCTE.PredicatePushDown`, add a separate `OnlyUsedAsStorage` branch. If the CTE is non-recursive and `Cte.PushDownPredicates` is non-empty, compose those expressions as the old code does with DNF, wrap the seed child in a `LogicalSelection`, clear the recorded list after applying it, and recurse into the seed child so normal PPD can push further. This producer-side DNF is an additional pre-filter for shared storage; it must not consume or remove the original reader-side predicates, because each reader still needs its own filter to preserve multi-reader semantics. If the CTE is recursive, ignore reader-collected predicates and only recurse into seed and recursive children with nil parent predicates. This preserves recursive fixpoint semantics.

Milestone 4 removes the old independent logical optimization path for sequence-managed CTEs. For non-recursive storage CTEs, `LogicalCTE.DeriveStats` can derive stats from `childStats` and child schemas instead of calling `DoOptimize`.

For recursive storage CTEs, do not rely on the default `BaseLogicalPlan.RecursiveDeriveStats` order. Implement `(*LogicalCTE).RecursiveDeriveStats` for `OnlyUsedAsStorage && RecursivePartLogicalPlan != nil`: first derive the seed child, immediately update `*SeedStat` from the seed stats, then derive the recursive child so `LogicalCTETable` reads the current seed/working-table stats, and finally combine seed and recursive stats the same way the old method did, including the distinct-row estimate for `IsDistinct`.

For non-storage reader `LogicalCTE.DeriveStats`, if `CTEClass.UseSequence` is set, do not call `DoOptimize`. Instead compute reader stats from `CTEClass.SeedPartLogicalPlan.StatsInfo()` and, for recursive CTEs, `CTEClass.RecursivePartLogicalPlan.StatsInfo()`. Keep the old `DoOptimize` fallback for CTEs that are not represented by a sequence, such as old-path recursive cases during incremental development.

Milestone 5 connects sequence producer physicalization to CTE readers. Update `ExhaustPhysicalPlans4LogicalCTE` so child required properties match the number of storage children. For a sequence-managed non-recursive storage CTE, keep the existing root and MPP behavior where valid. For recursive storage CTEs, do not produce MPP storage alternatives; require root physicalization for seed and recursive children.

Update `attach2Task4PhysicalCTEStorage` so it writes physical child plans back into `CTEClass`. For one child, set `CTE.SeedPartPhysicalPlan`. For two children, set both `CTE.SeedPartPhysicalPlan` and `CTE.RecursivePartPhysicalPlan`. This is necessary because `PhysicalSequence.Attach2Task` drops producer tasks on the root path and returns the main query task; the producer physicalization must still happen before main query readers are planned.

Milestone 6 keeps physical MPP behavior conservative. Update `ExhaustPhysicalPlans4LogicalSequence` so MPP shared CTE options are generated only when `EnableMPPSharedCTEExecution` is true and no producer CTE is recursive. Be precise about property requests: under a root property request, keep the root-compatible sequence choice; under an MPP property request, if shared CTE is disabled or any producer is recursive, do not generate a fake root-compatible choice to satisfy the MPP request. It is acceptable for the main query to use whatever physical shape is valid after the recursive CTE reader has forced root behavior, but do not create an all-MPP shared CTE path involving recursive storage.

Milestone 7 audits logical rules that currently treat every `LogicalCTE` as an independent optimization island. Start by generating the audit list from the current tree:

    rg -n "LogicalCTE|LogicalSequence|LogicalCTETable" pkg/planner/core -g'*.go'

Classify every hit as reader-only, storage-only, both, or unrelated. At minimum inspect and update these files:

- `pkg/planner/core/rule_eliminate_projection.go`
- `pkg/planner/core/rule_join_reorder.go`
- `pkg/planner/core/joinorder/join_order.go`
- `pkg/planner/core/rule_join_elimination.go`
- `pkg/planner/core/rule/rule_partition_processor.go`
- `pkg/planner/core/rule_semi_join_rewrite.go`
- `pkg/planner/core/rule_decorrelate.go`
- `pkg/planner/core/rule_correlate.go`
- `pkg/planner/core/rule_generate_column_substitute.go`
- `pkg/planner/core/rule/rule_order_aware_join_reorder.go`
- `pkg/planner/core/rule/rule_max_min_eliminate.go`
- `pkg/planner/core/rule_push_down_sequence.go`
- `pkg/planner/core/rule/collect_column_stats_usage.go`

Do not blanket-enable every rule for recursive CTE internals. The first pass should change guards from "skip every `LogicalCTE`" to "skip CTE readers, and skip recursive storage nodes for rules that are not proven safe." For storage non-recursive CTEs, allow normal child traversal where the rule already supports ordinary single-child or multi-child logical plans.

Milestone 8 updates `RecheckCTE`. A storage `LogicalCTE` inside `Sequence` is a producer, not a reader. `findCTEs` should not mark it as an outermost reader reference. It should recurse into the storage node's children, or into `CTEClass.SeedPartLogicalPlan` / `RecursivePartLogicalPlan` if the child list is not available during a transition period.

Milestone 9 implements column pruning only after predicate pushdown and stats are stable. The safe first version may keep producer schemas unpruned for recursive CTEs. For non-recursive CTEs, aggregate used columns from all readers before pruning the producer body. Do not allow different readers to demand incompatible producer schemas; CTE storage and readers must keep positional schema mapping consistent.


## Concrete Steps

Start from the repository root:

    cd /home/misaka/dev/gocode/tidb.worktrees/cte-opt-refactor
    git status --short

Confirm the current code paths before editing:

    rg -n "tryToBuildSequence|OnlyUsedAsStorage|PushDownPredicates|RecursivePartLogicalPlan|ExhaustPhysicalPlans4LogicalSequence" pkg/planner/core

Implement Milestone 1 and Milestone 2 together because the logical shape needs storage child synchronization to remain coherent. Keep the diff focused on:

- `pkg/planner/core/logical_plan_builder.go`
- `pkg/planner/core/operator/logicalop/logical_cte.go`
- `pkg/planner/core/recheck_cte.go`

Add or extend logical-shape tests near `pkg/planner/core/logical_plans_test.go`. Prefer extending an existing CTE logical plan test if one exists. If a new top-level `func TestXxx(t *testing.T)` is added to an existing `*_test.go`, run `make bazel_prepare` later and include any generated Bazel metadata changes.

Implement Milestone 3 and Milestone 4 after the shape tests pass. Keep recursive reader predicate behavior conservative: no outer predicate from `SELECT * FROM recursive_cte WHERE ...` should enter the recursive seed or recursive member.

Implement Milestone 5 and Milestone 6 after stats tests pass. Keep root physical behavior valid before attempting MPP validation. If physical planning becomes cyclic or reads nil `SeedPartPhysicalPlan`, stop and inspect whether producer child physicalization happens before main query child physicalization in `iterateChildPlan4LogicalSequence`.

Implement Milestone 7 and Milestone 8 as focused follow-up edits. Generate the audit list with `rg`, classify each CTE/Sequence hit, and record any skipped rule with a short reason in the implementation notes or PR description. For each logical rule changed, include a short code comment only when the CTE reader/storage distinction is not obvious from the condition.

Implement Milestone 9 last. If column pruning for CTE producers grows large, split it into a follow-up PR and leave producer pruning conservative in this change.


## Validation and Acceptance

Use the `WIP` profile during implementation. Use targeted tests only until the behavior is stable.

Before running package tests under `pkg/planner/core`, check failpoint usage:

    rg -n --fixed-strings -- "failpoint." pkg/planner/core
    rg -n --fixed-strings -- "testfailpoint." pkg/planner/core
    test -f pkg/planner/core/BUILD.bazel && rg -n --fixed-strings -- "@com_github_pingcap_failpoint//:failpoint" pkg/planner/core/BUILD.bazel

Because `pkg/planner/core` uses failpoints, run targeted planner tests through the failpoint wrapper:

    ./tools/check/failpoint-go-test.sh pkg/planner/core -run 'Test.*CTE.*Sequence|Test.*CTE.*Predicate' -count=1

Suggested regression coverage:

- Logical shape: a materialized non-recursive CTE builds `LogicalSequence` even when `tidb_opt_enable_mpp_shared_cte_execution` is off.
- Logical shape: a recursive CTE builds the logical `Sequence` producer/main-query shape, but does not produce an MPP shared CTE physical path.
- Physical gate: with MPP allowed or enforced but `tidb_opt_enable_mpp_shared_cte_execution=off`, a materialized CTE query must not use the MPP shared CTE path.
- Same-clause mixed case: `WITH RECURSIVE` containing a non-recursive CTE should not be excluded merely because the clause uses the `RECURSIVE` keyword.
- Non-recursive predicate pushdown: a filter on a materialized CTE reader is consumed by the producer and pushed to the underlying `DataSource` when safe.
- Reader-filter preservation: after producer-side DNF pre-filtering is added, each CTE reader still retains its original filter predicates so multi-reader queries remain semantically unchanged.
- Recursive predicate safety: an outer filter on a recursive CTE reader remains outside the recursive fixpoint and does not alter the generated rows.
- Dependent CTEs: if `c2` reads `c1`, predicates collected from `c2` are applied before optimizing `c1` only when they flow through a `c1` reader in `c2` and remain non-recursive-safe.

For user-visible recursive semantics, add an executor-level regression if planner-only inspection is not enough:

    ./tools/check/failpoint-go-test.sh pkg/executor/test/cte -run TestRecursiveCTEPredicateNotPushedIntoFixpoint -count=1

Use the failpoint wrapper for this package because `pkg/executor/test/cte` uses failpoints.

If MPP shared CTE behavior changes, run the targeted enforcempp test:

    go test -tags=intest,deadlock ./pkg/planner/core/casetest/enforcempp -run TestMPPSharedCTEScan -count=1

Before claiming completion or PR readiness, use the `Ready` profile:

    git diff --check
    make lint

Run `make bazel_prepare` before Ready validation if any trigger applies:

- Go files were added, removed, renamed, or moved.
- A new top-level Go test function was added to an existing `*_test.go`.
- Bazel files changed.
- `go.mod` or `go.sum` changed.
- Bazel target metadata changed.
- Bazel dependency or toolchain errors occurred.

If `make bazel_prepare` is run, inspect and include resulting `BUILD.bazel`, `*.bazel`, or `*.bzl` changes.

Acceptance criteria:

- The new tests fail on the old behavior and pass after the implementation.
- Non-recursive materialized CTE predicates are pushed into the producer body through `LogicalSequence`, not delayed until an independent `DoOptimize` in stats derivation.
- Recursive CTE results are unchanged for outer filters that would be unsafe to push into the fixpoint.
- Recursive CTEs do not get MPP shared CTE physical alternatives.
- `tidb_opt_enable_mpp_shared_cte_execution=off` still disables MPP shared CTE physical execution even though logical `Sequence` construction remains enabled.
- Existing targeted CTE, planner, and MPP tests pass.


## Idempotence and Recovery

The logical-shape and predicate-pushdown changes should be safe to rerun through tests. If a test leaves generated testdata changes, inspect them before keeping them.

If a partial implementation breaks physical planning with nil `SeedPartPhysicalPlan` or `RecursivePartPhysicalPlan`, first check these invariants:

- Every sequence-managed storage `LogicalCTE` has child 0 equal to `CTEClass.SeedPartLogicalPlan`.
- Recursive storage `LogicalCTE` has child 1 equal to `CTEClass.RecursivePartLogicalPlan`.
- `SetChild` / `SetChildren` on storage `LogicalCTE` clears stale physical plan fields.
- Recursive storage `LogicalCTE.RecursiveDeriveStats` derives seed stats before recursive-member stats and updates `SeedStat` between the two.
- `attach2Task4PhysicalCTEStorage` writes physical child plans back to `CTEClass` before the main query child is physicalized.
- Reader `LogicalCTE.DeriveStats` does not call old `DoOptimize` when `CTEClass.UseSequence` is true.

If recursive predicate pushdown accidentally changes result rows, revert only the recursive predicate consumption branch and keep the logical `Sequence` shape. The shape change and predicate movement are separable.

Avoid broad revert commands. Use `git diff` to identify the smallest bad hunk and patch it directly.


## Artifacts and Notes

Important code references:

- `pkg/planner/core/logical_plan_builder.go`: `buildWith`, `buildCte`, `tryBuildCTE`, `tryToBuildSequence`.
- `pkg/planner/core/operator/logicalop/logical_cte.go`: `LogicalCTE`, `CTEClass`, `PredicatePushDown`, `DeriveStats`.
- `pkg/planner/core/operator/logicalop/logical_sequence.go`: `LogicalSequence.PredicatePushDown`, `PruneColumns`, `DeriveStats`.
- `pkg/planner/core/operator/physicalop/physical_cte.go`: `PhysicalCTE`, `PhysicalCTEStorage`, `findBestTask4LogicalCTE`.
- `pkg/planner/core/operator/physicalop/physical_sequence.go`: `ExhaustPhysicalPlans4LogicalSequence`, including the physical `EnableMPPSharedCTEExecution` gate.
- `pkg/planner/core/task.go`: `attach2Task4PhysicalCTEStorage`, `attach2Task4PhysicalSequence`.
- `pkg/planner/core/find_best_task.go`: `iterateChildPlan4LogicalSequence`.
- `pkg/planner/core/recheck_cte.go`: temporary outermost CTE marking logic.

Plan update note, 2026-04-28: Initial plan created after code reading. The plan explicitly separates logical recursive CTE inclusion from physical MPP support, and treats recursive reader predicate pushdown as unsafe unless separately proven.

Plan update note, 2026-04-28: Addressed independent agent review. Added the physical migration of the MPP shared CTE session-variable gate, sequence-managed reader predicate collection rules that bypass the old `IsOuterMostCTE` blocker for non-recursive CTEs, custom recursive storage stats ordering, a broader rule audit process, and the correct failpoint-enabled executor CTE test command.

Plan update note, 2026-04-28: Addressed second review pass. Clarified exact physical behavior for MPP property requests when shared CTE is disabled or recursive producers exist, and stated explicitly that producer-side DNF filtering must not remove reader-side filters.


## Interfaces and Dependencies

Expected internal additions or changes:

- In `pkg/planner/core/operator/logicalop/logical_cte.go`, `CTEClass` may gain:

        UseSequence bool

  The exact name can change, but the purpose must remain clear: it marks that this CTE has a sequence-managed producer, so readers should not trigger old independent `DoOptimize` during stats derivation.

- In `pkg/planner/core/operator/logicalop/logical_cte.go`, `*LogicalCTE` should implement:

        SetChildren(...base.LogicalPlan)
        SetChild(i int, child base.LogicalPlan)
        RecursiveDeriveStats(colGroups [][]*expression.Column) (*property.StatsInfo, bool, error)

  `SetChildren` and `SetChild` must delegate to `BaseLogicalPlan` and then synchronize storage children back to `CTEClass`. `RecursiveDeriveStats` must preserve the special seed-before-recursive stats order for recursive storage CTEs and may delegate to the base implementation for ordinary readers and non-recursive cases.

- In `pkg/planner/core/operator/logicalop/logical_sequence.go`, `*LogicalSequence` should implement producer-aware predicate pushdown. It must process the main query first, then producers right-to-left.

- In `pkg/planner/core/task.go`, `attach2Task4PhysicalCTEStorage` should write selected physical child plans back into `CTEClass` for both non-recursive and recursive storage producers.

- In `pkg/planner/core/operator/physicalop/physical_sequence.go`, `ExhaustPhysicalPlans4LogicalSequence` must check `EnableMPPSharedCTEExecution` before adding MPP shared CTE property choices. This preserves the existing session variable contract after logical `Sequence` construction is always enabled for materialized CTEs.

No new external dependencies should be added. No `go.mod` or `go.sum` changes are expected.
