# Comment Guide

## General Rules

These rules apply to all comments in the codebase. The Maintenance Comments section below adds structure for a specific subset of changes.

### Core Principles

- Comments SHOULD explain non-obvious intent, constraints, invariants, concurrency guarantees, SQL/compatibility contracts, or important performance trade-offs, and SHOULD NOT restate what the code already makes clear.
- Explain **why**, not **what**. If the code communicates what it does, the comment should add the reasoning or constraint behind the choice.
- Keep exported-symbol doc comments, and prefer semantic constraints over name restatement.

### Formatting

**Block comments** (standalone line above code) use complete sentences with capitalization and terminal punctuation:

```go
// BuildKeyRanges constructs the key ranges for the given index scan.
// It returns an empty slice when no valid range can be derived.
```

**Inline comments** (end of code line) are lowercase without terminal punctuation:

```go
resp, err := client.Send(ctx, req) // nil resp is valid when err != nil
```

### Placement Principles

Choose the location that explains the concept once and avoids duplication:

| Location | Focus |
|----------|-------|
| Package / file level | High-level design, module purpose. MUST be understandable without prior knowledge of the code. |
| Struct / type declaration | Purpose, lifecycle, invariants, field relationships. For interfaces, MUST document the overall contract on the type and describe each method's behavior and preconditions. |
| Function declaration | Inputs, outputs, contract, preconditions. Do NOT re-explain the data structures. |
| Function body | Algorithm phases, non-obvious branches, reasoning behind specific logic. |

### Protobuf

TiDB's protobuf definitions mostly live in external repositories (for example `tipb`, `kvproto`). When adding or modifying `.proto` messages in-tree, follow the same principles as struct comments: document purpose, field semantics, and enum value meanings including intentionally unused or reserved values.

## Maintenance Comments

For most code, the general rules above are sufficient.
This section adds structure for the subset of changes where a plain doc comment is not enough.

### When to Use

Use maintenance comments when a change introduces any of the following:

- A proof strategy or semantic model that spans multiple helpers in one file.
- A sound-but-conservative algorithm where missing precision is intentional.
- An allowlist/denylist/classification table whose omissions change optimization behavior.
- Two nearby helpers or result shapes that sound similar but guarantee different things.
- A hidden coupling between code structure and tests/update workflow that future maintainers could easily miss.

### When NOT to Use

Do not apply this section to every comment. These common cases need only standard doc comments:

- A function/type whose name and signature already make the contract clear (standard godoc is enough).
- A single-branch edge-case comment that does not affect cross-cutting invariants.
- TODO/FIXME notes about future cleanup — use `// TODO(owner-or-issue): description` so TODOs can be tracked and prioritized.
- One-liner clarifications on non-obvious syntax, library usage, or performance micro-optimization.

If the code has no conservative boundary, no split guarantees, and no hidden test coupling, a plain doc comment satisfies the general rules and this section does not apply.

### Placement Rules

Prefer the closest location that explains the whole maintenance contract once:

1. File-level overview comment.
   Use when one conceptual model drives multiple helpers in the file.
   Place it below imports and above the first helper/type.
2. Entrypoint-level comment.
   Use when one exported API or one internal owner helper owns the contract.
   Place it on the function/type that callers will start from.
3. Local branch/block comment.
   Use only for one surprising branch that would still be unclear after the higher-level comment.

Classification tables are not local-branch exceptions. If an allowlist/denylist
defines a file-wide safety or precision boundary, document that contract at
file level or on the entrypoint that consults the table, not only on
the table declaration itself.

Do not duplicate the full explanation in all three places. Write the overview once, then let shorter local comments point back to the key distinction.

### Required Content

A maintenance comment should make these questions easy to answer:

1. What guarantee is this code proving or enforcing?
2. What boundary is intentionally conservative, incomplete, or unsupported?
3. What kind of future change must update code, comment, and tests together?

If the change defines two related outputs with different scopes, name that distinction directly instead of relying on test names or PR context.

See Examples A–C below for real-world applications of these questions.
These examples illustrate the upper end of comment detail; most code only needs the General Rules above.

### Anti-Patterns

Avoid these, because they cause rework later:

- Hiding the real contract only in tests or PR descriptions.
- Using issue-id-only comments such as `// special case for #12345`.
- Writing only local comments when the important distinction spans the whole file.
- Attaching the only allowlist/denylist contract to a table declaration when the
  boundary applies to the file or the entrypoint logic that reads the table.
- Explaining the happy path but not the conservative boundary.
- Adding long prose to `AGENTS.md` instead of putting examples/runbook detail under `docs/agents/`.

### Example A: Two Related Guarantees

`columnStatsUsageCollector` (type comment) and `CollectColumnStatsUsage` (function comment) live in
`pkg/planner/core/rule/collect_column_stats_usage.go` ~390 lines apart, but coordinate to establish
one contract: the set of histogram-needed columns is strictly narrower than predicate columns.

Type comment (on `columnStatsUsageCollector`):

```go
// columnStatsUsageCollector collects predicate columns and/or histogram-needed columns from logical plan.
// Predicate columns are the columns whose statistics are utilized when making query plans, which usually occur in where conditions, join conditions and so on.
// Histogram-needed columns are the columns whose histograms are utilized when making query plans, which usually occur in the conditions pushed down to DataSource.
// The set of histogram-needed columns is the subset of that of predicate columns.
```

Function comment (on `CollectColumnStatsUsage`, same file):

```go
// CollectColumnStatsUsage collects column stats usage from logical plan.
// predicate indicates whether to collect predicate columns and histNeeded indicates whether to collect histogram-needed columns.
// The predicate columns are always collected while the histNeeded columns are depending on whether we use sync load.
// First return value: predicate columns
```

The type comment defines the two concepts and their subset relationship; the function comment
explains the caller-facing split. Together they prevent a maintainer from collapsing the two
concepts and accidentally loading full histogram stats for every predicate column.

### Example B: Conservative Classification Table

`ruleTableEntry` in `pkg/planner/core/joinorder/conflict_detector.go` is a classification-table
contract used by join-order conflict detection. The comment does not only define the enum values;
it also explains why one case is intentionally unused today, what practical consequence that has,
and what future feature work would have to revisit it:

```go
// ruleTableEntry encodes whether a given algebraic property holds for a pair of
// join types (see Table 2 and Table 3 in the paper):
//
//	0 — property does NOT hold; a conflict rule must be generated.
//	1 — property holds unconditionally.
//	2 — property holds only when the null-rejection condition is satisfied.
//
// Currently, value 2 is unused because:
//  1. TiDB does not support FULL OUTER JOIN, which is the main source of
//     conditional entries in the paper's tables.
//  2. extractJoinGroup() only admits non-inner joins that have at least one
//     equi-condition, which implicitly guarantees null-rejection on both sides.
//     This allows assoc(LEFT, LEFT) and assoc(RIGHT, RIGHT) to be treated as
//     unconditional (value 1). If non-inner joins without equi-conditions are
//     admitted in the future, null-rejection checks must be added here.
//
// The value 2 is retained as a placeholder for future extension.
type ruleTableEntry int
```

This names the guarantee, the conservative boundary, the practical consequence (assoc entries
become unconditional), and the future extension point — all in one place.

### Example C: State-Transition Safety Comment

`delayForAsyncCommit` in `pkg/ddl/ddl.go` uses a two-level comment structure.
The doc comment names the safety property; a body comment inside the MDL branch
explains the transaction-interleaving scenario that proves the property:

Doc comment:

```go
// delayForAsyncCommit sleeps `SafeWindow + AllowedClockDrift` before a DDL job finishes.
// It should be called before any DDL that could break data consistency.
// This provides a safe window for async commit and 1PC to commit with an old schema.
func delayForAsyncCommit() {
```

Body comment (inside the `if vardef.IsMDLEnabled()` branch):

```go
	// If metadata lock is enabled. The transaction of DDL must begin after
	// pre-write of the async commit transaction, then the commit ts of DDL
	// must be greater than the async commit transaction. In this case, the
	// corresponding schema of the async commit transaction is correct.
	// suppose we're adding index:
	// - schema state -> StateWriteOnly with version V
	// - some txn T started using async commit and version V,
	//   and T do pre-write before or after V+1
	// - schema state -> StateWriteReorganization with version V+1
	// - T commit finish, with TS
	// - 'wait schema synced' finish
	// - schema state -> Done with version V+2, commit-ts of this
	//   transaction must > TS, so it's safe for T to commit.
```

The doc comment tells readers *what* safety property is protected.
The body comment shows *why* it works by walking through the interleaved transaction scenario.
A future maintainer can see which transition order matters and why a seemingly simpler timing
change could be wrong.

### Review Checklist

Before finishing a change that used this section, verify:

- A maintainer can find the contract without reading tests first.
- The comment names the conservative boundary explicitly.
- The comment tells the reader what future edits require synchronized test updates.
- Nearby helper names and comments do not imply a stronger guarantee than the code actually provides.

## Comment Maintenance

- **Incorrect comments are bugs.** Fix factually wrong comments immediately when discovered, with the same priority as a code bug.
- **Add knowledge when discovered.** When you learn something non-obvious while debugging or reviewing, capture it in a comment at the relevant location.
- **Fix grammar/spelling that impairs reading.** Ignore cosmetic-only issues that do not affect comprehension.
- **Avoid comment-only churn.** Do not reformat or reword comments that are already clear, unless there is a concrete readability improvement.
