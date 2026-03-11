# Commenting Examples

This file is explanatory and non-normative.

The normative source is `./comment-guide.md`.
If this file conflicts with the rule file, the rule file wins.

This appendix shows the kinds of comment sets that justify more than a plain doc comment. These are examples of *when* and *how* to preserve a maintenance contract, not templates to copy mechanically.

## How to Use This File

Read this file after the rule file when you need help deciding:

- whether a change needs maintenance documentation at all
- which scope should carry the explanation
- how to document a conservative boundary without writing a long file summary
- how to keep a required comment short

Most code still needs only short doc comments or no new comment.

## Example 1: Short But Mandatory Boundary Comment

A required comment does not need to be long.

### Good

```go
// Keep the fast path limited to single-range scans.
// Multi-range plans use the generic path to preserve retry semantics.
if len(ranges) == 1 {
    ...
}
```

### Why this is good

- The branch looks simple.
- The reason is not obvious from the code.
- A future maintainer might "simplify" it away without this comment.

### Avoid

```go
// Fast path.
if len(ranges) == 1 {
    ...
}
```

That restates the branch but loses the contract.

## Example 2: Two Related Outputs With Different Guarantees

Use this pattern when the code produces two related results that future readers may accidentally collapse into one concept.

The names below are illustrative pseudocode, not current TiDB APIs.

### Good split across type and function

```go
// statsUsageSetCollector records two related sets of columns from a logical plan.
//
// Predicate columns are any columns whose statistics may affect planning.
// Histogram-needed columns are the narrower subset whose histograms may be
// consulted during planning, typically after predicates are pushed down to a
// DataSource.
//
// Do not treat these sets as equivalent. Widening histogram-needed columns to
// all predicate columns increases stats loading and changes planner behavior.
type statsUsageSetCollector struct {
    ...
}
```

```go
// CollectStatsUsageSets returns predicate columns and, optionally, the
// narrower histogram-needed subset.
//
// Callers must not assume the two results have the same scope. Predicate
// columns may be present even when histogram-needed columns are empty.
func CollectStatsUsageSets(...) (...) {
    ...
}
```

### Why this is good

- The type comment defines the two concepts once.
- The function comment documents the caller-visible split.
- The combined comment set names the contract without repeating the full explanation in both places.

## Example 3: Conservative Classification Table

Use this pattern when a rule table, matrix, allowlist, or denylist defines a wider safety or precision boundary.

### Good

```go
// ruleTableEntry encodes whether a given algebraic property holds for a pair
// of join types.
//
//   0 — the property does not hold; a conflict rule must be generated
//   1 — the property holds unconditionally
//   2 — the property holds only when the null-rejection condition is satisfied
//
// Value 2 is intentionally unused today:
//
//   1. FULL OUTER JOIN is unsupported, which removes the main source of
//      conditional entries.
//   2. The current join-group extraction logic only admits the supported
//      non-inner cases when the required null-rejection property already holds.
//
// If unsupported non-inner cases are admitted in the future, both this table
// and the code that interprets it must be revisited together.
type ruleTableEntry int
```

### Avoid

```go
// 2 is reserved for future use.
```

That does not explain why it is reserved or what would invalidate the assumption.

## Example 4: State-Transition Safety Comment

Use this pattern when a function relies on a state-transition argument or proof sketch that a future maintainer could easily break by simplifying the code.

### Good

```go
// delayForAsyncCommit waits for SafeWindow + AllowedClockDrift before a DDL job
// finishes.
//
// Call this before any DDL transition that could otherwise let async commit or
// 1PC observe an unsafe schema transition.
func delayForAsyncCommit() {
    if vardef.IsMDLEnabled() {
        // With MDL enabled, safety depends on the DDL transaction beginning
        // after the async-commit transaction has prewritten, so the DDL commit
        // ts stays above the async transaction's commit ts.
        //
        // Example when adding an index:
        //   - schema enters StateWriteOnly at version V
        //   - txn T starts with version V and prewrites
        //   - schema enters StateWriteReorganization at version V+1
        //   - T commits at ts = TS
        //   - schema sync completes
        //   - schema enters Done at version V+2, and the DDL commit ts must be
        //     greater than TS
        //
        // That ordering is what makes it safe for T to commit with the old
        // schema view.
        ...
    }
}
```

### Avoid

```go
// Sleep here to avoid races.
```

That is too vague to preserve the real invariant.

## Example 5: Compatibility Boundary on Persisted State

Use this pattern when changing persisted formats, protobuf fields, enum values, or mixed-version behavior.

### Good

```go
// jobState is persisted in system metadata and may be read by older binaries
// during rolling upgrades.
//
// Add new values only at the end. Never reuse retired numeric values. Keep the
// wire meaning stable across mixed-version clusters.
enum JobState {
    ...
}
```

### Why this is good

- It names the compatibility surface directly.
- It tells future editors what changes are safe and what changes are not.
- It is short, but it captures a high-risk maintenance boundary.

## Example 6: Asymmetric Similar Paths

Use this pattern when two similar-looking branches are intentionally not equivalent.

### Good

```go
// The local path may return stale leaseholder info and is only valid for
// best-effort routing. The distributed path is required for correctness when
// planning retries.
if bestEffort {
    ...
} else {
    ...
}
```

### Why this is good

Without this comment, the branches look mergeable.

## Example 7: Avoid Speculative Motivation

Code agents frequently under-comment contracts and over-comment guessed rationale.

### Avoid

```go
// This is faster because map lookup is expensive.
```

That is unsafe unless the current task or nearby evidence actually supports the claim.

### Better

```go
// Keep the precomputed slice stable across retries so repeated scans observe
// the same ordering.
```

This documents an observable contract instead of a guessed performance story.

## Example 8: Small Local Note That Should Stay Local

Not every non-obvious detail needs maintenance documentation.

### Good

```go
resp, err := client.Send(ctx, req) // nil resp is valid when err != nil
```

### Why this is enough

- The point is local.
- It does not define a wider design contract.
- Expanding this into an overview comment would add noise.

## Quick Heuristics

Add or expand a comment when any of these are true:

- a reviewer would need tests to understand the rule
- the code is intentionally conservative or lossy
- two similar outputs or paths have different guarantees
- a table, enum, threshold, or switch encodes domain semantics
- a future simplification could silently break correctness or compatibility

Stay local when the point is only:

- one surprising line
- one library quirk
- one small syntax clarification
- one obvious follow-on from the code directly above
