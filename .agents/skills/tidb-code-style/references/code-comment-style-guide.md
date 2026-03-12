# TiDB Code Comment Style Guide

## Scope

This style guide applies to code comments in TiDB, including comments in Go, protobuf definitions, and parser grammar files.

The goal is not to add more comments everywhere, but to preserve the knowledge that is hardest to recover from code alone: distributed-system semantics, MySQL compatibility constraints, lifecycle, scoping, invariants, and non-obvious trade-offs.

## Core Style Rules

- Prefer descriptive comments, not imperative comments.
- Write comments in American English with standard grammar and punctuation.
- Prefer full sentences for comments that describe behavior, contracts, lifecycle, or invariants.
- Keep comments close to the code they describe.
- Use Markdown sparingly when it improves readability.
- Wrap long comments when practical and avoid noisy prose.
- Use relative links for files inside the same repository.

## Block vs Inline Comments

Block comments belong above the code they describe and should use full sentences.

```go
// BuildKeyRanges converts pushed-down access conditions into TiKV key ranges.
// It returns only ranges that are valid for the statement snapshot currently in use.
func BuildKeyRanges(...) (...)
```

Inline comments are acceptable for short local clarifications, but should still be concise, accurate, and easy to read.

```go
stmtTS := sc.GetStmtReadTS() // this timestamp fixes visibility for the whole statement.
retryable = false            // do not retry after the schema version has changed.
```

Avoid chatty or redundant inline comments.

```go
x++ // Increment x.
```

## Comment Placement Principles

### Type and Data Structure Comments

Comments for types and structs belong at the type declaration.
They should explain the purpose, lifecycle, and invariants of the type.

For TiDB, type comments are especially valuable when a type has one or more of these properties:

- it is scoped to a session, transaction, statement, or schema version
- it is safe to reuse only after reset
- it is shared across goroutines or protected by a lock or atomic discipline
- it caches state derived from InfoSchema, statistics, or remote components
- it encodes a MySQL-compatible behavior that is not obvious from the implementation

Document:

- who creates the object
- when it is reset or discarded
- whether the zero value is valid
- who is allowed to mutate it
- what data may become stale and under which conditions

Do not repeat these details in every method comment.

### Struct Field Comments

Field comments should explain why the field exists and how it is used over time.
They are most useful when a field has non-obvious ownership, reset rules, concurrency rules,
or compatibility meaning.

Good field comments often answer questions like:

- who populates this field
- when it becomes valid
- when it must be cleared
- whether it is best-effort, cached, or authoritative
- whether it must agree with another field or external snapshot

### Function and Method Comments

Function comments should focus on the contract of the function: inputs, outputs, side effects,
preconditions, postconditions, and failure modes.

For TiDB code, good function comments frequently need to say whether the function:

- uses the current statement snapshot or acquires a new timestamp
- reads or mutates session state
- may access InfoSchema, statistics, or remote storage
- may block, retry, spill to disk, or perform RPCs
- preserves MySQL-compatible semantics or intentionally diverges from them
- is deterministic for the same inputs and snapshot

Describe what the function guarantees, not the entire implementation.
If the tricky part is algorithmic, explain that inside the function body instead.

### Algorithmic Comments Inside Function Bodies

Comments inside a function body should explain non-obvious logic, processing phases, or design
trade-offs.
Focus on why this step exists and what invariant it preserves.

These comments are especially important in TiDB for logic such as:

- AST rewriting and name resolution
- logical-to-physical plan transformation
- decorrelation, predicate pushdown, and partition pruning
- timestamp selection and stale-read handling
- schema-change coordination and online DDL behavior
- retry loops, backoff, and error classification
- memory tracking, spill decisions, and failpoint-only branches

### Overview and Design Comments

Overview comments should be understandable with zero local context.
Assume the reader knows Go and SQL, but not this part of the codebase.

A good design comment should explain:

- the abstraction being modeled
- where it sits in the TiDB execution flow
- the most important invariants
- why the chosen design exists
- one concrete example when the abstraction is hard to visualize

Avoid introducing unexplained project-local jargon.

## TiDB-Specific Priorities

### 1. Explain Scope Precisely

In TiDB, many bugs come from mixing scopes that look similar in code but mean different things in
behavior. Comments should make the scope explicit when relevant:

- session-scoped
- transaction-scoped
- statement-scoped
- schema-version-scoped
- snapshot-scoped
- per-execution versus reusable across executions

If a value is recomputed on retry, inherited from `SessionVars`, or frozen for one statement,
say so.

### 2. Explain Distributed-System Boundaries

When code interacts with TiKV, TiFlash, PD, or another remote subsystem, comments should make clear:

- what is local versus remote state
- what timestamp, schema, or metadata snapshot is assumed
- whether the result is cached, eventually consistent, or authoritative
- what retry or fallback behavior is expected

Do not assume the boundary is obvious from function names alone.

### 3. Explain MySQL Compatibility and Intentional Deviations

TiDB often follows MySQL semantics even when the internal implementation is very different.
When behavior exists for compatibility, say so explicitly.
When behavior intentionally differs, document the reason and user-visible consequence.

This is particularly important in:

- expression semantics
- warnings and error codes
- SQL mode handling
- DDL behavior
- auto-increment and auto-random behavior
- locking and transaction isolation details

### 4. Explain Optimizer and Executor Trade-offs

Planner and executor code should document why a rule or fallback exists.
Good comments mention:

- when a rewrite or pushdown is legal
- when a transformation is disabled for correctness
- which properties must be preserved, such as ordering or cardinality assumptions
- whether the rule is heuristic or cost-based
- what happens when statistics are missing or stale

### 5. Explain Reset and Reuse Semantics

TiDB contains many objects that are reused for performance.
Whenever reuse is non-obvious, comments should document:

- which fields survive reuse
- which fields must be reset
- who owns the reset
- whether stale state can leak across statements or sessions

### 6. Explain Test Intent, Not Just Test Steps

Test comments should describe the protected invariant, regression, or compatibility rule.
A future maintainer should understand what behavior must not change.

Prefer this:

```go
func TestStaleReadUsesConsistentSchemaAndDataSnapshot(t *testing.T) {
    // This test protects the invariant that a stale read resolves both schema
    // and data from the chosen snapshot, so a newer DDL must not leak in.
}
```

Over this:

```go
func TestStaleReadUsesConsistentSchemaAndDataSnapshot(t *testing.T) {
    // Create table.
    // Insert rows.
    // Run stale read.
}
```

## Comment Types and Examples

### Top-Level Design Comment

```go
// Package stmtctx defines statement-scoped execution state shared by planning,
// execution, and diagnostics.
//
// A StatementContext is reset before each statement. It records warnings,
// execution counters, memory and disk trackers, plan metadata, and snapshot-
// dependent state that must not leak across statements.
//
// The package exists as a boundary between session state and lower execution
// layers so that planner and executor code can share statement-local state
// without introducing unnecessary package dependencies.
package stmtctx
```

### API or Interface Comment

```go
// StatsReader reads statistics visible to the current planning snapshot.
//
// Implementations must not mix statistics derived from a newer schema version
// than the InfoSchema used to build the current plan. Returning older stats is
// acceptable when the caller is prepared to fall back conservatively.
type StatsReader interface {
    GetTableStats(tblID int64) (*statistics.Table, bool)
}
```

### Function Comment

```go
// BuildPointGetPlan builds a point-get plan for a unique-key lookup.
//
// The function returns false when the access path cannot guarantee at most one
// row under the current schema and expression semantics. It may consult table
// metadata and session variables, but it does not execute KV requests.
func BuildPointGetPlan(...) (base.Plan, bool, error) {
    ...
}
```

### Struct and Field Comments

```go
// StatementCache stores values derived during one statement execution.
//
// It is owned by StatementContext and must be cleared before the next
// statement. The cache is best-effort only: callers must be prepared to
// recompute values when an entry is missing.
type StatementCache struct {
    // ranges stores access ranges derived from the current statement's AST and
    // InfoSchema snapshot. They become invalid after statement reset.
    ranges map[int][]*ranger.Range

    // warnCount mirrors the number of warnings visible through SHOW WARNINGS.
    // It is updated during execution and read by diagnostics code afterward.
    warnCount uint16
}
```

### Phase Comments in Function Bodies

```go
func (b *PlanBuilder) buildSelect(...) (base.Plan, error) {
    // Resolve table and column names against the statement's InfoSchema
    // snapshot before any rewrite mutates the AST.
    ...

    // Rewrite subqueries and scalar expressions into the planner's internal
    // form so later optimization rules can reason about them uniformly.
    ...

    // Construct the initial logical plan. Cost-based and rule-based
    // optimization happens afterward and must preserve the semantics fixed by
    // the previous phases.
    ...
}
```

### Retry or Concurrency Comment

```go
for {
    // Retry only errors that are safe under the current statement contract.
    // Once the schema version observed by planning is invalid, the caller must
    // rebuild the plan instead of replaying execution blindly.
    ...
}
```

## Comment Maintenance

- Add comments when you discover missing knowledge that would slow down the next reader.
- Treat incorrect comments as bugs and fix them immediately.
- Update comments in the same change when behavior, invariants, or scope changes.
- Fix grammar or wording when it materially improves clarity.
- In review, prefix minor wording suggestions with `nit:`.

## Review Checklist

When reviewing a TiDB comment, ask:

- Does it explain a contract, invariant, scope boundary, or non-obvious trade-off?
- Would a reader understand the behavior without referring to any doc, test, or PR history?
- Does it say why the code exists, not just what the code literally does?
- Is the comment still true after this change?
- Does it mention statement/session/schema/timestamp scope when that scope matters?
- Does it call out MySQL compatibility or distributed-system assumptions when relevant?
