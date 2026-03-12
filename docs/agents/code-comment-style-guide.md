# TiDB Code Comment Style Guide

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

## Comment Guidance by Code Element

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

## Comment Types and Examples

### Top-Level Design Comment

A top-level design comment can live on the declaration that defines the abstraction
boundary. For example:

```go
// MutateBuffers is a memory pool for table-related allocations that exists to
// reuse statement-local buffers across row mutations.
//
// It is used by AddRecord, UpdateRecord, and DeleteRecord. Callers borrow one
// logical buffer at a time through GetXXXBufferWithCap and must finish using it
// before asking for another, because the inner slices are intentionally reused.
//
// This design keeps hot write paths from repeatedly allocating short-lived
// slices while still confining the reused state to one statement's table
// mutation flow.
type MutateBuffers struct {
    ...
}
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

## Comment Maintenance and Review Checklist

### Comment Maintenance

- Add comments when you discover missing knowledge that would slow down the next reader.
- Treat incorrect comments as bugs and fix them immediately.
- Update comments in the same change when behavior, invariants, or scope changes.
- Fix grammar or wording when it materially improves clarity.
- In review, prefix minor wording suggestions with `nit:`.

### Review Checklist

When reviewing a diff, recall our guide and ask:

- Does it have enough comments? Do we need to add crucial comments so someone without domain knowledge can understand the diff?
- Do its comments meet the style guidance in this document?
- Do its comments explain a contract, invariant, scope boundary, or non-obvious trade-off?
- Would a reader understand the behavior without referring to another doc, test, or PR history?
- Do its comments say why the code exists, not just what the code literally does?
- Is the comment around the changed context still true after this change?
