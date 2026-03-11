# Commenting Standards

This file is the normative source for comment rules in Go and protobuf source files.

For examples, see `./commenting-examples.md`.
For a compact review checklist, see `./comment-review-checklist.md`.

If either file conflicts with this one, this file wins.

## Scope

- This file applies to Go and protobuf source files.
- Reuse the content rules in other languages when helpful, but do not blindly apply Go doc-comment conventions outside Go.
- Exported-symbol requirements in this file are Go-specific.

## Intent

Comments preserve maintenance-critical knowledge that the code alone does not reliably communicate.

The goal is not to maximize comment count. The goal is to avoid forcing future readers to reconstruct hidden contracts from tests, PR text, issue history, or scattered helper logic.

## Default Posture

- Do not narrate routine code.
- For routine local code, prefer no new comment over a weak comment.
- When a change touches a high-risk surface or introduces a non-obvious contract, prefer a short contract comment over silence.
- In those cases, omission is a correctness and maintenance problem, not a style nit.
- A maintenance comment is not a special tag or syntax. It is an ordinary doc comment or block comment whose content preserves a maintenance contract that would otherwise be easy to lose.

## Mandatory Comment Triggers

You **must** add or update a comment when a change does any of the following:

- changes a caller-visible guarantee, invariant, precondition, postcondition, or side effect
- introduces or changes a conservative boundary, approximation, lossy transformation, unsupported case, or intentional incompleteness
- makes two related outputs, helpers, code paths, or result shapes intentionally different in scope or guarantees
- encodes a non-obvious semantic boundary, compatibility rule, or conservative classification in a table, matrix, enum, allowlist, denylist, switch, threshold, or "magic" constant
- relies on ordering, ownership, lifecycle, invalidation, caching, visibility, atomicity, or lock assumptions that are not obvious from the code
- changes persisted metadata, on-disk layouts, wire formats, protobuf meaning, schema semantics, or mixed-version / rolling-upgrade behavior
- relies on a proof idea, phase structure, or state-transition argument concentrated in one complex function
- would require a reviewer to read tests, PR text, issue history, or multiple helpers to recover the contract

If a trigger fires, silence is usually worse than a short precise comment.

## Quick Decision Flow

Check in this order:

1. **Is this an exported Go symbol?**
   - If yes, it must have a doc comment even when no mandatory trigger fires.
   - Then decide whether that doc comment also needs to carry any maintenance contract from the triggers below.

2. **Did a mandatory trigger fire?**
   - If yes, add or update a comment.
   - Do not skip the comment just because the code looks locally readable.

3. **What is the shortest sufficient form?**
   Prefer the smallest form that preserves the contract:
   - inline comment
   - short local block comment
   - doc comment on a function or type
   - overview/design comment

4. **Where should the comment live?**
   Put it at the narrowest scope that explains the contract once.

5. **Is the rationale uncertain?**
   - Do not guess.
   - Document the observable contract, boundary, ordering, or compatibility rule instead of speculating about motivation.

6. **No exported-symbol requirement applies, no trigger fired, and no nearby comment became stale?**
   - Do not add a new comment.

## Sufficiency Test

A comment is sufficient only if a future maintainer can answer the relevant questions **without reading tests first**:

- What guarantee or invariant matters here?
- Where is the conservative, unsupported, or compatibility boundary?
- Why are nearby similar-looking choices, outputs, or paths **not** equivalent?
- What must remain in sync if this logic changes?

If the answer would still require reverse-engineering from tests or helper call graphs, the comment is too weak or placed too low.

## Comment Escalation Ladder

When a comment is required, prefer the shortest sufficient form:

1. **Inline comment**
   Use for a single local fact.

   Example:
   ```go
   resp, err := client.Send(ctx, req) // nil resp is valid when err != nil
   ```

2. **Short local block comment**
   Use for one branch, boundary, or ordering constraint.
   A few lines are usually enough.

3. **Doc comment on a function, type, or enum**
   Use when the contract is owned by one entrypoint or declaration.

4. **Overview/design comment**
   Use only when one model or boundary spans multiple helpers and no narrower anchor can explain it once.

Do not jump to a file-level overview when a short function or type comment would be enough.

## Core Rules

- Explain **why**, not **what**.
- Do not restate what the code already makes clear.
- Exported Go symbols **must** have doc comments. Keep them accurate when behavior, guarantees, or signatures change.
- Prefer semantic constraints, invariants, and caller-visible guarantees over name restatement.
- When changing behavior, update or remove nearby stale comments in the same diff. Incorrect comments are bugs.
- Do not invent intent, history, or performance rationale that is not supported by the code, tests, nearby design docs, or the task context.
- If a reviewer would reasonably ask "why is this not simpler?" and the answer is non-obvious, capture that answer in a short comment near the owning scope.

## Formatting

**Block comments** use complete sentences with capitalization and terminal punctuation.

```go
// BuildKeyRanges constructs key ranges for the given index scan.
//
// It returns an empty slice when no valid range can be derived.
```

**Inline comments** are brief fragments, usually lowercase, and do not need terminal punctuation.

```go
resp, err := client.Send(ctx, req) // nil resp is valid when err != nil
```

Additional formatting rules:

- Do not translate the code line by line into comments.
- In Go doc comments, start with the documented symbol name when practical.
- Keep wording concrete. Name the guarantee, boundary, incompatibility, or invariant directly.
- Prefer a short accurate comment over no comment when a mandatory trigger fires.
- Prefer a short accurate comment over a long narrative when a short comment is sufficient.

## Placement

Choose the narrowest location that explains the contract once and avoids duplication.

| Location | Focus |
|---|---|
| Overview / design level | High-level design, semantic model, compatibility boundary, conservative boundary, or other maintenance context readers need before the relevant code |
| Struct / type declaration | Purpose, lifecycle, invariants, field relationships, ownership, initialization expectations, and overall contract |
| Function declaration | Inputs, outputs, preconditions, side effects, and caller-visible guarantees |
| Function body | Algorithm phases, surprising branches, local reasoning, ordering constraints, or non-obvious safety / performance constraints |

Additional placement rules:

- Use overview/design comments sparingly.
- Prefer the narrowest scope that explains the contract once.
- Place an overview/design comment near the top of the file only when no narrower location can express the design clearly without duplication or loss of context.
- Do not add an overview/design comment for length alone or as a generic file summary.
- A **comment set** may span two nearby scopes when that is clearer than repetition. For example, a type comment may define two concepts and a function comment may document the caller-visible split between them.
- Do not duplicate the full explanation at overview, type, and function level. State the governing distinction once; keep local comments shorter.
- If a classification table or enum defines a wider contract than its declaration alone reveals, document the contract on the owner that interprets it, not only on the data declaration.

## Interfaces and Schemas

- For interfaces, document the overall contract on the type.
- Document individual methods only when behavior, preconditions, ownership, concurrency, or error semantics are not obvious from the name and signature.
- For stateful structs, document ownership, initialization, who mutates the fields, and when the state becomes invalid or obsolete when that information matters for safe maintenance.
- For schema-like definitions such as protobuf messages, enums, wire-format structs, persisted metadata layouts, or on-disk state, document purpose, field semantics, enum meanings, versioning constraints, and reserved or intentionally unused values when they affect compatibility.

## Maintenance Comments

For most code, the general rules above are sufficient. Use maintenance comments only when a plain doc comment would fail to preserve a maintenance-critical contract.

### When to Use

Add maintenance documentation when a change does any of the following:

- introduces or changes a conservative boundary, unsupported case, or intentional loss of precision
- introduces or changes a classification table, rule matrix, allowlist, or denylist
- defines related outputs, helpers, or result shapes with different guarantees or scopes
- relies on a semantic model or invariant that spans multiple helpers in one file
- relies on a phase structure, state-transition argument, or safety proof concentrated in one complex function
- introduces hidden coupling where future edits must update multiple pieces together
- introduces a rule that is easy to violate by "simplifying" similar-looking code

### High-Risk Surfaces

Bias toward documenting the contract when a change touches any of the following:

- persisted metadata or on-disk layouts
- wire, RPC, protobuf, or schema compatibility
- rolling-upgrade or mixed-version behavior
- version-gated or feature-gated semantics
- ownership, lifecycle, invalidation, or caching boundaries
- concurrency ordering, lock assumptions, memory visibility, or retry semantics
- planner, optimizer, classification, or pruning logic with conservative precision boundaries
- asymmetry between fast paths, fallback paths, sync / async paths, or local / distributed paths

### When NOT to Use

Do not use maintenance comments for:

- a function or type whose contract is already clear from an ordinary doc comment
- a single local edge-case note that does not affect wider guarantees
- TODO/FIXME notes about future cleanup; use `TODO(owner-or-issue): description`
- one-line clarifications on non-obvious syntax, library usage, or small performance details
- generic reminders such as "update tests too" when the coupling is already obvious from the diff

If the code has no conservative boundary, no split guarantees, no hidden coupling, and no reviewer would need outside context to understand it, a normal doc comment or no new comment is enough.

### Required Content

For a change that needs maintenance documentation, the chosen comment set must make the following discoverable **without reading tests first**:

1. the guarantee, invariant, or contract being enforced
2. any intentionally conservative, incomplete, lossy, or unsupported boundary
3. any distinction between related outputs, branches, phases, or result shapes that future readers might otherwise collapse
4. any non-obvious ordering, ownership, lifecycle, or compatibility constraint
5. any hidden coupling that future edits must update together, but only when that coupling is a real maintenance risk

Not every individual comment must restate all five points. The requirement applies to the chosen comment set as a whole.

### Placement Rules

Prefer the closest location that explains the whole maintenance contract once:

1. **Overview/design comment**

   Use when one conceptual model, compatibility boundary, algorithm, or conservative boundary governs the relevant code.

2. **Entrypoint-level comment**

   Use when one exported API, one type, or one internal owner helper owns the contract.

3. **Local branch/block comment**

   Use only for one surprising branch, proof step, or ordering argument that would still be unclear after the higher-level comment.

Additional placement rules:

- Classification tables are not local-branch exceptions. If a table defines a wider safety, compatibility, or precision boundary, document that contract at overview/design level or on the entrypoint that consults the table, not only on the table declaration itself.
- Do not duplicate the full explanation in all three places. Write the governing distinction once, then let shorter local comments point back to it.

## Anti-Patterns

Avoid these patterns:

- hiding the real contract only in tests, PR descriptions, or issue threads
- writing only local comments when the important distinction spans the whole design or entrypoint
- attaching the only classification-table contract to the table declaration when the real boundary belongs to the code that interprets the table
- explaining the happy path but not the conservative or unsupported boundary
- repeating type or data-structure invariants in function comments when the declaration already documents them
- inventing motivation, performance claims, or historical reasons that are not grounded in the current task context
- omitting a short comment only because a longer comment would feel excessive

If an issue or PR matters, summarize the reason in the comment first, then add the reference as supporting context.

## Comment Maintenance

- Incorrect comments are bugs. Fix factually wrong comments immediately.
- When changing behavior, update or remove nearby stale comments in the same diff.
- When a new invariant, boundary, or ordering constraint becomes clear during debugging or review, capture it at the relevant location.
- Fix grammar or spelling when it impairs comprehension.
- Avoid comment-only churn. Do not broadly reword comments that are already clear unless there is a concrete readability or accuracy improvement.