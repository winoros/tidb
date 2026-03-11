# Comment Review Checklist

This file is derived from `./comment-guide.md`.
It is a review aid only. It introduces no new policy.

Use it at the end of a change, or when reviewing an agent-authored diff.

## Omission Check

Before deciding that no new comment is needed, ask:

- Did this change alter a guarantee, invariant, ordering rule, ownership rule, compatibility rule, or conservative boundary?
- Would a reviewer need to read tests, PR text, issue history, or another helper to understand the contract?
- Could a future maintainer plausibly "simplify" this code and silently break correctness?
- Does a table, enum, switch, threshold, or special-case branch encode meaning that is wider than the syntax suggests?
- Are two similar-looking outputs or paths intentionally asymmetric?

If any answer is **yes**, a short comment is usually required.

## Before Keeping a New Comment

- Does the code itself already make this obvious?
- Is this comment preserving a real maintenance contract rather than narrating the code?
- Is the chosen scope the narrowest place that can explain the point once?
- Is a shorter comment enough?

If the answer is **no** to these questions, delete or simplify the comment.

## For Maintenance Comments

- Can a maintainer find the contract without reading tests first?
- Does the comment set name the guarantee or invariant being enforced?
- Does it name any intentionally conservative, incomplete, lossy, or unsupported boundary?
- Does it distinguish related outputs, phases, branches, or result shapes when future readers might otherwise collapse them?
- Does it mention non-obvious ordering, ownership, lifecycle, or compatibility constraints?
- Does it mention hidden coupling only when that coupling is genuinely non-obvious?
- Does it avoid duplicating the full explanation at overview, type, and function level?

## Accuracy Checks

- Did the diff update or remove nearby stale comments?
- Are exported Go symbols documented?
- Does the comment describe observable behavior rather than guessed intent?
- Are any performance or historical claims grounded in the task context?
- Does the wording avoid implying a stronger guarantee than the code actually provides?

## Formatting Checks

- Are block comments complete sentences?
- Are inline comments brief and local?
- Does the comment avoid line-by-line translation of the code?
- Is the wording concrete and easy to scan?

## Final Test

A future maintainer who has not read the PR should still be able to answer:

1. What guarantee matters here?
2. Where is the conservative or unsupported boundary?
3. Why is this not safely simplifiable?
4. What future change would require extra care?

If the diff makes one of those questions important and the comments do not help answer it, the code is under-commented.