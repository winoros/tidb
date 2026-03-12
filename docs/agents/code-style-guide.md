# TiDB Code Style Guide for Agents

This document is the repository-level code style guide referenced by `AGENTS.md`.
Normative repository policy remains in root `AGENTS.md`.

## Go and Backend Code

- Follow existing package-local conventions first and keep style consistent with nearby files.
- Keep diffs minimal. Avoid unrelated refactors, broad renames, moves, or formatting-only churn unless explicitly requested. However, this MUST NOT justify omitting necessary comments.
- Code MUST be self-documenting through clear naming and structure.
  - Example: when implementing a well-known algorithm, naming MUST be clear enough to make the approach recognizable; if naming alone may not make intent obvious, add a brief comment.
  ```go
  // Unclear: name does not convey what is being checked.
  func check(cols []*Column) bool { ... }

  // Self-documenting: intent is obvious from the name.
  func allColumnsNotNull(cols []*Column) bool { ... }
  ```
- Keep error handling actionable and contextual; avoid silently swallowing errors.
  ```go
  // Bad: caller loses all context about where the error originated.
  if err != nil {
      return err
  }

  // Good: wraps with context so the call chain is traceable.
  if err != nil {
      return errors.Annotate(err, "failed to resolve table schema")
  }
  ```
- `//nolint` directives MUST include the linter name and a brief reason (for example `//nolint:errcheck // intentionally ignoring Close error on read-only file`).
- For new source files (for example `*.go`), include the standard TiDB license header (copyright + Apache 2.0) by copying from a nearby file and updating year if needed.
- Code comment style and review MUST follow `docs/agents/code-comment-style-guide.md`. Writing clear, sufficient comments is a core quality requirement; do not skip comments for changed code to save time or reduce diff size.

## Tests and Testdata

- Keep test changes minimal and deterministic; avoid broad golden/testdata churn unless required.
- Follow `tidb-test-guidelines` for test placement, naming, fixture reuse, `shard_count`, planner testdata layout, and recording workflow details.

## Docs and Command Snippets

- Commands in docs SHOULD be copy-pasteable from repository root unless explicitly scoped.
- Use explicit placeholders such as `<package_name>`, `<TestName>`, and `<dir>`.
- Documentation updates SHOULD keep terminology, policy wording, and command conventions consistent across related docs.
- Keep guidance executable and concrete; avoid ambiguous phrasing.
