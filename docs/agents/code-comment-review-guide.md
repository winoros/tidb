# TiDB Code Comment Review Guide

## Comment Maintenance

- Add comments when you discover missing knowledge that would slow down the next reader.
- Treat incorrect comments as bugs and fix them immediately.
- Update comments in the same change when behavior, invariants, or scope changes.
- Fix grammar or wording when it materially improves clarity.
- In review, prefix minor wording suggestions with `nit:`.

## Review Checklist

When reviewing a TiDB comment, ask:

- Does it explain a contract, invariant, scope boundary, or non-obvious trade-off, and does it follow the style guide?
- Would a reader understand the behavior without referring to any doc, test, or PR history?
- Does it say why the code exists, not just what the code literally does?
- Is the comment still true after this change?
- Does it mention statement/session/schema/timestamp scope when that scope matters?
- Does it call out MySQL compatibility or distributed-system assumptions when relevant?
