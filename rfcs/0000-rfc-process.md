# RFC 0000 — The RFC process

- **Status:** Accepted
- **Scope:** how design is recorded in this repository

## Summary

Every substantive idea in WaveDB gets a numbered RFC in `rfcs/`. The corpus is
the **single record of the project's design and progress**: the RFC is the *why*
and the *shape*, its status header is *where it stands*, and the code is the
*how*. (This corpus replaced the former `todo.md` / `todo_done.md` work log —
see [below](#relationship-to-the-retired-todo-log).) When an RFC and the code
disagree, the code wins for behaviour and the RFC wins for intent — and one of
them is then wrong and should be fixed.

## Why RFCs

The other doc surfaces each answer a different question, and neither answers
this one:

- **Crate READMEs** describe the *target* architecture as a reference manual —
  organised by component, written for someone using the crate.
- **`AGENTS.md`** is a compressed operating brief for an agent.

What neither holds is a *stable, per-idea record*: "here is one decision, the
problem behind it, the design, and the roads not taken." That is the RFC. An
idea that was tried and dropped leaves no trace in a README (it documents what
*is*) — an RFC keeps the dead idea findable and labelled (its filename carries
`-DEPRECATED`), so nobody re-proposes it a year later.

## Rules

1. **One idea per file.** If the title needs an "and", consider two RFCs (or
   one genuinely-unified idea — use judgement, mirror the file-length rule in
   spirit).
2. **Incremental numbers, four digits, never reused.** The next RFC is the
   highest existing number + 1, deprecated files included. A number is a
   permanent handle; renumbering breaks links.
3. **Descriptive kebab-case slug** after the number:
   `0009-anchors-succession-and-history.md`.
4. **Status header** at the top of every file (see the vocabulary in the
   [index](README.md#status-vocabulary)).
5. **Delivery status shows in the filename.** Non-baseline states carry an
   uppercase marker suffix, so a directory listing *is* the roadmap:
   - `-WIP` — in implementation now (the active work item);
   - `-PLANNED` — accepted, will be built, not started;
   - `-PLANNED-LOW` — deferred, low priority;
   - `-DEPRECATED` — superseded or rejected.

   Baseline states — *Accepted* (policy), *Implemented*, *Partial* — carry **no**
   marker. Changing status is a **rename**, never a delete: the number and the
   file's history stay. A deprecated/superseded file is reduced to *what it
   proposed / why it lost / a link to its replacement* so the dead idea stays
   findable. (The markers are spelled in full — the original request for this
   corpus wrote "DECREPTED"; the conventional `DEPRECATED` is used.)
6. **Ground claims in code.** An RFC cites the crate and, where it helps, the
   module that implements it — so "is this still true?" is checkable.

## Shape of an RFC

A loose template — omit sections that do not apply:

```
# RFC NNNN — Title

- **Status:** …
- **Supersedes / Superseded by:** RFC-XXXX   (when relevant)
- **Crates:** …
- **Code:** key paths

## Summary        — one paragraph a hurried reader can stop at.
## Motivation     — the problem, and why the obvious answer is wrong.
## Design         — the decision, in enough detail to rebuild from.
## Alternatives   — what else was considered, and why it lost.
```

## Relationship to the retired todo log

Earlier in the rebuild, `todo.md` (remaining work + a "DOING" section) and
`todo_done.md` (dated landing notes) tracked progress chronologically. That role
now belongs to this corpus: a shipped RFC records its landing date in its status
header, a `WIP` / `PLANNED` / `PLANNED-LOW` filename marks in-flight work, and
the index's [current-state summary](README.md#current-state) plus status column
give the at-a-glance view. The todo files have been removed; nothing links to
them.
