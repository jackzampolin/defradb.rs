# Survey: `crates/query-types/`

## Purpose
Shared type vocabulary for the DefraDB query engine: the data types passed
between parsing, planning, and execution. Defines `Filter` (+ operators,
evaluation, splitting), `Select`/`OrderBy`/`OrderDirection`, `Mutation`/
`MutationType`, `Doc`/`DocStatus`, `DocumentMapping`/`RenderKey`, cursor
request params, `CollectionProvider` trait, query limits, and the `QueryError`
taxonomy. ~1.05k lines top-level + ~1.6k mapper + ~3k filter (2/3 of which is
`filter_tests.rs`). No execution, no IO, no storage — pure in-memory types and
the helper logic that operates on them.

## State machines
None with concurrency or distributed semantics. The only lifecycle-ish enums
are `DocStatus` {Active, Deleted} (a flat 2-state flag, set once by
`mark_deleted`, no transition graph) and `MutationType` {Create, Update,
Delete, Upsert} (a tag, not a state machine). Filter evaluation and
split_by_relation are recursive tree walks over JSON, not protocols.

## Modelable candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| filter-split equivalence | Lean | `split_by_relation(f)` scalar∧relation parts recombine to f's truth set; `split_alias`/`strip_aggregate_alias` preserve semantics on the non-aggregate domain | no | low |
| filter-eval determinism | Lean | `matches_json_object` / `matches_scalar_value` are total + deterministic; `_and`/`_or`/`_not` obey boolean-algebra laws | no | low |

Both are real but borderline: they are local GraphQL-query semantics, not
distributed/consistency/security behavior. `filter_tests.rs` (1211 lines) plus
the query integration suite already pin them against Go parity with concrete
oracles. A Lean restatement would mostly mirror the tests; the live risk is
JSON type-coercion drift (numbers, datetime equality, null ordering in
`operators.rs`), which fixtures catch directly. Cursor request types here are a
thin DTO; the cursor codec itself was surveyed separately (`cursor.md`,
plumbing). Nothing maps to existing slices (B3, convergence, claim, kms, auth,
replicator, commits, integrity, acp, CRDT-laws).

## Verdict
**Plumbing / type vocabulary.** No concurrency, no replication, no eventual
consistency, no security state machine, no content-addressing. The only
non-trivial logic is filter splitting/evaluation, which is law-shaped but
low-priority and thoroughly test-covered. `model_worthy: false` in practice;
the two Lean candidates are listed as low-priority optional only.
