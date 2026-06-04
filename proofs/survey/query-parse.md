# Survey: `crates/query-parse/`

## Purpose
GraphQL/SDL parsing front-end for the query pipeline. Turns request strings into
typed structures: GraphQL query/mutation/subscription parsing (`query_parse/`),
the `_cursor` pagination wrapper, SDL → `CollectionVersion` schema parsing
(`sdl_parse/`), GraphQL schema generation (`schema_gen/`), and `Select` → Go-JSON
conversion (`select_convert.rs`). Pure, single-node, synchronous transformation —
no concurrency, no persistence, no network, no access control.

## State machines
None. There are no lifecycle/status enums with transitions and no emergent
multi-component protocols. Control flow is recursive-descent parsing plus
validation. The only non-trivial algorithm is Tarjan-style SCC cycle detection in
`sdl_parse/builder_cycles.rs` (`detect_collection_set`), which groups mutually-
referencing types into collection sets and assigns relative field indices used by
downstream CID generation. That is a deterministic pure function, not a state
machine, and the CID-convergence properties it feeds are owned by other crates.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none — plumbing) | none | parse/validate is deterministic IO transformation; covered by unit + integration tests (`query`, `fts`, sdl_generate) | n/a | n/a |

Notes on things considered and rejected:
- **Cursor arg validation** (forward/backward conflict, non-negative first/last,
  empty-token rejection): finite boolean validation, fully covered by the dense
  unit tests in `query_parse/cursor.rs`. No proof beyond reading adds value.
- **SDL cycle detection / field-ID assignment**: deterministic and feeds
  content-addressed CIDs, which superficially looks Lean-shaped. But the
  content-addressing/convergence invariants are already modeled by the
  **convergence** (DAG) and **integrity** slices and the Lean merge-algebra; this
  crate only computes the grouping, and its correctness is a Go-parity concern
  validated by integration tests (same SDL → same collection set → same CID),
  not an algebraic law in need of proof.
- **Query depth/width limits** (`limits.rs`): a simple counter bound, DoS guard;
  trivially testable, nothing to model.

## Verdict
**Plumbing.** No model-worthy candidates. This is a deterministic parsing/
validation front-end whose correctness is established by reading the code and by
the existing unit + integration test suites. Nothing concurrent, distributed,
security-stateful, or algebraically deep lives here that an existing slice does
not already cover.
