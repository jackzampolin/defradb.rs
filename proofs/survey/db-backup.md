# Survey: `crates/db-backup/`

## Purpose
Database export/import (backup) for DefraDB, extracted from `db` (#669 decomposition).
Public API: `export_database` (serialize collections to JSON), `import_database`
(load JSON into the DB, returns `ImportStats`). Plus reusable helpers:
`classify_schema_fields`, `compute_doc_id_new`, `json_to_graphql_input`.

## Responsibilities
- Export: query each collection via the GraphQL runner, flatten relation fields to
  `_fooID` FKs, compute a fresh content-addressed `_docIDNew`, remap cross-doc FKs to
  the new IDs, recompute IDs, emit JSON (sorted by collection_id for Go parity).
- Import: parse JSON, validate field names, strip+defer self-ref FKs, create docs via
  `AutoCommitMutator`, then patch self-ref FKs with a follow-up update.

## State machines
None explicit. The only multi-step protocol is the export's three-phase pipeline
(Phase 1 compute initial `_docIDNew` -> Phase 2 single-pass FK remap + recompute ->
Phase 3 emit). It is a deterministic batch transform, not a concurrent/lifecycle FSM.
Import is sequential create-then-update per doc; no concurrency, no retries.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| export FK-remap determinism | Lean | Phase-2's single pass over collections (CID-sorted) yields the same `_docIDNew` map regardless of collection order, and is a true fixpoint (no doc needs a second remap pass) | no | low |
| docID content-addressing | Lean | `compute_doc_id_new` is a pure function of field values (FK-excluded, null-stripped) — same content => same ID; covered generically by content-addressing/CID work elsewhere | partial (CID/content-addressing themes already appear in convergence/integrity slices) | low |

## Verdict
**Plumbing, not model-worthy.** This crate is JSON (de)serialization, schema-field
classification, and GraphQL-input string building — glue with no concurrency, no
replication, no access-control, no liveness. Integration tests (`--test backup`:
restore/dump/purge) cover behavior; Go-parity is the real spec. The one faintly
interesting property (export's single-pass FK-remap actually reaching a correct
fixpoint across collection ordering) is a low-priority determinism nicety, easily
guarded by a property test against the existing backup suite rather than a Lean proof.
`compute_doc_id_new` content-addressing is just a thin wrapper over `Document`'s ID
generation, which lives (and would be modeled) in the document/CID layer, not here.
