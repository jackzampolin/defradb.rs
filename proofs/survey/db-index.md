# Survey: `crates/db (index)/`

> **Now a module, not a crate.** `db-index` was folded into `db` as `crates/db/src/index/`. The slice below is unchanged; only its location moved.

## Purpose
Secondary-index manager for DefraDB collections (extracted from `db` per #669).
Owns index lifecycle (create / drop / load-from-schema / bulk-index) and
per-document index maintenance during mutations (create / update / delete).
Depends only on storage/datastore/schema/document. ~860 LOC across 3 files.

## Main responsibilities
- `IndexManager`: in-memory map of `name -> IndexType`, lifecycle ops.
- ID allocation via `IndexIDSequenceKey` read-modify-write (single-writer assumed).
- `extract_index_values`: map a document to the set of index-entry tuples,
  expanding array fields and taking the **Cartesian product** across composite
  index fields; missing fields -> `Null`; DateTime CBOR-string coercion (#72).

## State machines
- **Index lifecycle** (implicit): absent -> created -> (bulk-indexed) -> dropped.
  Drop is idempotent (`Ok(false)` if absent). Pure local CRUD over storage; no
  concurrency model beyond the documented single-writer assumption on `next_index_id`.
- **Per-doc maintenance** (implicit): on create/update/delete the manager must keep
  stored index entries consistent with the document's current field values. Update
  = delete-old-tuples then save-new-tuples (guarded by `old != new`).
  No multi-node / replication / security state here.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| index-value-extraction-determinism | Lean | `extract_index_values` is a pure deterministic function of (doc, index_desc, schema); array expansion + Cartesian product yields exactly `∏ |field_i|` tuples, order well-defined | no | low |
| cartesian-product-laws | Lean | `cartesian_product`: `len(prod) = ∏ len(set_i)`; empty input -> `[[]]` (unit); each output tuple has arity = #fields; row-major ordering | no | low |
| index-maintenance-consistency | Lean | after `on_document_update(old,new)` stored entry set = `extract(new)` (no stale `extract(old)\extract(new)` tuples remain, no missing ones) | no | medium |

## Verdict
**Borderline plumbing.** The lifecycle, ID sequencing, and storage save/delete are
glue already exercised by integration tests (`--test query` index_management,
`--test fts`, ACP index). The one genuinely proof-worthy nugget is the
**value-extraction algebra**: array expansion + Cartesian product + the
delete-old/save-new maintenance invariant (stale-entry freedom). These are small
Lean lemmas about a pure function, not a state machine; none is covered by an
existing slice. Priority is low/medium — a bug here is caught by query-result
integration tests, and the function is self-contained. No TLA+ candidate
(no concurrency/replication/security beyond the documented single-writer caveat,
which is an assumption pushed to the transaction layer, not modeled here).

`model_worthy: true` (Lean only, low/medium priority) — but defer unless the
index value-extraction path is being hardened.
