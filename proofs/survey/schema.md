# Survey: crates/schema

## Purpose
Schema definition types for DefraDB: `FieldKind`/`ScalarKind`/`CType` (numeric values
byte-match Go for datastore interop), `CollectionVersion`/`FieldDescription`,
index/policy/embedding/relation descriptors, a Go-mirrored definition-validation pipeline
for collection patches, and CID generation for schema-definition blocks (field /
collection / collection-set) via defra-core `Block` + DAG-CBOR.

## State machines
- **Collection-version patch validation** (`definition_validation/`): an old-state →
  new-state transition relation. `UPDATE_VALIDATORS` enforce immutability invariants
  (name, version_id, collection_id, policy, indexes, encrypted_indexes, sources,
  branchable, field properties, field order) plus single-active-version; `GLOBAL_VALIDATORS`
  enforce well-formedness (unique names, type/kind compatibility, embedding rules). This is
  the schema-evolution lifecycle gate.
- **Relation primary-side resolution** (`validation.rs`): each named relation must have
  exactly one primary side across the two collections (cross-collection constraint).

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Schema-def CID determinism | Lean | same field/collection/collection-set content ⇒ byte-identical DAG-CBOR ⇒ same CID; distinct content ⇒ distinct CID | yes — `defra-core.md` "CID content-addressing determinism" + `Block::new` canonicalization; this crate is a caller | low |
| collection-set CID order-invariance | Lean | `generate_collection_set_cid` invariant under permutation of circular-relation member CIDs (Block::new sorts links) | yes — subsumed by defra-core `Block::new` link-ordering canonicalization | low |
| Definition-update immutability monoid | Lean | the immutability validators are sound: any accepted (old→new) patch preserves name/id/version_id/policy/indexes/branchable; rejects all mutations of those (totality over the field set) | no | low |
| Single-active-version / relation-primary invariant | Lean | ≤1 active version per collection_id; each relation has exactly one primary side | no | low |

## Verdict
**Not model-worthy as a standalone slice.** The crate's algebraic core — content-addressing
determinism of schema blocks — is already covered by the `defra-core` Lean candidate (the
real DAG-CBOR/CID logic lives there; `cid.rs` is construction plumbing on top). The
validation pipeline is a flat set of pure predicate functions mirroring Go's
`definition_validation.go`; its correctness is "matches Go," which FFI parity + the
`validation_tests`/`property_tests`/`collection_tests` suites already exercise. The
schema-immutability invariant is conceptually adjacent to the B3 `INV_NoSplitOwnership`
finding (merge-time write-once), but that proof obligation belongs to the merge/replication
path, not this create/update validator. No concurrency, no replication, no adversary state
here — pure deterministic definition logic. Treat as plumbing.
