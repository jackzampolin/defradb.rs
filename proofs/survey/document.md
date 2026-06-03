# document — formal-modelability survey

## Purpose
Runtime document types and the *content-addressing front door*. Holds a `Document`
(ordered field map + CRDT field types + dirty flag), the `NormalValue` type-safe value
enum, `FieldValue` wrapper, and `DocID` (content-addressed identifier). Core pipeline:
JSON -> `NormalValue` -> canonical DAG-CBOR -> SHA2-256 multihash -> CIDv1 -> UUIDv5
(`SDN_NAMESPACE_V0`) -> `DocID`. Also JSON/CBOR round-trip encoding, JSON-path traversal
for indexing, and scalar-kind coercion (DateTime String<->Time #72 repair). Pure,
single-threaded, in-memory value handling — no storage, no network, no transactions.

## State machines
- **None explicit.** `is_dirty` is a single bool flag (clean on `from_cbor`, dirty on
  mutation), not a lifecycle with multi-step transitions. No status/phase enum.
- **Content-addressing pipeline (implicit, deterministic function):** the JSON ->
  CBOR -> hash -> DocID chain is a pure deterministic mapping, not a concurrent protocol.
  The only correctness concern is *determinism / Go byte-parity*, not interleaving.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| DocID content-addressing determinism | Lean | same content (mod field order & null-omission) -> byte-identical CBOR -> identical CID/DocID; distinct content -> distinct DocID (collision-resistance modulo SHA-256) | partial — assumed by Convergence/Integrity TLA+ slices and the db-blocks-survey "CID determinism" item; no standalone proof | low |
| canonical CBOR key ordering | Lean | `canonical_cbor_key_order` is a total order (len-first, then bytewise) and key sort is a deterministic canonical form | implied by content-addressing determinism above | low |
| null-omission invariance | Lean | setting a field to null == omitting it, in `to_cbor` (`toMap(true)` parity) -> same DocID | part of determinism candidate | low |
| CBOR/JSON round-trip fidelity | none | `from_cbor . to_cbor` and json<->normal preserve value (DateTime downgrade is the known lossy case, repaired by `coerce_stored_value_for_kind`) | covered by unit + FFI parity + integration tests | low |
| scalar-kind coercion soundness | none | `json_to_normal_value_for_kind` / `coerce_stored_value_for_kind` keep write-path and reindex-path index bytes identical | covered by db-index slice + unit tests (#72) | low |

## Verdict
**Not independently model-worthy (model_worthy: false).** This is deterministic, pure,
single-threaded value/encoding plumbing — the *producer* twin of `db-blocks`. Its sole
nontrivial property, content-addressing determinism, is an algebraic fact the existing
TLA+ Convergence/Integrity slices already *assume* as their well-formed-input precondition,
and which the db-blocks survey already records as a low-priority Lean candidate. The
round-trip and coercion behaviors are exhaustively pinned by Go FFI parity and integration
tests (golden CIDs, e.g. `bafkreigwbnjspcyc35...`). No concurrency, adversary, replication,
or eventual-consistency state machine lives here. If anything were ever modeled, it would be
a single shared Lean lemma `content -> DocID` determinism, deduplicated against the
db-blocks/convergence slices rather than a new dedicated model.
