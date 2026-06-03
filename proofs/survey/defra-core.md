# Survey: `crates/defra-core`

## Purpose
Foundational types/traits shared across DefraDB.rs: content-addressed IPLD `Block`
(DAG-CBOR + CIDv1/SHA2-256), CRDT delta payloads (LWW/counter/composite/collection/
schema-def), `DocID` parse/format, signature & encryption block types, signing-config
plumbing (thread-local + DID store), Merkle batch-root, and IPLD traversal. Wire-compat
mirror of Go's `internal/core/`.

## State machines
- `DocumentStatus { Active=1, Deleted=2 }` — value enum, no transition logic here.
- `SigningKeyType` / `SignatureType` — type-tag enums, no lifecycle.
- Signing-config resolution (`resolve_signing_config_with_flag`) and the global
  DID→SigningConfig / DID→bearer-token stores are runtime plumbing (no protocol state).
- No explicit lifecycle/transition machine in this crate. Block creation is a pure
  function; CRDT *merge* state machines live in `crates/crdt` + `db-merge`, not here.

## Candidates

| Name | Kind | Property | Already-modeled | Priority |
|---|---|---|---|---|
| CID content-addressing determinism | Lean | `block == block` ⇒ canonical DAG-CBOR bytes equal ⇒ same CID; distinct canonical content ⇒ distinct CID (injectivity of generate_cid over canonical encoding) | no | medium |
| Block.new canonicalization | Lean | `Block::new` is idempotent & order-insensitive: sorting heads/links + empty→None yields a unique normal form, so CID is independent of input link ordering | no | medium |
| Merkle batch-root determinism | Lean | `compute_merkle_root` is invariant under input permutation (CIDs sorted) and injective enough that distinct CID sets give distinct roots (collision modulo SHA256) | no | low |
| DocID parse/format round-trip | Lean | `from_string ∘ to_string = id` and `from_bytes ∘ to_bytes = id`; version!=V0 always rejected (uvarint round-trip total) | no | low |
| CRDT delta-payload merge laws | Lean | assoc/comm/idem of LWW & counter merges | yes (Lean lww/counter/composite, src in `crates/crdt`) | low |
| Block integrity before merge | TLA+ | signed block verified before applied | yes (`Integrity.tla`) | low |

## Verdict
**Model-worthy: borderline-yes (low/medium).** The crate is mostly type definitions
and IO/plumbing (signing-config stores, document/collection structs, error/transaction
shells) — those are pure glue, covered by integration + unit tests. The one class with
proof value not yet covered by an existing slice is **content-addressing determinism**:
canonical serialization → CID injectivity and `Block::new` link-ordering canonicalization
underpin the "same content ⇒ same DocID/CID" parity guarantee. CRDT merge laws and block
integrity are already modeled (Lean / `Integrity.tla`); the delta *payload* types here add
nothing new. Recommend a small Lean content-addressing slice only if CID/DocID parity
becomes contested; otherwise existing tests suffice.
