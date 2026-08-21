# db-blocks — formal-modelability survey

> **Now a module, not a crate.** `db-blocks` was folded into `db` as `crates/db/src/block/builder/`. The slice below is unchanged; only its location moved.

## Purpose
IPLD block *builder* for document mutations. Serializes a `Document` mutation into
CRDT delta blocks (LWW / Counter field blocks + a Composite root), content-addresses
each via DAG-CBOR -> CID, optionally encrypts field deltas and signs blocks, then
writes blocks to blockstore and updates per-field heads in headstore. Matches Go's
`ProcessBlock -> updateHeads` flow for wire compatibility. This is the *write/produce*
side of the CRDT DAG; the *merge/consume* side lives in `db-merge` + `crdt`.

## State machines
- **Head update protocol (implicit):** per field, `DocHeadsSnapshot::load` scans
  `/d/{doc}/`, computes `priority = max_priority + 1`, deletes prior heads, writes new
  head + priority index. Composite status: 1 (live) / 2 (delete). This is the producer
  half of the DAG-head state machine — but it runs inside a single storage transaction,
  not concurrently; concurrent branch reconciliation happens on the merge side.
- No explicit lifecycle/status enums with multi-step transitions beyond the above.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| CID content-addressing determinism | Lean/either | same document content -> same CID (byte-identical DAG-CBOR) | partial — convergence/integrity slices assume it; not a standalone proof | low |
| priority monotonicity | none/Lean | each update's priority = max prior head priority + 1 (strictly increasing per doc) | implied by Convergence DAG model | low |
| deterministic head ordering | none | heads sorted by CID string match Go ordering | covered by FFI parity / integration tests | low |

## Verdict
**Not independently model-worthy.** This crate is deterministic, single-transaction
construction/serialization plumbing. Its correctness is "produces the same bytes/CIDs/
heads as Go," which is exactly what FFI parity and integration tests validate. The
algebraic content (CID determinism, priority monotonicity, head ordering) is the
*construction* counterpart of properties already covered by the existing Lean CRDT-laws
slice (LWW/Counter/Composite merge) and the TLA+ Convergence/Integrity slices, which
assume well-formed deterministically-built blocks as their input. No concurrency,
adversary, or eventual-consistency state machine lives here that those slices don't
already cover. model_worthy: false.
