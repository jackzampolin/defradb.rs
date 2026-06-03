# Survey: `crates/blockstore/`

## Purpose

IPFS-compatible IPLD blockstore: a thin public-API layer (`DefraBlockstore`,
`Blockstore` trait) over `storage::stores::Blockstore`. Adds two process-wide
LRU caches (block bytes; merged-CID positives), optional hash-on-read
verification, and P2P merge-tracking (unmerged → merged lifecycle). Real
storage/transaction logic lives in the `storage` crate; this crate is content
addressing + caching + a clean trait surface.

## State machines

- **Merge-tracking lifecycle (implicit):** a P2P-mode block is `put` as
  *unmerged*, surfaced via `get_unmerged`, then `mark_as_merged` / `is_merged`.
  Local mode treats every block as merged. This is the only stateful protocol
  here, and it is the storage-layer half of the P2P sync state machine already
  modeled at the protocol level.
- **hash-on-read flag:** an `AtomicBool` toggle (verify vs. trust). Two states,
  no transitions worth a spec.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| CID verification soundness | Lean | `verify_block_cid(cid,data)=Ok ⟺ sha256(data)=cid.digest ∧ code=SHA2-256`; tampered data / non-SHA256 always rejected | partial — Integrity slice abstracts the "verify before merge" gate (crypto boundary assumed) | low |
| merge-tracking lifecycle | TLA+ | merged is monotone; a block read as merged stays merged; unmerged set = stored ∖ merged | yes — Convergence / Commits / Integrity slices | low |
| cache ↔ storage coherence | TLA+ | LRU caches never return a value diverging from committed storage (no stale-positive after delete/merge) | no | low |

## Verdict

**Plumbing, not model-worthy.** The merge lifecycle's correctness is a
protocol concern already covered by the Convergence, Commits, and Integrity
slices; here it is just txn-guarded flag flips. `verify_block_cid` is a clean
content-addressing soundness statement but reduces to the EUF-CMA / hash
crypto boundary the Integrity slice already assumes, and unit tests cover the
three branches directly. The cache-coherence question (could a stale LRU
positive outlive a delete/eviction?) is the only genuinely unmodeled behavior,
but caches are populated only after commit and evicted on `delete`, so it is
single-process plumbing best left to integration tests. No new slice
recommended; `model_worthy: false`.
