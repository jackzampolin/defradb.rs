# Survey: `crates/storage/`

## Purpose
The complete storage layer: a `corekv` KV abstraction (Reader/Writer/Txn/Store traits)
over four backends (memory, redb, fjall, rocksdb), six namespaced stores (datastore,
blockstore, headstore, systemstore, peerstore, encstore) atop a rootstore, an
order-preserving value encoding (CockroachDB-style) for secondary indexes, secondary
index iterators (simple/unique/fulltext/range/eq), and an at-rest `EncryptedStore`
value-encryption wrapper.

## State machines
- **SSI conflict detection (`backends/shared.rs` `ConflictTracker`)**: the real one. Each
  txn opens at a snapshot `version`; on commit it scans every txn committed after its
  snapshot and aborts (`TxnConflict`) if a committed write hit a key/range it read, or a
  committed read hit a key it wrote. Monotonic version counter + committed read/write-set
  log. This is the Rust reimplementation of Go badger's serializable-snapshot-isolation
  behavior across concurrently committing transactions. Has a documented carve-out
  (`is_document_collection_scan_prefix`) that suppresses conflicts for full-collection
  document scans — a soundness-relevant heuristic.
- **Txn lifecycle**: active -> committed/discarded, ownership-enforced (commit/discard
  consume self); single-txn, not adversarial. (Same lifecycle surveyed in `datastore.md`.)
- **EncryptedStore auth (implicit)**: value bound to its key via AES-GCM AAD; relocated or
  wrong-key reads must fail loudly, never return silent garbage.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| SSI snapshot-isolation correctness | TLA+ | concurrently committing txns: a committed schedule is serializable; no lost update / write-skew survives commit (every accepted commit has no read/write overlap with a txn committed after its snapshot) | no | high |
| SSI collection-scan carve-out soundness | TLA+ | the `d/d/`,`/d/` scan-prefix conflict suppression never drops a *true* write-skew conflict (only unrelated-insert false positives) | no | medium |
| Order-preserving encoding monotonicity | Lean | for each type, `a < b ⇒ encode_asc(a) <lex encode_asc(b)` and descending inverts; cross-type marker ordering total | no (storage proptests exist, no proof) | medium |
| Encoding round-trip | Lean | `decode(encode(x)) == x` and consumes exactly its bytes, all types incl. json/time/float | no (proptests exist) | low |
| EncryptedStore key-binding | TLA+/either | decrypt under wrong key or relocated key always errors, never yields plaintext (AAD soundness) | no | low |

## Verdict
**Model-worthy.** Unlike the `datastore` shim, this crate *owns* transaction isolation
and atomicity. The `ConflictTracker` SSI state machine is a genuine concurrent protocol
with an adversarial interleaving dimension and a hand-rolled carve-out heuristic whose
soundness is not provable by reading code — a high-value TLA+ slice. The order-preserving
encoding is the canonical Lean target (monotonicity + round-trip across a marker-tagged
type lattice); proptests cover examples but not the universal law. Index iterators,
chunking, namespacing, and backend glue are plumbing covered by unit/integration tests.
