# Session 3 Summary: Block Integrity & CID Determinism

## Scope

Deep audit of block integrity verification, CID computation determinism, and verification gaps across the blockstore, P2P ingestion, and backup/restore paths.

## Findings

| # | Severity | Title | Status |
|---|----------|-------|--------|
| 23 | Medium | [No CID verification on put()](23-no-cid-verification-on-put.md) | Blockstore stores data without verifying hash matches CID |
| 24 | Medium | [Unsupported hash algorithm bypass](24-unsupported-hash-algorithm-bypass.md) | Non-SHA2-256 CIDs skip ALL verification |
| 25 | Informational | [CID determinism: dual CBOR codecs verified](25-cid-determinism-dual-cbor-verified.md) | Both ciborium and serde_ipld_dagcbor produce deterministic output |
| 26 | Informational | [Time encoding Go-compatible](26-time-encoding-rfc3339-go-compatible.md) | RFC3339Nano format matches Go, deterministic |
| 27 | Low | [Backup no block-level integrity](27-backup-no-block-level-integrity.md) | Import uses GraphQL (safe), but backup file has no checksum |
| 28 | Informational | [Block CID from serialized bytes](28-block-construction-cid-from-serialized-bytes.md) | CID computation is correct and atomic |
| 29 | Medium | [PushLog no CID verification](29-p2p-pushlog-no-cid-verification-before-storage.md) | P2P blocks stored without hash verification |

Plus from earlier sessions:
| 18 | Medium | [Block CID not verified before merge](18-block-cid-not-verified-before-merge.md) | hash_on_read disabled by default |
| 20 | Informational | [Field iteration order deterministic](20-field-iteration-order-deterministic.md) | Sorted by CID, CRDT ops commutative |

## Security Checklist Results

| Check | Result | Finding |
|-------|--------|---------|
| hash_on_read default state | **FAIL** — disabled by default (`AtomicBool::new(false)`) | 18 |
| Hash verification failure handling | **PASS** — returns `Error::HashMismatch` (hard error) when enabled | — |
| Unsupported hash algorithm handling | **FAIL** — verification silently skipped, `return Ok(())` | 24 |
| LRU cache integrity | **PASS** — cache bypassed when hash_on_read enabled | — |
| CID computation determinism | **PASS** — canonical CBOR ordering, Go-compatible float encoding | 25 |
| Sorted heads/links | **PASS** — `Block::new()` sorts by CID bytes | 20 |
| DocID namespace stability | **PASS** — `SDN_NAMESPACE_V0` hardcoded as constant | 25 |
| Block construction integrity | **PASS** — CID computed from serialized bytes atomically | 28 |
| P2P block verification at ingestion | **FAIL** — PushLog and CAR store blocks without CID verification | 29, 23 |
| Backup/restore integrity | **PARTIAL** — documents regenerated (safe), but backup file not checksummed | 27 |

## Architecture Analysis

### Block Integrity Defense Layers

```
Layer 1: P2P Ingestion (PushLog, CAR)
    ├── CID verification on receive?        NO  ← Finding 29
    ├── Hash algorithm validation?           NO  ← Finding 24
    └── Signature verification?              NO  ← ACP Finding 18

Layer 2: Blockstore (put/get)
    ├── CID verification on put()?           NO  ← Finding 23
    ├── CID verification on get()?           OPTIONAL (hash_on_read disabled by default) ← Finding 18
    └── Cache integrity?                     PASS (cache bypassed when hash_on_read enabled)

Layer 3: Merge Handler
    ├── CID re-verification before decode?   NO  ← Finding 18
    └── Block signature verification?        NO  ← ACP Finding 18

Layer 4: Bitswap (pull-based)
    └── CID verification?                    YES (iroh-bitswap verifies)
```

The only layer that currently verifies CIDs is Bitswap (pull-based), which is controlled by the requesting node. All push-based paths (PushLog, CAR) and the blockstore itself lack verification.

### CID Determinism — Fully Sound

The CID computation chain is deterministic and Go-compatible:
1. Document CBOR: canonical key ordering + shortest float encoding ✓
2. Block DAG-CBOR: `serde_ipld_dagcbor` deterministic encoding ✓
3. Head/link sorting: lexicographic by CID bytes ✓
4. Hash: SHA2-256 via `sha2` crate ✓
5. CID: `Cid::new_v1(DAG_CBOR_CODEC, multihash)` ✓
6. DocID: UUID v5 with stable namespace ✓
7. Time encoding: RFC3339 nano, Go-compatible ✓

No CID divergence risk identified between Rust and Go nodes.

## Recommended Priority

1. **Add CID verification on PushLog ingestion** (Finding 29) — highest impact, blocks the primary P2P attack vector
2. **Reject unsupported hash algorithms** (Finding 24) — prevents verification bypass
3. **Enable hash_on_read for P2P blockstores** (Finding 18) — defense-in-depth
4. **Add backup file checksum** (Finding 27) — low urgency

## Files Audited

- `crates/blockstore/src/lib.rs` (full — 443 lines)
- `crates/blockstore/src/traits.rs` (full — 139 lines)
- `crates/blockstore/src/error.rs` (full — 31 lines)
- `crates/document/src/encoding.rs` (full — 740 lines)
- `crates/document/src/doc_id.rs` (full — 235 lines)
- `crates/document/src/document.rs` (full — 623 lines)
- `crates/document/src/field.rs` (full — 152 lines)
- `crates/document/src/value.rs` (full — 608 lines)
- `crates/document/src/normal.rs` (full — 447 lines)
- `crates/document/src/json_leaf.rs` (full — 158 lines)
- `crates/defra-core/src/block.rs` (full — 729 lines)
- `crates/defra-core/src/ipld/cid_convert.rs` (full — 44 lines)
- `crates/crdt/src/lib.rs` (full — 45 lines)
- `crates/crdt/src/lww.rs` (full — 341 lines)
- `crates/crdt/src/composite.rs` (full — 470 lines)
- `crates/db/src/block_builder/mod.rs` (full — 324 lines)
- `crates/db/src/backup/mod.rs` (full — 148 lines)
- `crates/db/src/backup/import.rs` (full — 204 lines)
- `crates/db/src/dump.rs` (full — 61 lines)
- `crates/p2p/src/sync/manager/process/pushlog.rs` (full — 300 lines)
- `crates/p2p/src/sync/manager/process/bitswap.rs` (full — 167 lines)
- `crates/p2p/src/sync/coordinator/event_handler/car.rs` (full — 82 lines)
- `crates/crypto/src/merkle_proof/proof_node.rs` (full — 75 lines)
