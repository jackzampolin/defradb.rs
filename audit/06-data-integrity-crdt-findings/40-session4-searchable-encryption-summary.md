# Session 4 Summary: Searchable Encryption Deep-Dive

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 of 6
**Date**: 2026-02-21
**Focus**: SE tag computation, key management, artifact storage, P2P distribution, query integration

## Scope

Audited the searchable encryption (SE) subsystem end-to-end:
- Tag computation (`crates/crypto/src/se/tag.rs`) — HMAC-SHA256 construction
- Artifact generation (`crates/db/src/se/artifact_gen.rs`) — document → artifacts
- SE coordinator (`crates/db/src/se/coordinator.rs`) — key + identity management
- Artifact storage (`crates/db/src/se/storage.rs`) — store/query artifacts
- P2P distribution (`crates/p2p/src/message/se.rs`, `two_stream/runner.rs`) — push/receive
- Push path (`crates/db/src/push_docs.rs`) — replicator push integration
- Query integration (`crates/query/src/`) — planner/runner SE support
- Merge path (`crates/db/src/merge_handler/`) — SE on replication
- Integration tests (`encrypted_index.rs`, `encrypted_acp.rs`)

## Files Audited

| File | Lines | Status |
|------|-------|--------|
| `crates/crypto/src/se/tag.rs` | 206 | Fully audited |
| `crates/crypto/src/se/artifact.rs` | 223 | Fully audited |
| `crates/crypto/src/se/mod.rs` | 64 | Fully audited |
| `crates/crypto/tests/go_compat_se.rs` | 300 | Fully audited |
| `crates/db/src/se/coordinator.rs` | 242 | Fully audited |
| `crates/db/src/se/artifact_gen.rs` | 273 | Fully audited |
| `crates/db/src/se/storage.rs` | 178 | Fully audited |
| `crates/db/src/se/mod.rs` | 36 | Fully audited |
| `crates/db/src/push_docs.rs` | 435 | SE-relevant portions |
| `crates/p2p/src/message/se.rs` | 456 | Fully audited |
| `crates/p2p/src/two_stream/runner.rs` | 216 | SE handler sections |
| `crates/p2p/src/two_stream/handler/branchable_se.rs` | 113 | Fully audited |
| `crates/storage/src/keys/datastore/misc.rs` | 115 | DatastoreSE key |
| `crates/keyring/src/lib.rs` | 96 | SE key constant |
| `crates/ffi/src/se_key.rs` | 46 | SE key FFI |
| `tools/integration-test/tests/encrypted_index.rs` | 88 | Fully audited |
| `tools/integration-test/tests/encrypted_acp.rs` | 151 | Fully audited |

## Findings

### New Findings (This Session)

| # | Title | Severity | Category |
|---|-------|----------|----------|
| 31 | SE tag computation sound for equality search | GREEN | Construction verified |
| 32 | Push docs creates coordinator without identity — tags not isolated | MEDIUM | Identity isolation |
| 33 | Artifact storage key reveals document-tag associations | MEDIUM | Storage leakage |
| 34 | SE receiver not implemented — artifacts silently discarded | HIGH | P2P integration |
| 35 | No SE artifact validation on P2P receive path | MEDIUM | P2P security |
| 36 | SE enc_key stored as plain Vec<u8> — no zeroization | MEDIUM | Key lifecycle |
| 37 | SE query evaluation not integrated into Rust planner/runner | HIGH | Query integration |
| 38 | Replicator query leakage — complete access pattern visibility | INFORMATIONAL | Privacy analysis |
| 39 | Merge handler does not generate SE artifacts for replicated documents | MEDIUM | Replication |

### Cross-Referenced Findings (From Stream 1)

| # | Title | Severity | Status |
|---|-------|----------|--------|
| 01-10 | SE tag UTF-8 lossy conversion diverges from Go | HIGH | 1.0 blocker |
| 01-15 | SE domain separator delimiter collision | LOW-MEDIUM | Known limitation |
| 01-16 | SE enc_key not zeroized and default all-zeros | MEDIUM | Confirmed (= Finding 36) |
| 01-17 | SE deterministic tags enable frequency analysis | INFORMATIONAL | Acknowledged |
| 01-18 | SE artifact metadata leakage to replicators | MEDIUM | Design trade-off |
| 01-19 | SE HMAC key accepts any length without validation | LOW-MEDIUM | Defense in depth |

## Security Checklist Results

| Check | Result |
|-------|--------|
| 1. Tag computation scheme | SOUND — HMAC-SHA256 with proper domain separation |
| 2. Frequency analysis resistance | NO — deterministic tags, by design |
| 3. Tag isolation between identities | BROKEN — push_docs doesn't pass identity (Finding 32) |
| 4. SE key management | WEAK — no zeroization, all-zeros default (Finding 36) |
| 5. IND-CPA security | NO — deterministic scheme, by design |
| 6. Artifact storage leakage | YES — full metadata visible (Finding 33) |
| 7. P2P SE artifact distribution | INCOMPLETE — receiver not implemented (Finding 34) |
| 8. Query evaluation with SE | NOT IMPLEMENTED — no planner/runner integration (Finding 37) |
| 9. Replicator query leakage | COMPLETE VISIBILITY — by design (Finding 38) |
| 10. Field name leakage | YES — index_id = field name in plaintext |

## Critical Path Items for 1.0

1. **Fix UTF-8 lossy conversion** (Finding 01-10) — SE tags incompatible with Go without this
2. **Implement SE receiver** (Finding 34) — Rust replicators cannot serve SE queries
3. **Integrate SE into query planner/runner** (Finding 37) — SE queries non-functional
4. **Pass identity to coordinator in push_docs** (Finding 32) — tag isolation broken

## Architecture Assessment

The SE subsystem has a clean separation of concerns:
- `crypto::se` — pure cryptographic primitives (sound)
- `db::se` — coordination, artifact generation, storage (API complete, partially wired up)
- `p2p::message::se` — wire format (complete)
- `p2p::two_stream` — transport (send works, receive stub only)
- `query` — NOT integrated

The primitives are correct but the end-to-end pipeline is incomplete. The send path (Rust → Go replicator) works; the receive path (Rust as replicator) and query path (Rust evaluating SE queries) are not yet implemented.
