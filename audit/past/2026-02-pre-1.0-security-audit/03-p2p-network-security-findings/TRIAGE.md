# P2P Network Security — Triage Report

**Stream**: 03 - P2P Network Security
**Date**: 2026-02-21
**Findings**: 52 individual findings (excluding 3 session summaries)
**Breakdown**: 2 CRITICAL, 8 HIGH, 13 MEDIUM, 10 LOW/INFO, 19 GREEN

---

## 1. Findings Table

Sorted by severity (CRITICAL first, GREEN last).

| # | Severity | Title | Status | One-Line Summary |
|---|----------|-------|--------|------------------|
| 12 | CRITICAL | Two-stream handler accepts messages without signature verification | CONFIRMED | Primary replication path (PushLog/DocSync/BranchableSync) deserializes and processes messages without calling `verify_message()` — any Noise-authenticated peer can forge sender_id |
| 20 | CRITICAL | AccessMode::Controlled is never activated — collection access control is dead code | CONFIRMED | `AccessMode::Open` is hardcoded everywhere; the entire per-collection replicator access control system never executes |
| 00 | HIGH | Two-stream protocol has no message size limit | CONFIRMED | `read_to_end()` without `take()` in 5 code sites allows a peer to send arbitrarily large data, causing OOM |
| 01 | HIGH | No swarm-level connection limits | CONFIRMED | No max inbound/outbound connection limits configured; Go uses 100/400 watermarks |
| 21 | HIGH | DocSync, BranchableSync, and CAR fetch have no access checks | CONFIRMED | Three sync protocols skip `check_access()` entirely — any peer can enumerate documents, collections, and fetch full DAGs |
| 30 | HIGH | Unbounded tokio task spawning per peer | CONFIRMED | Every incoming two-stream opens a `tokio::spawn` with no per-peer or global concurrency limit |
| 31 | HIGH | DocSyncRequest.doc_ids is an unbounded array | CONFIRMED | `Vec<String>` with no length limit triggers O(n) database lookups per request and unbounded response size |
| 42 | HIGH | No per-peer rate limiting | CONFIRMED | Zero rate limiting across all P2P protocols; a peer can send unlimited messages per second |
| 43 | HIGH | No per-peer connection limits (extends Finding 01) | CONFIRMED | No `max_connections_per_peer`, no connection establishment rate limit; extends Finding 01 |
| 44 | HIGH | Two-stream `read_to_end` has no timeout (Slowloris) | CONFIRMED | All 5 `read_to_end` calls run without `tokio::time::timeout`; a peer sending 1 byte/minute ties up tasks forever |
| 02 | MEDIUM | Kademlia hardcoded to Mode::Server instead of ModeAuto | CONFIRMED | Every node unconditionally responds to DHT queries; Go uses `dht.ModeAuto` |
| 03 | MEDIUM | Kademlia MemoryStore loses DHT state on restart — eclipse attack surface | CONFIRMED | Ephemeral routing table creates a post-restart window for eclipse attacks |
| 05 | MEDIUM | Yamux uses all defaults — no max concurrent streams limit | CONFIRMED | `yamux::Config::default()` in all code paths; combined with no connection limits, effective stream count is unbounded |
| 09 | MEDIUM | Identify address flooding — all remote addresses added to Kademlia without limit | CONFIRMED | All `listen_addrs` from Identify added to Kademlia with no count cap or address validation |
| 13 | MEDIUM | Broadcast signing failure silently drops field blocks | CONFIRMED | `is_ok()` pattern on field block signing silently omits blocks with no logging |
| 14 | MEDIUM | PushLogCodec signing/verification is dead code in production | CONFIRMED | Codec registered with zero protocols; creates false security signal for auditors |
| 16 | MEDIUM | serde_cbor `#[serde(flatten)]` produces indefinite-length CBOR maps — signature divergence risk | CONFIRMED | PushLogRequest still uses `#[serde(flatten)]`, which would break cross-Go signature verification if enabled |
| 23 | MEDIUM | GossipSub access check uses relay peer, not message originator | CONFIRMED | `propagation_source` (relay) is checked instead of message publisher; authorized relay passes unauthorized messages |
| 32 | MEDIUM | Pending DAGs HashMap has unbounded growth | CONFIRMED | `pending_dags` map grows monotonically with no eviction, TTL, or capacity limit |
| 33 | MEDIUM | DAG fetcher spawns unbounded concurrent tasks per reply | CONFIRMED | N head CIDs in a DocSync/BranchableSync reply spawn N long-lived fetcher tasks with no concurrency limit |
| 35 | MEDIUM | CAR response blocks stored without origin verification | CONFIRMED | Decoded CAR blocks stored via `put_many()` without verifying CID content hashes or root membership |
| 46 | MEDIUM | Channel bounds audit — one unbounded channel found | CONFIRMED | `failure_tx` uses `UnboundedSender`; all other channels are bounded at 256 |
| 47 | MEDIUM | Timeout map — two-stream reads have no timeout | CONFIRMED | Comprehensive timeout audit; main gap is the 5 `read_to_end` sites (cross-ref Finding 44) |
| 49 | MEDIUM | PendingResponses HashMap has no eviction | CONFIRMED | No periodic sweep of stale pending response entries; low practical risk due to existing timeouts |
| 50 | MEDIUM | CAR response collects unbounded DAG from blockstore | CONFIRMED | `collect_dag_blocks` recursively collects entire DAG into memory with no block count or size limit |
| 52 | MEDIUM | No global memory budget or per-peer memory tracking | CONFIRMED | No overarching mechanism to cap combined P2P memory usage across all components |
| 22 | MEDIUM | Bitswap serves blocks without collection-level access checks | CONFIRMED | `BitswapStoreAdapter` returns any block by CID to any peer; documented assumption about ingress control is incomplete |
| 04 | LOW | GossipSub flood_publish amplifies to all subscribed peers | CONFIRMED | Locally-published messages sent to ALL peers, not just mesh (D=6); matches Go, appropriate for small networks |
| 07 | LOW | Identify protocol leaks exact build version to all peers | CONFIRMED | `defradb-rs/{version}` announced via Identify; standard practice, enables targeted version scanning |
| 15 | LOW | No message replay protection | CONFIRMED | UUID v4 message_id but no timestamp/nonce/expiration; mitigated by idempotent block storage |
| 17 | LOW | GossipSub messages skip application-level signature verification | CONFIRMED | Application-layer signature not checked on GossipSub path; mitigated by transport-level `MessageAuthenticity::Signed` |
| 19 | LOW | Two-stream response signing failure sends unsigned reply | CONFIRMED | Access-denied and invalid-CID error paths send unsigned replies on signing failure; inconsistent with main path |
| 24 | LOW | GossipSub topic names leak collection IDs to mesh peers | CONFIRMED | Raw collection ID used as topic name; any mesh peer can observe subscriptions |
| 34 | LOW | CBOR triple-try deserialization amplifies large message cost | CONFIRMED | Buffer parsed up to 3 times sequentially; constant-factor amplifier, only matters without size limits |
| 36 | LOW | Bounded channels create backpressure-induced memory accumulation | CONFIRMED | Blocked senders hold allocated memory; consequence of Findings 00/30, not independently exploitable |
| 45 | LOW | GossipSub uses default mesh parameters | CONFIRMED | All libp2p defaults; reasonable for small networks, should be documented |
| 51 | LOW | Yamux default max concurrent streams = 256 | CONFIRMED | Extends Finding 05; 256 streams/connection is reasonable but multiplied by unlimited connections |
| 54 | LOW | DagSyncConfig default has unlimited depth | CONFIRMED | `max_depth: None` but mitigated by poll-based fetcher's 20-iteration cap |
| 06 | GREEN | Noise protocol is mandatory — no downgrade path | CONFIRMED | Noise XX with Ed25519 is sole transport; no plaintext fallback; correct |
| 08 | GREEN | TCP port reuse is safe due to Noise authentication | CONFIRMED | Port reuse does not enable hijacking; required for address reporting |
| 10 | GREEN | GossipSub ValidationMode::Strict and SHA256 message IDs correct | CONFIRMED | Strict validation, signed messages, content-addressed dedup; all correct |
| 11 | GREEN | No hardcoded bootstrap peers — all user-configurable | CONFIRMED | No embedded DNS seeds or default peers; correct for configured networks |
| 18 | GREEN | Core sign_message/verify_message logic is sound | CONFIRMED | 4-point verification complete, error handling strict, all 13 tests pass |
| 25 | GREEN | Replicator management is admin-only — no self-registration | CONFIRMED | No P2P message type for replicator self-registration; correct separation of control/data planes |
| 26 | GREEN | PushLog access check ordering is correct | CONFIRMED | Access check before CID parsing; no information leakage on denial |
| 27 | GREEN | Collection ID matching is exact — no wildcards or inheritance | CONFIRMED | HashMap exact-match lookup; no ambiguity in authorization |
| 28 | GREEN | Registry operations are atomic (RwLock-protected) | CONFIRMED | `parking_lot::RwLock` ensures no TOCTOU within the registry |
| 37 | GREEN | DAG fetch depth correctly capped at 20 iterations | CONFIRMED | Hard iteration cap with iterative (not recursive) link traversal |
| 38 | GREEN | CID parsing errors handled gracefully — no panics | CONFIRMED | All CID parse sites use `try_from`/`read_bytes` with proper error handling |
| 39 | GREEN | PushLog handler always sends response — no peer left hanging | CONFIRMED | All code paths (access denied, invalid CID, error, success) send explicit responses |
| 40 | GREEN | Bitswap retry logic is bounded — no infinite loop | CONFIRMED | Per-block timeouts, 20-iteration cap, completion tracking prevent infinite retries |
| 48 | GREEN | PeerStateTracker has proper memory bounds | CONFIRMED | Three-level memory bounding with LRU eviction; model implementation |
| 53 | GREEN | Replication loop has proper concurrency control | CONFIRMED | Semaphore-based worker pool (32 workers); correct pattern for merge processing |

---

## 2. Themes

### Theme A: Unbounded Resource Consumption (The Core Problem)

**Findings: 00, 01, 05, 30, 31, 33, 42, 43, 44, 50, 52**

The single largest category. The P2P stack has almost no admission control at the network boundary. An attacker who completes a Noise handshake (trivial -- generate an Ed25519 keypair) gains access to:
- Unlimited connections (01, 43)
- Unlimited streams per connection (05, 51)
- Unlimited message sizes (00)
- Unlimited concurrent tasks (30, 33)
- Unlimited request array sizes (31)
- No read timeouts (44)
- No rate limiting (42)
- No global memory budget (52)

These findings form a **kill chain**: unlimited connections x unlimited streams x unlimited message size x no timeouts = straightforward OOM crash from a single attacker.

### Theme B: Dead or Bypassed Access Control

**Findings: 12, 14, 20, 21, 22, 23**

The access control system exists in code but is architecturally disconnected from production message flow:
- `AccessMode::Controlled` is never activated (20)
- Three of six sync protocols skip access checks entirely (21)
- Signature verification exists but is never called (12, 14)
- GossipSub checks the relay peer instead of the originator (23)
- Bitswap serves any block to any peer (22)

This means the P2P layer currently operates as a fully open system -- any connected peer can read and write all data through at least one protocol path.

### Theme C: Protocol-Level Data Integrity Gaps

**Findings: 12, 13, 16, 19, 35**

Messages arrive and are processed without application-level integrity verification:
- No signature verification on the primary replication path (12)
- Silent block drops on signing failure (13)
- CBOR encoding divergence that would break cross-implementation signatures (16)
- Unsigned error responses on signing failure (19)
- CAR blocks stored without CID content verification (35)

### Theme D: Unbounded Internal Data Structures

**Findings: 32, 46, 49, 50**

Several internal HashMaps and channels grow without bound:
- `pending_dags` has no eviction (32)
- `failure_tx` is an unbounded channel (46)
- `PendingResponses` has no periodic sweep (49)
- CAR response collection has no size limit (50)

These are slow-burn memory leaks rather than immediate crashes, but under sustained load they compound with Theme A.

### Theme E: Kademlia / DHT Hardening

**Findings: 02, 03, 09**

The Kademlia configuration diverges from Go in ways that increase attack surface:
- Server mode instead of auto-detect (02)
- Ephemeral routing table (03)
- No limit on addresses per peer from Identify (09)

These enable DHT amplification and eclipse attacks but are lower severity because DefraDB doesn't rely heavily on DHT for its primary replication path.

### Theme F: Information Disclosure (Acceptable)

**Findings: 07, 24**

Minor information leakage through standard libp2p protocols. Version strings and topic names are visible to mesh peers. Both match Go behavior and are inherent to the protocol design.

---

## 3. Actionable vs Informational

### Must Fix (1.0 Blockers)

These are confirmed vulnerabilities that allow a single malicious peer to crash or compromise a production node.

| # | Title | Why It Blocks 1.0 |
|---|-------|--------------------|
| 12 | Two-stream no signature verification | Any peer can forge replication messages on the primary data path |
| 20 | AccessMode::Controlled never activated | Per-collection access control is entirely dead code |
| 00 | Two-stream no message size limit | Single peer can OOM the node via `read_to_end` |
| 44 | Two-stream read_to_end no timeout | Slowloris ties up all tokio tasks indefinitely |
| 01/43 | No connection limits | Unlimited connections exhaust file descriptors and memory |
| 30 | Unbounded task spawning | Unlimited concurrent tasks amplify all other DoS vectors |
| 21 | DocSync/BranchableSync/CAR no access checks | Any peer can enumerate all documents and fetch full DAGs |
| 42 | No per-peer rate limiting | No throttling on any protocol; enables event loop saturation |

### Should Fix (Pre-1.0)

These have real exploit potential but require specific conditions or have partial mitigations.

| # | Title | Why Pre-1.0 |
|---|-------|-------------|
| 31 | DocSyncRequest.doc_ids unbounded array | Amplification attack via O(n) database lookups |
| 05/51 | Yamux no stream limits | Multiplier for connection-based attacks |
| 35 | CAR response no origin verification | Arbitrary block injection into blockstore |
| 33 | DAG fetcher unbounded task fan-out | Single reply can spawn thousands of long-lived tasks |
| 32 | Pending DAGs unbounded growth | Slow memory leak under sustained attack |
| 50 | CAR response unbounded DAG collection | Memory exhaustion on outbound CAR serving |
| 16 | serde_cbor flatten CBOR divergence | Will break cross-Go signatures when verification is enabled |
| 14 | PushLogCodec signing is dead code | False security signal; should be removed or activated |
| 23 | GossipSub checks relay not originator | Authorization bypass via authorized relay |
| 09 | Identify address flooding to Kademlia | Routing table bloat from a single malicious peer |
| 52 | No global memory budget | No defense-in-depth against combined resource exhaustion |
| 46 | Unbounded failure_tx channel | Slow memory leak under replication errors |
| 22 | Bitswap no collection access checks | Cross-collection information disclosure via CID guessing |
| 47 | Timeout map gaps | Documents the two-stream timeout gap (cross-ref 44) |

### Accept Risk / Backlog

These are design trade-offs, Go-parity items, or low-impact findings.

| # | Title | Rationale |
|---|-------|-----------|
| 02 | Kademlia Mode::Server | DHT not critical path; matches many libp2p deployments |
| 03 | Kademlia MemoryStore eclipse surface | Mitigated by configured bootstrap peers in practice |
| 04 | GossipSub flood_publish | Go parity; appropriate for small networks |
| 07 | Identify version leakage | Standard practice; defense-in-depth only |
| 13 | Broadcast signing failure silent drop | Ed25519 signing failure is extremely unlikely |
| 15 | No message replay protection | Idempotent block storage makes replay harmless |
| 17 | GossipSub no app-level signature check | Transport-level signing provides equivalent protection |
| 19 | Two-stream unsigned error response | Error paths only; receivers don't verify anyway |
| 24 | GossipSub topic name leakage | Inherent to pubsub design; collection IDs are opaque hashes |
| 34 | CBOR triple-try deserialization | 3x constant factor; irrelevant once size limits are added |
| 36 | Channel backpressure memory | Consequence of 00/30; not independently exploitable |
| 45 | GossipSub default mesh parameters | Reasonable defaults; document for operators |
| 49 | PendingResponses no eviction | Low practical risk; timeout cleanup works correctly |
| 54 | DagSyncConfig unlimited depth | Mitigated by 20-iteration cap in poll fetcher |

### No Action (GREEN)

Confirmed safe. These document that the audit checked these areas and found them correct.

| # | Title |
|---|-------|
| 06 | Noise mandatory, no downgrade |
| 08 | TCP port reuse safe with Noise |
| 10 | GossipSub strict validation + SHA256 IDs |
| 11 | No hardcoded bootstrap peers |
| 18 | sign_message/verify_message logic sound |
| 25 | Replicator management admin-only |
| 26 | PushLog access check ordering correct |
| 27 | Collection ID matching exact, no wildcards |
| 28 | Registry RwLock atomic, no TOCTOU |
| 37 | DAG depth capped at 20 |
| 38 | CID parsing graceful error handling |
| 39 | PushLog response always sent |
| 40 | Bitswap retry bounded, no infinite loop |
| 48 | PeerStateTracker proper memory bounds |
| 53 | Replication loop semaphore concurrency control |

---

## 4. Recommended Fix Order

The fixes are ordered to maximize security improvement per unit of effort, with earlier fixes often being prerequisites for later ones.

### Phase 1: Stop the Bleeding (Week 1)

These are small, surgical changes that close the most dangerous attack vectors.

**1. Add message size limits to two-stream `read_to_end` (Finding 00)**
- 5 call sites, same 1-line fix each: `stream.take(MAX_MESSAGE_SIZE).read_to_end(&mut buf)`
- Closes the OOM vector that amplifies every other finding
- Effort: ~1 hour

**2. Add read timeouts to two-stream `read_to_end` (Finding 44)**
- Wrap each `read_to_end` in `tokio::time::timeout(Duration::from_secs(30), ...)`
- Closes the Slowloris vector
- Effort: ~1 hour

**3. Add swarm connection limits (Findings 01, 43)**
- 3 lines in `mod.rs`: `.with_max_established_per_peer(2).with_max_established_incoming(400).with_max_pending_incoming(128)`
- Caps the connection multiplier for all other attacks
- Effort: ~30 minutes

**4. Add task spawning concurrency limit (Finding 30)**
- Add a `tokio::sync::Semaphore` (e.g., 64 permits) to the two-stream runner
- Caps concurrent inbound message processing
- Effort: ~2 hours

### Phase 2: Authentication and Authorization (Week 2)

These require more design thought but are necessary for security correctness.

**5. Add signature verification to two-stream handler (Finding 12)**
- Call `verify_message()` after deserialization in `handle_request_stream` and `handle_response_stream`
- Override `sender_id` with transport peer ID (matching Go)
- Effort: ~4 hours (including test updates)

**6. Activate AccessMode::Controlled (Finding 20)**
- Add a mechanism (config flag or auto-detect when ACP is configured) to switch from Open to Controlled
- Effort: ~4 hours

**7. Add access checks to DocSync, BranchableSync, and CAR handlers (Finding 21)**
- Add `check_access()` calls at the start of each handler
- For CAR: add `collection_id` to request message or implement CID-to-collection lookup
- Effort: ~8 hours (CAR requires message format change)

### Phase 3: Input Validation and Bounds (Week 3)

Harden message processing against malicious input.

**8. Validate DocSyncRequest.doc_ids length (Finding 31)**
- Add `MAX_DOC_IDS` constant, reject requests exceeding it
- Effort: ~1 hour

**9. Fix serde_cbor flatten on PushLogRequest (Finding 16)**
- Duplicate MetaData fields (same fix already applied to PushLogReply)
- Effort: ~2 hours

**10. Add DAG fetcher concurrency limit (Finding 33)**
- Use `Semaphore` or `JoinSet` to cap concurrent `poll_fetch_dag` tasks at 16
- Effort: ~2 hours

**11. Add pending_dags eviction (Finding 32)**
- Add TTL (5 min) and capacity limit (1000) with periodic cleanup
- Effort: ~4 hours

**12. Add CAR response size limits (Finding 50)**
- Cap `collect_dag_blocks` at 1000 blocks or 16MB total
- Effort: ~2 hours

**13. Verify CID content hashes in CAR response handler (Finding 35)**
- Check `hash(data) == cid` for each decoded block before `put_many()`
- Effort: ~2 hours

### Phase 4: Defense in Depth (Week 4+)

Polish and hardening for production readiness.

**14. Add per-peer rate limiting (Finding 42)**
- Token bucket rate limiter (e.g., `governor` crate) at the event dispatch layer
- Effort: ~8 hours

**15. Cap Identify addresses per peer (Finding 09)**
- `info.listen_addrs.iter().take(MAX_IDENTIFY_ADDRS)` — 1 line
- Effort: ~30 minutes

**16. Fix GossipSub originator check (Finding 23)**
- Use `message.source` instead of `propagation_source` for access check (requires Finding 12 first for signature verification)
- Effort: ~4 hours

**17. Replace unbounded failure_tx channel (Finding 46)**
- Switch to bounded `mpsc::Sender` with capacity 1024
- Effort: ~1 hour

**18. Remove or document dead PushLogCodec signing code (Finding 14)**
- Either remove keypair from PushLogCodec or add a comment explaining it is reserved for future use
- Effort: ~1 hour

**19. Add Kademlia Mode::Auto (Finding 02)**
- Change `Mode::Server` to `Mode::Auto` (or make configurable)
- Effort: ~30 minutes

**20. Add global memory monitoring (Finding 52)**
- Integrate jemalloc stats or process memory tracking; add Prometheus metrics
- Effort: ~8 hours

---

## Summary

The P2P stack has strong foundations (Noise encryption, GossipSub strict validation, sound signing logic, good CID error handling) but lacks the outer defensive shell needed for production. The primary risk is that **any peer completing a Noise handshake has near-unlimited access to node resources and data**. The recommended fix order prioritizes closing the OOM/Slowloris kill chain first (Phase 1, ~1 day of work), then authentication gaps (Phase 2, ~2 days), then input validation (Phase 3, ~2 days), and finally defense-in-depth (Phase 4, ongoing).

Phases 1-3 should be completed before 1.0. Phase 4 items are important for production hardening but acceptable as fast-follow.
