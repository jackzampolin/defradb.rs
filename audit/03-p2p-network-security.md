# Audit Stream 3: P2P Network Security

## Scope

The P2P networking layer built on libp2p. Audit covers:
- Transport encryption configuration (Noise, TLS)
- Peer authentication and identity verification
- Trust boundaries (what can untrusted peers do?)
- Replication protocol security (message validation, ordering)
- Discovery and bootstrap security
- DOS vectors (resource exhaustion, amplification, connection floods)
- Pubsub message validation
- Peer scoring and banning

## Key Questions

- Is transport encryption mandatory and correctly configured?
- Can a peer impersonate another peer?
- Can a malicious peer cause data corruption via replication?
- Are there resource limits on connections, messages, bandwidth?
- Can pubsub be abused for amplification?
- Is the DHT configuration safe against eclipse attacks?
- What happens when a peer sends malformed/oversized messages?

## Crates of Interest

- `p2p/`
- `db/` (replication event handling)
- `crdt/` (merge of replicated data)

## Recon Findings

### Surface Area
- **P2P crate**: 102 Rust files, ~18,049 LOC (core) + 928 LOC (integration tests)
- **Key modules**: host/ (swarm), sync/ (coordinator 3,300+ LOC), bitswap/ (registry 458 LOC), message/, signing.rs

### Transport & Crypto
- TCP with port reuse, **Noise protocol** (Ed25519), **Yamux** muxing
- All messages **cryptographically signed** (UUID v4 ID + peer keypair)
- Peer ID derived from pubkey, verified against sender_id on receive

### Replication Protocol
- **PushLog**: Request-response (CBOR, 30s timeout), protocol `/defradb/rep_req/0.0.1`
- **GossipSub**: Topics: `doc-sync`, `encryption`, `{collection_id}`, `{document_id}`
- **Bitswap**: IPFS-compatible (v1.0-1.2), DAG block sync after PushLog
- **Kademlia DHT**: Server mode, MemoryStore (non-persistent)

### Trust Model
- **ReplicatorRegistry**: Per-collection authorization (collection_id -> Set<PeerId>)
- **AccessMode**: Open (default, no ACP) or Controlled (per-collection checks)
- Access checked at SyncCoordinator level BEFORE blocks stored

### Red Flags
- **HIGH: No per-message size limits** - CBOR deserialization could be expensive
- **HIGH: No per-connection rate limiting** - Single peer can flood messages
- **MEDIUM: GossipSub uses default mesh size** - Potentially large on popular topics
- **MEDIUM: AccessMode::Open is default** - No ACP means all peers can replicate
- **MEDIUM: Memory-only Kademlia** - DHT state lost on restart (eclipse attack surface)
- **LOW: No mDNS** - Not a vulnerability, but limits local discovery
- **LOW: Two-stream protocol is custom** - Requires careful Go compatibility testing

### Green Strengths
- All messages cryptographically signed
- Per-collection fine-grained authorization
- Access checked before storage (prevents unauthorized blocks)
- Timeouts on requests (no indefinite hangs)

### Test Coverage: GOOD (928 LOC across 7 test files)
- Gap: No DOS resistance tests, no message size/rate tests, no pubsub mesh stability tests

## Estimated Scope

**MEDIUM: 3-5 sessions**

### Session 1: Transport Config, LibP2P Setup, Discovery (HIGH) — COMPLETE

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/host/p2p_host/mod.rs` | 182-220 | Noise protocol, Yamux muxing, TCP port reuse |
| `crates/p2p/src/behaviour.rs` | 172-177, 225-230 | Identify protocol, Kademlia DHT (MemoryStore, Mode::Server) |
| `crates/p2p/src/host/p2p_host/swarm.rs` | 28-75 | ConnectionEstablished, Kademlia bootstrap |
| `crates/p2p/src/host/p2p_host/protocols.rs` | 16-64 | Identify event handler, address flooding |

**Checklist**:
- [x] Noise mandatory (no downgrade) — **GREEN** (Finding 06)
- [x] Both SwarmBuilder paths identical noise/yamux — **GREEN** (Finding 06)
- [x] TCP port reuse safe with Noise — **GREEN** (Finding 08)
- [x] No hardcoded bootstrap peers — **GREEN** (Finding 11)
- [x] GossipSub Strict + SHA256 message IDs — **GREEN** (Finding 10)
- [x] Yamux default streams unlimited — **MEDIUM** (Finding 05)
- [x] No swarm connection limits (Go has 100/400) — **HIGH** (Finding 01)
- [x] Kademlia Mode::Server vs Go ModeAuto — **MEDIUM** (Finding 02)
- [x] Kademlia MemoryStore eclipse surface — **MEDIUM** (Finding 03)
- [x] GossipSub flood_publish amplification — **LOW** (Finding 04)
- [x] Identify version leakage — **LOW** (Finding 07)
- [x] Identify address flooding to Kademlia — **MEDIUM** (Finding 09)

**Findings**: 01-11 (5 GREEN, 1 HIGH, 4 MEDIUM, 2 LOW)

### Session 2: Message Signing/Verification (CRITICAL) — COMPLETE

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/signing.rs` | 53-97 (sign), 118-157 (verify) | UUID v4 ID, CBOR serialization, 4-point verification |
| `crates/p2p/src/codec.rs` | 153-242 | PushLogCodec signing integration, optional keypair |
| `crates/p2p/src/two_stream/handler/inbound.rs` | 24-208 | Two-stream request/response handling (NO verification) |
| `crates/p2p/src/sync/coordinator/broadcast.rs` | 92-120 | Signing in broadcast path, silent failure |
| `crates/p2p/tests/signing_tests.rs` | all (13 tests) | Tampered message, wrong sig, pubkey mismatch |
| `crates/p2p/tests/codec_tests.rs` | 14-130 | Codec roundtrip tests |

**Checklist**:
- [x] Signature cleared to empty before serializing (line 73) — **GREEN** (Finding 18)
- [x] CBOR serialization deterministic within Rust — **GREEN** (Finding 18), but **MEDIUM** cross-Go due to `#[serde(flatten)]` (Finding 16)
- [x] Peer ID derived from pubkey, then compared to sender_id — direction correct — **GREEN** (Finding 18)
- [x] sender_id parsing: malformed → `Error::InvalidPeerId`, does not panic — **GREEN** (Finding 18)
- [x] Pubkey decoding: wrong key type → `Error::PublicKeyDecode` — **GREEN** (Finding 18)
- [x] Signature verification: Ed25519 via libp2p (no timing vulnerability) — **GREEN** (Finding 18)
- [x] All 4 verification checks AND'd (sequential `?`) — **GREEN** (Finding 18)
- [x] Error types all map to rejection, not warning — **GREEN** (Finding 18)
- [x] All 13 signing tests validate rejection paths — **GREEN** (Finding 18)
- [x] Two-stream handler has NO verify_message calls — **CRITICAL** (Finding 12)
- [x] PushLogCodec signing is dead code (no protocols registered) — **MEDIUM** (Finding 14)
- [x] GossipSub skips application-level verification (transport-level mitigates) — **LOW** (Finding 17)
- [x] Broadcast field block signing failure silently dropped — **MEDIUM** (Finding 13)
- [x] No message replay protection (UUID v4 only, no timestamp) — **LOW** (Finding 15)
- [x] PushLogRequest uses `#[serde(flatten)]` → indefinite-length CBOR map — **MEDIUM** (Finding 16)
- [x] Error response signing failure sends unsigned reply — **LOW** (Finding 19)

**Findings**: 12-19 (1 GREEN, 1 CRITICAL, 3 MEDIUM, 3 LOW)

### Session 3: Authorization Model, Access Checks (CRITICAL) — COMPLETE

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/sync/coordinator/access.rs` | 22-43 | AccessMode::Open fast-path, per-collection replicator check |
| `crates/p2p/src/bitswap/registry.rs` | 28-64 | Per-collection HashMap authorization |
| `crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` | 25-48, 119-145 | Access check BEFORE CID parsing |
| `crates/p2p/src/sync/coordinator/event_handler/gossip.rs` | 25-34 | GossipSub message drop on denial |
| `crates/p2p/src/bitswap/access.rs` | 1-45 | Bitswap access (no checks - enforced at coordinator) |
| `crates/p2p/src/sync/coordinator/event_handler/doc_sync.rs` | 12-81 | DocSync request/reply — NO access check |
| `crates/p2p/src/sync/coordinator/event_handler/branchable_sync.rs` | 12-73 | BranchableSync request — NO access check |
| `crates/p2p/src/sync/coordinator/event_handler/car.rs` | 13-43 | CAR fetch request — NO access check |
| `crates/p2p/src/bitswap/store.rs` | 57-86 | Bitswap Store trait — serves any block by CID |
| `crates/ffi/src/p2p/node.rs` | 207-217 | Production coordinator construction — hardcoded Open |

**Checklist**:
- [x] AccessMode::Open is DEFAULT — **CRITICAL**: Controlled never activated (Finding 20)
- [x] AccessMode::Open fast-path skips all checks — always taken in production (Finding 20)
- [x] Per-collection replicator check: exact HashMap match, no wildcards — **GREEN** (Finding 27)
- [x] Registry: HashMap-based, no role hierarchy, no ACP integration — correct by design (Finding 27)
- [x] add_replicator: admin-only via HTTP/CLI, no self-registration — **GREEN** (Finding 25)
- [x] remove_replicator: RwLock-atomic, immediate effect — **GREEN** (Finding 28)
- [x] PushLog: check_access BEFORE CID parsing — **GREEN** (Finding 26)
- [x] PushLog: on denial, error response + early return — **GREEN** (Finding 26)
- [x] PushLog two-stream: same access check pattern — **GREEN** (Finding 26)
- [x] GossipSub: uses propagation_source, not message originator — **MEDIUM** (Finding 23)
- [x] GossipSub: topic names leak collection IDs — **LOW** (Finding 24)
- [x] Bitswap: no access checks (by design) — **MEDIUM** (Finding 22)
- [x] DocSync/BranchableSync/CAR: NO access checks at all — **HIGH** (Finding 21)

**Findings**: 20-29 (1 CRITICAL, 1 HIGH, 2 MEDIUM, 1 LOW, 4 GREEN, 1 summary)

### Session 4: Replication Protocol Security (CRITICAL) — COMPLETE

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/codec.rs` | 25-61 | **MAX_MESSAGE_SIZE=16MB**, `reader.take()` enforcement (protected path) |
| `crates/p2p/src/two_stream/handler/inbound.rs` | 33-75, 98-102 | **NO size limit** — `read_to_end` on Vec::new() |
| `crates/p2p/src/two_stream/runner.rs` | 82-207 | 6 `tokio::spawn` per stream type, no concurrency limit |
| `crates/p2p/src/two_stream/handler/car.rs` | 12-61 | CAR stream read + decode, no validation |
| `crates/p2p/src/message/docsync.rs` | 14-22 | `doc_ids: Vec<String>` — unbounded array |
| `crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` | 12-214 | PushLog request flow — CID validation, response always sent |
| `crates/p2p/src/sync/coordinator/event_handler/doc_sync.rs` | 83-170 | DocSync reply — spawns N tasks per N CIDs |
| `crates/p2p/src/sync/coordinator/event_handler/branchable_sync.rs` | 75-155 | BranchableSync reply — same fan-out |
| `crates/p2p/src/sync/coordinator/event_handler/bitswap.rs` | 52-122 | Retry logic — bounded by timeouts |
| `crates/p2p/src/sync/coordinator/dag_fetcher.rs` | 23-141 | DAG depth capped at 20 iterations |
| `crates/p2p/src/sync/manager/process/pending_dag.rs` | 14-206 | Pending DAG map — no eviction |
| `crates/p2p/src/sync/manager/links.rs` | 73-127 | Iterative (not recursive) link traversal |
| `crates/p2p/src/sync/coordinator/event_handler/car.rs` | 46-80 | CAR response — blocks stored without verification |

**Checklist**:
- [x] Finding 00 confirmed: 5 code sites, 7 call paths, no additional `read_to_end` instances
- [x] Unbounded tokio task spawning per peer — **HIGH** (Finding 30)
- [x] DocSyncRequest.doc_ids unbounded array — **HIGH** (Finding 31)
- [x] Pending DAGs unbounded growth — **MEDIUM** (Finding 32)
- [x] DAG fetcher task fan-out — **MEDIUM** (Finding 33)
- [x] CBOR triple-try deserialization — **LOW** (Finding 34)
- [x] CAR response no origin verification — **MEDIUM** (Finding 35)
- [x] Channel backpressure memory accumulation — **LOW** (Finding 36)
- [x] DAG depth capped at 20 — **GREEN** (Finding 37)
- [x] CID parsing graceful errors — **GREEN** (Finding 38)
- [x] PushLog always sends response — **GREEN** (Finding 39)
- [x] Bitswap retry bounded — **GREEN** (Finding 40)

**Findings**: 30-41 (2 HIGH, 3 MEDIUM, 2 LOW, 4 GREEN, 1 summary)

### Session 5: Resource Limits & Edge Cases (MEDIUM) — COMPLETE

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/host/p2p_host/mod.rs` | 35-36, 195-213 | IDLE_CONNECTION_TIMEOUT=60s, yamux defaults, no connection limits |
| `crates/p2p/src/sync/coordinator/mod.rs` | 80-116 | No Semaphore/RateLimiter found in coordinator |
| `crates/p2p/src/behaviour.rs` | 191-223 | GossipSub config (SHA256 msg_id, heartbeat 1s, Strict, default mesh) |
| `crates/p2p/src/sync/peer_state/tracker/mod.rs` | 22-30, 90-101 | Memory bounds: 10K CIDs/peer, 1M total, 1K peers |
| `crates/p2p/src/sync/peer_state/tracker/memory.rs` | 13-84 | Global limit enforcement with LRU eviction |
| `crates/p2p/src/sync/replication/loop_runner.rs` | 136-176 | Semaphore(32) for concurrent merge workers |
| `crates/p2p/src/sync/replication/config.rs` | 5-25 | ReplicationConfig: batch_size=50, max_workers=32 |
| `crates/p2p/src/sync/dag_sync/config.rs` | 10-102 | DagSyncConfig: max_depth=None, max_concurrent=16 |
| `crates/p2p/src/sync/coordinator/event_handler/car.rs` | 13-42 | CAR response: unbounded DAG collection |
| `crates/p2p/src/sync/car.rs` | 72-114 | collect_dag_blocks: recursive, no depth/size limit |
| `crates/p2p/src/two_stream/handler/mod.rs` | 35-39 | PendingResponses: HashMap no eviction |

**Checklist**:
- [x] Per-peer connection limits: **NO** — Finding 43 (HIGH)
- [x] Per-peer rate limiting: **NO** — Finding 42 (HIGH)
- [x] Per-peer concurrent stream limit: yamux 256/conn, unlimited connections — Finding 51 (LOW)
- [x] Yamux max concurrent streams: 256 default, 16MB buffer — Finding 51
- [x] ALL async operations have timeouts: **NO** — 5 `read_to_end` without timeout — Finding 44 (HIGH)
- [x] DAG fetcher depth: capped at 20, breadth uncapped — Finding 54 (LOW)
- [x] GossipSub mesh size: D=6, D_lo=5, D_hi=12 (defaults) — Finding 45 (LOW)
- [x] GossipSub message cache: bounded (5 slots × 64KB) — Finding 45
- [x] Channel bounds: all bounded(256) except `failure_tx` unbounded — Finding 46 (MEDIUM)
- [x] Slowloris: two-stream reads have NO timeout — Finding 44 (HIGH)
- [x] Memory tracking: no per-peer or global budget — Finding 52 (MEDIUM)
- [x] PeerStateTracker memory bounds: **GREEN** — Finding 48
- [x] Replication loop concurrency: Semaphore(32) — **GREEN** — Finding 53

**Findings**: 42-55 (3 HIGH, 4 MEDIUM, 3 LOW, 2 GREEN, 1 summary)

## Completion Status

### All 5 Sessions Complete

| Session | Focus | Findings | CRITICAL | HIGH | MEDIUM | LOW | GREEN |
|---------|-------|----------|----------|------|--------|-----|-------|
| 1 | Transport, Discovery | 00-11 | 0 | 1 | 4 | 2 | 5 |
| 2 | Message Signing | 12-19 | 1 | 0 | 3 | 3 | 1 |
| 3 | Authorization Model | 20-29 | 1 | 1 | 2 | 1 | 4+1 |
| 4 | Replication Protocol | 30-41 | 0 | 2 | 3 | 2 | 4+1 |
| 5 | Resource Limits | 42-55 | 0 | 3 | 4 | 3 | 2+1 |
| **Total** | | **56** | **2** | **7** | **16** | **11** | **16+3** |

### Top Priority Findings (CRITICAL + HIGH)

| # | Finding | Severity | Session |
|---|---------|----------|---------|
| 00 | Two-stream no message size limit | HIGH | Recon |
| 12 | Two-stream no signature verification | CRITICAL | 2 |
| 20 | AccessMode::Controlled never activated | CRITICAL | 3 |
| 01 | No swarm connection limits | HIGH | 1 |
| 21 | DocSync/BranchableSync/CAR no access checks | HIGH | 3 |
| 30 | Unbounded task spawning per peer | HIGH | 4 |
| 31 | DocSyncRequest.doc_ids unbounded array | HIGH | 4 |
| 42 | No per-peer rate limiting | HIGH | 5 |
| 43 | No per-peer connection limits (confirms 01) | HIGH | 5 |
| 44 | Two-stream read_to_end no timeout (Slowloris) | HIGH | 5 |

### Cross-Stream Themes

**1. Missing defense-in-depth**: Transport layer (Noise) is strong, but no redundancy at connection, message, or rate limiting layers. If any single layer fails or is bypassed, there are no fallbacks.

**2. Two-stream protocol is the weakest link**: The Go-compatible two-stream handler concentrates the most severe findings: no size limits (00), no signature verification (12), no read timeouts (44), no access checks for DocSync/BranchableSync/CAR (21).

**3. Unbounded operations pattern**: Multiple components lack bounds — task spawning (30), doc_ids arrays (31), pending DAGs (32), DAG fetcher fan-out (33), connection counts (43), message rates (42).

**4. Strong components exist**: PeerStateTracker (48) and replication loop (53) demonstrate that bounded, eviction-aware designs are achievable in this codebase. These should serve as models for fixing the unbounded components.
