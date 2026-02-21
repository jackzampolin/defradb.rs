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

### Session 1: Transport Config, LibP2P Setup, Discovery (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/host/p2p_host/mod.rs` | 182-220 | Noise protocol, Yamux muxing, TCP port reuse |
| `crates/p2p/src/behaviour.rs` | 172-177, 225-230 | Identify protocol, Kademlia DHT (MemoryStore, Mode::Server) |

**Checklist**: Noise mandatory (no downgrade), Yamux params, DHT eclipse protections, bootstrap config

### Session 2: Message Signing/Verification (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/signing.rs` | 53-97 (sign), 118-157 (verify) | UUID v4 ID, CBOR serialization, 4-point verification |
| `crates/p2p/src/codec.rs` | 153-242 | PushLogCodec signing integration, optional keypair |
| `crates/p2p/tests/signing_tests.rs` | all (13 tests) | Tampered message, wrong sig, pubkey mismatch |
| `crates/p2p/tests/codec_tests.rs` | 14-130 | Codec roundtrip tests |

**Checklist**: Signature cleared before serializing, pubkey-to-peerid binding, conditional keypair handling

### Session 3: Authorization Model, Access Checks (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/sync/coordinator/access.rs` | 22-43 | AccessMode::Open fast-path, per-collection replicator check |
| `crates/p2p/src/bitswap/registry.rs` | 28-64 | Per-collection HashMap authorization |
| `crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` | 25-48, 119-145 | Access check BEFORE CID parsing |
| `crates/p2p/src/sync/coordinator/event_handler/gossip.rs` | 25-34 | GossipSub message drop on denial |
| `crates/p2p/src/bitswap/access.rs` | 1-45 | Bitswap access (no checks - enforced at coordinator) |

**Checklist**: Access before storage, no wildcard/inheritance in registry, GossipSub metadata leakage

### Session 4: Replication Protocol Security (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/codec.rs` | 27, 39-61 | **MAX_MESSAGE_SIZE=16MB**, `reader.take()` enforcement |
| `crates/p2p/src/two_stream/handler/inbound.rs` | 33-75 | **NO size limit** - `read_to_end` on Vec::new() - HIGH RISK |
| `crates/p2p/src/sync/coordinator/event_handler/pushlog.rs` | 12-104 | PushLog request flow, CID validation |
| `crates/p2p/src/sync/coordinator/event_handler/bitswap.rs` | 11-50 | Block storage, pending_dag_missing limits |

**Checklist**: Two-stream unbounded read (DoS), message type validation, CID parsing errors, DAG depth bomb

### Session 5: Resource Limits & Edge Cases (MEDIUM)

| File | Lines | Focus |
|------|-------|-------|
| `crates/p2p/src/host/p2p_host/mod.rs` | 35-36 | IDLE_CONNECTION_TIMEOUT=60s |
| `crates/p2p/src/sync/coordinator/mod.rs` | 80-116 | No Semaphore/RateLimiter found |
| `crates/p2p/src/behaviour.rs` | 191-223 | GossipSub config (SHA256 msg_id, heartbeat 1s, ValidationMode::Strict) |

**Checklist**: No per-peer rate limiting, no per-peer connection limits, GossipSub mesh defaults, DAG depth limits
