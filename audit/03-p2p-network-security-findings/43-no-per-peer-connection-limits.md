# Finding 43: No Per-Peer Connection Limits (Confirmed from Finding 01)

**Severity**: HIGH
**Category**: Resource Exhaustion
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

Session 1 Finding 01 identified that the Rust implementation lacks swarm-level connection limits (Go has 100 inbound, 400 total). This finding confirms the gap persists and extends it: there is also no per-peer connection limit, no connection establishment rate limit, and no connection timeout shorter than the 60-second idle timeout.

## Evidence

**Swarm config** (`host/p2p_host/mod.rs:204,213`):
```rust
.with_swarm_config(|cfg| cfg.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
```

The `SwarmConfig` supports `.with_max_incoming_connections()`, `.with_max_outgoing_connections()`, and `.with_max_connections_per_peer()` — none of which are called.

**libp2p defaults** (from libp2p-swarm source): no connection limits by default — unlimited inbound, unlimited outbound, unlimited per-peer.

**Idle connection timeout**: 60 seconds (`IDLE_CONNECTION_TIMEOUT`). A peer that completes the Noise handshake and then sends nothing will hold the connection for 60 seconds.

## Attack Scenario

1. Attacker opens 10,000 TCP connections to a DefraDB node
2. Each completes Noise handshake (resource-intensive: X25519 ECDH + ChaCha20-Poly1305)
3. Each idle connection holds memory for: TCP socket, Noise session state, yamux multiplexer state
4. Node runs out of file descriptors or memory
5. Legitimate peers can no longer connect

## Recommendation

Call `with_max_incoming_connections(100)`, `with_max_connections_per_peer(2)` on the `SwarmConfig`. Consider also using libp2p's `ConnectionLimits` behaviour.
