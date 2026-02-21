# Finding: No Swarm-Level Connection Limits

**Stream**: 03 - P2P Network Security
**Severity**: HIGH
**Category**: Denial of Service
**Status**: CONFIRMED

## Summary

The Rust P2P host does not configure any swarm-level connection limits. An attacker can open an unlimited number of inbound connections, exhausting file descriptors, memory, and CPU. Go DefraDB uses `connmgr.NewConnManager(100, 400)` to cap connections — Rust has no equivalent.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/host/p2p_host/mod.rs` | 204, 213 | `with_swarm_config` only sets idle timeout, no connection limits |

## Details

### What's Missing

libp2p-swarm provides `SwarmConfig::with_max_established_per_peer()`, `with_max_established_incoming()`, `with_max_established_outgoing()`, and `with_max_pending_incoming()`. None of these are configured:

```rust
// Current code — only idle timeout is set
.with_swarm_config(|cfg| cfg.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT))
```

### Go Comparison

Go DefraDB configures a connection manager via `go-p2p`:

```go
connmgr.NewConnManager(100, 400, connmgr.WithGracePeriod(time.Second*20))
```

This enforces:
- **Low watermark**: 100 connections (start pruning above this)
- **High watermark**: 400 connections (hard cap)
- **Grace period**: 20 seconds before new connections are eligible for pruning

Rust has **none of these protections**.

### Attack Scenario

1. Attacker opens TCP connections to the node's listen port
2. Each connection completes Noise handshake (attacker has a valid Ed25519 keypair — trivially generated)
3. Node allocates per-connection state: Yamux session, protocol negotiation buffers, Kademlia routing entry, GossipSub peer state
4. At hundreds/thousands of connections, the node exhausts file descriptors (typical OS default: 1024) or memory
5. Node becomes unresponsive to legitimate peers

### Compounding Factors

- Each connection triggers `kademlia.add_address()` and `kademlia.bootstrap()` (swarm.rs:48-75), adding routing table entries and DHT queries per connection
- Each connection triggers Bitswap pre-announce (`on_identify` with 3 protocol versions), adding per-peer state in iroh-bitswap
- The `peer_addrs: HashMap<PeerId, Multiaddr>` grows unbounded (mod.rs:76)
- With `flood_publish(true)`, each new GossipSub peer increases publish fan-out

### Why Idle Timeout Doesn't Help

The 60-second idle timeout only closes connections with no active substreams. An attacker can keep connections alive by periodically opening Kademlia or Identify substreams, which are always accepted.

## Remediation

Add connection limits to the swarm config:

```rust
.with_swarm_config(|cfg| {
    cfg.with_idle_connection_timeout(IDLE_CONNECTION_TIMEOUT)
       .with_max_established_per_peer(2)
       .with_max_established_incoming(400)
       .with_max_pending_incoming(128)
})
```

Consider also implementing a connection manager that proactively trims connections at a low watermark, matching Go's behavior.

## Test Gap

No test verifies that the node rejects connections beyond a configured limit. No load test opens many concurrent connections to verify stability.
