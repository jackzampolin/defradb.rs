# Finding: Yamux Uses All Defaults — No Max Concurrent Streams Limit

**Stream**: 03 - P2P Network Security
**Severity**: MEDIUM
**Category**: Denial of Service / Resource Exhaustion
**Status**: CONFIRMED

## Summary

Yamux muxing is configured with `yamux::Config::default()` in all code paths. The default configuration in yamux 0.12.1 (via libp2p-yamux 0.45.2) has no enforced limit on concurrent streams per connection. A single malicious peer can open hundreds of concurrent substreams (Kademlia, Identify, Bitswap, two-stream protocols), each consuming memory and task resources.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/host/p2p_host/mod.rs` | 195 | `yamux::Config::default` (TCP path) |
| `crates/p2p/src/host/p2p_host/mod.rs` | 197 | `yamux::Config::default` (relay client path) |
| `crates/p2p/src/host/p2p_host/mod.rs` | 209 | `yamux::Config::default` (non-relay TCP path) |

## Details

### Yamux Default Parameters (yamux 0.12.1)

| Parameter | Default | Impact |
|-----------|---------|--------|
| Max streams (inbound) | None (unbounded) | A peer can open unlimited streams |
| Receive window | 256 KB per stream | Memory per stream |
| Max buffer size | 16 KB (frame size) | Per-frame limit |

### Attack Scenario

1. Attacker connects to the node (single TCP connection, Noise-authenticated)
2. Attacker opens streams on various protocols:
   - Kademlia (`/ipfs/kad/1.0.0`)
   - Identify (`/ipfs/id/1.0.0`)
   - Bitswap (`/ipfs/bitswap/1.2.0`)
   - Two-stream protocols (`/defradb/rep_req/0.0.1`, etc.)
3. Each stream allocates a 256 KB receive window buffer
4. 1000 streams = ~250 MB of buffer memory from a single connection
5. Each two-stream substream spawns a tokio task (`runner.rs`), further consuming task scheduler resources

### Compounding with Finding 00

Finding 00 identified that two-stream handlers use `read_to_end()` without size limits. Combined with unlimited streams, an attacker can:
- Open N streams on two-stream protocols
- Send data on each, triggering N concurrent `read_to_end()` allocations
- Each grows unbounded — multiplied by N streams

### Go Comparison

Go's yamux defaults are similar — no explicit per-connection stream limit. However, Go's connection manager (100/400 limits) provides an outer bound that Rust lacks. The combination of unlimited connections AND unlimited streams per connection is more severe in Rust.

### Both Builder Paths Are Identical (Green)

Both the relay and non-relay paths use identical security configuration:
```rust
// With relay
.with_tcp(tcp_config, noise::Config::new, yamux::Config::default)
.with_relay_client(noise::Config::new, yamux::Config::default)

// Without relay
.with_tcp(tcp_config, noise::Config::new, yamux::Config::default)
```

This is correct — no downgrade path between the two builder branches.

## Remediation

Configure yamux with explicit stream limits:

```rust
let yamux_config = yamux::Config::default()
    .set_max_num_streams(256);  // Reasonable for DefraDB protocols
```

Or use the builder pattern if available in the yamux version. A limit of 256 concurrent streams per connection is generous for DefraDB's protocol set while preventing abuse.

## Test Gap

No test opens many concurrent streams on a single connection to verify the node remains stable.
