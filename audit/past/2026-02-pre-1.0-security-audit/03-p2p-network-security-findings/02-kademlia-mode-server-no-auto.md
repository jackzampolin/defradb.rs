# Finding: Kademlia Hardcoded to Mode::Server Instead of ModeAuto

**Stream**: 03 - P2P Network Security
**Severity**: MEDIUM
**Category**: Configuration / Amplification Risk
**Status**: CONFIRMED

## Summary

Kademlia is hardcoded to `Mode::Server`, meaning every node always responds to DHT queries from any peer. Go uses `dht.ModeAuto`, which lets libp2p decide based on NAT status. A server-mode node behind NAT wastes resources responding to queries it can't fully serve, and publicly-reachable nodes unconditionally participate in DHT amplification without any rate limiting.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/behaviour.rs` | 229-230 | `kademlia.set_mode(Some(Mode::Server))` |
| `crates/p2p/src/behaviour.rs` | 316-317 | Same in test path |

## Details

### Current Configuration

```rust
// crates/p2p/src/behaviour.rs:229-230
let mut kademlia = kad::Behaviour::new(local_peer_id, kad_store);
kademlia.set_mode(Some(Mode::Server));
```

### Go Comparison

```go
// go-p2p/host.go:52
dualdht.DHTOption(dht.Mode(dht.ModeAuto))
```

`ModeAuto` lets libp2p detect whether the node is publicly reachable:
- **Reachable**: Operate as server (respond to queries)
- **Behind NAT**: Operate as client (only query, don't respond)

### Amplification Risk

In `Mode::Server`, the node responds to `FIND_NODE` and `GET_VALUE` queries from any peer. A malicious peer can:

1. Send rapid `FIND_NODE` queries with random target IDs
2. The node responds with its k-closest peers (up to k=20 entries, each with PeerId + multiaddrs)
3. Response is typically larger than the request — amplification factor ~5-10x
4. No per-peer rate limiting on Kademlia responses exists

Combined with Finding 01 (no connection limits), an attacker opening many connections can generate significant outbound traffic through Kademlia amplification.

### Additional Differences from Go

Go also configures:
- `dht.Concurrency(10)` — limits concurrent DHT operations
- `dht.NamespacedValidator("pk", record.PublicKeyValidator{})` — validates stored records
- **Dual DHT** — separates public and private routing tables

Rust uses none of these. The single-mode DHT with `MemoryStore` and default concurrency means:
- No record validation (accepts any DHT PUT)
- Default concurrency (3 in libp2p-kad, vs Go's 10)
- No separation of LAN/WAN routing

## Remediation

1. Use `Mode::Auto` instead of `Mode::Server` (or make it configurable)
2. Consider adding the `NamespacedValidator` equivalent for record validation
3. Consider adding per-peer rate limiting on Kademlia responses

## Test Gap

No test verifies Kademlia mode behavior or rate limiting of DHT queries.
