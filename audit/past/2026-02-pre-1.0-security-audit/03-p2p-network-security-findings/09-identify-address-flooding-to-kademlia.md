# Finding: Identify Address Flooding — All Remote Addresses Added to Kademlia Without Limit

**Stream**: 03 - P2P Network Security
**Severity**: MEDIUM
**Category**: Routing Table Poisoning
**Status**: CONFIRMED

## Summary

When the Identify protocol receives a peer's information, the handler adds ALL of the peer's `listen_addrs` to Kademlia without any limit on count or validation of address reachability. A malicious peer can claim thousands of listen addresses, flooding the Kademlia routing table with entries that point to the same peer or to victim addresses (for reflection attacks).

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/host/p2p_host/protocols.rs` | 47-53 | Loops over ALL `info.listen_addrs` |
| `crates/p2p/src/host/p2p_host/swarm.rs` | 48-51 | Also adds peer_addr on ConnectionEstablished |

## Details

### Current Code

```rust
// protocols.rs:47-53
for addr in &info.listen_addrs {
    debug!(peer_id = %peer_id, address = %addr, "Adding peer address to Kademlia");
    self.swarm
        .behaviour_mut()
        .kademlia
        .add_address(&peer_id, addr.clone());
}
```

No limit on:
- Number of addresses per peer
- Address format validation (private IPs, multicast, localhost)
- Whether the address is actually reachable

### Attack Scenarios

**Routing table bloat**: A malicious peer sends Identify with 10,000 listen addresses. All are added to Kademlia's routing table for that peer's k-bucket entry. While MemoryStore has limits on records (1024), the routing table address list per peer is unbounded.

**Address reflection**: A malicious peer claims listen addresses belonging to victim machines. When other peers query the DHT and discover this peer, they attempt connections to the victim addresses — using the DefraDB node as an unwitting traffic reflector.

**Routing pollution**: By claiming addresses on many different subnets, a peer can appear to be "close" to more DHT key spaces, increasing its influence over routing decisions.

### Mitigating Factors

- libp2p-kad's k-bucket implementation limits entries per bucket (k=20 by default)
- Each peer ID maps to one k-bucket position, so address flooding doesn't fill multiple buckets
- DefraDB doesn't store arbitrary records in the DHT (no DHT PUT operations observed)

### Go Comparison

Go's libp2p Identify handler also adds addresses, but Go has the connection manager as an outer bound. Additionally, Go uses `dualdht.DualDHT` which separates LAN and WAN routing, limiting cross-contamination.

## Remediation

1. Cap the number of addresses accepted per peer from Identify (e.g., max 10)
2. Validate address format — reject obviously invalid addresses (localhost, multicast, unroutable)
3. Consider address verification — only add addresses that are confirmed reachable

```rust
const MAX_IDENTIFY_ADDRS: usize = 10;
for addr in info.listen_addrs.iter().take(MAX_IDENTIFY_ADDRS) {
    self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr.clone());
}
```

## Test Gap

No test sends a peer with many listen addresses via Identify and verifies the routing table is bounded.
