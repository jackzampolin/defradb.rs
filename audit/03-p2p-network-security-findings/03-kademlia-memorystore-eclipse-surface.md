# Finding: Kademlia MemoryStore Loses DHT State on Restart — Eclipse Attack Surface

**Stream**: 03 - P2P Network Security
**Severity**: MEDIUM
**Category**: Peer Discovery / Eclipse Attack
**Status**: CONFIRMED

## Summary

Kademlia uses `MemoryStore`, meaning the entire routing table and any stored DHT records are lost on node restart. A restarted node must rediscover the network from scratch using only bootstrap peers. This creates a window where an attacker who controls the bootstrap path can eclipse the node — filling its routing table with attacker-controlled peers before legitimate peers are discovered.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/behaviour.rs` | 227-228 | `MemoryStore::new(local_peer_id)` |
| `crates/p2p/src/host/p2p_host/swarm.rs` | 44-75 | Bootstrap triggered on every connection |

## Details

### Current Behavior

```rust
// crates/p2p/src/behaviour.rs:227
let kad_store = MemoryStore::new(local_peer_id);
```

`MemoryStore` has default limits:
- `max_records`: 1024
- `max_provided_keys`: 1024
- `max_value_bytes`: 65536

These are reasonable, but the store is **ephemeral**. On restart:
1. Routing table is empty
2. No cached peer addresses
3. Only bootstrap peers (user-configured) are known

### Bootstrap Mechanism

On `ConnectionEstablished`, the node adds the peer to Kademlia and calls `bootstrap()`:

```rust
// swarm.rs:48-75
self.swarm.behaviour_mut().kademlia.add_address(&peer_id, peer_addr);
let _ = self.swarm.behaviour_mut().kademlia.bootstrap();
```

This means the routing table is built entirely from peers discovered through connections. There's no persistence, no peer verification, and no k-bucket diversity enforcement beyond what libp2p-kad provides by default.

### Eclipse Attack Scenario

1. Node restarts (routing table empty)
2. Node connects to configured bootstrap peer(s)
3. Attacker is on the same network and connects before legitimate peers
4. Attacker responds to `FIND_NODE` queries with attacker-controlled peer IDs
5. Node's routing table fills with attacker peers
6. All subsequent DHT lookups route through attacker-controlled nodes
7. Attacker can censor content routing (Bitswap provider records), inject false peer addresses, or isolate the node

### Mitigating Factors

- DefraDB's primary replication uses direct GossipSub and PushLog, not DHT content routing
- Bitswap `FindProviders` returns all connected peers (protocols.rs:240-242), not just DHT results
- In practice, DefraDB networks are small and peers are explicitly configured
- The attack requires controlling the bootstrap path, which is harder in configured networks

### Why This Is Still Medium Severity

- Kademlia is used for peer discovery (bootstrap triggers `FIND_NODE`)
- A successful eclipse prevents discovery of new legitimate peers
- If a node only knows attacker peers, the attacker controls what content and peers are discoverable
- The node adds ALL listen_addrs from Identify to Kademlia (protocols.rs:47-53) without limit or validation — a malicious peer can claim many addresses to fill routing table slots

## Remediation

1. Consider persisting the Kademlia routing table (e.g., to the datastore) so restarts retain known-good peers
2. Limit the number of addresses added per peer from Identify (currently unbounded)
3. Consider adding peer diversity checks for k-bucket entries

## Test Gap

No test verifies behavior after node restart with an empty Kademlia store. No test verifies resilience to routing table poisoning via Identify address flooding.
