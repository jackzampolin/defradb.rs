# Finding: No Hardcoded Bootstrap Peers — All User-Configurable (GREEN)

**Stream**: 03 - P2P Network Security
**Severity**: INFORMATIONAL
**Category**: Discovery Configuration
**Status**: CONFIRMED — NO ISSUE

## Summary

There are no hardcoded bootstrap peers in the Rust implementation. All peer discovery happens through user-configured peers passed via CLI or API. Bootstrap is triggered dynamically when connections are established. This matches Go's behavior and is correct for a permissioned/configured network.

## Verified Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/host/p2p_host/swarm.rs` | 72-75 | Bootstrap triggered on ConnectionEstablished |
| `crates/p2p/src/behaviour.rs` | 225-230 | Kademlia created with empty routing table |

## Details

### Bootstrap Mechanism

The node starts with an empty Kademlia routing table. When a peer connection is established (via explicit dial from CLI `--peers` flag or API), the handler:

1. Adds the peer's address to Kademlia (swarm.rs:48-51)
2. Triggers `kademlia.bootstrap()` (swarm.rs:75)

Bootstrap performs iterative `FIND_NODE` queries to discover more peers through the connected peer. There are no IPFS bootstrap nodes, no hardcoded DNS seeds, and no default peer lists.

### Go Comparison

Go also uses user-configurable peers via `--peers` CLI flag or `net.peers` config:
```go
// Default: empty list
"net.peers": []string{}
```

Both implementations require explicit peer configuration.

### Security Implications

**Positive**: No dependency on external bootstrap infrastructure that could be compromised or censored.

**Negative**: See Finding 03 — without persistent state, a restarted node with no configured peers has no way to rejoin the network. Operators MUST configure bootstrap peers.

## Conclusion

Bootstrap configuration is correct. No hardcoded peers that could be compromised. This item is documented to confirm it was audited.
