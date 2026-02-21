# Finding: TCP Port Reuse Is Safe Due to Noise Authentication (GREEN)

**Stream**: 03 - P2P Network Security
**Severity**: INFORMATIONAL
**Category**: Transport Security
**Status**: CONFIRMED — NO ISSUE

## Summary

TCP port reuse (`tcp::Config::default().port_reuse(true)`) causes outgoing connections to use the listen port as the source port, making the node's listen address visible to the remote side. This matches Go-libp2p's behavior and is required for ActivePeers address reporting. Port reuse does not enable connection hijacking because every connection is Noise-authenticated.

## Verified Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/host/p2p_host/mod.rs` | 182-186 | `tcp::Config::default().port_reuse(true)` |
| `crates/p2p/src/host/p2p_host/swarm.rs` | 36-41 | `send_back_addr` used as peer address with port reuse |

## Details

### What Port Reuse Does

Without port reuse: Outgoing connections use an ephemeral source port (e.g., 49152). The remote peer sees this ephemeral port, not the node's listen port. This breaks address reporting — ActivePeers would return ephemeral addresses.

With port reuse: Outgoing connections use the listen port (e.g., 9171) as the source port. The remote peer's `send_back_addr` is the actual listen address.

### Why It's Safe

1. **Noise authentication**: Even if an attacker can predict the source port, they cannot inject packets into an established Noise session. Noise provides authenticated encryption — any injected data fails AEAD verification.

2. **No TCP connection hijacking**: Hijacking a TCP connection requires predicting the TCP sequence number AND injecting before the legitimate packet arrives. Port reuse makes the source port predictable, but the sequence number is still random (OS-provided).

3. **Required for correctness**: Without port reuse, `peer_addrs` (mod.rs:76) would contain ephemeral addresses for incoming connections. The comment at mod.rs:182-185 correctly explains this.

### Go Comparison

Go-libp2p enables port reuse by default. This is intentional and documented behavior for libp2p implementations.

## Conclusion

TCP port reuse is correctly used and does not introduce security risk given Noise authentication. This item is documented to confirm it was audited.
