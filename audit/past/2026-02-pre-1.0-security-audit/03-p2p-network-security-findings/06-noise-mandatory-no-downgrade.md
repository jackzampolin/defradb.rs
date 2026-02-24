# Finding: Noise Protocol Is Mandatory — No Downgrade Path (GREEN)

**Stream**: 03 - P2P Network Security
**Severity**: INFORMATIONAL
**Category**: Transport Security
**Status**: CONFIRMED — NO ISSUE

## Summary

Noise protocol (XX handshake with Ed25519) is the sole transport security mechanism. There is no plaintext fallback, no SecIO, and no way to disable encryption. This is correct and secure. Both SwarmBuilder paths (with and without relay) use identical `noise::Config::new` constructors.

## Verified Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/host/p2p_host/mod.rs` | 195 | `.with_tcp(tcp_config, noise::Config::new, yamux::Config::default)` |
| `crates/p2p/src/host/p2p_host/mod.rs` | 197 | `.with_relay_client(noise::Config::new, yamux::Config::default)` |
| `crates/p2p/src/host/p2p_host/mod.rs` | 209 | `.with_tcp(tcp_config, noise::Config::new, yamux::Config::default)` |

## Details

### Why This Is Secure

1. **No fallback**: libp2p's `SwarmBuilder::with_tcp()` takes a single security upgrade function. There is no `with_plaintext()` or `with_secio()` alternative in the call chain.

2. **Identical config for both paths**: The relay and non-relay builder paths use the same `noise::Config::new` — no accidental plaintext in one path.

3. **Noise XX handshake**: `noise::Config::new` uses the XX handshake pattern, which provides mutual authentication. Both peers prove knowledge of their Ed25519 private key during the handshake.

4. **Peer ID binding**: libp2p's Noise implementation binds the Noise handshake to the peer's libp2p identity. The remote peer's PeerId is derived from their Ed25519 public key, which is verified during Noise negotiation. A peer cannot impersonate another peer's PeerId.

5. **Ed25519 only**: The node always generates Ed25519 keypairs (`Keypair::generate_ed25519()` at mod.rs:98,112). No weaker key types are supported.

### Go Comparison

Go uses `libp2p.DefaultTransports` which includes both Noise and TLS. Rust uses only Noise, which is equally secure. The absence of TLS is not a vulnerability — Noise XX with Ed25519 provides equivalent confidentiality, integrity, and authentication.

## Conclusion

Transport encryption is correctly configured. No finding. This item is documented to confirm it was audited.
