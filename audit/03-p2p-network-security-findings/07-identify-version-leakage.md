# Finding: Identify Protocol Leaks Exact Build Version to All Peers

**Stream**: 03 - P2P Network Security
**Severity**: LOW
**Category**: Information Disclosure
**Status**: CONFIRMED

## Summary

The Identify protocol announces `defradb-rs/{CARGO_PKG_VERSION}` (e.g., `defradb-rs/0.5.0`) as the agent version to every connected peer, and `/defra/identify/0.0.1` as the protocol version. This is standard practice and matches Go's behavior, but the exact version enables targeted exploitation if a vulnerability is discovered in a specific release.

## Affected Files

| File | Lines | Detail |
|------|-------|--------|
| `crates/p2p/src/behaviour.rs` | 173-175 | Protocol and agent version configuration |
| `crates/p2p/src/behaviour.rs` | 272-274 | Same in test path |

## Details

### What's Announced

```rust
identify::Config::new("/defra/identify/0.0.1".to_string(), local_public_key)
    .with_agent_version(format!("defradb-rs/{}", env!("CARGO_PKG_VERSION")));
```

Every connected peer receives:
- **Protocol version**: `/defra/identify/0.0.1`
- **Agent version**: `defradb-rs/0.5.0` (exact Cargo package version, set at compile time)
- **Public key**: The node's Ed25519 public key
- **Listen addresses**: All addresses the node is listening on

### Why This Is Low Severity

1. **Standard practice**: Go DefraDB, IPFS, and most libp2p implementations announce version strings
2. **Required for protocol negotiation**: Identify is fundamental to libp2p's multistream protocol selection
3. **Public key is already exchanged**: Via Noise handshake, so Identify doesn't add new exposure
4. **Listen addresses are observable**: A peer already knows the address it connected to

### When It Could Matter

- If a critical vulnerability is found in defradb-rs version X, an attacker can scan the network and target only nodes running that version
- The version string distinguishes Rust nodes from Go nodes, which may have different vulnerability profiles
- Combined with listen address disclosure, enables fingerprinting of specific nodes

### Go Comparison

Go announces its version via the default libp2p Identify configuration. The exact format differs but equivalent information is disclosed.

## Remediation

No immediate action needed. If desired:
1. Make the agent version configurable (allow operators to set a generic string)
2. Consider omitting the patch version (announce `defradb-rs/0.5` instead of `defradb-rs/0.5.0`)

These are defense-in-depth measures, not critical fixes.
