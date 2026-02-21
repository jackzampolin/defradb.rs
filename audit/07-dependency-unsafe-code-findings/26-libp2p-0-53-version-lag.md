# libp2p 0.53.2: Version Lag and Dependency Cascade

**Severity:** Medium
**Category:** Outdated dependency — P2P networking
**Status:** At least one major version behind latest

## Summary

libp2p v0.53.2 is used primarily because `iroh-bitswap` requires it (noted in Cargo.toml: "Using 0.53 to match iroh-bitswap's dependency"). The latest libp2p is 0.54+. Being behind means missing security fixes, performance improvements, and carrying older sub-crate versions with their own issues.

## Affected Crate(s)

- `libp2p` v0.53.2 (direct workspace dependency)

## Sub-crate Versions

| Sub-crate | Version | Notes |
|-----------|---------|-------|
| libp2p-noise | 0.44.0 | Noise protocol — security-critical |
| libp2p-yamux | 0.45.2 | Uses yamux 0.12.1 (also has yamux 0.13.8 in tree) |
| libp2p-gossipsub | 0.46.1 | Pub/sub messaging |
| libp2p-kad | 0.45.3 | Kademlia DHT |
| libp2p-identify | 0.44.2 | Peer identification |
| libp2p-relay | 0.17.2 | Circuit relay |
| libp2p-tcp | 0.41.0 | TCP transport |
| libp2p-request-response | 0.26.3 | Request-response protocol |
| libp2p-quic | 0.10.3 | QUIC transport (pulls in vulnerable ring 0.16.20) |
| libp2p-stream | 0.1.0-alpha.1 | Stream protocol (alpha!) |

## Key Issues

### 1. libp2p-stream is alpha
`libp2p-stream` v0.1.0-alpha.1 is used for the two-stream protocol. Alpha crates may have breaking changes and are not considered production-ready.

### 2. yamux version duplication
Two versions of yamux are in the tree:
- `yamux 0.12.1` via `libp2p-yamux 0.45.2` (libp2p's own)
- `yamux 0.13.8` via other dependencies

### 3. QUIC pulls in vulnerable ring
Even though the project only uses TCP transport features, `libp2p-quic 0.10.3` is pulled in and brings the vulnerable `ring 0.16.20`.

### 4. lru soundness issue
libp2p-swarm depends on `lru 0.12.5` which has a known soundness bug (see finding 23).

## Remediation

1. **Upgrade libp2p** to latest stable (requires updating iroh-bitswap first or finding an alternative)
2. **Disable QUIC feature** if possible to avoid pulling in vulnerable ring 0.16.20
3. **Evaluate libp2p-stream stability** — using an alpha crate for a core protocol is a risk
