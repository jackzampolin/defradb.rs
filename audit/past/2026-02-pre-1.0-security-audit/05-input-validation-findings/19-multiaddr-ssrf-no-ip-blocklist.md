# Multiaddr SSRF — No Private IP Blocklist

**Severity**: MEDIUM
**Category**: Input Validation — Network Security
**Status**: Confirmed

## Summary

`validate_multiaddr()` only checks that the address starts with `/`. There is no validation of IP ranges at any layer — not in the HTTP handler, not in the P2P adapter, not in libp2p itself. An authenticated attacker with `P2pPeerConnect` or `P2pReplicatorAdd` permission can use the P2P connect endpoints to probe internal networks, target cloud metadata services, and port-scan private IP ranges.

## Affected Files

- `crates/http/src/validation.rs:50-63` — `validate_multiaddr()` format-only check
- `crates/http/src/handlers/p2p/peers.rs:110-127` — `connect_peer()` handler
- `crates/http/src/handlers/p2p/replicators.rs:109-112` — `add_replicator()` handler
- `crates/cli/src/p2p_adapter.rs:464-483` — P2P adapter passes to libp2p
- `crates/p2p/src/address.rs:13-35` — `parse_multiaddr_with_peer_id()` no IP check
- `crates/p2p/src/host/p2p_host/mod.rs:334-348` — `dial_peer()` passes directly to swarm

## Details

### Validation Chain (All Layers)

| Layer | Component | IP Validation |
|-------|-----------|---------------|
| HTTP | `validate_multiaddr()` | None — only `starts_with('/')` |
| HTTP | `connect_peer()` handler | Calls `validate_multiaddr()` only |
| P2P | `parse_multiaddr_with_peer_id()` | Parses format, no IP check |
| P2P | `P2PHost::dial_peer()` | Passes directly to `swarm.dial()` |
| libp2p | `Swarm::dial()` (v0.53) | No private IP filtering |

**Zero IP range validation across the entire flow.**

### The Vulnerable Code

```rust
// crates/http/src/validation.rs:50-63
pub fn validate_multiaddr(address: &str) -> Result<(), HttpError> {
    if address.trim().is_empty() {
        return Err(HttpError::BadRequest("address cannot be empty".to_string()));
    }
    if !address.starts_with('/') {
        return Err(HttpError::BadRequest(format!(
            "invalid multiaddr '{}': must start with '/' ...", address
        )));
    }
    Ok(())  // Accepts ANY multiaddr — no IP validation
}
```

### Addresses That Pass Validation

All of these are accepted and dialed:

| Address | Risk |
|---------|------|
| `/ip4/127.0.0.1/tcp/8080/p2p/<id>` | Localhost SSRF |
| `/ip4/169.254.169.254/tcp/80/p2p/<id>` | AWS/GCP metadata service |
| `/ip4/10.0.0.1/tcp/22/p2p/<id>` | Internal network scan |
| `/ip4/172.16.0.1/tcp/5432/p2p/<id>` | Internal database probe |
| `/ip4/192.168.1.1/tcp/80/p2p/<id>` | LAN probe |
| `/ip6/::1/tcp/9171/p2p/<id>` | IPv6 localhost |
| `/dns4/internal.local/tcp/443/p2p/<id>` | DNS rebinding |

### Attack Vectors

1. **Port scanning**: Observe timing differences between open/closed ports
2. **Cloud metadata**: Target 169.254.169.254 on EC2/GCE for instance credentials
3. **Internal service discovery**: Probe common ports (5432, 6379, 3306, 8080)
4. **Resource exhaustion**: Submit thousands of connect requests to internal hosts

### Mitigating Factor

The endpoints require NAC permission (`P2pPeerConnect` / `P2pReplicatorAdd`), so an unauthenticated attacker cannot exploit this. However, any authenticated user with P2P permissions can.

### Note on libp2p

libp2p's `Multiaddr::from_str()` validates the multiaddr *format* (protocol components are syntactically valid), but does **not** check IP ranges. The `Protocol::Ip4` component accepts any valid IPv4 address, including private ranges.

## Remediation

Add IP range validation to `validate_multiaddr()`:

```rust
use std::net::IpAddr;

fn is_private_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback() || v4.is_private() || v4.is_link_local()
            || v4.octets()[0..2] == [169, 254]  // AWS metadata
        }
        IpAddr::V6(v6) => v6.is_loopback(),
    }
}
```

Also add a maximum length check (1024 bytes) to prevent DoS via extremely long multiaddr strings.

## Test Gap

- No test verifies that private IP addresses are rejected
- The existing tests in `validation.rs` explicitly accept `127.0.0.1` and `::1` as valid
- No integration test attempts SSRF via the P2P connect endpoint
