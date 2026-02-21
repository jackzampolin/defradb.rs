# Finding 20: AccessMode::Controlled Is Never Activated — Collection Access Control Is Dead Code

**Severity: CRITICAL**
**Category: Authorization Bypass**
**Status: Confirmed**

## Summary

`AccessMode::Controlled` — the mode that enables per-collection replicator checks — is never set in production code. Every code path that constructs a `SyncCoordinator` hardcodes `AccessMode::Open`, which causes `check_access()` to return `Ok(())` unconditionally for all peers. The entire two-tier access control system is dead code.

## Evidence

### Production construction in FFI (the only production entry point)

`crates/ffi/src/p2p/node.rs:211`:
```rust
let (mut coordinator, sync_events_rx) = SyncCoordinator::with_head_provider(
    handle.clone(),
    blockstore.clone(),
    SyncConfig::default(),
    p2p::bitswap::AccessMode::Open,  // <-- hardcoded Open
    Arc::new(p2p::bitswap::ReplicatorRegistry::new()),
    ...
)
```

### All constructors default to Open

`crates/p2p/src/sync/coordinator/constructor.rs:35`:
```rust
pub async fn new(...) -> Result<...> {
    Self::with_access_control(..., AccessMode::Open, ...)
}
```

`crates/p2p/src/sync/coordinator/constructor.rs:65`:
```rust
pub async fn with_collection_store(...) -> Result<...> {
    Self::with_access_control(..., AccessMode::Open, ...)
}
```

### check_access fast-path

`crates/p2p/src/sync/coordinator/access.rs:24`:
```rust
pub(super) fn check_access(&self, peer_id: &PeerId, collection_id: &str) -> Result<()> {
    if self.access_mode.is_open() {
        return Ok(());  // <-- always taken in production
    }
    // Everything below is dead code
    ...
}
```

### No mechanism to switch to Controlled

Grep across entire codebase for `AccessMode::Controlled` returns only:
- The enum definition itself (`access.rs:38`)
- Unit test assertions (`access.rs:55-56`)
- A doc comment (`constructor.rs:85`)

No CLI flag, environment variable, config file, or HTTP endpoint exists to activate Controlled mode.

## Impact

**All peers connected to the swarm can replicate all collections.** The ReplicatorRegistry exists and is maintained (peers are added/removed), but its checks are never evaluated. A node operator who adds replicators expecting per-collection access control receives no actual protection.

## Go Comparison

Go DefraDB also defaults to Open mode but has a mechanism to switch to Controlled mode when ACP policies are configured. The Rust implementation lacks this switching mechanism entirely.

## Recommendation

Either:
1. Add a mechanism to activate `AccessMode::Controlled` when ACP is configured (matching Go behavior), or
2. Remove the dead code and document that all replication is open (avoiding false security assumptions)
