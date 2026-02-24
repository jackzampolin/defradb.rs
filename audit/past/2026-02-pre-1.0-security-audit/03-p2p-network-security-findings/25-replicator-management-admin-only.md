# Finding 25: Replicator Management Is Admin-Only — No Self-Registration

**Severity: GREEN**
**Category: Authorization Model**
**Status: Verified**

## Summary

The replicator registry can only be modified through the HTTP API or CLI, both of which require local admin access. No P2P protocol message allows a remote peer to register itself as a replicator. This prevents unauthorized peers from granting themselves collection access.

## Evidence

### HTTP Handlers Require NAC Permissions

`crates/http/src/handlers/p2p/replicators.rs:94`:
```rust
pub async fn add_replicator(...) -> Result<...> {
    require_permission(&state, &identity, NodePermission::P2pReplicatorAdd).await?;
    ...
}
```

`crates/http/src/handlers/p2p/replicators.rs:137`:
```rust
pub async fn remove_replicator(...) -> Result<...> {
    require_permission(&state, &identity, NodePermission::P2pReplicatorDelete).await?;
    ...
}
```

### Internal Command Handler — Not Exposed to P2P

`crates/p2p/src/host/command_handler/messaging.rs:228-244`:
```rust
pub(super) fn handle_create_replicator(
    &mut self,
    peer_id: PeerId,
    collections: Vec<String>,
    response: tokio::sync::oneshot::Sender<Result<()>>,
) {
    // This is called via internal HostCommand, not from P2P protocol messages
    self.replicators.remove_peer(&peer_id);
    for collection_id in &collections {
        self.replicators.add_replicator(collection_id, peer_id);
    }
    ...
}
```

The `HostCommand::CreateReplicator` variant is only sent via `P2PHostHandle`, which is only accessible to local code (HTTP handlers, CLI, FFI).

### No P2P Message Type for Self-Registration

The `HostEvent` enum (`host/event.rs`) has no variant for replicator registration from remote peers. The only P2P message types that reach the coordinator are:
- PushLogRequest
- GossipMessage
- DocSyncRequest/Reply
- BranchableSyncRequest/Reply
- CarFetchRequest/Response
- BitswapBlockReceived/Complete

None of these modify the replicator registry.

### Input Validation

The HTTP handler validates collection names (`validate_collection_name`) and multiaddrs (`validate_multiaddr`) before processing, preventing injection-style attacks.

## Conclusion

The admin-only control plane for replicator management is correctly separated from the P2P data plane. No remote peer can modify the registry.
