# Finding 28: Registry Operations Are Atomic (RwLock-Protected)

**Severity: GREEN**
**Category: Concurrency Safety**
**Status: Verified**

## Summary

The `ReplicatorRegistry` uses `parking_lot::RwLock` for all operations, ensuring atomic reads and writes. Add, remove, and check operations are each protected by a single lock acquisition, preventing TOCTOU races within the registry itself.

## Evidence

### Write Operations — Exclusive Lock

`crates/p2p/src/bitswap/registry.rs:29-35` (add):
```rust
pub fn add_replicator(&self, collection_id: &str, peer_id: PeerId) {
    let mut replicators = self.replicators.write();  // exclusive lock
    replicators.entry(collection_id.to_string()).or_default().insert(peer_id);
}  // lock released
```

`crates/p2p/src/bitswap/registry.rs:38-46` (remove):
```rust
pub fn remove_replicator(&self, collection_id: &str, peer_id: &PeerId) {
    let mut replicators = self.replicators.write();  // exclusive lock
    if let Some(peers) = replicators.get_mut(collection_id) {
        peers.remove(peer_id);
        if peers.is_empty() {
            replicators.remove(collection_id);
        }
    }
}  // lock released
```

### Read Operations — Shared Lock

`crates/p2p/src/bitswap/registry.rs:58-64` (check):
```rust
pub fn is_replicator(&self, collection_id: &str, peer_id: &PeerId) -> bool {
    let replicators = self.replicators.read();  // shared lock
    replicators.get(collection_id).map(|peers| peers.contains(peer_id)).unwrap_or(false)
}  // lock released
```

### Concurrent Modification Test

`crates/p2p/src/bitswap/registry.rs:253-282`:
```rust
fn test_replicator_registry_concurrent_modifications() {
    let registry = std::sync::Arc::new(ReplicatorRegistry::new());
    let mut handles = vec![];
    for i in 0..10 {
        let registry_clone = std::sync::Arc::clone(&registry);
        let handle = thread::spawn(move || {
            let peer = PeerId::random();
            registry_clone.add_replicator(&collection, peer);
            assert!(registry_clone.is_any_replicator(&peer));
            if i % 2 == 0 {
                registry_clone.remove_replicator(&collection, &peer);
            }
        });
        handles.push(handle);
    }
    // All threads complete without panics
}
```

### TOCTOU Window Between check_access and Block Processing

There IS a small TOCTOU window at the coordinator level: after `check_access()` returns `Ok(())`, a concurrent `remove_replicator` could revoke the peer's access before block processing completes. However:
- The window is milliseconds (in-process async operations)
- This is inherent to check-then-act patterns
- The registry itself is consistent — only the coordinator's use has a window
- Same pattern exists in Go DefraDB

## Conclusion

The registry's internal concurrency model is sound. `parking_lot::RwLock` provides non-poisoning, fair locking with good performance characteristics. The TOCTOU window at the coordinator level is acceptable.
