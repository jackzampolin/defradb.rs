# Finding 27: Collection ID Matching Is Exact — No Wildcards or Inheritance

**Severity: GREEN**
**Category: Authorization Model**
**Status: Verified**

## Summary

The `ReplicatorRegistry` uses exact `HashMap<String, HashSet<PeerId>>` lookups for collection ID matching. There are no wildcards, regex patterns, prefix matching, or role inheritance. A replicator authorized for collection "users" has access to exactly "users" and nothing else.

## Evidence

### Registry Data Structure

`crates/p2p/src/bitswap/registry.rs:17`:
```rust
pub struct ReplicatorRegistry {
    replicators: RwLock<HashMap<String, HashSet<PeerId>>>,
}
```

### Exact Match Lookup

`crates/p2p/src/bitswap/registry.rs:58-64`:
```rust
pub fn is_replicator(&self, collection_id: &str, peer_id: &PeerId) -> bool {
    let replicators = self.replicators.read();
    replicators
        .get(collection_id)      // HashMap::get — exact string match
        .map(|peers| peers.contains(peer_id))
        .unwrap_or(false)
}
```

- `HashMap::get` performs exact key comparison — no substring, glob, or regex matching
- Missing collection returns `None` → `unwrap_or(false)` → denied
- No "super-replicator" or "replicate-all" role exists

### Test Confirmation

`crates/p2p/src/bitswap/registry.rs:186-191`:
```rust
registry.add_replicator("users", peer);
assert!(registry.is_replicator("users", &peer));
assert!(!registry.is_replicator("posts", &peer));  // exact: no cross-collection
```

### No Role Hierarchy

The registry has no concept of:
- Wildcards (e.g., `*` for all collections)
- Prefix matching (e.g., `users*` for `users_v1`, `users_v2`)
- Role inheritance (e.g., admin → replicator)
- Group-based access (e.g., "group:admins")

Each peer × collection pair is individually registered.

## Conclusion

The access control model is simple, correct, and has no ambiguity in collection matching. This is the right design for a replicator registry.
