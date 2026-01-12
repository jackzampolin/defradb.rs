# DefraDB.rs Implementation Roadmap

## Subsystem Dependencies

```
Independent (no dependencies):
├── crdt ✅ DONE
├── crypto
├── storage
└── schema

Depends on Storage:
└── blockstore

Depends on Multiple:
├── query (needs: schema, storage, crdt)
└── p2p (needs: crypto, blockstore, crdt)
```

## Parallel Development Strategy

**Phase 1 - Foundations (can work in parallel)**
- ✅ crdt - Complete with tests
- storage - RocksDB integration + multi-store
- crypto - Signing & encryption
- schema - GraphQL SDL parser

**Phase 2 - Integration (requires Phase 1)**
- blockstore - IPLD + CID (needs: storage)
- query - Query planner (needs: schema, storage, crdt)

**Phase 3 - Networking (requires Phase 2)**
- p2p - libp2p + sync (needs: crypto, blockstore, crdt)

## Worktree Setup

```bash
# Already created:
git worktree list

# Switch between subsystems:
cd ../defradb.rs-storage   # Work on storage
cd ../defradb.rs-crypto    # Work on crypto
cd ../defradb.rs-schema    # Work on schema

# Merge when ready:
cd ../defradb.rs-<subsystem>
cargo test --all
git push -u origin feat/<subsystem>
# Create PR, merge to main
```

## Test Strategy for Go Compatibility

Each subsystem should have:
1. **Unit tests** - Rust implementation correctness
2. **Property tests** - CRDT properties (commutativity, idempotence)
3. **Compatibility tests** - Compare behavior with Go DefraDB (TODO)

Example compatibility test approach:
```rust
// Test against Go implementation's test vectors
#[test]
fn test_lww_compatible_with_go() {
    let go_test_vector = include_bytes!("testdata/go_lww_deltas.json");
    // Apply same deltas, verify same result
}
```

Test vectors can be extracted from Go DefraDB's test suite.
