# nac/ — Named Access Control tests

```
cargo test -p integration-test --test nac
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `core_operations.rs` | 2 | NAC policy add, resource creation with identity |
| `operations.rs` | 2 | NAC CRUD operations under policy |
| `document_acp.rs` | 2 | Document-level ACP enforcement with NAC |
| `relation_admin.rs` | 2 | Admin relation management |
| `p2p_management.rs` | 2 | NAC with P2P replication |
| `cross_compartment_isolation.rs` | 2 | Cross-compartment access isolation |
| `policy_evolution.rs` | 2 | Policy updates and evolution |

**16 tests, 0 ignored.** All pass on both Go and Rust nodes.
