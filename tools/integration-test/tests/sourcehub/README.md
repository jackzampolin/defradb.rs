# sourcehub/ — SourceHub integration tests

```
cargo test -p integration-test --test sourcehub
```

Tests start their own SourceHub devnet automatically via `TestCluster::builder().with_source_hub()`.

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `smoke.rs` | 1 | Basic SourceHub connectivity and token creation |
| `compartments.rs` | 1 | Multi-identity compartment isolation (5 identities, 3 policies) |
| `policy_lifecycle.rs` | 1 | Full on-chain policy lifecycle: create, verify, grant, revoke |
| `p2p_acp.rs` | 1 | ACP enforcement with SourceHub backend over P2P replication |
| `stubs.rs` | 4 | Circuit breaker fail-closed, policy cache with grant/revoke cycle |

**6 active, 2 ignored** (Go+SourceHub not yet supported).
