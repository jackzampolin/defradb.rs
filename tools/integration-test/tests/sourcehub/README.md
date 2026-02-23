# sourcehub/ — SourceHub integration tests

```
cargo test -p integration-test --test sourcehub -- --ignored
```

All tests require a running SourceHub node and are `#[ignore]` by default.

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `smoke.rs` | 1 | Basic SourceHub connectivity and token creation |
| `compartments.rs` | 1 | SourceHub compartment operations |
| `policy_lifecycle.rs` | 1 | Policy creation and lifecycle via SourceHub |
| `p2p_acp.rs` | 1 | ACP enforcement with SourceHub backend over P2P |
| `stubs.rs` | 4 | Circuit breaker trip/recovery, policy cache TTL expiry |

**0 active tests, 8 ignored.** All require SourceHub infrastructure.

## Running

These tests require a SourceHub node. Start one locally, then:

```
cargo test -p integration-test --test sourcehub -- --ignored --test-threads=1
```
