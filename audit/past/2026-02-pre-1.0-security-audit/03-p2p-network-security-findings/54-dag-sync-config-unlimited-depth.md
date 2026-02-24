# Finding 54: DagSyncConfig Default Has Unlimited Depth

**Severity**: LOW
**Category**: Configuration Audit
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

The `DagSyncConfig` supports a `max_depth` parameter but defaults to `None` (unlimited). This is mitigated by the poll-based DAG fetcher which caps at 20 iterations (Finding 37), but the DagSync component itself has no inherent depth limit.

## Evidence

**Default config** (`sync/dag_sync/config.rs:93-102`):
```rust
impl Default for DagSyncConfig {
    fn default() -> Self {
        Self {
            block_fetch_timeout: Duration::from_secs(30),
            max_depth: None, // Unlimited
            max_concurrent_fetches: NonZeroUsize::new(16).unwrap(),
        }
    }
}
```

**Good defaults**:
- `block_fetch_timeout: 30s` — prevents indefinite waiting per block
- `max_concurrent_fetches: 16` — limits parallel Bitswap fetches via `NonZeroUsize`

**Mitigating factor**: The primary DAG fetch path (`coordinator/dag_fetcher.rs:74`) caps at 20 iterations regardless of this config:
```rust
for iteration in 0..20 {
    // ...
}
```

## Assessment

The unlimited default depth is acceptable because:
1. The active fetch path has a 20-iteration cap
2. DefraDB DAGs are typically shallow (Collection → Composite → LWW = 3 levels)
3. The `max_concurrent_fetches: 16` limits parallel work

However, `max_depth: None` in the DagSync config could be a concern if that code path is used directly (bypassing the poll-based fetcher).

## Recommendation

Set `max_depth` default to `Some(NonZeroUsize::new(20).unwrap())` to match the poll-based fetcher's hardcoded cap.
