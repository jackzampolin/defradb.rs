# Finding: SourceHub Policy Cache Has No Refresh Mechanism — Stale Reads Permanent

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Cache Staleness / Consistency
**Status**: CONFIRMED

## Summary

The `SourceHubAcpAdapter` reads policies exclusively from the local `ZanzibarStore` cache — `list_policies()` and `get_policy()` never query SourceHub on-chain. There is no cache invalidation, no TTL, no periodic refresh, and no on-demand sync mechanism. Policies added by other nodes (or directly on-chain) will never appear in a node's local policy list. More critically, if a policy is somehow removed or superseded on-chain, the local node continues to enforce it indefinitely.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/cli/src/sourcehub_acp_adapter.rs:103-111` | `list_policies()` | Reads from local store only |
| `crates/cli/src/sourcehub_acp_adapter.rs:113-121` | `get_policy()` | Reads from local store only |
| `crates/cli/src/doc_acp_adapter.rs:64-69` | `validate_and_get_managing_relations()` | Queries local store by policy ID — fails if policy not cached |

## Details

### Read Path: Local Cache Only

```rust
// crates/cli/src/sourcehub_acp_adapter.rs:103-111
async fn list_policies(&self) -> Result<Vec<PolicyInfo>, String> {
    let policies = self
        .local_store
        .list_policies()
        .await
        .map_err(|e| format!("failed to list policies: {}", e))?;
    Ok(policies.iter().map(policy_to_info).collect())
}
```

The adapter's `list_policies()` and `get_policy()` are HTTP API operations — they power the `GET /api/v0/acp/policy` endpoint. They always return what's in the local store, never what's on-chain.

### Write Path: On-Chain + Local Cache

Only `add_policy()` touches both on-chain and local. There is no `sync_policies()`, `refresh_policy()`, or similar.

### Staleness Scenarios

**Scenario 1: Multi-Node Policy Creation**
- Node A creates policy P1 on-chain and caches it locally
- Node B's local store does NOT see P1 via `list_policies()`
- Node B receives a replicated document that references P1
- `DocumentAcpAdapter::validate_and_get_managing_relations("P1", ...)` fails — policy not in local store
- Relationship operations (grant/revoke) on Node B fail for documents under P1

**Scenario 2: On-Chain Policy Update**
- SourceHub governance mechanism updates or supersedes a policy
- Local node continues to use the old cached version
- Permission checks via `check_doc_access()` go on-chain (correct) but relationship validation against cached policy structure may diverge

**Scenario 3: Node Restart**
- On restart, the local store (if persistent) retains cached policies
- Policies added by other nodes during downtime are not discovered
- Node is blind to the broader policy landscape

### Contrast with Permission Checks

Notably, **permission checks** (`check_doc_access`) do NOT use the local cache — they go on-chain via `SourceHubDocumentACP::check_doc_access()` → `provider.verify_access()`. This is correct. The staleness only affects:
1. Policy listing/viewing (cosmetic)
2. Relationship validation (functional — blocks grant/revoke operations)
3. Schema deployment referencing uncached policy IDs

### Contrast with Go Implementation

The adapter comments on line 88-89 state:
> "Go doesn't cache locally at all (it queries Source Hub on-demand)"

This means Go has no staleness problem — it always queries SourceHub. The Rust implementation introduced local caching as a performance optimization, creating a consistency gap that doesn't exist in Go.

## Mitigating Factors

1. Permission checks (the security-critical path) go on-chain — not affected by cache staleness
2. In typical single-node deployments, only one node creates policies
3. The P2P test (`sourcehub_p2p_acp.rs`) works around this by explicitly calling `acp_policy_add()` on Node 1 to cache the policy locally

## Remediation

1. Add a `sync_policies()` method that queries SourceHub for all policies and updates the local cache
2. On `get_policy()` miss, fall back to on-chain query before returning None
3. Add a periodic refresh timer or cache TTL
4. Match Go behavior: query SourceHub on-demand for `validate_and_get_managing_relations()`

## Test Coverage

No test verifies behavior when a node queries for a policy it hasn't locally cached. The P2P test explicitly works around this gap.
