# Finding: SourceHub Policy Add is Non-Atomic — On-Chain Success + Local Cache Failure Leaves Orphaned Policy

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Consistency / State Divergence
**Status**: CONFIRMED

## Summary

The `SourceHubAcpAdapter::add_policy()` method performs a three-step sequence: (1) validate and store policy locally, (2) submit on-chain via SourceHub, (3) re-store under on-chain ID if it differs. Steps 2 and 3 are not atomic — if on-chain submission succeeds but the local re-cache in step 3 fails, the policy exists on-chain but the local node cannot find it by on-chain ID. Subsequent schema deployments referencing the on-chain policy ID will fail to resolve the policy locally.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/cli/src/sourcehub_acp_adapter.rs:73-77` | Step 1 | Local store with locally-computed ID |
| `crates/cli/src/sourcehub_acp_adapter.rs:79-84` | Step 2 | On-chain submission (irreversible) |
| `crates/cli/src/sourcehub_acp_adapter.rs:91-98` | Step 3 | Re-store under on-chain ID (can fail) |
| `crates/cli/src/doc_acp_adapter.rs:64-69` | Downstream | `validate_and_get_managing_relations` queries local store by on-chain ID |

## Details

### The Three-Step Sequence

```rust
// crates/cli/src/sourcehub_acp_adapter.rs:55-101
async fn add_policy(&self, yaml: &str) -> Result<String, String> {
    // Step 1: Validate and store locally with counter-derived ID
    let counter = self.counter.fetch_add(1, Ordering::SeqCst);
    let policy = acp::policy_yaml::build_policy(&parsed, counter)?;
    self.local_store.store_policy_with_options(&policy, &options).await?;

    // Step 2: Submit on-chain (IRREVERSIBLE)
    let policy_id = self.sourcehub_acp.add_policy("", yaml).await?;

    // Step 3: Re-store under on-chain ID (CAN FAIL INDEPENDENTLY)
    if policy_id != policy.id {
        let mut on_chain_policy = policy;
        on_chain_policy.id = policy_id.clone();
        self.local_store
            .store_policy_with_options(&on_chain_policy, &options)
            .await
            .map_err(|e| format!("failed to cache policy with on-chain ID: {}", e))?;
    }

    Ok(policy_id)
}
```

### Failure Scenarios

**Scenario A: Step 2 fails (on-chain rejection)**
- Local store has policy under counter-derived ID
- On-chain has nothing
- Counter has already incremented (non-reversible)
- Local store has an orphaned policy that shouldn't exist
- **Impact**: Cluttered local store; next add_policy uses counter+1

**Scenario B: Step 3 fails (local storage error after on-chain success)**
- Policy exists on-chain with on-chain ID
- Local store has policy under counter-derived ID only
- `DocumentAcpAdapter::validate_and_get_managing_relations()` queries by on-chain ID → returns `policy not found`
- Schema deployment with `@policy(id: "<on-chain-id>")` fails at relationship validation
- **Impact**: Node is stuck — policy is on-chain but can't be used locally until manual intervention

**Scenario C: Counter divergence**
- Counter starts at 1 on every node restart
- Counter is never synced with on-chain state
- Counter-derived IDs are transient (used only for step 1 DPI validation)
- This is benign IF step 3 always succeeds, but problematic if it doesn't

### The Counter Problem

```rust
// crates/cli/src/sourcehub_acp_adapter.rs:28-29
counter: AtomicU64::new(1),
```

The counter starts at 1 on every process startup. It's used by `build_policy()` to generate a locally-unique policy ID via double SHA-256. This ID has no relationship to the on-chain ID and exists purely for the DPI validation store. After on-chain submission returns the real ID, the policy is re-indexed. But:

1. On restart, counter resets to 1
2. If the same YAML is re-submitted, the same local ID is generated
3. `store_policy_with_options` may overwrite the previous entry
4. If the on-chain ID was different (e.g., due to on-chain counter state), the re-index target changes

### Why This Matters for SourceHub

In the local ACP path, the counter-derived ID IS the final ID — no re-indexing step exists. The adapter's three-step dance is unique to SourceHub and introduces a consistency window that doesn't exist in the local path.

## Mitigating Factors

1. Step 3 failure requires a local storage error, which is rare for in-memory or persistent stores
2. The `add_policy` method returns an error on step 3 failure, so the caller knows it failed
3. Retrying `add_policy` with the same YAML will get the same on-chain ID (idempotent on-chain)

## Remediation

1. If step 3 fails, attempt cleanup of the step 1 entry
2. Consider a "reconcile" endpoint that syncs local policy cache from on-chain state
3. Log the on-chain policy ID in the step 3 error message so operators can manually reconcile

## Test Coverage

No test verifies behavior when local cache write fails after successful on-chain submission.
