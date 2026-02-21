# Finding: SourceHub DocumentACP Ignores Managing Relations Parameter

**Stream**: 02 - Access Control Policy
**Severity**: LOW
**Category**: Provider Equivalence Gap
**Status**: CONFIRMED

## Summary

`SourceHubDocumentACP::add_actor_relationship()` and `delete_actor_relationship()` accept a `_managing_relations: &[String]` parameter but never use it (prefixed with `_` to suppress unused warnings). The `DocumentAcpAdapter` computes managing relations from the local policy cache and passes them in, but the SourceHub implementation ignores them entirely, delegating authorization to the on-chain SourceHub module.

This is **architecturally correct** — SourceHub validates manager permissions on-chain. But it means the managing relations validation in `DocumentAcpAdapter` (lines 57-87) is dead code when running in SourceHub mode: it validates against the local cache, passes the result to `SourceHubDocumentACP`, which ignores it and lets SourceHub validate independently.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/sourcehub/src/dac.rs:187` | `_managing_relations` | Parameter ignored |
| `crates/sourcehub/src/dac.rs:212` | `_managing_relations` | Parameter ignored |
| `crates/cli/src/doc_acp_adapter.rs:110-112` | `validate_and_get_managing_relations()` | Computed but ignored by SourceHub |
| `crates/acp/src/local.rs` (add_actor_relationship) | `managing_relations` | USED for local validation |

## Details

### SourceHub Path (ignores managing relations)

```rust
// crates/sourcehub/src/dac.rs:179-202
async fn add_actor_relationship(
    &self,
    requestor: &Did,
    target: &Did,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
    relation: &str,
    _managing_relations: &[String],  // ← IGNORED
) -> Result<bool> {
    let bearer_token = self.create_bearer_token(requestor.as_str())?;
    let subject = did_to_subject(target);
    self.provider.set_relationship(&bearer_token, policy_id, resource_name, doc_id, relation, &subject)
        .await.map_err(provider_err)
}
```

### Local Path (uses managing relations)

In `LocalDocumentACP::add_actor_relationship()`, the managing relations are used to verify that the requestor has the right to grant the relation — checking that the requestor holds one of the managing relations for the target relation.

### The Redundancy

The flow for SourceHub relationship operations:

1. `DocumentAcpAdapter::add_doc_relationship()` called
2. `validate_and_get_managing_relations()` queries **local cache** for policy → validates relation exists → computes managers
3. Passes managers to `SourceHubDocumentACP::add_actor_relationship()`
4. SourceHub ignores the managers parameter
5. SourceHub validates authorization **on-chain** using its own manager logic

Steps 2-3 are redundant validation against the local cache. If the local cache is stale (see finding 32), step 2 could incorrectly reject a valid relation that exists on-chain but not in the local cache.

### Security Implications

**Not a vulnerability** — SourceHub performs its own authorization check on-chain. The risk is:
1. **False rejection**: Local cache rejects a valid relation that exists on-chain
2. **Inconsistent error messages**: Errors from local validation vs on-chain validation differ
3. **Behavioral divergence**: Local path validates managers locally; SourceHub path validates on-chain — different error timing and messages

## Mitigating Factors

1. SourceHub's on-chain validation is authoritative — this is correct behavior
2. The `DocumentAcpAdapter` validation is a belt-and-suspenders check (redundant but not harmful)
3. In practice, the local cache is populated by the same node that adds relationships

## Remediation

1. Consider skipping `validate_and_get_managing_relations()` in SourceHub mode to avoid false rejections from stale cache
2. Or ensure local cache is always in sync before validating (see finding 32)

## Test Coverage

No test verifies that SourceHub relationship operations work correctly when local cache is stale or missing the policy.
