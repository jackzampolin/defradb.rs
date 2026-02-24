# Finding: Recovery Mode Bypasses On-Chain SourceHub Permissions

**Stream**: 02 - Access Control Policy
**Severity**: HIGH
**Category**: Authentication Bypass / Provider Equivalence
**Status**: CONFIRMED

## Summary

The `AcpMergeHandler` recovery bypass (finding 00) applies equally to SourceHub mode. During recovery (`is_recovery=true`), all ACP checks are skipped — including on-chain SourceHub permission verification. This means blocks are merged without querying SourceHub, bypassing the entire on-chain authorization model. While recovery mode bypass was already identified as HIGH severity for local ACP, the SourceHub case is more severe because: (1) the on-chain model is the authoritative source of truth, and (2) the bypass creates an unauditable divergence between on-chain state and local state.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/db/src/acp_merge_handler.rs:192-205` | Recovery bypass | Skips ACP for ALL providers including SourceHub |
| `crates/cli/src/commands/start/server.rs:486-520` | Provider selection | Same `AcpMergeHandler` used for SourceHub |

## Details

### The Bypass Code

```rust
// crates/db/src/acp_merge_handler.rs:192-205
async fn handle_block(&self, cid: &Cid, block_data: &[u8], metadata: BlockMetadata<'_>)
    -> Result<MergeOutcome, Self::Error>
{
    if metadata.is_recovery {
        tracing::debug!(cid = %cid, "Recovery mode: delegating to inner handler without ACP check");
        return self.inner.handle_block(cid, block_data, metadata).await.map_err(Into::into);
    }
    // ... normal ACP checks ...
}
```

### Why This Is Worse for SourceHub

In **local ACP mode**:
- Recovery bypass skips local tuple checks
- The tuples themselves are in the same database being recovered
- Recovery is self-consistent — the ACP state and document state recover together
- Admin who triggers recovery has physical access to the node

In **SourceHub ACP mode**:
- Recovery bypass skips ON-CHAIN permission verification
- On-chain state is the authoritative record of who can access what
- Recovery creates local state that may violate on-chain permissions
- The on-chain ledger has no record of the recovery bypass
- There is no on-chain audit trail of blocks merged during recovery

### Attack Scenario

1. Attacker gains ability to trigger recovery mode (see finding 00 for vectors)
2. In SourceHub mode, recovery merges all pending blocks without on-chain checks
3. Blocks from unauthorized peers are merged into the database
4. On-chain SourceHub shows the attacker has no permissions
5. But the local database contains their unauthorized modifications
6. No on-chain audit trail records the bypass

### Provider-Agnostic Design Flaw

The `AcpMergeHandler` uses `Arc<dyn DocumentACP>` — it's provider-agnostic. This means the recovery bypass was designed with local ACP in mind (where recovery mode re-ingesting local data is reasonable) but applies identically to SourceHub (where bypassing on-chain authorization is a more severe violation).

## Mitigating Factors

1. Recovery mode requires specific triggers (version sync, startup recovery)
2. The same mitigating factors from finding 00 apply
3. In practice, SourceHub deployments may have additional operational controls

## Remediation

1. In SourceHub mode, recovery should still query on-chain permissions (SourceHub is external and unaffected by local recovery)
2. Or add a provider-specific recovery policy: local ACP allows bypass, SourceHub ACP requires on-chain checks
3. At minimum, log recovery operations at WARN level in SourceHub mode with explicit messaging about on-chain bypass

## Test Coverage

No test verifies recovery mode behavior in SourceHub ACP mode.
