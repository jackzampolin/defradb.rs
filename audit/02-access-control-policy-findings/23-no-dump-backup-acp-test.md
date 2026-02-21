# Finding: No Integration Test for Dump or Backup/Restore with ACP

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM (test gap for HIGH vulnerabilities)
**Category**: Test Gap
**Status**: CONFIRMED
**Session**: S4 - Integration Test Validation
**Related Findings**: 01 (dump bypasses ACP — HIGH), 16 (debug dump no NAC check — MEDIUM)

## Summary

The `dump.rs` and `backup_restore.rs` integration tests contain zero ACP awareness. Neither test deploys an ACP policy, creates protected documents, or verifies that dump/backup operations respect access control boundaries. The `dump.rs` test is additionally `#[ignore]`d and runs without any ACP configuration.

## Evidence

### dump.rs (34 lines)

```rust
// Runs without .with_acp_local() — no ACP at all
#[tokio::test]
#[ignore]  // Currently disabled
async fn rust_dump() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    dump_test(cluster).await;
}
```

Grep for ACP-related terms in `dump.rs`: **zero matches** for `acp`, `policy`, `identity`, `permission`.

The test creates a plain document, calls `client.dump()`, and asserts the result array is non-empty. It does not:
- Deploy an ACP policy
- Create ACP-protected documents
- Verify that dump output excludes protected data
- Verify that dump output excludes ACP store contents (policy YAML, relation tuples)
- Test with any identity/authentication

### backup_restore.rs (160 lines)

Grep for ACP-related terms in `backup_restore.rs`: **zero matches** for `acp`, `policy`, `identity`, `permission`.

```rust
// Runs without .with_acp_local() — no ACP at all
for_each_runtime!(backup_restore, backup_restore_test);
```

The test creates documents without identity, exports/imports them, and verifies CID stability. It does not:
- Deploy an ACP policy
- Create ACP-protected documents with an identity
- Verify that backup export excludes documents the caller can't read
- Verify that ACP state (policies + relations) is preserved across backup/restore
- Verify that imported documents retain their ACP protection
- Test `backup_export` with an unauthorized caller identity

### Contrast with export_database()

Finding 01 confirmed that `export_database()` correctly uses the query executor with ACP (`runner.execute()`), while `print_dump()` bypasses it entirely. But no test verifies either path's ACP behavior.

## Missing Tests

### Dump with ACP

```
1. Deploy ACP policy + schema, create protected document as Alice
2. Call dump as unauthenticated user → should be rejected (currently: returns all data)
3. Call dump as non-admin → should be rejected
4. Verify dump output does NOT contain ACP store entries (policy YAML, relation tuples)
```

### Backup/Restore with ACP

```
1. Deploy ACP policy + schema, create protected document as Alice
2. Export as Alice (owner) → should include protected document
3. Export as Bob (no relation) → should exclude protected document
4. Truncate and import the backup as Alice
5. Verify ACP protection is preserved after restore (Bob still can't read)
6. Verify ACP relation tuples survive the round-trip
```

## Severity Rationale

MEDIUM because:
- Finding 01 (dump bypass) is already HIGH and confirmed
- The absence of tests means any fix to Finding 01 would lack regression coverage
- Backup/restore ACP preservation is a real production concern for data migration
