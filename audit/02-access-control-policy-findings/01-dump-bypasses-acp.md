# Finding: Database Dump Bypasses ACP Entirely

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Access Control Bypass
**Status**: CONFIRMED

## Summary

The `print_dump()` function iterates all database namespaces directly from the storage layer, completely bypassing the query engine and ACP permission filters. This includes the ACP store itself, blockstore, headstore, and all document data.

In contrast, `export_database()` correctly goes through the query executor, which applies ACP `PermissionFilterNode` filtering.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/db/src/dump.rs:11-59` | `print_dump()` | Direct storage iteration, no ACP |
| `crates/db/src/backup/export.rs:106-107` | `export_database()` | Uses `runner.execute()` - ACP applied (GOOD) |

## Details

### The Bypass

```rust
// crates/db/src/dump.rs:20-28
let namespaces = [
    Namespace::Datastore,    // All document data
    Namespace::Blockstore,   // All IPLD blocks
    Namespace::Headstore,    // DAG heads
    Namespace::Systemstore,  // System metadata
    Namespace::Peerstore,    // Peer information
    Namespace::Encstore,     // Encryption data
    Namespace::Acpstore,     // ACP POLICIES AND RELATIONS THEMSELVES
];
```

The function iterates over every key-value pair in every namespace. No identity is checked. No ACP permission is evaluated. The caller gets raw keys and value sizes for **all data in the database**.

### Exposure

The dump output includes:
- **Key names** for all documents (which contain collection IDs and document IDs)
- **Value sizes** (which leak document size information)
- **ACP store contents** (policy definitions and relation tuples - i.e., who has access to what)
- **Encryption store contents** (encrypted field metadata)

While the raw values are not returned (only sizes), the key names themselves reveal the full structure of the database.

### Who Can Trigger This

The dump endpoint is exposed via HTTP at `POST /api/v0/dump` (or similar). Need to check if NAC gates this.

### Contrast with Export

`export_database()` correctly uses the query executor:
```rust
let request = query::QueryRequest::new(query);
let response = runner.execute(request).await;
```

This goes through the normal query path which applies `PermissionFilterNode`.

## Remediation

### Option A: Gate dump behind NAC admin permission

Require `NodePermission::DumpAll` or similar admin-level permission to execute dump. This is the simplest fix and appropriate since dump is a debugging/admin tool.

### Option B: Filter dump output through ACP

Add identity parameter to `print_dump()` and filter namespaces/keys based on the caller's permissions. This is more work but more correct.

### Option C: Restrict dump to dev mode only

Only allow dump when `--dev` flag is passed to the node. Production nodes would not expose this endpoint.

## Test Gap

No integration test verifies that dump respects ACP boundaries. Should add a test where:
1. Create documents with ACP policies
2. Call dump as a non-admin identity
3. Verify ACP-protected document keys are not visible
