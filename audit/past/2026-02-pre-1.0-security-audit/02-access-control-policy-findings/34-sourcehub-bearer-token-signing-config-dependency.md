# Finding: SourceHub Bearer Token Requires Global Signing Config — Fails for Unknown DIDs

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Authentication / Operational Fragility
**Status**: CONFIRMED

## Summary

Every write operation in `SourceHubDocumentACP` requires a bearer token signed by the requestor's private key. The `create_bearer_token()` method looks up the signing config from a global store (`defra_core::signing::get_identity(did)`). If the DID's private key is not in the global store, the operation fails with `PermissionDenied`. This creates a tight coupling between the node's key store and SourceHub ACP operations — a node can only create bearer tokens for identities whose private keys it holds. This is fundamentally at odds with the P2P model where a node may need to operate on behalf of documents owned by remote identities.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/sourcehub/src/dac.rs:42-77` | `create_bearer_token()` | Requires DID in global signing store |
| `crates/sourcehub/src/dac.rs:102` | `register_doc_object()` | Calls `create_bearer_token(identity)` |
| `crates/sourcehub/src/dac.rs:189` | `add_actor_relationship()` | Calls `create_bearer_token(requestor)` |
| `crates/sourcehub/src/dac.rs:214` | `delete_actor_relationship()` | Calls `create_bearer_token(requestor)` |
| `crates/sourcehub/src/dac.rs:247` | `unregister_doc_object()` | Calls `create_bearer_token(&owner_did)` — owner may be remote |

## Details

### Bearer Token Creation

```rust
// crates/sourcehub/src/dac.rs:42-77
fn create_bearer_token(&self, did: &str) -> std::result::Result<String, acp::Error> {
    let signing_config = defra_core::signing::get_identity(did).ok_or_else(|| {
        acp::Error::PermissionDenied(format!("no signing config found for DID: {}", did))
    })?;
    // ... create JWT with 5-minute validity ...
}
```

### The `unregister_doc_object` Problem

The most severe manifestation is in `unregister_doc_object()`:

```rust
// crates/sourcehub/src/dac.rs:229-252
async fn unregister_doc_object(&self, policy_id: &str, resource_name: &str, doc_id: &str) -> Result<()> {
    // Query on-chain for the owner
    let (_is_registered, owner_did) = self.provider
        .query_object_owner(policy_id, resource_name, doc_id)
        .await.map_err(provider_err)?;

    // Create bearer token from OWNER's signing config
    let bearer_token = self.create_bearer_token(&owner_did)?;  // ← Owner may be remote!
    self.provider.archive_object(&bearer_token, ...).await.map_err(provider_err)
}
```

This method:
1. Queries SourceHub for the document's owner DID
2. Tries to create a bearer token using the **owner's** private key
3. The owner may be a remote identity whose private key this node doesn't have

This means `unregister_doc_object()` can only succeed when the owner's private key is in the local node's signing store — which is typically only the node operator's own identity.

### Impact on Operations

| Operation | Who's Bearer Token | Works When |
|-----------|-------------------|------------|
| `register_doc_object` | Document creator | Creator is local (typical) |
| `add_actor_relationship` | Requestor | Requestor is local (typical for HTTP requests) |
| `delete_actor_relationship` | Requestor | Requestor is local (typical for HTTP requests) |
| `unregister_doc_object` | Document **owner** | Owner's key is local (PROBLEMATIC) |

### P2P Implications

In P2P scenarios, the merge handler calls `check_doc_access()` which only does read queries (no bearer token needed). But if a collection is deleted or truncated, `unregister_doc_object()` is called for each document — and will fail for documents owned by remote identities.

### Contrast with Local ACP

`LocalDocumentACP::unregister_doc_object()` doesn't need a bearer token — it directly deletes the tuple from local storage. The SourceHub path adds an authentication requirement that doesn't exist in the local path, creating a functional asymmetry between providers.

## Mitigating Factors

1. In typical usage, a node only creates/unregisters its own documents
2. `register_doc_object` and relationship operations work correctly for local identities
3. `check_doc_access()` (the security-critical path) doesn't require bearer tokens — it uses public queries

## Remediation

1. For `unregister_doc_object`, consider using the node's own bearer token if the node has been granted admin/owner permissions on-chain
2. Document the operational constraint that nodes can only unregister documents they own
3. Add graceful handling for the case where owner's signing config is unavailable

## Test Coverage

No test verifies `unregister_doc_object` behavior when the owner's signing config is not in the local store.
