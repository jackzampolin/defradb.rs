# FFI Test Coverage: Comprehensive on Feature Branch, Not on Main

- **Severity:** MEDIUM
- **Category:** Test Coverage / FFI Safety
- **Status:** Confirmed — extensive coverage on `jack/ffi-rust-compat` branch, absent from `main`

## Summary

The `jack/ffi-rust-compat` branch contains a comprehensive Go Rust FFI client at `tests/clients/rustffi/` that directly calls ~73 of 84 Rust FFI entry points via CGO. This client is used by Go integration tests when `DEFRA_CLIENT_RUST_FFI=true` is set, exercising the full FFI boundary. However, on the `main` branch, this client wrapper does not exist — the `tests/clients/rustffi/` directory contains only the compiled library with no Go source files.

## Affected Files

- `tests/clients/rustffi/defra.go` (on `jack/ffi-rust-compat`) — 2298-line Go wrapper calling 73+ Rust FFI functions
- `tests/clients/rustffi/wrapper.go` (on `jack/ffi-rust-compat`) — High-level client implementing `clients.Client` interface
- `tests/clients/rustffi/identity.go` (on `jack/ffi-rust-compat`) — Identity management for FFI tests
- `tests/integration/client.go` (on `jack/ffi-rust-compat`) — Adds `DEFRA_CLIENT_RUST_FFI` env var and `RustFFIClientType`
- `tools/ffi-test/src/runner.rs` — Orchestrates FFI test runs

## Details

### Coverage on `jack/ffi-rust-compat` Branch

The Go wrapper calls these Rust FFI functions directly via CGO using snake_case names:

**Fully covered (73 functions):**
- Core: `defra_init`, `defra_version`, `defra_free_string`, `new_node`, `node_close`
- Query: `exec_request`, `exec_request_in_txn`
- Schema: `add_schema`, `get_collections`, `get_collections_in_txn`
- Transactions: `begin_txn`, `commit_txn`, `rollback_txn`
- Collections: `get_collection_by_name`, `has_collection`, `delete_collection`, `truncate_collection`, `find_collection_by_id`, `set_active_collection_version`, `patch_collection`, `get_collection_by_version_id`, `delete_collection_versions`
- Views: `add_view`, `refresh_views`
- Migrations: `set_migration`, `set_migration_in_txn`
- Lens: `lens_add`, `lens_list`
- Indexes: `create_index`, `delete_index`, `get_indexes`, `list_all_indexes`
- Encrypted indexes: `add_encrypted_index`, `delete_encrypted_index`, `list_encrypted_indexes`, `list_all_encrypted_indexes`
- NAC: `get_nac_status`, `enable_nac`, `disable_nac`, `re_enable_nac`, `add_nac_actor_relationship`, `delete_nac_actor_relationship`
- DAC: `add_dac_policy`, `add_dac_actor_relationship`, `delete_dac_actor_relationship`
- Identity: `get_node_identity`
- Block: `block_verify_signature`
- Subscriptions: `create_subscription`, `poll_subscription`, `close_subscription`, `poll_graphql_subscription`, `close_graphql_subscription`, `create_merge_complete_subscription`
- P2P: `new_node_with_p2p`, `p2p_peer_info`, `p2p_active_peers`, `p2p_connect`, `p2p_add_replicator`, `p2p_delete_replicator`, `p2p_list_replicators`, `p2p_retry_replicators`, `p2p_add_collections`, `p2p_delete_collections`, `p2p_list_collections`, `p2p_add_documents`, `p2p_delete_documents`, `p2p_list_documents`, `p2p_sync_documents`, `p2p_sync_branchable_collection`, `p2p_sync_collection_versions`
- Backup: `basic_export`, `basic_import`
- SE: `set_se_encryption_key`

**Not covered (~11 functions):**
- `batch_start`, `batch_sign` — Batch signing not wrapped
- `collection_create` — Document creation via FFI (tests use GraphQL mutations instead)
- `delete_documents` — Document purge not wrapped
- `is_json_array`, `parse_duration`, `parse_string_array` — Utility/parse functions
- `get_dac_policy`, `list_dac_policies` — DAC policy read operations
- `create_identity`, `RegisterIdentity` — Identity creation/registration

### Memory Management Pattern

The Go wrapper follows a consistent and correct pattern:
```go
result := C.some_ffi_function(...)
if result.status != 0 {
    err := C.GoString(result.error)
    C.defra_free_string(result.error)  // Always freed
    return ..., fmt.Errorf("ffi: ... failed: %s", err)
}
value := C.GoString(result.value)
C.defra_free_string(result.value)  // Always freed
```

All CStrings passed TO Rust are allocated with `C.CString` and freed with `defer C.free`. All CStrings returned FROM Rust are freed with `C.defra_free_string`. This pattern is consistent across all 73 wrapped functions.

### Gap: Not on Main Branch

The coverage is excellent on the feature branch but:
1. The `main` branch has no Go Rust FFI client wrapper
2. CI on `main` does not run FFI integration tests against the Rust library
3. Regressions in the Rust FFI layer could be introduced on `main` without detection

## Remediation

1. **Merge the `jack/ffi-rust-compat` branch** (or its FFI client portion) to `main`
2. **Add CI step** that runs at least a smoke test subset of FFI integration tests
3. **Add Go wrappers** for the 11 missing functions (batch signing, identity creation, document purge, parse utilities)

## Test Gap

On `main`: 84/84 functions lack cross-language testing.
On `jack/ffi-rust-compat`: ~11/84 functions lack Go wrappers (batch signing, identity creation, parse utilities).
