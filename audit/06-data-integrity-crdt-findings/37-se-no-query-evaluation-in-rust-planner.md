# Finding: SE Query Evaluation Not Integrated into Rust Query Planner/Runner

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: HIGH (encrypted index queries cannot be evaluated on Rust nodes)
**Category**: Searchable Encryption / Query Integration
**Status**: NEW (1.0 gap)

## Summary

The Rust query planner and runner have no references to SE artifacts, encrypted indexes, or SE query evaluation. While the SE coordinator has `to_field_queries()` for converting search values to tags, and the storage module has `fetch_doc_ids()` for local tag lookup, neither is called from the query execution path. This means encrypted index queries on a Rust node either ignore the encrypted index or fail.

## Evidence

### Query Runner Has No SE References

Grep for `artifact|Artifact|se::|SECoordinator|fetch_doc_ids|store_artifacts` in `crates/query/src/` returned zero matches in the runner or planner modules. The only matches were in unrelated modules (response formatting, SDL parsing).

### Query Planner Has No Encrypted Index Selection

The planner's index selection logic (`crates/query/src/planner/index_selection/`) was searched for `encrypted|SE|search_tag|artifact` — no matches. The planner does not consider encrypted indexes when building query plans.

### SE Storage API Exists but Is Unused in Query Path

`crates/db/src/se/storage.rs:77-136` — `fetch_doc_ids` is defined and tested, but no call site exists in the query runner. It's only used in tests.

### SE Coordinator's to_field_queries Exists but Is Unused

`crates/db/src/se/coordinator.rs:146-171` — `to_field_queries` converts equality filter predicates into SE tag queries. No call site exists outside tests.

### Integration Test Doesn't Test SE Queries

`tools/integration-test/tests/encrypted_index.rs:59-64`:

```rust
// Verify queries still work with encrypted indexes
let products = node
    .query("query { Product { name sku price } }")
    .expect("query products");
```

This query retrieves ALL products — it does not test an equality filter that would require SE index lookup (e.g., `query { Product(filter: {name: {_eq: "Widget"}}) { ... } }`).

## Impact

### Encrypted Index Queries Don't Use the Index

When a user creates an encrypted index and then queries with a filter on that field:

1. The planner doesn't know about encrypted indexes → treats it as a regular filter
2. The query does a full collection scan with post-filter decryption
3. On a replicator node (which stores encrypted data), the query cannot decrypt → returns no results

### P2P SE Workflow Broken End-to-End

The full SE workflow requires:
1. Producer generates artifacts (IMPLEMENTED)
2. Producer pushes artifacts to replicator (IMPLEMENTED)
3. Replicator stores artifacts (NOT IMPLEMENTED — Finding 34)
4. Producer queries replicator with tags (NOT IMPLEMENTED — no planner/runner integration)
5. Replicator does tag lookup and returns doc IDs (STORAGE EXISTS but not wired up)

Steps 3-5 are all missing on the Rust side.

## Affected Code

- `crates/query/src/planner/` — no encrypted index support
- `crates/query/src/runner/` — no SE query evaluation
- `crates/db/src/se/coordinator.rs:146-171` — `to_field_queries` unused in production
- `crates/db/src/se/storage.rs:77-136` — `fetch_doc_ids` unused in production

## Remediation

### Phase 1: Planner Integration

Modify the index selection logic to recognize encrypted indexes and generate SE query plans when an equality filter matches an encrypted-indexed field.

### Phase 2: Runner Integration

In the query runner, when executing an SE query plan:
1. Create `SECoordinator` with the current identity and SE key
2. Call `to_field_queries()` to convert filter predicates to tag queries
3. For local queries: call `fetch_doc_ids()` directly
4. For remote queries: send `QuerySEArtifactsRequest` to replicator via P2P

## Test Gap

- No integration test for SE equality filter queries
- No integration test for SE queries against a replicator
- `encrypted_index.rs` only tests index CRUD, not query evaluation
