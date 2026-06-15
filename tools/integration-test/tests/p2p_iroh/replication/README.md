# Replication Tests

40 passing, 0 ignored.

## Files

- `collection_sub.rs` — Collection subscription: add/remove/get P2P collections, error cases (all pass)
- `document.rs` — Document subscription: single/multi-doc sync via iroh (all pass)
- `document_sub.rs` — Document-level subscriptions: add/remove/sync, error handling (all pass)
- `replication.rs` — Core replication: batch, update, delete, GraphQL filter queries over replicated data (all pass). Note: the filter test reads replicated data with a `filter:` argument; it does NOT cover replication-side filtering (a replicator predicate gating which documents are pushed). Filtered-replication coverage over iroh lives in `tools/integration-test/tests/p2p/filtered_replication.rs` (the `*_iroh` tests).
- `replicator.rs` — Replicator lifecycle: CRUD, CRDT counters, restart persistence (all pass)
