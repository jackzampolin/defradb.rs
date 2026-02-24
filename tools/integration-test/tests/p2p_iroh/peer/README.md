# Peer Tests

27 passing, 16 ignored.

## Files

- `crdt.rs` — PCounter/PNCounter CRDT replication (all pass)
- `create.rs` — Peer creation and subscription (2 ignored)
- `delete.rs` — Peer deletion lifecycle (all pass)
- `events.rs` — Peer event subscriptions (14 ignored)
- `schema.rs` — Schema version cross-replication (all pass)
- `update.rs` — Peer update and restart (all pass)

## Ignored Tests

### events.rs (14 tests)
All require peer event subscription API (GraphQL subscriptions or SSE event bus).
The Go tests listen for join/left events when peers subscribe/unsubscribe to
collections and documents. The Rust iroh transport emits these events internally
but the test harness doesn't yet expose an event subscription client.

### create.rs (2 tests)
- `create_with_p2p_collection` — iroh gossip is bidirectional, overrides one-way replicator semantics
- `create_with_collection_and_subscription` — needs GraphQL subscription support
