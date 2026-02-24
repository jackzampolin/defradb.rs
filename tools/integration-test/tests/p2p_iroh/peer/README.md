# Peer Tests

42 passing, 1 ignored.

## Files

- `crdt.rs` — PCounter/PNCounter CRDT replication (all pass)
- `create.rs` — Peer creation and subscription (1 ignored)
- `delete.rs` — Peer deletion lifecycle (all pass)
- `events.rs` — Peer event subscriptions (all pass)
- `schema.rs` — Schema version cross-replication (all pass)
- `update.rs` — Peer update and restart (all pass)

## Ignored Tests

### create.rs (1 test)

- `create_with_p2p_collection` — iroh gossip is bidirectional, overrides one-way replicator directionality semantics
