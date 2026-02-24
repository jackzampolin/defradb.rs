# ACP (Access Control Policy) Tests

53 passing, 12 failing.

## Files

- `acp.rs` — Local ACP policy enforcement with iroh transport (2 failing)
- `dac.rs` — Document Access Control with permissioned replication (9 failing)
- `nac.rs` — Node Access Control via SourceHub (all pass)
- `trust_boundary.rs` — Trust boundary enforcement (1 failing)

## Failing Tests

All 12 failures involve document replication under controlled-mode access control.
Tests time out waiting for documents to replicate between iroh peers.

Root cause: main's security audit added `check_access_str()` to the sync coordinator
which calls `PeerState.is_connected()` using libp2p PeerId keys. Iroh peer IDs cannot
be parsed as libp2p PeerIds, so connected iroh peers are not recognized in controlled
mode. Only replicator-registered peers pass (via string-based replicator registry).

### acp.rs (2 tests)
- `iroh_acp_replication` — ACP-protected doc fails to replicate
- `iroh_acp_multi_identity` — multi-identity ACP replication fails

### dac.rs (9 tests)
- `replicator_permissioned_local` — permissioned replication with local ACP
- `replicator_permissioned_sourcehub` — permissioned replication with SourceHub ACP
- `replicator_with_doc_actor_relationship` — doc-actor relationship replication
- `subscribe_add_get_permissioned_local` — subscribe with local ACP permissions
- `subscribe_add_get_permissioned_sourcehub` — subscribe with SourceHub permissions
- `subscribe_add_get_with_doc_actor_relationship` — subscribe with doc-actor relations
- `create_private_sync_after_relationship` — private doc sync after relationship grant
- `delete_private_docs_different_nodes` — delete private docs across nodes
- `update_private_docs_different_nodes` — update private docs across nodes

### trust_boundary.rs (1 test)
- `iroh_trust_boundary` — trust boundary enforcement between iroh peers

Tracked in issue #501.
