# acp/ — Access Control Policy tests

```
cargo test -p integration-test --test acp
```

## Files

| File | What it covers |
|------|----------------|
| `basic.rs` | Basic ACP policy add, document CRUD with identity |
| `custom_policy.rs` | Custom relation policy behavior beyond the stock owner/reader/updater/deleter layout |
| `policy_validation.rs` | Go parity for policy-add validation and YAML edge cases |
| `link_collection.rs` | Go parity for `@policy(id:, resource:)` DRI acceptance and rejection cases |
| `register_ops.rs` | Go parity for anonymous vs identity-backed document register/read/update/delete behavior |
| `relation_queries.rs` | Go parity for ACP-aware `COUNT`, `AVG`, and relation object queries across protected/public joins |
| `relationship.rs` | Go parity for document-actor relationship add/delete behavior |
| `index.rs` | ACP-aware index creation and indexed query filtering |
| `multi_identity.rs` | Multiple identities accessing the same document |
| `multi_role.rs` | Reader/writer role grants and enforcement |
| `node_access.rs` | Node-level access control |
| `p2p.rs` | ACP enforcement across P2P replication topologies |
| `revoke_lifecycle.rs` | Grant → revoke → re-grant lifecycle |
| `negative.rs` | Unauthorized access denial, _commits ACP, dump auth, anonymous create, NAC enforcement |
| `negative_p2p.rs` | P2P merge-denial, policy transition guards |
| `p2p_lifecycle.rs` | Go parity for DAC P2P add/update/delete/subscribe/replicator lifecycle families, adapted to local ACP receiving-node semantics |
| `audit.rs` | CID time-travel ACP bypass, policy transition boundary |
| `transaction_rollback.rs` | ACP visibility rollback on discarded transactions |
| `xarchive_access_matrix.rs` | Cross-archive access matrix |

The Rust ACP suite now covers the original Go DAC families for `add_policy`, `link_collection`, `register_and_{read,update,delete}`, `count`, `avg`, `relation_objects`, `relationship`, `index`, and the main `dac/p2p` lifecycle cases (`add`, `update`, `delete`, `subscribe`, `replicator`), plus Rust-only regression coverage for audit findings and transactional ACP behavior.

The remaining Go `dac/p2p` relationship-propagation families are not local-ACP parity gaps anymore:
- `replicator_with_doc_actor_relationship`
- `subscribe_with_doc_actor_relationship`

Rust already exercises those semantics in the SourceHub-backed iroh suite at `tools/integration-test/tests/p2p_iroh/acp/dac.rs`. Those tests are the Rust parity home for the final Go `dac/p2p` relationship families and require `sourcehubd` in the test environment, so they are tracked separately from the local-ACP suite.

The corresponding local-ACP product gap is tracked in issue `#772`: local ACP does not currently replicate document-actor relationship tuples across peers. That non-replication remains intentionally asserted in `negative_p2p.rs`.

### Ignored

| Test | Reason |
|------|--------|
| `go_commits_acp_denied` | Go does not filter `_commits` queries by ACP (upstream bug) |
| `go_cid_time_travel_acp_bypass` | Go does not have the CID time-travel ACP fix |
| `go_go_p2p_merge_denial` | Go does not carry owner DID in PushLog Creator field |
| `go_rust_p2p_merge_denial` | Go does not carry owner DID in PushLog Creator field |
| `go_go_acp_p2p` | Go does not carry owner DID in PushLog Creator field |
| `go_rust_acp_p2p` | Go does not carry owner DID in PushLog Creator field |
