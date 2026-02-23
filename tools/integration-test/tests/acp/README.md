# acp/ — Access Control Policy tests

```
cargo test -p integration-test --test acp
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `basic.rs` | 2 | Basic ACP policy add, document CRUD with identity |
| `multi_identity.rs` | 2 | Multiple identities accessing the same document |
| `multi_role.rs` | 2 | Reader/writer role grants and enforcement |
| `node_access.rs` | 2 | Node-level access control |
| `p2p.rs` | 3 | ACP enforcement across P2P replication topologies |
| `revoke_lifecycle.rs` | 2 | Grant → revoke → re-grant lifecycle |
| `negative.rs` | 4 | Unauthorized access denial, _commits ACP |
| `negative_p2p.rs` | 5 | P2P merge-denial, policy transition guards |
| `audit.rs` | 2 | CID time-travel ACP bypass |
| `xarchive_access_matrix.rs` | 2 | Cross-archive access matrix |

**29 active tests, 5 ignored.**

### Ignored

| Test | Reason |
|------|--------|
| `go_commits_acp_denied` | Go does not filter `_commits` queries by ACP (upstream bug) |
| `go_cid_time_travel_acp_bypass` | Go does not have the CID time-travel ACP fix |
| `rust_rust_p2p_merge_denial` | ACP relationship grants replicate with document data — merge-denial not enforced |
| `go_go_p2p_merge_denial` | Same as above |
| `go_rust_p2p_merge_denial` | Same as above |
