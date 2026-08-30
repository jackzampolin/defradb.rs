# Sync Tests

27 passing, 0 ignored.

## Files

- `branchable.rs` — Branchable collection sync: multi-head, branched versions, error cases (all pass)
- `doc.rs` — Document sync: single/multi-node, version sync, auto-subscribe behavior (all pass)
- `overlay_origin.rs` — A -> B -> C chain: a head hint the gossip overlay delivers from a peer the transport never dialed must not leave a permanently deferred pending root on C (pass)
- `overlay_rebroadcast.rs` — Same chain with `DEFRA_P2P_REBROADCAST_ON_MERGE=true`: C converges on A's documents through B's post-merge re-announcement alone (pass)
- `sync.rs` — Core iroh sync: document sync, collection version sync, branchable/CID error cases (all pass)
- `version.rs` — Collection version sync: initial, patch, view with lens transforms (all pass)

## Notes

The `version::with_view` and `version::with_view_activated_and_queried` tests require:
- The Go repo's copy lens WASM binary (build with `cd ~/go/src/github.com/sourcenetwork/defradb/tests/lenses && make build`)
- Nodes running in development mode (file-path WASM loading via HTTP is blocked in production mode)
