# Survey: `crates/cli/`

## Purpose
The `defra` binary's library half: clap-based command parsing (`cli.rs`,
`commands/`), config loading (`config/`), logging, and a set of `*_adapter` /
`*_syncer` / `*_pusher` modules. The adapters implement traits defined in other
crates (`p2p`, `acp`, `db`, `db_merge`) by delegating to the real logic there.
This is the composition root that wires concrete DB/blockstore/transport types
into the node at `start` time and forwards client commands over HTTP.

## State machines
None native to this crate. A grep for `enum *State/Status/Phase`, `transition`,
or `StateMachine` returns nothing. The modules that *look* stateful are pure
forwarders:
- `version_syncer.rs` / `transport_version_syncer.rs` — recursive CID ancestry
  walk (`VecDeque` + `HashSet`), but the merge decision lives in
  `db_merge::DbMergeHandler`; this is the same walk B3 abstracts from
  `dag_fetcher.rs`.
- `p2p_doc_pusher.rs` / `transport_doc_pusher.rs` — one-line delegation to
  `db_merge::push_existing_docs`.
- `acp_adapter.rs`, `doc_acp_adapter.rs`, `nac_adapter.rs`, `txn_adapter.rs`,
  etc. — trait impls over `acp` / `db` calls.

## Candidates
| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none) | — | — | — | — |

The two behaviors a reviewer might flag are not owned here:
- version-sync ancestry walk → covered by **B3 filtered-replication** (`Converge`,
  `INV_DagComplete`) which abstracts `db_merge`/`dag_fetcher.rs`.
- doc-push replication → covered by the **replicator-lifecycle** slice
  (`Replicator_DESIGN.md`), abstracting `push_existing_docs`.

## Verdict
**Plumbing — not model-worthy.** The CLI crate is glue: argument parsing,
config, and adapter wiring. All correctness-bearing protocol logic (DAG sync,
replication, ACP gating, merge convergence) lives in `db_merge` / `p2p` / `acp`
and is already modeled or surveyed under those crates. Integration tests
(`--test basic`, `--test p2p`, `--test acp`) cover the command surface.
