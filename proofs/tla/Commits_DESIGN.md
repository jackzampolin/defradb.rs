# Commits ACP Dual-Path Filtering

## Brainstorming Outcome

The footgun is not just "document queries need ACP." A protected document has two
observable content surfaces:

- the materialized User query result;
- the raw CRDT commit/delta blocks returned by `_commits` or delivered over P2P.

The safety property is therefore:

`INV_BothPathsGated`: for every reader `r` and protected document `d`, if `r` is not in
the ACP grant set for `d`, then `r` never obtains `d` via the User path, via local
`_commits`, or via replicated commit blocks.

The model uses the smallest witnessing shape: one protected document, two commit blocks,
one authorized owner, and one unauthorized reader/peer. Commit blocks are treated as
content-bearing because the deltas are enough to reconstruct the document. This keeps the
property about the missing gate, not about CRDT merge details.

## Source Anchors

- ACP permission being checked is `DocumentPermission::Read`; update/delete also imply
  read in `crates/acp/src/permission.rs:14`.
- The document ACP trait exposes document registration and `check_doc_access` in
  `crates/acp/src/dac.rs:40`, with registered docs denying anonymous/non-granted
  identities.
- Regular User query plans are wrapped with `PermissionFilterNode` when a collection has
  an ACP policy in `crates/query/src/planner/builder/mod.rs:160`.
- `PermissionFilterNode` calls
  `check_doc_access_with_overlay(..., DocumentPermission::Read, ...)` and fails closed in
  `crates/query/src/plan/permission_filter.rs:84`.
- The query runner routes `_commits` away from regular collection selects in
  `crates/query/src/runner/query/select.rs:30`.
- `_commits` fetches commit history, then applies a separate per-commit ACP filter in
  `crates/query/src/runner/commits.rs:654`, calling
  `check_doc_access_with_overlay(..., DocumentPermission::Read, ...)` at
  `crates/query/src/runner/commits.rs:792`.
- P2P merge-side ACP filtering is in `crates/db-merge/src/acp_merge_handler.rs:71`: with
  strict replicated-doc access enabled, protected composites require local
  `DocumentPermission::Read` before merge.
- Embedded setup wires that strict merge-side mode for SourceHub-backed ACP in
  `crates/embedded/src/node.rs:496`.
- P2P egress also installs a Bitswap peer-block request filter in
  `crates/p2p/src/behaviour.rs:287`, implemented in
  `crates/p2p/src/bitswap/filter.rs:45`.

## Model

`proofs/tla/Commits.tla:1` has three gate knobs:

- `UserGateMode` abstracts the regular materialized document query.
- `CommitsGateMode` abstracts `_commits` returning raw CRDT blocks.
- `ReplicationGateMode` abstracts P2P delivery of commit blocks to a peer.

Each knob is either `"ACP"` or `"Open"`. `Authorized(r, d)` is `r \in Grant[d]`.
`Open` models the bug class where that code path does not consult ACP.

`ObtainedContent(r, d)` becomes true if the reader gets the materialized document, any
local `_commits` block for `d`, or any replicated block for `d`.

## Runs

Run these from `proofs/tla`:

```bash
./tools/tlc -metadir states/commits_red_useronly -config MC_Commits_Red_UserOnly.cfg MC_Commits_Red_UserOnly.tla
./tools/tlc -metadir states/commits_red_repl -config MC_Commits_Red_ReplicationUngated.cfg MC_Commits_Red_ReplicationUngated.tla
./tools/tlc -metadir states/commits_green -config MC_Commits_Green.cfg MC_Commits_Green.tla
```

Observed verdicts on 2026-06-02:

- `MC_Commits_Red_UserOnly`: RED as expected. `INV_BothPathsGated` is violated in
  state 2 when `CommitsRead(eve, doc)` puts `{"create", "update"}` in
  `commitBlocks[eve]`.
- `MC_Commits_Red_ReplicationUngated`: RED as expected. `INV_BothPathsGated` is
  violated in state 2 when `ReplicateBlock(owner, eve, "create")` puts `"create"` in
  `receivedBlocks[eve]`.
- `MC_Commits_Green`: GREEN. TLC checked the complete bounded state graph with 5 states
  generated, 4 distinct states, depth 3, and no invariant violation.

## Boundary

This model proves the structural obligation that every content-bearing path must be gated
by the same ACP grant set. It does not prove Rust implementation conformance, CRDT merge
correctness, cryptographic confidentiality, or policy-store correctness. It also does not
model explicit authorized replay as a separate authority; the safety case here is the
non-authorized reader/peer path from the CLAUDE.md footgun.
