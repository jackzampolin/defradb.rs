# B3 — Filter-Aware DAG Completeness: TLA+ Model Design

**Date:** 2026-06-02 · **Branch:** `feat/p2p-tla-modeling` · **Status:** approved, pre-implementation

This is the design for a TLA+ model of DefraDB.rs's filtered P2P replication.
Its job is to settle one decision that gates `#1013` (consumed by the sibling
`defradb.rs-p2p-control` worktree): **Model A vs Model B for filtered
replication.** The model reproduces the Go `#2721` "partial DAG sync will never
merge" failure, then proves which fix holds for which deployment.

Scope of this session: **M1 (convergence baseline) + M2 (the filter).** The
`#1012 A2` management-channel auth model (M3) is deferred to its own spec.

---

## Deployment target (why this model, not an abstraction in a vacuum)

The consumer is **defra-agent**. Agents accept `AgentRequest` documents; remote
control happens by writing a request locally and gossiping it over P2P. Requests
are handled per-DID. With many agents on one network, today **every agent
replicates every other agent's request traffic** and filters by DID at query
time — noisy, and agents hold data they shouldn't (a relevance *and*
access-control *and* resource problem).

Grounded facts (from the defra-agent + Go DefraDB source):

| Fact | Source |
|---|---|
| `AgentRequest` is `@branchable` — CRDT branching is real | `defra-agent/crates/defra-agent-protocol/schemas/agent/agent_request.graphql` |
| `agent_did` is the recipient/filter field, and it is **immutable** | same schema; filter applied at `defra-agent/crates/defra-agent/src/watcher/query.rs:66` |
| Filtering today is **read/claim-time**, not P2P-level | `watcher/query.rs`, `trigger_engine/subscription_source.rs:28` |
| Every agent subscribes collection-wide (`add_collections`) | `defra-agent-desktop-core/src/client/schema.rs:36` |
| Requests carry cross-doc relational FKs (`caused_by_parent_request_id`, `retry_parent_request`, `retry_root_request`, `superseded_by_request`) — **not** CRDT `Heads` links | `agent_request.graphql` |
| Delta-DAG is **per-document**; `Heads` links are within-document; no cross-doc causal edges | Go `internal/core/block/block.go`; Rust `crates/db-merge/src/merge_handler/` |
| `#2721` hole = **branching + partial sync** (a head whose parent on another branch wasn't fetched); blocks all merges for the doc | Go `internal/db/merge.go` `loadComposites`; test `TestMerge_DualBranchWithOneIncomplete_CouldNotFindCID` |
| `#2721`'s **shipped fix is Model A**: "walk the entire graph before merging" — Rust already does this | Go issue `#2721`; Rust `coordinator/dag_fetcher.rs`, full-ancestry walk before merge |
| Field-level filtering is a **future GraphSync feature**, not today's reality | Go `block.go` `DAGLink.Name` comment |

### The key insight: `agent_did` is immutable → two separable failure classes

Because a request's owner DID never changes, **a single document never flips in
or out of the filter mid-history.** The defra-agent filter is therefore purely
*document-grain*. That cleanly splits correctness into two classes that have
*different* answers:

1. **Within a subscribed document (same DID).** Concurrent `status`/
   `lifecycle_state` mutations branch the DAG; partial sync can leave a head
   with a missing parent → `#2721`. Fixed by **Model A** (full within-doc
   ancestry walk before merge). Always works here, because owner is immutable so
   within-doc ancestry is never filtered out.

2. **Across documents (relational FKs to other DIDs).** A subscribed request
   references a request owned by another DID that is *intentionally not
   replicated*. The question is whether that dropped edge is **safe**. It is —
   relational FKs are not CRDT merge dependencies — and dropping it *is* the
   access-control / relevance win.

`agent_did` document-grain filtering therefore needs **Model A only**. Model B
becomes necessary only under the *field-grain* GraphSync future, where a
document's own causal ancestry can be filtered out.

---

## Abstraction (spec symbols → real things)

| Spec symbol | Real thing |
|---|---|
| `Agents` | nodes, each with a stable `DID` |
| `Docs` | `AgentRequest` documents; each has immutable `owner(d) ∈ DID` (the `agent_did`) |
| `Block` | a CRDT composite delta (head). Has `doc(b)`, `parents(b) ⊆ Block` (within-doc `Heads`) |
| branching | two agents mutate one request concurrently → two heads sharing a parent |
| `relRef(d) ⊆ Docs` | cross-doc relational FKs (parent/retry/supersede) — **not** causal parents |
| `subscribed(a, d)` | filter predicate. **Doc grain (defra-agent):** `owner(d) = DID(a)`. **Field grain (GraphSync):** a per-field predicate that can exclude some `parents(b)` |
| `store`, `heads`, `syncing`, `synced` | blockstore presence + `DagSyncState` (`crates/p2p/src/sync/dag_sync/state.rs`) |

## State machine (actions)

`CreateRequest` · `MutateRequest` (may branch) · `Gossip(head)` (announce CID
only — mirrors `broadcast_update`) · `BeginFetch` / `FetchBlock` (Bitswap pull of
a missing parent) · `Merge(b)` **guarded: all `parents(b)` present locally**
(mirrors `loadComposites` recursion) · `Drop(a, d)` (filtered peer declines to
subscribe). Weak fairness on delivery/fetch for the liveness properties.

**The naive / A / B difference is a single guard on the fetch action:** does the
ancestry walk fetch a parent even when that parent's block/field is filtered out?

---

## Invariants (the deliverable)

### M1 — Convergence baseline (no filter, document grain)
- **`INV_DagComplete`** (safety): no merged block lacks a locally-present parent.
  *Naive sync (merge a head without walking) must violate this → reproduces `#2721`.*
- **`INV_Converge`** (liveness): under fair delivery, all agents reach the same
  per-doc head-set. *Model A (full-ancestry walk before merge — the shipped Go
  fix) must satisfy it.*

### M2 — DID-predicate filter (document grain = defra-agent)
- **`INV_SubsetConverge`**: for docs where `owner(d) = DID(a)`, agent `a`
  converges exactly as an unfiltered node would. *Claim: holds under Model A,
  because owner is immutable so within-doc ancestry is never filtered out.*
- **`INV_RelRefSafe`**: dropping a cross-DID relational reference never blocks any
  CRDT merge. *This is the access-control / relevance win — proves filtering out
  foreign-DID docs is sound. Also surfaces the `#2717` read outcome: a dangling
  ref resolves to "absent" — a tolerated, intended result, not a merge failure.*

### M2 — field-grain branch (GraphSync future / resource-constrained peers)
- Show *naive* field-level filtering re-creates a `#2721` hole *inside* a
  document's DAG, and that **Model A defeats the resource-savings purpose** (it
  fetches the filtered ancestry anyway). Then evaluate whether **Model B**
  (placeholder / skip nodes for filtered parents) satisfies a weaker
  **`INV_VisibleConverge`** *without* fetching filtered ancestry.

---

## A-vs-B recommendation (handed to `defradb.rs-p2p-control`)

> **For defra-agent's `agent_did` filter: Model A.** Push the DID predicate down
> to the subscription/replication layer, keep full within-document ancestry
> fetch, and rely on `INV_RelRefSafe` to drop foreign-DID relational refs.
> **Model B is not needed** for this deployment — the filter is document-grain on
> an immutable field. **Model B is required only if/when field-level GraphSync
> filtering ships** for resource-constrained peers. Each half is backed by a TLC
> verdict: counterexample for naive; green for A on the subscribed set; the B
> trade-off characterized for the field grain.

---

## Tooling & layout

- **TLC first** (explicit-state; best for the liveness props at N=2–3 agents,
  ≤~6 blocks), written **Apalache-compatible** (type annotations, avoid TLC-only
  idioms). Re-evaluate after the M1 spike if the state space explodes.
- **Spike-then-parametric** spec layout:
  - `specs/M1Convergence.tla` (+ `.cfg`) — tiny, get TLC green fast.
  - `specs/DagReplication.tla` — one parametric spec over
    `Grain ∈ {Doc, Field}` × `FetchPolicy ∈ {Naive, FullWalkA, FilteredMergeB}`,
    driven by several `.cfg` files so naive / A / B are an apples-to-apples diff.
  - `specs/README.md` — plain-English invariant → TLC verdict → source-module map.
- Per CLAUDE.md's "no `docs/`, no planning documents" rule, these specs are the
  research work-product of this branch, kept under `specs/`. **Confirm placement
  with Jack before any merge to main.** If the branch stays unmerged research,
  the constraint does not bite.

## Milestones
1. **Tooling spike** — `M1Convergence.tla` green under TLC.
2. **M1** — baseline convergence; reproduce `#2721` counterexample under naive sync.
3. **M2 (doc grain)** — DID filter; `INV_SubsetConverge` + `INV_RelRefSafe` green.
4. **M2 (field grain)** — naive re-breaks; characterize the Model A vs Model B trade-off.
