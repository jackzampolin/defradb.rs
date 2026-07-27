# B3 — Filter-Aware DAG Completeness: TLA+ Model Design

**Date:** 2026-06-02 · **Branch:** `feat/p2p-tla-modeling` · **Status:** approved, pre-implementation (rev 2 — assumptions verified)

This is the design for a TLA+ model of DefraDB.rs's filtered P2P replication.
Its job is to settle the decision that gates `#1013` (consumed by the sibling
`defradb.rs-p2p-control` worktree): **which fetch/merge policy makes filtered
replication correct, and under what assumptions.** The model reproduces the Go
`#2721` "partial DAG sync will never merge" failure, then proves which fix holds
for which deployment.

Scope of this session: **M1 (convergence baseline) + M2 (the filter).** The
`#1012 A2` management-channel auth model (M3) is deferred to its own spec.

---

## Deployment target (why this model, not an abstraction in a vacuum)

The consumer is **Gents** (`source-inc/gents`). Agents accept `AgentRequest` documents; remote
control happens by writing a request locally and gossiping it over P2P. Requests
are handled per-DID. With many agents on one network, today **every agent
replicates every other agent's request traffic** and filters by DID at query
time — noisy, and agents hold data they shouldn't (a relevance *and*
access-control *and* resource problem). B3 pushes that DID predicate *down* to
the subscription/replication layer.

## Grounded facts (verified against source, 2026-06-02)

`gents:` anchors were re-verified against the renamed consumer repository,
`source-inc/gents` `main`, on 2026-07-27. Other consumer path fragments are the
unmodified 2026-06-02 snapshot and may have drifted (e.g. the test-only
`override_child_agent_did` helper no longer exists, and the desktop crate is now
`gents-desktop-core`). The immutability gap this design surfaced has since been
closed — see *Post-design status (2026-07-27)* below the table.

| Fact | Source | Status |
|---|---|---|
| `AgentRequest` is `@branchable`; multi-writer (requester creates, claimer updates `status`/`lifecycle_state`/`claimed_at`) | `gents:crates/gents-schemas/schemas/agent/agent_request.graphql:1`; `lifecycle/claim.rs:258` | ✅ branching is real |
| `agent_did` is the **sole** P2P relevance/filter key | `gents:crates/gents/src/watcher/query.rs:73` | ✅ confirmed |
| Filtering today is **read/claim-time**, not P2P-level; every agent subscribes collection-wide | `watcher/query.rs`, `trigger_engine/subscription_source.rs:28`, `desktop-core/.../client/schema.rs:36` | ✅ the noise problem |
| `agent_did` set **only at create time** in production (`create_AgentRequest`); the sole post-create mutation is a **test** helper | `toolset/delegate.rs:88,91` (create); `tests/r4_subagent_tools.rs:424` (`override_child_agent_did`, test-only) | ⚠️ write-once **by convention, not enforced** (2026-06-02; since enforced — see *Post-design status*) |
| **No** schema/ACP immutability constraint on `agent_did` (just `String @index`) | `agent_request.graphql:3` | ⚠️ assumption is unguaranteed (2026-06-02; since closed — that line is now `String @index @immutable`, see *Post-design status*) |
| Multiple agent instances can share one `agent_did`; claim safety is **CRDT CAS** (`update where status=pending`), not FIFO/lock | `watcher.rs:102`, `lifecycle/claim.rs:258-310` | ✅ eventual-consistent claim |
| Cross-request refs (`caused_by_parent_request_id`, `retry_parent_request`, `retry_root_request`, `superseded_by_request`) are bare `String @index` scalars, **not** `@relation` | `agent_request.graphql:6-8,30` | ✅ scalar FKs |
| Merge handler **never** dereferences a cross-doc ref; relations resolved only at query time; dangling ref → graceful "absent" | Rust `crates/db-merge/src/merge_handler/mod.rs:69`; Go `internal/db/merge.go:343`; `client/document.go:238` (format-only validation) | ✅ no merge dependency |
| Delta-DAG is **per-document**; `Heads` links within-document; no cross-doc causal edges | Go `internal/core/block/block.go`; Rust `merge_handler/` | ✅ confirmed |
| `#2721` hole = **branching + partial sync within one doc**; shipped fix = "walk the entire graph before merging" (Model A), already done in Rust | Go `#2721`, `merge_test.go:143` `TestMerge_DualBranchWithOneIncomplete_CouldNotFindCID`; Rust `coordinator/dag_fetcher.rs` | ✅ Model A is the proven fix |
| Field-level filtering is a **future GraphSync** feature, not today's reality | Go `block.go` `DAGLink.Name` comment | ✅ future |

### Post-design status (2026-07-27)

Recommendation 3 has since been implemented. `agent_did` is
`String @index @immutable` on Gents `main`
(`gents:crates/gents-schemas/schemas/agent/agent_request.graphql:3`), and
defradb.rs enforces `@immutable` on the paths this design demanded: local
updates (`crates/db/src/collection/validation.rs`), peer-authored deltas at
merge time — the E1 shape (`crates/db-merge/src/merge_handler/composite_fields.rs`)
— and replication filters, which require every referenced field to be
`@immutable` (`crates/replication-filter/src/lib.rs`). The Axis-2 "Mutable"
scenarios and Recommendation 3 below are the 2026-06-02 analysis that motivated
that work.

## Fresh-review findings (rev 1 → rev 2)

Two load-bearing claims were adversarially verified:

- **`INV_RelRefSafe` — CONFIRMED.** Cross-request refs are scalar strings, the
  merge handler never loads referenced docs, dangling refs degrade to query-time
  "absent." Dropping foreign-DID docs cannot block a merge. The access-control /
  relevance win is sound.
- **"`agent_did` is immutable" — REFUTED as a guarantee, downgraded to an
  assumption.** Production sets it only at create, so it is write-once *in
  practice*, but nothing enforces it (no constraint, `@branchable`, a test
  mutates it). **This is the central correction in rev 2:** the clean "Model A
  suffices" story holds *only under an unenforced immutability assumption*, and
  the model must treat filter-key stability as an explicit variable, not a fact.

**Rev 2.1 — two follow-up verifications on the immutability question:**
- **No production reassignment exists (verified).** Every "move to another agent"
  — delegation (`toolset/delegate.rs:77`), subagent spawn
  (`tool_call_lifecycle/subagent_request.rs:88`), retry/supersede, failover
  (re-claim under the *same* DID via CAS) — creates a **new request document**
  linked by `caused_by_parent_request_id`/`retry_parent_request`, never an
  in-place `agent_did` change. `override_child_agent_did` is a test fixture only.
  ⇒ **Enforcing immutability breaks no real feature.** The S3 "immutability *or*
  handoff" choice collapses to **immutability only** (handoff was only needed if
  reassignment were real).
- **DefraDB has no field-immutability mechanism today (verified).** No
  write-once/`@immutable` constraint on document field *values* exists in
  defradb.rs or Go (the `immutable.*` hits are schema-metadata IDs and the
  `immutable.Option` library). So enforcement is real work with two shapes:
  **(E1) a merge-time write-once field constraint** in defradb.rs (P2P-safe:
  must reject an `agent_did`-changing delta at *merge*, since a peer can author
  one); or **(E2) structural** — key the subscription filter on the
  **content-addressed create-block** value of `agent_did` (immutable by
  construction; no new DB feature, but trusts the create block as the source of
  truth). The model abstracts over E1/E2 — it proves immutability is
  necessary+sufficient; the mechanism is a downstream implementation choice for
  p2p-control / defradb.rs.

---

## Corrected core framing: two orthogonal axes

Rev 1 conflated "document-grain filter" with "safe." Verification shows two
independent axes determine correctness:

**Axis 1 — ancestry fetch policy** (`Naive` / `FullWalkA` / `FilteredMergeB`).
Governs **DAG-completeness / convergence** (the `#2721` axis). Whether a node
that accepts a head fetches the head's full within-document causal ancestry
before merging.

**Axis 2 — filter-key stability** (`Immutable` / `Mutable`). Governs whether a
document can **flip in or out of a peer's subscription mid-history**. The filter
predicate (`agent_did = me`) reads a field that lives *inside* the replicated
CRDT document. If that field is mutable, a request can change ownership, which:
(a) makes the *new* owner need ancestry written under the *old* owner, and
(b) — the real hazard — can leave the *old* owner never receiving the
reassignment block (it filtered the doc out), so old and new owner disagree on
who owns the request: **split ownership**.

These compose. Gents is **WholeDoc filter scope** (the whole request is in
or out), so it never creates *within-doc* causal holes — Axis 1 is answered by
Model A. But at design time its filter key was **Mutable-by-default**
(unenforced — since closed, see *Post-design status*), so Axis 2 was live and
had previously been assumed away. The GraphSync future is **SubDoc scope**,
which *does* create within-doc holes and is where Model B (Axis 1) earns its
keep.

---

## Abstraction (spec symbols → real things)

| Spec symbol | Real thing |
|---|---|
| `Agents` | nodes; each runs one or more agent instances, each with a `DID` |
| `Docs` | `AgentRequest` documents; each has `owner(d, t) ∈ DID` — the `agent_did` **as a function of DAG state**, since it is a mutable field |
| `Block` | a CRDT composite delta (head). `doc(b)`, `parents(b) ⊆ Block` (within-doc `Heads`), and the field-writes it carries (incl. possible `agent_did` write) |
| branching | two writers mutate one request concurrently → two heads sharing a parent |
| `relRef(d)` | cross-doc scalar FKs (parent/retry/supersede) — **not** causal parents |
| `subscribed(a, d)` | filter predicate over `a`'s view of `owner(d)`. **WholeDoc:** `owner(d)=DID(a)`. **SubDoc (future):** per-field predicate excluding some `parents(b)` |
| `claimed(a, d)` | agent instance `a` has CAS-claimed `d` (`status pending→processing`) |
| `store`,`heads`,`syncing`,`synced` | blockstore presence + `DagSyncState` (`crates/p2p/src/sync/dag_sync/state.rs`) |

## State machine (actions)

`CreateRequest` · `MutateRequest` (may branch; may write `owner` if key mutable)
· `Gossip(head)` (announce CID only — mirrors `broadcast_update`) ·
`BeginFetch`/`FetchBlock` (Bitswap pull of a missing parent **from an available
provider**) · `Merge(b)` **guarded: all `parents(b)` present locally** (mirrors
`loadComposites` recursion) · `ClaimCAS(a,d)` (succeeds only if local view shows
`status=pending`) · `Subscribe`/`Unsubscribe(a,d)` (filter (de)selects a doc as
its `owner` view changes). Weak fairness on delivery/fetch for liveness.

**Environment assumption (made explicit):** `ProviderAvailable` — every block in a
subscribed doc's ancestry is held by ≥1 reachable provider. Model A liveness
depends on it; the model flags the failure when an un-subscribing old owner GCs
the only copy of ancestry the new owner still needs.

The Naive / A / B difference is a single guard on the fetch action.

---

## Invariants (the deliverable)

### M1 — Convergence baseline (no filter)
- **`INV_DagComplete`** (safety): no merged block lacks a locally-present parent.
  *Naive sync must violate this → reproduces `#2721`.*
- **`INV_Converge`** (liveness): under fair delivery + `ProviderAvailable`, all
  agents reach the same per-doc head-set. *Model A must satisfy it.* Formalized
  via a **reference observer**: for each doc `d`, every node's merged head-set of
  `d` equals that of a hypothetical full-replication observer.

### M2 — WholeDoc filter, key IMMUTABLE (the ideal Gents target)
- **`INV_SubsetConverge`**: for docs where `owner(d)=DID(a)`, agent `a`'s merged
  head-set for `d` equals the reference observer's. *Holds under Model A.*
- **`INV_RelRefSafe`** ✅ verified true: dropping a cross-DID relational ref never
  blocks a merge; it resolves to query-time "absent." *The access-control win.*
- **`INV_ClaimUnique`**: at most one instance drives a request to `processing`
  **in the merged state**. Holds because the DID filter keeps exactly the
  same-DID instances mutually replicating, so their claim blocks converge and CAS
  resolves. (Note: this is *eventual* uniqueness — a concurrent claim race before
  convergence is a pre-existing CRDT-CAS property, **not** introduced by B3; the
  model shows filtering is **claim-neutral**, neither causing nor fixing it.)

### M2 — WholeDoc filter, key MUTABLE (the then-unenforced reality — the new finding)
- **`INV_NoSplitOwnership`** (safety): no request is simultaneously "owned"
  (filter-matched + actionable) by two distinct DIDs in their respective local
  views. *Expected to FAIL under Mutable + naive subscription:* the old owner
  filters out the very reassignment block that would tell it to stop. The model
  produces this counterexample, then shows **enforcing filter-key immutability
  closes it**. (The ownership-handoff alternative is dropped: rev 2.1 verified no
  production reassignment exists, so handoff solves a non-problem — immutability
  is both safe and sufficient.)

### M2 — SubDoc filter (GraphSync future / resource-constrained peers)
- Show *naive* field-level filtering re-creates a `#2721` hole *inside* a doc's
  DAG, and that **Model A defeats the resource-savings purpose** (it fetches the
  filtered ancestry anyway). Then evaluate whether **Model B** (placeholder/skip
  nodes for filtered parents) satisfies a weaker **`INV_VisibleConverge`**
  *without* fetching filtered ancestry. Model B's merge semantics are deliberately
  a *research output*, not a commitment.

---

## Scenarios checked (map to `.cfg` files)

| # | Scope | Key | FetchPolicy | Expected verdict |
|---|---|---|---|---|
| S1 | none | — | Naive | `INV_DagComplete` **violated** → reproduces `#2721` |
| S1′| none | — | FullWalkA | M1 invariants **green** (the shipped fix) |
| S2 | WholeDoc | Immutable | FullWalkA | `INV_SubsetConverge`,`INV_RelRefSafe`,`INV_ClaimUnique` **green** — the ideal |
| S3 | WholeDoc | Mutable | FullWalkA | `INV_NoSplitOwnership` **violated**; immutability **or** handoff closes it |
| S4 | SubDoc | — | FullWalkA vs FilteredMergeB | A over-fetches (no savings); characterize whether B holds `INV_VisibleConverge` |

---

## Recommendation (handed to `defradb.rs-p2p-control`)

> **1. Use Model A** (full within-document ancestry walk before merge) for B3.
> Verified sufficient for convergence/DAG-completeness; it is the already-shipped
> Go/Rust behavior.
>
> **2. Filtering out foreign-DID documents is safe** (`INV_RelRefSafe`, verified):
> relational FKs are scalar, not merge dependencies.
>
> **3. Filtering on `agent_did` is only safe if the filter key is immutable —
> which DefraDB does NOT currently enforce.** Verified: no production code
> reassigns `agent_did` (all ownership moves are new docs), so immutability is
> safe to enforce and breaks nothing. But DefraDB has **no field-immutability
> mechanism today**, so this is real work — either **(E1)** a merge-time
> write-once field constraint in defradb.rs (rejects an `agent_did`-changing
> delta at merge, since a peer can author one), or **(E2)** key the subscription
> filter on the **content-addressed create-block** value (immutable by
> construction). Without one of these, filtered replication admits a
> split-ownership hazard (`INV_NoSplitOwnership`) that unfiltered replication
> does not have. *This enforcement is downstream implementation (defradb.rs /
> Gents), not TLA+ work — the model proves it is necessary+sufficient.*
> *[2026-07-27: implemented — see Post-design status.]*
>
> **4. Model B is needed only if/when field-level GraphSync filtering ships** for
> resource-constrained peers (SubDoc scope). Not required for the `agent_did`
> deployment.

---

## Tooling & layout

- **TLC for everything that matters here.** The headline properties are liveness
  (`INV_Converge`, `INV_VisibleConverge`) and TLC checks liveness/fairness
  directly. **Correction vs rev 1:** Apalache does *bounded* checking and
  *inductive* invariants for **safety** only — it does **not** check liveness. So
  Apalache is at most a *safety-scaling fallback* (`INV_DagComplete`,
  `INV_RelRefSafe`, `INV_NoSplitOwnership`, `INV_ClaimUnique`) if TLC's state
  space explodes; the liveness props stay on TLC. Write specs to avoid TLC-only
  idioms where free, but do not claim full Apalache portability.
- **Spike-then-parametric** layout:
  - `proofs/tla/M1Convergence.tla` (+ `.cfg`) — tiny, get TLC green fast (S1/S1′).
  - `proofs/tla/DagReplication.tla` — one parametric spec over `FilterScope ∈
    {None,WholeDoc,SubDoc}` × `KeyMutability ∈ {Immutable,Mutable}` × `FetchPolicy
    ∈ {Naive,FullWalkA,FilteredMergeB}`, driven by the S1–S4 `.cfg` files.
  - `proofs/tla/README.md` — plain-English invariant → TLC verdict → source-module map.
- **Bounds (TLC):** N=2–3 agents/DIDs, ≤2 docs, ≤6 blocks, ≤1 owner-reassignment.
  Minimal `#2721` needs 1 doc + 3 blocks (root + 2 concurrent children) + 2 nodes;
  `INV_NoSplitOwnership` needs 2 DIDs + 1 reassignment; `INV_RelRefSafe` needs 2
  docs + a cross-ref. All within TLC reach.
- Per CLAUDE.md's "no `docs/`, no planning documents" rule, these specs are the
  research work-product of this branch, kept under `proofs/tla/`. **Confirm placement
  with Jack before any merge to main.** If the branch stays unmerged research,
  the constraint does not bite.

## Milestones
1. **Tooling spike** — `M1Convergence.tla` green under TLC (S1′); S1 yields the `#2721` trace.
2. **M1** — baseline convergence + reference-observer formalization.
3. **M2 WholeDoc/Immutable** (S2) — `INV_SubsetConverge` + `INV_RelRefSafe` + `INV_ClaimUnique` green.
4. **M2 WholeDoc/Mutable** (S3) — exhibit `INV_NoSplitOwnership` counterexample; show the two mitigations close it.
5. **M2 SubDoc** (S4) — naive re-breaks; characterize the Model A vs Model B trade-off.
