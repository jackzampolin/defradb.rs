# P2P Formal Modeling (TLA+)

Branch: `feat/p2p-tla-modeling` · Worktree adjacent to `defradb.rs/`

> ## ▶ START HERE — run the `brainstorming` skill first
> **Do not write any spec until you have run a superpowers `brainstorming`
> session** to pin down the exact invariant set and the modeling approach
> (TLC vs Apalache, Model A vs B for B3). Invoke it with the Skill tool
> (`superpowers:brainstorming`). Capture the outcome back into this file first.

> ### ✅ Brainstorm outcome (2026-06-02, rev 2 — assumptions verified) — see `specs/DESIGN.md`
> - **Tooling:** TLC for everything (it's the liveness/convergence props that
>   matter). Apalache is at most a *safety-only* scaling fallback — it does **not**
>   check liveness (rev-1 "Apalache-compatible" claim corrected).
> - **Scope this session:** M1 (convergence baseline) + M2 (the filter). M3 auth
>   (`#1012 A2`) deferred to its own spec.
> - **Grounded in defra-agent (verified):** `AgentRequest` is `@branchable`,
>   multi-writer (requester + claimer) so branching is real; `agent_did` is the
>   sole P2P filter key; filtering is read-time today (every agent replicates
>   everyone — noisy). B3 = push the DID predicate down to the subscription layer.
> - **Two corrections from fresh review:**
>   1. ✅ `INV_RelRefSafe` **CONFIRMED** — cross-request refs are scalar `String`
>      FKs (not `@relation`); merge never dereferences them; dangling → query-time
>      "absent." Dropping foreign-DID docs is safe.
>   2. ⚠️ `agent_did` immutability **REFUTED as a guarantee** — production sets it
>      only at create (write-once *by convention*), but **nothing enforces it**
>      (no constraint, `@branchable`, a test mutates it). Rev-1's "clean Model A"
>      story depended on an unenforced assumption.
> - **Corrected framing — two orthogonal axes:** (1) ancestry fetch policy
>   (Naive/FullWalkA/FilteredMergeB) → DAG-completeness; (2) filter-key stability
>   (Immutable/Mutable) → whether a doc flips in/out of a peer's subscription
>   mid-history → ownership/access hazard. defra-agent is WholeDoc scope (Axis 1 =
>   Model A) but Mutable-key-by-default (Axis 2 live). GraphSync future is SubDoc
>   scope (where Model B earns its keep).
> - **Recommendation:** (1) **Model A** (full within-doc ancestry walk — already
>   shipped). (2) Foreign-DID docs safe to drop (`INV_RelRefSafe`). (3) **Filtering
>   on `agent_did` is safe only if the key is immutable, which the schema does NOT
>   enforce → recommend a schema/ACP immutability constraint (or an ownership-
>   handoff protocol)**; else split-ownership hazard (`INV_NoSplitOwnership`).
>   (4) **Model B only if field-level GraphSync filtering ships.**
> - **Invariants:** `INV_DagComplete`, `INV_Converge` (M1, ref-observer);
>   `INV_SubsetConverge`, `INV_RelRefSafe`, `INV_ClaimUnique` (M2 WholeDoc/immutable);
>   `INV_NoSplitOwnership` (M2 WholeDoc/mutable — expected to fail, mitigations
>   close it); `INV_VisibleConverge` (M2 SubDoc / Model B). Env assumption:
>   `ProviderAvailable`.
> - **Layout:** `specs/M1Convergence.tla` spike → parametric `specs/DagReplication.tla`
>   (`FilterScope × KeyMutability × FetchPolicy` via S1–S4 `.cfg`s) → `specs/README.md`
>   (invariant→verdict map). Confirm `specs/` placement with Jack before merge to main.

Goal: build **TLA+ models of DefraDB.rs's P2P protocols** so we can check the
correctness-critical properties (convergence, DAG-completeness, auth) *before*
committing to implementations. This is greenfield — no `.tla` exists in the repo
today (checked). Start small, model the real code paths, get TLC (or Apalache)
running green, then grow coverage.

> This worktree is the **formal-modeling counterpart** to the sibling
> `defradb.rs-p2p-control` worktree (#1012 + #1013). The highest-value model is
> the one that unblocks **B3 — filter-aware DAG completeness** for filtered
> replication. Land the model here; consume the conclusion there.

---

## Why now

Two design-stage features need a correctness argument we can't make by reading code:

1. **Filtered replication DAG-completeness (#1013 B3)** — the headline.
   A filtered peer that receives a head block whose causal parents were filtered
   out **never merges**. This is the exact failure class of Go
   `sourcenetwork/defradb#2721` ("Partial DAG sync will never merge"; test gap
   `#2717`). We need a model that lets us *prove* which of these holds:
   - **Model A — full-ancestry sync:** the filter applies only to the
     *announce/subscription* set; the DAG walk always fetches full causal
     ancestry. Invariant: every accepted head is eventually mergeable.
   - **Model B — filtered-merge semantics:** define explicit semantics for
     merging across filtered-out parents (placeholder/skip nodes, etc.) and
     prove convergence still holds.
   The model decides A vs B. That decision gates all of #1013's implementation.

2. **Management-channel auth state machine (#1012 A2)** — a smaller, very
   checkable model: actor-DID auth token → NAC node-permission gate → coordinator
   mutation. Properties: no admin mutation executes without a verified actor-DID;
   PeerID alone never authorizes; revoked DID can't replay. Catches the
   "unauthenticated remote node-config mutation" failure mode before it ships.

---

## What to model (the real system, not an abstraction in a vacuum)

Anchor the spec to these source modules so the model tracks reality:

| Concern | Source of truth |
|---------|-----------------|
| DAG sync plan / walk | `crates/p2p/src/sync/dag_sync/{plan,state,sync}.rs` |
| Merge / head acceptance | `crates/db-merge/src/{merge_handler/,head_provider.rs,push_docs.rs}` |
| Broadcast / push path | `crates/p2p/src/sync/broadcaster.rs`, `sync/replication/` |
| Head provider / heads | `crates/p2p/src/sync/head_provider.rs` |
| Replicator model | `crates/p2p/src/replicator.rs` |
| Subscriptions / topics | `crates/p2p/src/sync/coordinator/subscriptions.rs` |
| Manage-channel auth (future) | `crates/p2p/src/iroh/endpoint_streams.rs`, `crates/http/src/auth_middleware.rs` |

CRDT background: deltas form a Merkle-DAG; convergence depends on every node
eventually seeing the full causal history of every head it accepts. The filter
breaks the "eventually sees everything" assumption — that's the crux.

---

## Suggested milestones

1. **Tooling** — pick TLA+ Toolbox / TLC, or Apalache (symbolic, better for
   larger state). Decide and note it here. Get a trivial spec model-checking green.
2. **M1 — Convergence baseline.** Model unfiltered Merkle-DAG replication between
   N nodes: blocks with parent sets, gossip/announce, fetch-missing-ancestors,
   merge. Invariant: **eventual convergence** (all nodes reach the same head set
   under fair delivery). This is the control case — it MUST hold before filtering
   means anything.
3. **M2 — Add the filter (#1013 B3).** Introduce a per-(peer,collection)
   predicate on the announce set. Encode Model A and Model B as alternatives.
   Check the **DAG-completeness invariant**: no node accepts a head it can't
   transitively merge. Find the `#2721` counterexample under naive filtering;
   show Model A (or B) eliminates it. **This is the deliverable that unblocks B3.**
4. **M3 — Auth state machine (#1012 A2).** Model actor-DID token issuance →
   verification → NAC gate → mutation, with an adversary peer (valid PeerID,
   no/invalid/revoked DID). Safety: no mutation without a fresh verified DID.

## Output of this worktree
- The `.tla` / `.cfg` specs + a short README in this worktree explaining each
  invariant in plain English and what TLC/Apalache proved (or the counterexample
  trace it found).
- A crisp **A-vs-B recommendation for B3** to hand to `defradb.rs-p2p-control`.
- (Stretch) link invariants to Rust assertions/tests so the model and code don't
  drift.

> **Note on repo conventions:** CLAUDE.md says "no docs/ directories, only
> README/CLAUDE/Cargo.toml." TLA+ specs and a model README are the work product
> of this branch (not speculative planning docs) — keep them in a clearly-scoped
> `specs/` or similar and confirm with Jack before merging where they live. If
> the models stay in this worktree as a research branch and don't merge to main,
> that constraint doesn't bite.

## Precedent in the ecosystem
Downstream `defra-agent` uses **Lean** for proofs (`PairingReconcile.lean`,
`defra-agent#180`; see sibling `defra-agent-lean-verification-sweep/` worktree).
The user asked for **TLA+** here specifically — TLA+/TLC is the better fit for
*temporal/liveness* properties of a concurrent replication protocol (convergence,
"eventually merges"), which is exactly the B3 question. Lean is better for the
pure functional/algebraic obligations. Worth knowing both exist.

## Before starting
- `git fetch && git rebase origin/main` (branched from `c4969c06`).
- Read `dag_sync/plan.rs` + `dag_sync/state.rs` first — they are the walk/state
  the model abstracts.
- Use the `brainstorming` skill to pin down the exact invariant set before writing spec.
