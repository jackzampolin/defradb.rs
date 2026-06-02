# B3 P2P Replication — TLA+ Specs

Formal models for DefraDB.rs filtered P2P replication (defra-agent's B3 requirement).
The design rationale, grounded facts, and full invariant derivations live in [DESIGN.md](DESIGN.md).
This file is the operational guide: how to run TLC, what each run proves, and the recommendation.

---

## Quick start

```
cd specs
```

All commands use `./tools/tlc -config <cfg> <module>.tla`. Run them one at a time.
(TLC's default scratch dir under `states/` is timestamped per second; if you script
all eight in a sub-second loop, pass a unique `-metadir states/runN` to each to avoid
a metadir collision.)

---

## The eight runs

```bash
# Run 1 — GREEN  Model A (FullWalkA) converges: control case, reproduces S1'
./tools/tlc -config M1Convergence.cfg M1Convergence.tla

# Run 2 — RED    Naive fetch violates Converge: reproduces Go #2721 "never merges" (S1)
./tools/tlc -config M1Naive.cfg M1Convergence.tla

# Run 3 — GREEN  WholeDoc+Immutable: INV_SubsetConverge + INV_RelRefSafe (S2)
#                (INV_NoSplitOwnership also holds here, but trivially — single owner, no
#                 reassignment; the real split-ownership test is run 4/S3)
./tools/tlc -config MC_S2.cfg MC_S2.tla

# Run 4 — RED    Mutable filter key: INV_NoSplitOwnership violated (split ownership) (S3)
./tools/tlc -config MC_S3.cfg MC_S3.tla

# Run 5 — GREEN  Immutable key closes the split (S3)
./tools/tlc -config MC_S3_Fixed.cfg MC_S3.tla

# Run 6 — RED    Naive field-grain filter: INV_VisibleConverge violated (field-grain #2721) (S4)
./tools/tlc -config MC_S4_Naive.cfg MC_S4.tla

# Run 7 — RED    Model A over-fetches: INV_NoFilteredFetch violated (S4)
./tools/tlc -config MC_S4_FullWalkA.cfg MC_S4.tla

# Run 8 — GREEN  Model B converges on visible set without fetching filtered blocks (S4)
./tools/tlc -config MC_S4_ModelB.cfg MC_S4.tla
```

---

## Invariants, verdicts, and sources

| Invariant / Property | Plain English | Verdict (run) | Source it abstracts |
|---|---|---|---|
| `Converge` | all nodes eventually merge all blocks | GREEN run 1, RED run 2 | `crates/p2p/src/sync/coordinator/dag_fetcher.rs` ancestry walk |
| `INV_DagComplete` | no merged block lacks a merged parent | holds under `Merge`; relaxed by Model B (by design) | `crates/db-merge/src/merge_handler/` `loadComposites` recursion |
| `INV_SubsetConverge` | subscribed docs fully converge | GREEN run 3 | defra-agent watcher DID filter (`watcher/query.rs`) |
| `INV_RelRefSafe` | dropping a foreign-DID relational ref never blocks a merge | GREEN run 3 | scalar `String` FK; merge never derefs it |
| `INV_NoSplitOwnership` | at most one DID owns a doc across all nodes | RED run 4 (mutable key), GREEN run 5 (immutable key) | `agent_request.graphql` `agent_did` (write-once by convention, unenforced) |
| `INV_VisibleConverge` | every non-filtered visible block eventually merges | RED run 6 (Naive), GREEN run 8 (Model B) | GraphSync field-filter (future feature) |
| `INV_NoFilteredFetch` | a node never fetches a block it filters out | RED run 7 (FullWalkA over-fetches), GREEN run 8 (Model B) | resource-savings goal of field-level filtering |

`Converge` is defined in `M1Convergence.tla`; all other invariant/property names are defined in `DagReplication.tla`.

---

## Findings

### Model B convergence is non-trivial

A naive Model B that anchors its ancestry fetch only on `wanted` heads strands
non-filtered side-ancestors: `MergeB`'s relaxed parent-guard merges the head
(clearing `wanted`) before a non-filtered grandparent is fetched, so that
grandparent never arrives and `INV_VisibleConverge` fails.

The committed Model B anchors the fetch on `wanted ∪ merged` (see `FetchTarget`
in `DagReplication.tla`, `FetchPolicy = "FilteredMergeB"` branch), which
converges. Plain `Merge` (FullWalkA) does not have this problem because its
strict parent-guard forces ancestors to merge first.

This is why `DESIGN.md` flags Model B's merge semantics as a research output
requiring care: getting convergence right is non-trivial, and the relaxed
`INV_DagComplete` guard is a deliberate, load-bearing trade-off.

---

## Recommendation (B3 / defradb.rs-p2p-control)

1. **Ship Model A.** Full within-doc ancestry walk before merge is already in
   Go and Rust. Run 1 proves it converges; run 2 shows the #2721 bug without it.
   No change needed here.

2. **Drop foreign-DID docs safely.** `INV_RelRefSafe` (run 3) proves that
   cross-request scalar FKs (`caused_by_parent_request_id`, `retry_parent_request`,
   etc.) are not merge dependencies. Filtering them out at the P2P layer is sound.

3. **Enforce `agent_did` immutability.** A mutable filter key causes split
   ownership (run 4); making it immutable closes it (run 5). DefraDB has no
   field-immutability mechanism today. Two shapes:
   - **(E1)** merge-time write-once constraint in defradb.rs (P2P-safe: must
     reject an `agent_did`-changing delta at merge, not just at write time).
   - **(E2)** key the subscription filter on the content-addressed create-block
     value of `agent_did` (immutable by construction; no new DB feature needed).
   The model abstracts over E1/E2 — it proves immutability is necessary and
   sufficient; the mechanism is an implementation choice for defradb.rs / defra-agent.

4. **Model B only if field-level GraphSync filtering is built.** For today's
   whole-document filtering, Model A suffices. Model B earns its keep only when
   a resource-constrained node needs to skip individual field-blocks inside a
   doc's DAG (runs 6-8). Its relaxed `INV_DagComplete` guard and the convergence
   subtlety in the Findings section above must be understood before implementing it.
