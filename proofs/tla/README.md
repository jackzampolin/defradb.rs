# B3 P2P Replication — TLA+ Specs

Formal models for DefraDB.rs P2P protocols. The B3 filtered-replication model (the
first slice) leads; later slices model adjacent P2P concerns. Design rationale and
grounded facts for B3 live in [DESIGN.md](DESIGN.md); each later slice has its own
`*_DESIGN.md`. This file is the operational guide: how to run TLC, what each run proves.

**Model families** (all under `proofs/tla/`, run with `./tools/tlc`):
- **B3 filtered replication** — the eight runs below (`DESIGN.md`).
- **DAG convergence** under partition/restart/eviction (`Convergence_DESIGN.md`) +
  the Lean merge-algebra half under [`../lean/`](../lean/README.md).
- **Multi-instance claim-uniqueness** (`Claim_DESIGN.md`).
- **KMS key distribution** (`Kms_DESIGN.md`).
- **Encrypted LWW restart/replay** (`EncryptedLwwReplay_DESIGN.md`) — durable
  ciphertext and DEK re-drive preserve the existing LWW winner.
- **Management-channel auth** (`Auth_DESIGN.md`).
- **Replicator lifecycle** — backfill/live/resume delivery (`Replicator_DESIGN.md`).
- **ACP-on-commits** — dual-path (User + Commits) access gating (`Commits_DESIGN.md`).
- **Block integrity** — signature verification before merge (`Integrity_DESIGN.md`).
- **ACP soundness** — no-escalation (Lean) + revocation consistency (`Acp_DESIGN.md`);
  Lean half under [`../lean/`](../lean/README.md).
- **Mixed-field materialization** — Counter×LWW componentwise document state
  (`MixedFieldMaterialization_DESIGN.md`).
- **Index reconciliation** — secondary indexes track the winning CRDT value
  (`IndexReconciliation.tla`).
- **Document materialization status** — delete/update status stays componentwise
  (`DocumentMaterialization.tla`).

Run all configured models at once with `./run-all.sh` (red/green oracle, exits non-zero on mismatch).

---

## Quick start

```
cd proofs/tla
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
| `INV_DagComplete` | no merged block lacks a merged parent | holds under `Merge`; relaxed by Model B (by design) | `crates/db/src/merge/merge_handler/` `loadComposites` recursion |
| `INV_SubsetConverge` | subscribed docs fully converge | GREEN run 3 | Gents watcher DID filter (`watcher/query.rs`) |
| `INV_RelRefSafe` | dropping a foreign-DID relational ref never blocks a merge | GREEN run 3 | scalar `String` FK; merge never derefs it |
| `INV_NoSplitOwnership` | at most one DID owns a doc across all nodes | RED run 4 (mutable key), GREEN run 5 (immutable key) | `agent_request.graphql` `agent_did` (unenforced at model time; since `@immutable` + enforced) |
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
   sufficient; the mechanism is an implementation choice for defradb.rs / Gents.
   *[2026-07-27: implemented — `agent_did` is `@immutable` in the Gents schema,
   and defradb.rs enforces it at update (`crates/db/src/collection/validation.rs`),
   at merge (E1, `crates/db/src/merge/merge_handler/composite_fields.rs`), and in
   replication filters (`crates/replication-filter/src/lib.rs`).]*

4. **Model B only if field-level GraphSync filtering is built.** For today's
   whole-document filtering, Model A suffices. Model B earns its keep only when
   a resource-constrained node needs to skip individual field-blocks inside a
   doc's DAG (runs 6-8). Its relaxed `INV_DagComplete` guard and the convergence
   subtlety in the Findings section above must be understood before implementing it.

---

## Management-Channel Auth Runs

Design notes live in [Auth_DESIGN.md](Auth_DESIGN.md). These runs model the
remote node-configuration gate for P2P collections, P2P replicators, DAC policy,
and NAC relationships.

```bash
# RED: PeerID-only management entry point reaches executed with no actor token.
./tools/tlc -metadir states/auth_red_peeronly -config MC_Auth_Red_PeerOnly.cfg MC_Auth_Red_PeerOnly.tla

# RED: cached authorization admits a token after expiry/revocation.
./tools/tlc -metadir states/auth_red_stale -config MC_Auth_Red_Stale.cfg MC_Auth_Red_Stale.tla

# RED: token-only authorization ignores the mutation's required permission.
./tools/tlc -metadir states/auth_red_wrongscope -config MC_Auth_Red_WrongScope.cfg MC_Auth_Red_WrongScope.tla

# GREEN: strict actor-DID + current NAC permission gate.
./tools/tlc -metadir states/auth_green -config MC_Auth_Green.cfg MC_Auth_Green.tla
```

| Invariant | Plain English | Verdict | Source note |
|---|---|---|---|
| `INV_NoMutationWithoutVerifiedActor` | no node-config mutation executes without a fresh signature-verified actor-DID; PeerID alone never authorizes | RED `MC_Auth_Red_PeerOnly`, GREEN `MC_Auth_Green` | HTTP `auth_middleware.rs` + `identity_extractor.rs`; Iroh `endpoint_streams.rs` is transport PeerID/signature, not actor-DID auth |
| `INV_NoStaleReplay` | expired, revoked, invalid, absent, or replayed credentials cannot reach `authorized` | RED `MC_Auth_Red_Stale`, GREEN `MC_Auth_Green` | token freshness from `identity_extractor.rs`; current NAC check from `nac_guard.rs` |
| `INV_PermissionScoped` | execution requires the exact node permission for that mutation, not just any valid token | RED `MC_Auth_Red_WrongScope`, GREEN `MC_Auth_Green` | `route_permissions.rs`; P2P, DAC, and NAC handlers repeat `require_permission` |
| `INV_AllEntryPointsGated` | every mutating remote management entry point uses the actor-DID gate | GREEN `MC_Auth_Green` | HTTP P2P collection/replicator, DAC policy, and NAC relationship routes are gated; current Iroh sync streams are not modeled as node-config mutation entry points |

Auth finding: no node-config mutation executes without a fresh, scope-correct,
non-revoked actor-DID on the gated HTTP management paths. The source review did
not find a current Iroh stream that mutates node configuration; if one is added,
PeerID/transport signatures are insufficient and it must receive an actor-DID
gate equivalent to the HTTP path. Embedded/direct adapter calls are internal or
FFI-wrapped surfaces; exposing them to untrusted callers requires the same gate.

---

## DAG Convergence Runs (partition / restart / eviction)

Design notes in [Convergence_DESIGN.md](Convergence_DESIGN.md). Strengthens the B3
fair-delivery model to **eventual connectivity** + bounded synced-CID eviction + node
restart. The order-independence half (CRDT merge is a commutative monoid) is proved in
Lean under [`../lean/`](../lean/README.md).

```bash
# GREEN: under eventual connectivity + fair head rediscovery, a partitioned node
#        eventually receives and merges every accepted head history.
./tools/tlc -metadir states/conv_eventual    -config MC_Conv_Eventual.cfg        MC_Conv_Eventual.tla
# GREEN: same, with MaxSynced=1 FIFO eviction + one restart per node (hints cleared,
#        durable merge state preserved).
./tools/tlc -metadir states/conv_restart     -config MC_Conv_RestartEviction.cfg MC_Conv_RestartEviction.tla
# RED: without head rediscovery, reconnected peers never learn missed heads → no convergence.
./tools/tlc -metadir states/conv_norediscov  -config MC_Conv_NoHeadRediscovery.cfg MC_Conv_NoHeadRediscovery.tla
```

Convergence = (TLA+: delivery under eventual connectivity, above) × (Lean: merge is
commutative/associative/idempotent, `proofs/lean/`). Neither half alone suffices.

---

## Multi-Instance Claim-Uniqueness Runs

Design notes in [Claim_DESIGN.md](Claim_DESIGN.md). N agent instances sharing one
`agent_did` claim a request via CRDT compare-and-swap. All target `MC_Claim_Common.tla`.

```bash
# GREEN: eventual claim-uniqueness (one winner in the merged state), unfiltered and filtered.
./tools/tlc -metadir states/claim_u_e -config MC_Claim_Unfiltered_Eventual.cfg  MC_Claim_Common.tla
./tools/tlc -metadir states/claim_f_e -config MC_Claim_Filtered_Eventual.cfg    MC_Claim_Common.tla
# RED: execution-uniqueness FAILS — both instances start work after concurrent local CAS
#      (a pre-existing CRDT-CAS property, NOT introduced by filtering; same filtered/unfiltered).
./tools/tlc -metadir states/claim_u_x -config MC_Claim_Unfiltered_Execution.cfg MC_Claim_Common.tla
./tools/tlc -metadir states/claim_f_x -config MC_Claim_Filtered_Execution.cfg   MC_Claim_Common.tla
# RED: if filtering SPLITS same-DID instances into different replication sets, even
#      eventual claim-uniqueness breaks.
./tools/tlc -metadir states/claim_split -config MC_Claim_Split_Eventual.cfg     MC_Claim_Common.tla
```

Finding: filtering is **claim-neutral** as long as same-DID instances stay mutually
replicating; CRDT-CAS does not guarantee execution-uniqueness; the filter partition
MUST contain the full same-DID instance set.

---

## KMS Key-Distribution Runs

Design notes in [Kms_DESIGN.md](Kms_DESIGN.md). Encryption-key gossip over pubsub
(libp2p+iroh) with a recipient-bound ECIES envelope abstraction (a node can use an
envelope iff it is the intended recipient; real ECIES math is an assumed boundary).

```bash
# GREEN: policy-gated distribution; authorized nodes get the key, others can't use the envelope.
./tools/tlc -metadir states/kms_g   -config MC_Kms_Green.cfg              MC_Kms_Gossip.tla
# GREEN: revocation/replay checked against authorization at response time.
./tools/tlc -metadir states/kms_rrg -config MC_Kms_RevokeReplay_Green.cfg MC_Kms_Replay.tla
# RED: no policy gate → unauthorized node obtains key (INV_OnlyAuthorizedHasKey).
./tools/tlc -metadir states/kms_np  -config MC_Kms_NoPolicy_Red.cfg       MC_Kms_Gossip.tla
# RED: ciphertext broadcast to a non-recipient (INV_OnlyIntendedRecipientDecrypts).
./tools/tlc -metadir states/kms_bc  -config MC_Kms_BroadcastCiphertext_Red.cfg MC_Kms_Gossip.tla
# RED: revoked node still obtains key (INV_RevokedCannotObtain).
./tools/tlc -metadir states/kms_rv  -config MC_Kms_Revoke_Red.cfg         MC_Kms_Replay.tla
# RED: replayed request grants key to a now-unauthorized node (INV_NoReplayGrant).
./tools/tlc -metadir states/kms_rp  -config MC_Kms_Replay_Red.cfg         MC_Kms_Replay.tla
```

Finding: every authorized node eventually gets the key (under eventual connectivity); no
unauthorized/revoked/replaying node obtains a usable key — modulo the ECIES recipient-binding assumption.

---

## Replicator Lifecycle Runs

Design notes in [Replicator_DESIGN.md](Replicator_DESIGN.md). A directional replicator
(connect → backfill → live → backoff/resume) delivering to a target peer.

```bash
# RED: naive replicator drops its in-flight doc on disconnect and never re-backfills.
./tools/tlc -metadir states/rep_naive -config MC_Replicator_Naive_Red.cfg       MC_Replicator_Naive_Red.tla
# GREEN: resumable replicator re-checks target heads on reconnect and re-pushes the gap.
./tools/tlc -metadir states/rep_green -config MC_Replicator_Resumable_Green.cfg MC_Replicator_Resumable_Green.tla
```

`INV_BackfillComplete` / `INV_LiveDelivery` / `INV_NoLoss` hold for the resumable model
(no doc — even one dropped mid-push — is permanently lost under eventual connectivity);
`INV_NoLoss` fails for the naive one.

---

## ACP-on-Commits (dual-path) Runs

Design notes in [Commits_DESIGN.md](Commits_DESIGN.md). The CLAUDE.md footgun: ACP must
gate **both** the User (materialized doc) and Commits (raw delta blocks) read paths, and
the replication path.

```bash
# RED: only the User path is gated — Eve reads the document via _commits.
./tools/tlc -metadir states/com_user -config MC_Commits_Red_UserOnly.cfg          MC_Commits_Red_UserOnly.tla
# RED: replication ungated — Eve receives a commit block for a doc she can't access.
./tools/tlc -metadir states/com_repl -config MC_Commits_Red_ReplicationUngated.cfg MC_Commits_Red_ReplicationUngated.tla
# GREEN: both read paths + replication check ACP.
./tools/tlc -metadir states/com_green -config MC_Commits_Green.cfg                 MC_Commits_Green.tla
```

`INV_BothPathsGated`: an unauthorized reader obtains the document's content via neither
path, locally or over replication.

---

## Block Integrity Runs

Design notes in [Integrity_DESIGN.md](Integrity_DESIGN.md). An adversary gossips forged
blocks against a `VerifyThenMerge` gate (EUF-CMA unforgeability is the assumed boundary).
All target `MC_Integrity_Attacks.tla`.

```bash
# RED: no signature check — a forged block merges (INV_NoForgedMerge).
./tools/tlc -metadir states/int_nc -config MC_Integrity_Red_NoCheck.cfg       MC_Integrity_Attacks.tla
# RED: sig checked but author not bound — a spoofed-author block merges (INV_AuthorBinding).
./tools/tlc -metadir states/int_so -config MC_Integrity_Red_SigOnly.cfg       MC_Integrity_Attacks.tla
# RED: a signature replayed over different content verifies (INV_NoReplayForge).
./tools/tlc -metadir states/int_rp -config MC_Integrity_Red_ReplayNoCheck.cfg MC_Integrity_Attacks.tla
# GREEN: VerifyThenMerge gate — nothing forged merges.
./tools/tlc -metadir states/int_g  -config MC_Integrity_Green.cfg             MC_Integrity_Attacks.tla
# GREEN: the gate does not block honest blocks from converging.
./tools/tlc -metadir states/int_hc -config MC_Integrity_HonestConvergence.cfg MC_Integrity_Attacks.tla
```

---

## ACP Soundness Runs (Lean + TLA+)

Design notes in [Acp_DESIGN.md](Acp_DESIGN.md). **Lean** (`../lean/Acp/Soundness.lean`,
`cd ../lean && lake build`) proves the check algebra: `INV_CheckSound`, `INV_NoEscalation`,
and `INV_PositiveRemovalNoGrant` — plus `buggyDifferenceOverGrants`, a negative theorem
showing a mishandled exclusion operator over-grants. All `[propext, Quot.sound]`, no `sorry`.

**TLA+** proves revocation consistency over replicated tuples:

```bash
# GREEN: a revocation propagates; no node grants the revoked permission.
./tools/tlc -metadir states/acp_g -config MC_Acp_Green.cfg          MC_Acp_Green.tla
# RED: a stale policy cache still grants a revoked permission (INV_RevocationConsistent).
./tools/tlc -metadir states/acp_r -config MC_Acp_StaleCache_Red.cfg MC_Acp_StaleCache_Red.tla
```
