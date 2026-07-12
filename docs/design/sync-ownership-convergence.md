# Sync ownership convergence — head hints + receiver pull (#1116)

**Status:** design for review. Formal model: `proofs/tla/SyncOwnership.tla`
(+ `SyncOwnership_DESIGN.md`). Kernel stage is #1115 (in flight). No inversion
code lands from this worktree.

Three fleet storms in five days (defra-agent#630 hub saturation, cold-join
herd, defra-agent#696 empty-store genesis) all live in one seam: this
implementation gives the **sender** ownership of sync state (push worker
expanding and pushing full DAGs, persisted CID-scoped delivery ledger,
per-peer delivery timeouts, pending-DAG capacity used as push admission)
while **also** running a receiver-side `dag_fetcher` — two ownership models
fighting. The Go reference has one owner. This design converges on it.

All claims below are code-verified. Go citations are against
`sourcenetwork/defradb` v1.0.0 (`3de01484`); Rust citations against `main`
(`99a4626d`).

---

## 1. Corrections to the issue premises

Research for this design corrected four things in the #1116/#1115 framing;
the design is built on the corrected facts (see also the comparative-research
comment on #1116).

1. **Go does keep sender state — but it is marker-plus-rederive, not a
   delivery ledger.** Go persists `/rep/retry/id/{peer}` (a per-peer backoff
   schedule: `retryInfo{NextRetry, NumRetries, Retrying}`) and
   `/rep/retry/doc/{peer}/{docID}` (an empty-value dirty marker per doc)
   — `internal/keys/peerstore.go:14-16`, `replicator.go:567-599`. A retry
   **re-reads the doc's current heads from the headstore** and re-sends one
   head-block PushLog per head (`replicator.go:863-895`). No payloads, no
   CIDs, no per-delivery tracking are ever stored. The marker says "peer X is
   behind on doc Y"; everything else is rederived at send time.
2. **The 2s→16s backoff cited in #1116 is the SE artifact coordinator**
   (`internal/se/coordinator_retry.go:42-49`, scoped to searchable-encryption
   artifacts only). The document replicator ladder is
   **30s→1m→2m→4m→8m→16m→32m**, capped at the last rung, swept every 2s
   (`node/node.go:111-118`, `replicator.go:41-45,706-710`). Stage 3 mirrors
   the document ladder, not the SE one.
3. **Rust already has the per-CID single-flight and the already-merged
   fast-path.** `ProcessQueue` (`crates/p2p/src/sync/queue.rs:12-22`)
   mirrors Go's `processQueue`, and `process_pushlog` checks
   `blockstore.is_merged(cid)` before storing
   (`crates/p2p/src/sync/manager/process/pushlog.rs:125-225`). What #1115
   actually adds: a **cheap shed before full decode** (today the transport
   CBOR-decodes the entire request — 16 MiB cap — and checks signatures
   before any admission decision, `crates/p2p/src/iroh/endpoint_streams.rs:203-235`),
   single-flight coverage of the **fetch trigger**, and counters.
4. **The divergence predates iroh.** Full-DAG sender push was born on libp2p
   (`851bd97d`, 2026-02-11, PR #376: *"instead of relying on Bitswap to fetch
   field blocks (which breaks after connection reset), push the complete DAG
   directly via PushLog"*; also `3f468958`: bitswap "doesn't work reliably
   cross-platform"). The iroh migration (PR #497, 2026-02-24) then fossilized
   it: bitswap was never ported (PR #497 chose CAR-based sync, explicitly
   rejecting iroh-blobs over the CID/BLAKE3 mismatch) and was feature-gated
   to `libp2p-transport` in PR #991. Five months of incident-driven hardening
   followed (#592 → #843 → #1088 → #1099 → #1110), each treating a symptom of
   sender ownership. So: an explicit tactical workaround that the transport
   migration fossilized — not a policy decision to diverge from Go, and not a
   gap iroh created. Corollary: "go back to bitswap" is not the fix; the
   receiver-pull leg has since been rebuilt on iroh (CAR-first fetch PR #453,
   provider rotation PR #1095, selective CAR serving PR #1107).

Go also shares the #1113 bug: a failed collection-commit push calls
`handleReplicatorFailure` with an empty DocID (`replicator.go:417`), creating
a `/rep/retry/doc/{peer}/` marker whose replay (`getHeads(ctx, "")`) can
never succeed (`replicator.go:793-800`). The target model fixes this class
here (§4.2) rather than copying it; worth reporting upstream.

## 2. The Go reference model

**Receive** (`processPushlogRequest`, `internal/db/p2p/p2p.go:602-693`), in
order: decode one head block → re-derive CID and reject mismatch
(`p2p.go:615-628`) → per-CID single-flight (blocking serialize,
`p2p.go:629-637,728-774`) → `IsMerged` fast-path exit (`p2p.go:638-645`) →
self-access ACP gate (gossip only) → `syncDAG`: the receiver pulls every
missing ancestor via **bitswap** through a boxo blockservice session
(sequential DFS, 5s per-link timeout, merged-frontier short-circuit,
`sync_dag.go:41-130`) → **inline merge** → best-effort relay. In-flight sync
state is memory-only; durable state is blocks + a "to-merge" marker cleared
transactionally at merge (`internal/datastore/blockstore.go:50-83`).

**Send** (`SendUpdate`, `p2p.go:695-726`): one CBOR `PushLogRequest` carrying
**one head block** goes to each replicator over a direct stream (10s timeout,
one goroutine per peer, no queue), and fire-and-forget to the doc + collection
gossip topics. Success leaves no trace. Failure writes the marker pair above;
the 2s sweep rederives heads and re-sends on the capped ladder; first failure
per peer aborts the rest of that peer's docs until the next rung
(`replicator.go:722-786`).

**The replicator ack is load-bearing:** the reply is sent only after
`syncDAG` **and** merge complete, so the sender's marker clears only when the
receiver has durably converged on that head (`protocol/comm_channel.go:98-128`).
Go has **no self-re-arming receiver loop** — the sender ladder is the only
re-driver; receiver-side partial progress survives as unmerged blocks that
make the next attempt incremental.

## 3. Where the Rust implementation stands

Per-message wire format is already Go-identical (§6). The structural deltas:

| Concern | Go v1.0.0 | Rust main (99a4626d) |
|---|---|---|
| Replicator push payload | 1 head block | **entire ordered DAG** as a PushLog sequence (`push_worker.rs:280-332`) |
| Sender durable state | marker + rederive | newest `(cid, priority)` per (peer,doc) + dormant watermarks + version compare (`crates/storage/src/stores/peerstore.rs:189-316`, `retry_info.rs:23-35`) |
| Sender in-memory state | goroutine per push | worker pool, bounded backlog (items+bytes), per-peer fairness, (peer,cid) cooldowns, encode cache, fan-out coalescer |
| Delivery timeout | 10s per head | 30s per DAG sequence (`manager/config.rs:55`) |
| Ack meaning | merged | merged **or registered pending** (#1088 invariant) |
| Missing-DAG recovery | receiver bitswap pull, sender-paced re-push | receiver CAR/selective pull (`dag_fetcher.rs`) + pending-DAG registry, **plus** sender full re-push — both at once |
| Receiver re-drive | none (sender-paced) | unpaced: re-issue after partial fetch (`event_handler/bitswap.rs:12-77`), per-connect redrive (`event_handler/mod.rs:58-110`), 4,289 missing-link retries in minutes (#1112) |
| Collection commits | pushed, retry broken (empty-docID marker) | pushed, retry refused with a warn (`push_worker.rs:84-93`, #1113) |

Both halves own delivery simultaneously: the sender re-pushes DAGs on its
persisted ladder while the receiver independently re-fetches the same roots,
and the pending-DAG cap moonlights as push admission. Every recent storm is
an interaction of those two owners.

## 4. Target ownership model

**One owner: the receiver owns completion; the sender owns announcement.**
A PushLog is one head block treated as an idempotent, CID-verified hint.
The receiver pulls the missing DAG at its own pace from its own durable
want-queue. The sender keeps marker-plus-rederive state only.

### 4.1 Receiver state after convergence (authoritative)

| State | Durability | Bound | Today's source (kept/promoted) |
|---|---|---|---|
| Blocks + merged markers | durable | — | blockstore (unchanged) |
| Pending-DAG want-queue: root → missing links, providers, **per-root retry clock + capped backoff** | durable (`/p2p/pending_dag/{root}`) | `max_pending_dags`, overflow nacked | registry + store (`manager/process/pending_dag.rs`, `sync/pending_store.rs`) — reframed from "push admission" to the receiver's own fetch queue; retry clock is new (#1112) |
| Per-CID single-flight, is-merged fast path, pre-decode shed | memory | — | `queue.rs`, `process/pushlog.rs` + #1115 kernel |
| Fetch limiter + provider rotation + query reaping | memory | 4 concurrent fetches | `dag_fetcher.rs` (#1095) |

Receiver re-drives (all paced by the per-root clock; deviation from Go, see
§4.3): on block arrival (progress-driven, stays), on backoff expiry (new),
on peer connect (coalesced through the same clock instead of unconditional
re-emit).

### 4.2 Sender state after convergence (demoted)

| State | Durability | Bound | Notes |
|---|---|---|---|
| `/rep/retry/id/{peer}` — backoff schedule | durable | ≤ 1/peer | Go-parity key, kept |
| `/rep/retry/doc/{peer}/{docID}` — dirty marker, **empty value** | durable | ≤ 1/(peer,doc) | value shrinks from `PersistedPushRetry{cid, priority, pending, …}` to presence |
| `/rep/retry/col/{peer}/{collectionID}` — dirty marker for branchable collection commits | durable | ≤ 1/(peer,collection) | **new key, exceeds Go**; replay rederives collection heads — #1113 dissolves |
| Bounded hint queue (newest head per (peer,scope)) | memory | small | `push_backlog.rs` shrinks: no DAG expansion, no byte-cap pressure, no (peer,cid) cooldowns |

Marker lifecycle (the load-bearing rules, both TLA-modeled):
- **Written before send** (on local update / enqueue), **cleared only on an
  ack for the scope's current head** — an ack for a superseded head does not
  clear (models `complete_retry_document`'s version guard; prevents the
  stale-clear race, `MC_SyncOwnership_Red_StaleAckClears`).
- **Rederive at send time**: a retry re-reads current heads
  (`crates/db-merge/src/push_docs.rs:492-597` already does this) — never a
  stored CID. Ladder: 30s→32m Go-parity, 2s sweep.

### 4.3 Ack semantics: register-then-ack (documented deviation from Go)

Go acks after merge; Rust acks after **durable** pending registration
(#1088 `INV_SuccessImpliesRegisteredOrMerged` + #1099 durability). We keep
the Rust semantics: the ack transfers ownership sender→receiver, and is
honest because the registration is durable and receiver-re-driven. This is
wire-compatible (a reply is a reply; Go peers can't distinguish) and is what
lets the receiver pace large-DAG pulls without churning the sender's ladder.
The price: because the sender's marker clears at registration, the receiver
**must** own re-drive — hence the paced per-root retry clock, where Go needs
none. `MC_SyncOwnership_Red_VolatileRegistration` shows the ack is dishonest
without durability; `PendingDagRestart.tla` already fences the Rust side.

Overflow still nacks (`RATE_LIMITED_MESSAGE`): with a full want-queue the
receiver refuses ownership and the sender's marker stays — that is receiver
pacing, not sender admission. What gets deleted is the *rest* of the
admission complex: the in-process 25→500ms nack retry ladder
(`broadcast.rs:26-50`) collapses into the marker ladder.

### 4.4 Deletion list

| Deleted | Where | Replaced by |
|---|---|---|
| DAG expansion + ordered multi-block push (`load_ordered_dag_blocks`, multi-request `send_ordered_pushlogs_via_transport`) | `push_worker.rs` | single head-hint send, 10s timeout (Go parity) |
| Push encode cache | `sync/push_encode_cache.rs` | nothing — one small signed request per hint |
| Fan-out coalescer's DAG machinery (`expand_unfiltered_dag`, filter document snapshots) | `sync/push_fanout_coalescer.rs` | newest-head-per-(peer,scope) in the hint queue |
| CID-scoped ledger values: `PersistedPushRetry{cid, priority, pending}`, `compare_push_versions`, `observe_push_head` dormant watermarks, `activate_dormant_push_retries` | `crates/storage/src/stores/{retry_info,peerstore}.rs` | empty-value markers + rederive (§4.2) |
| (peer,cid) failure cooldowns, 30s DAG delivery timeout, byte-capped backlog admission | `push_backlog.rs`, `manager/config.rs` | bounded hint queue + marker ladder |
| In-process rate-limited-push retry ladder | `coordinator/broadcast.rs:26-50` | marker ladder |
| #1113 warn-and-drop paths | `push_worker.rs:84-93`, `peerstore.rs:199-203,282` | `/rep/retry/col` markers |
| Gossip direction filter (stage 4) | `event_handler/gossip.rs:26-55` | subscription-implies-acceptance + drop counters (#1114) |
| Push-time `SelectiveCarAccess` grants | `push_worker.rs:224-228` | hint-time grants for the head's DAG (mechanism kept, re-timed; `selective_car_access.rs` unchanged) |

Kept unchanged: gossip `broadcast_coalescer` (250ms window), token-bucket
rate limiter, signature verification, merge queue boundary, `/p2p/sync/status`
(fields change with the ledger).

## 5. Block transport for the pull path: extend the CAR request path (option a)

Options weighed:

- **(a) Extend the existing noq/iroh CAR path — recommended.**
  `CarFetchRequest{root, wanted_cids, recursive}` already is a single-shot
  want-list (exact CIDs, batches of 2048, dedup) with CAR-stream responses,
  provider fan-out with first-usable-wins, 16 MiB/10k-block server bounds,
  per-block CID verification on receipt, provider rotation, stall budgets and
  query reaping (`message/car.rs:14-51`, `iroh/endpoint_rpc.rs:716-825`,
  `sync/car.rs:18-21`, `dag_fetcher.rs`). What's missing is not transport —
  it's **pacing**: the per-root retry clock of §4.1. "Want-list-paced pull"
  = iterate (walk frontier → selective request → verify/store → recompute
  missing) under the fetch limiter and the root's backoff clock. No wire
  change; #1107's selective serving is the server half, shipped.
- **(b) Port bitswap want-list semantics onto iroh — not now.** The
  transport-neutral pieces (block store adapter, classifier/read-gate) exist,
  but the actual protocol (ledger, sessions, HAVE/DONT_HAVE presence,
  per-peer want tracking, cancellation) would be a new ALPN + session state
  machine. Its marginal win over (a) is streaming multi-provider scheduling
  within one session — worth revisiting only if (a)'s batch round-trips
  measurably bottleneck catch-up on real fleets. Revisit trigger: p50 DAG
  catch-up time dominated by round-trip count in `/p2p/sync/status` metrics.
- **(c) Revive the libp2p transport for bitswap — rejected.** defra-agent
  runs iroh; bitswap was abandoned for cause on this codebase (§1, item 4);
  resurrecting a second transport to obtain a fetch protocol inverts the
  dependency. (On actual libp2p meshes — the go-compat lane — real bitswap
  remains available behind `libp2p-transport` and serves the pull path
  against Go peers; that stays, gated, as-is: `host/command_handler/bitswap.rs:42-131`.)

## 6. go-compat: the target is strictly closer to the reference

Wire inventory (all CBOR; no protobuf anywhere in either implementation):

| Message | Go | Rust | After convergence |
|---|---|---|---|
| `PushLogRequest{MetaData, DocID, CID, CollectionID, Creator, Block}` | one per **head** (`protocol/pushlog.go:17-31`) | byte-compatible struct (`message/pushlog.rs:19-79`, definite-map CBOR, PascalCase, fixture-tested) but sent **N per DAG** | one per head — behavioral convergence, zero wire change |
| `PushLogReply` (metadata-only, `ErrMessage` on failure) | after merge | after register-or-merge | unchanged (documented deviation §4.3) |
| Gossip payload (PushLog minus signing fields) | doc + collection topics | same (`pushlog.rs:275-298`) | unchanged; #1114 makes symmetric-mesh *acceptance* match Go (Go has no direction filter) |
| DocSync / BranchableSync | pubsub-RPC | byte-fixture-gated in CI | unchanged |
| Block fetch | bitswap (boxo) | libp2p: real bitswap; iroh: CAR (Rust-only ALPN) | unchanged — mixed Go/Rust meshes are libp2p-only, where bitswap interops today |
| `/rep/retry/*` peerstore keys | marker + rederive | same keys, CID-scoped values | same keys, marker values + new `/rep/retry/col` (Rust-only key, invisible on the wire) |

Deltas needing no shim, argued: (1) head-only push is *what Go receivers
expect and what Go senders already do to us* — today's N-block sequences are
the anomaly (each lands in Go's `processPushlogRequest` as an independent
"head"); (2) ack timing is unobservable on the wire; (3) `/rep/retry/col` and
the CAR ALPN are node-local/Rust-mesh-local. **Flag: none of the deltas
require a compat shim.** One CI gap to close in stage 3: there is no
Go-emitted byte fixture for `PushLogRequest` (DocSync/Branchable have them;
PushLog compat is currently enforced transitively via signature verification
in mixed-cluster tests) — add one alongside the sender change.

Acceptance harness per stage: the go-compat CI suite (mixed Rust↔Go QUIC
clusters, trust-boundary, `parity_*` behavioral probes incl.
`parity_counter_storm_mixed`) + the #1108 storm harness.

## 7. What survives from the filed bugs

- **#1113 — dissolves.** No CID-scoped replay ledger exists to mis-key;
  collection commits get first-class `/rep/retry/col` markers. (Go has the
  same bug upstream; report it.)
- **#1112 — inbound half survives, folded into stage 2** as the per-root
  retry clock + bounded want-queue; the sender-backpressure half evaporates
  (re-hints are cheap and capped; payloads move only by pull). Overflow-nack
  stays as receiver pacing.
- **#1114 — simplifies to deletion** (stage 4): subscription implies
  acceptance; keep the rate limiter and ACP gates; add drop counters.
- **#1115 — is stage 1**, with corrected scope (§1, item 3): cheap shed
  before decode, fetch-trigger single-flight, counters. Build on it, don't
  redo it.
- Existing TLA models: `PushLogAdmission` (ack honesty) and
  `PendingDagRestart` (durable registration) invariants **carry over intact**
  — they are what makes register-then-ack sound. `PushBacklog` /
  `PushCoalescing` guard machinery that shrinks or dies; their green
  properties become trivial or moot at stage 3 (note added to their DESIGN
  docs then; the new `SyncOwnership` model owns the ownership-transfer
  invariants).

## 8. Staged plan

Each stage ships independently, leaves every mesh (Rust-only iroh, mixed
libp2p) working, and is gated by: full integration suite + go-compat CI +
#1108 storm harness (+ the 19-node empty-store genesis repro from
defra-agent#696 for stages 2–4).

1. **Stage 1 — receive-path kernel (#1115, in flight).** Cheap shed before
   full decode; single-flight covering pushlog registration *and* fetch
   trigger; counters (single-flight suppressions, fast-path exits).
   Storm-harness assertion: N same-CID announcements → 1 sync, N−1 cheap.
2. **Stage 2 — receiver-authoritative pull.** Per-root retry clock + capped
   backoff on pending-DAG entries; connect-redrive and post-partial-fetch
   re-issue routed through the clock; want-queue bounds + `/p2p/sync/status`
   surfacing clock state; hint-time `SelectiveCarAccess` grants (accepts both
   today's full-DAG pushes and head-only hints — forward-compatible with
   stage 3 senders and with Go senders, which are already head-only).
   Gate adds: defra-agent#696 repro calm; missing-link retry volume bounded.
3. **Stage 3 — sender demotion + ledger deletion.** Head-hint-only push;
   marker-plus-rederive ledger (`/rep/retry/{id,doc}` value shrink + new
   `/rep/retry/col`); ladder 30s→32m; delete list §4.4; ledger migration:
   existing `PersistedPushRetry` records collapse to markers on first sweep
   (one-way, no downgrade path — release-note it). Add the Go-emitted
   PushLog byte fixture to CI. Rolling upgrade is safe both ways: old
   receivers still fetch pushed heads via pending-DAG + CAR (that path is
   today's recovery path, hardened in stage 2); new receivers accept old
   senders' full-DAG sequences as N independent verified hints.
   Gate adds: `parity_counter_storm_mixed`; storm harness sender-cost
   assertions (bytes sent per re-announcement ~constant).
4. **Stage 4 — direction-filter deletion (#1114).** Subscription implies
   acceptance; delete `gossip.rs:26-55` block; drop counters; symmetric-mesh
   integration test.

Kill criteria / rollback: each stage is a normal PR revert; stage 3's ledger
migration is the only one-way step, hence its own release gate. If stage 2's
paced pull cannot keep the #696 repro calm with senders still full-DAG
pushing, stage 3 does not proceed.

## 9. Formal model

`proofs/tla/SyncOwnership.tla` models the ownership transfer: sender markers
(update → dirty → hint → ack-clear guarded by head currency), receiver
want-queue (bounded, durable, single-flight, paced fetch), crash. Green
proves `INV_ObligationConservation` (every behind-scope is marked, in flight,
or durably wanted — no silent loss), `INV_SingleFlight`,
`INV_ReceiverQueueBounded`, `INV_SenderMarkersOnly` (sender durable state
carries no CIDs/payloads), and `LIVE_EventualCurrency` under weak fairness
with free re-hints (idempotence exercised by construction). Reds:
doc-keyed-only markers (#1113's class), volatile registration (dishonest
ack), duplicate fetch spawn, and stale-ack marker clear. Registered in
`run-all.sh`; zero failures, no sorries-equivalent (all configs
model-checked).

## 10. Non-goals

State-snapshot sync for O(state) cold joins (separate design after this
lands); CRDT commit-granularity changes (defra-agent#687 app-side, schema
compaction future); SE artifact coordinator changes (its 2s→16s ladder and
regenerate-at-retry semantics are already marker-plus-rederive-shaped and
out of scope); upstreaming the Go empty-docID retry fix (report, don't
block).
