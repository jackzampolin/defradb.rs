# SyncOwnership — head-hint ownership transfer

This model carries the approved sender-demotion stage of #1116 into the current
proof layout. A sender durably records a document or collection scope, announces
one current composite/collection head, and clears its marker only after an
acknowledgement for that same current head. A receiver either reports the head
already merged or durably registers a bounded missing-DAG obligation before
acknowledging it. The receiver's #1123 per-root clock is the sole fetch-claim
boundary; CAR completion retires the durable obligation.

Companion models retain their narrower contracts:

- `PushLogAdmission.tla`: overflow nacks instead of laundering a dropped
  registration into success.
- `PendingDagRestart.tla`: a success-acked registration survives restart.
- `PushBacklog.tla`: resident outbound work and task handles remain bounded.
- `PushCoalescing.tla`: superseded live heads do not recreate stale work. Stage
  3 replaces its CID-valued durable ledger with the head-current marker guard
  modeled here.

## State and actions

`Docs` and `Cols` are both replayable scopes. `localV` and `mergedV` abstract
current composite or collection head CIDs with monotone versions.

- `dirty` is the permitted sender durability: a set of scopes only.
- `payloadLedger` is the current-main countermodel: sender durability contains
  a scope plus a particular CID/version.
- `inflight` contains idempotent head hints.
- `pending` is the receiver's bounded durable want queue (`Cap`). `FetchCap`
  separately bounds roots that have crossed the clock's fetch-ownership seam;
  durable wants may wait, but scheduled event handoffs and retained tasks may
  not form another queue behind the attempt semaphore.
- `serveAuth` records the volatile exact-root CAR grant installed before a
  hint. It is always lost on sender restart.
- `serveScopes` records the exact-root block-serving policy from which authority
  can be reconstructed. In the green policy the authenticated CAR requester,
  the requested root, and either durable replication configuration or ACP read
  authorization for the root reconstruct that capability after restart. This
  matches Go's Bitswap access boundary and avoids making durable completion
  depend on the ordering of a process-local gossip-neighbor observation. A
  fetch may use either the live grant or derive the rooted capability from this
  policy.
- `flights[root]` counts CAR fetch owners for an exact root.
- `drained` records whether a fetch owner retained its productive CAR stream
  through transport completion instead of cancelling after the first block.
- `lostCompletions` records a transport terminal result that the fetch owner
  cannot observe. The green `CompletionMode` latches early results, always
  drains state-bearing events, and services host response commands while event
  delivery is backpressured. Red modes discard an edge before waiter
  registration, stop draining behind saturated spawned workers, or block the
  host on a full event channel while an admitted handler waits for that host's
  response command. Each strands the sole fetch owner.
- `terminalFlights[root]` counts durable pending-record cleanup writers.
  Production admits one; the duplicate-terminal red policy permits two
  merge/quarantine observations to contend on the same storage key.
- `ProviderMode` distinguishes origin-bound recovery from authenticated-hop
  recovery and abstracts the least-qualified peer admitted to the provider
  rotation. Iroh may use the independently verified publisher only when that
  endpoint has a reconnectable transport route. A temporary disconnect is a
  stuttering interval, not a terminal disposition: the durable root stays
  pending and the one receiver clock waits for fair reconnection. Alternates
  require positive evidence for a
  CID on the missing frontier; connectivity or root possession is insufficient.
  An authenticated gossip hop is a red mode: the fleet demonstrated that a
  relay may possess the announced root while all linked descendants are absent.
  The other red modes retain an unroutable origin or an unverified relay.
- `OriginAuthMode` distinguishes an origin bound by native transport metadata
  or an endpoint-key signature from an unsigned peer ID copied out of the
  gossip payload. A signed CRDT block proves content authorship, not that the
  named endpoint currently holds and can serve the linked DAG.
- `FetchMode` distinguishes bounded rooted discovery followed by a capped CAR
  descendant closure from the already-known missing frontier from both
  extremes exposed by fleet evidence: an unbounded recursive historical-root
  walk, and exact-CID requests that have no routable provider after sender
  demotion removes per-CID advertisements. This is the Rust CAR analogue of
  Go's one per-root blockservice session: one receiver owner advances through
  linked blocks without opening a new transport round trip for every DAG
  layer. The existing CAR block/byte caps bound each response; a truncated
  response recomputes the frontier through the same owner and pacing clock.

`Update` records sender durability before exposing the live hint. `ReHintMarker`
has no stored version to replay and therefore must read `localV[scope]`.
`ProcessMerged` and `ProcessRegister` are the only success-ack actions.
`ProcessNack` leaves sender ownership intact. Every failed delivery, including
an actionable capacity nack, advances the one durable per-peer retry clock on
the Go-compatible 30s, 1m, 2m, 4m, 8m, 16m, 32m ladder. The 2-second sweep is
only a due-record poll; it is not an alternate retry cadence. Rust implements
that state machine once in `defra-p2p-adapter::retry`: CLI, embedded, and
defra-node, over both libp2p and Iroh, construct the same failure recorder and
marker-rederive loop. A disconnected peer is redialed only after its durable
schedule is due, and that failed delivery attempt advances the existing rung;
a later reconnect makes the same rung immediately due without resetting it.
The transport-specific document pusher and replay implementations have likewise
collapsed into one `P2PTransport` path, so no runtime can select different
marker, ack-fence, rederive, or pacing semantics.

`ClaimFetch` represents every
retry-expiry, connect-expedite, and partial-progress trigger after coalescing
through the same #1123 clock claim. A tick with no currently connected
qualified provider does not claim a fetch owner and does not exhaust the root;
it is a stuttering refinement until reconnect expedites that same clock.
The Rust conformance seam follows this literally: live registration, partial
CAR progress, reconnect, and restart may create or expedite a due root, but
only the receiver clock emits `DagNeedsFetch`. The clock reserves one of the
bounded root-owner slots before advancing a retry rung; the reservation covers
both the event handoff and retained fetch task, so pending roots do not form a
hidden task queue behind the attempt semaphore. In particular, registration
does not retain a transport reply or the pending-state writer while waiting on
the merge/fetch event channel. `CompleteFetch` does not remove `pending`:
single and batched Rust merge paths retain the same live registration until
merge/mark or quarantine reaches a durable terminal outcome. If merge returns
a transient error, the existing per-root clock sees the locally complete root
and re-emits merge work on its paced rung; restart and peer-connect resync are
not alternate cleanup/retry owners.
`Crash` clears in-memory fetch owners and, only in the volatile red policies,
receiver registrations or CAR-serving authority. In particular, a
collection-topic PushLog does not require an outbound replicator record or a
previously observed subscriber entry on the source: the authenticated CAR
requester and the exact requested root are checked against the source's durable
replicator/ACP policy to reconstruct the serving scope.

`CompleteTerminal` is also the cancellation point for a fetch claimed during
the merge-to-durable-cleanup window. Production represents this with the
pending generation lease: removing or superseding the registered root makes
the retained fetch owner non-current, so it cannot occupy `FetchCap` after the
obligation is terminal.

`ClaimMerge` distinguishes batching from concurrency: one receiver merge
writer may claim several ready roots and commit them together, but two
independent writers may not overlap. This matches Go's explicit P2P merge
serialization while retaining Rust's ordered batch transaction optimization.
Frontend and transport adapters are not allowed to select a different merge
ownership policy.

For a configured multi-hop replicator chain, the model composes once per hop.
After B completes `CompleteMerge` for A's head, B may become the sender of a
new downstream instance to C: B records its B→C scope marker and announces the
same current head only after B owns the complete DAG. This matches Go's
post-merge `Update` → `SendUpdate` replicator fanout. Rust performs that narrow
explicit-replicator fanout independently of optional gossip rebroadcast, so
stage 3 does not enable the stage-4 gossip-direction change. A root-only gossip
relay has not completed `Merge` and therefore cannot become the downstream
provider.

Reconnect is evidence that an existing sender scope marker is deliverable
again, so the runtime moves that peer's existing marker schedule to "due now"
without resetting its retry rung. This is an enabling event for
`ReHintMarker`, not a second sender retry loop or a second receiver fetch
trigger. On libp2p, the claimed receiver owner waits for the rooted CAR response
event (while retaining its bounded timeout and lease checks); transport
completion therefore releases the one-root admission slot without a polling
delay.

## Checked properties

- `INV_ObligationConservation`: when a receiver is behind the sender's current
  head, that exact current obligation is in sender durability, in flight,
  durably registered, or merged.
- `INV_SingleFlight`: at most one fetch owner exists for an exact root/CID.
- `INV_FetchOwnersBounded`: scheduled and running fetch ownership never
  exceeds `FetchCap`, including the receiver-clock event handoff.
- `INV_SingleMergeWriter`: one receiver-owned writer may hold a batch of ready
  roots, but independent merge writers never overlap.
- `INV_SingleTerminalWriter`: repeated merge/quarantine observations for one
  root coalesce through one durable metadata writer, so idempotent cleanup
  cannot become concurrent OCC writers.
- `INV_FetchOwnerDrainsResponse`: first-block progress cannot truncate the CAR
  response that the sole fetch owner needs to complete the DAG.
- `INV_ReceiverQueueBounded`: durable want registrations never exceed `Cap`.
- `INV_OnePendingHeadPerScope`: one sender retains only its current durable
  receiver obligation for a document or collection scope; a newer causal head
  atomically supersedes that sender's older root.
- `INV_PendingServiceable`: every success-acked pending root retains a
  restart-safe CAR-serving path.
- `INV_PendingHasRoutableProvider`: every success-acked root retains the
  independently authenticated publisher through a reconnectable transport
  route. Continuous connection is not required; temporary unavailability
  preserves the durable obligation without recording terminal exhaustion.
- `INV_PendingHasAuthenticatedProvider`: a receiver never transfers durable
  ownership to a provider selected from an unsigned payload claim or an
  unverified relay.
- `INV_PendingHasCompleteProvider`: neither the durable source nor an alternate
  fetch candidate may be an authenticated gossip hop that owns only the root;
  linked-CID alternates require positive availability evidence.
- `INV_PendingHasServingAuthorization`: a complete, routable origin must also
  be able to authorize the receiver at the block-serving boundary. With ACP
  enabled, endpoint authentication alone is insufficient: the endpoint must
  resolve the receiver's authenticated Defra identity (matching Go's identity
  challenge/token exchange), or an exact-root/configured replication grant
  must cover the request.
- `INV_KnownFrontierUsesSelective`: once the head block exposes the missing
  frontier, receiver work requests those exact CIDs instead of first walking
  and serializing the entire historical DAG.
- `INV_SenderMarkersOnly`: sender durable state contains no CID/version or
  payload delivery record.
- `INV_MarkersReplayable`: both document and collection scopes inhabit the same
  marker/rederive protocol.
- `LIVE_EventualCurrency`: under fair hint delivery and receiver dispatch, every
  scope eventually stays current despite drops, nacks, and one restart.
- `LIVE_EventualReceiverQuiescence`: currency is not enough; every merged durable
  receiver obligation is eventually retired.

The register-then-ack deviation from Go remains honest because `pending` is
durable in the green policy. An optimistic acknowledgement for an in-flight
fetch owner is deliberately not expressible.

## Configurations

| Configuration | Verdict | Boundary isolated |
| --- | --- | --- |
| `MC_SyncOwnership_Green.cfg` | GREEN | marker+rederive, durable registration, one fetch owner, current-head ack guard |
| `MC_SyncOwnership_Green_IrohOrigin.cfg` | GREEN | signed, transport-routable Iroh origin owns the complete linked DAG |
| `MC_SyncOwnership_Red_DocOnlyMarkers.cfg` | RED | a collection update is dropped after no collection marker was recorded |
| `MC_SyncOwnership_Red_PayloadLedger.cfg` | RED | current-main CID/payload-valued sender durability violates marker-only ownership |
| `MC_SyncOwnership_Red_VolatileRegistration.cfg` | RED | restart destroys the only state behind a success ack |
| `MC_SyncOwnership_Red_DuplicateFetch.cfg` | RED | two triggers claim the same root concurrently |
| `MC_SyncOwnership_Red_StaleAckClears.cfg` | RED | an old ack clears the marker for a newer head |
| `MC_SyncOwnership_Red_VolatileServeAuthority.cfg` | RED | sender restart loses the CAR authority behind a success-acked pending root |
| `MC_SyncOwnership_Red_RelayOnlyProvider.cfg` | RED | an unverified payload relay is recorded instead of a transport-authenticated recovery hop |
| `MC_SyncOwnership_Red_UnroutableOrigin.cfg` | RED | the recorded publisher has no direct-or-relayed CAR route, so durable receiver ownership cannot complete |
| `MC_SyncOwnership_Red_UnsignedIrohOrigin.cfg` | RED | an Iroh relay accepts an unsigned payload origin as if transport-authenticated |
| `MC_SyncOwnership_Red_RootOnlyHop.cfg` | RED | an authenticated gossip relay owns the root but cannot serve its linked descendants |
| `MC_SyncOwnership_Red_UnauthorizedOrigin.cfg` | RED | the authenticated origin owns the DAG but cannot authorize the receiver, so it serves metadata only |
| `MC_SyncOwnership_Red_CancelOnProgress.cfg` | RED | the receiver cancels a productive CAR stream after its first block, stranding descendants |
| `MC_SyncOwnership_Red_RecursiveFirst.cfg` | RED | a known missing frontier is delayed behind a recursive historical-root CAR walk |
| `MC_SyncOwnership_Red_EveryRoot.cfg` | RED | successive current heads from one sender/scope accumulate obsolete durable roots |
| `MC_SyncOwnership_Red_ParallelMerge.cfg` | RED | frontend-selected parallel merge workers overlap independent receiver writers |
| `MC_SyncOwnership_Red_DuplicateTerminal.cfg` | RED | two terminal observations concurrently delete the same durable pending root |
| `MC_SyncOwnership_Red_EdgeTriggeredCompletion.cfg` | RED | a fast CAR failure is discarded before the fetch owner registers its completion waiter |
| `MC_SyncOwnership_Red_WorkerSaturatedCompletion.cfg` | RED | saturated spawned workers stop draining the CAR completion required by an active fetch owner |
| `MC_SyncOwnership_Red_SharedServeWorkers.cfg` | RED | slow ownership registration consumes every worker needed to serve receiver-owned CAR recovery |
| `MC_SyncOwnership_Red_EagerIdentityLookup.cfg` | RED | a durable replicator grant waits behind an unnecessary reverse DID challenge until CAR timeout |
| `MC_SyncOwnership_Red_BlockingHostEvent.cfg` | RED | a full event channel blocks the host command needed to reply and release admitted work |
| `MC_SyncOwnership_Red_BusyExhaustion.cfg` | RED | a useful CAR coalesced behind an existing shared-CID storage owner is misclassified as terminal provider exhaustion instead of returning to the paced root clock |

Each red configuration checks only type safety plus the property it is meant to
violate, keeping its counterexample attributable.

## Runtime conformance fence

The deterministic ownership A/B integration test holds topology, logical DAG,
admission bounds, and transport constant while switching only sender delivery
shape. Both arms run a real receiving coordinator through pending registration,
CAR pull, merge, and quiescence. The current-policy arm demonstrates the
dependency-PushLog feedback for a collection DAG: the receiver safely stores
the legacy field PushLog as a descendant, the standalone composite dependency
consumes the fixed slot, the collection head receives the actionable capacity
nack, and the receiver does not reach the source's current head on the first
wave. The frozen sender then retains its logical-head marker, handles the nack,
retries/re-offers the root after the dependency obligation drains, and is
required to reach the same final state. The target arm witnesses one hint,
restart-safe rooted selective-CAR
authority, one receiver fetch owner, bounded pending and persisted high-water,
balanced registered/merged/quarantined terminal counts, zero retained handles
after shutdown, and a receiver head identical to the source without a sender
retry cycle. The test proves admission amplification and extra feedback work;
it does not claim a fair old sender is permanently unable to converge. The
delivery-shape-only worker fence remains separate and is not presented as
convergence evidence. Storage tests bind
document and collection marker migration, current-head rederivation, and stale
ack protection. `p2p_admission_restart` continues to bind durable
register-then-ack, and the mixed Go/Rust suite plus the Go-emitted PushLog byte
fixture bind wire compatibility.

Receiver terminal conformance treats the pending store as authoritative over
its bounded in-memory accounting cache. The resync snapshot shares the same
metadata writer as registration, terminal deletion, and quarantine, then
replaces (rather than unions) the cached root set. A terminal merge also clears
any older live pending entry for that exact root. Newly merged, batch-merged,
already-merged, and stale PushLog observations all enter the same idempotent
terminal reconciliation operation; raw live-map deletion is not exposed at the
coordinator boundary. Deterministic tests retain both fleet-observed red states:
a deleted durable record with a live pending entry must not resurrect persisted
accounting, and a root merged through another arrival path must leave neither
receiver representation live. These are runtime refinements of
`ClaimTerminal`/`CompleteTerminal`; they do not add a second cleanup clock or
change the model's legal ownership transitions.

The Iroh conformance suite also fans one multi-block head from a serviceable
origin to more receivers than the provider's fixed CAR worker reserve. Every
receiver must merge and retire both live and durable state with no terminal
fetch exhaustion. This isolates bounded DefraDB fan-out from downstream client
write/query amplification; it does not substitute for the full fleet gate.

The outbound backlog retains its queue-wide resident-byte admission cap as a
hard memory bound for variable-sized head blocks. This is not CID-valued
delivery state: bytes are counted only while a bounded head-hint job is resident
and are never persisted or used as a retry identity.

`issue1154_repro` adds the restart/liveness fence that the small A/B cannot: it
success-acks hundreds of durable receiver obligations, restarts the hub with a
one-root admission bound, and requires every current document to merge through
the preserved receiver clock. This catches both dormant reconnect schedules
and response-completion latency that can otherwise serialize the sender and
receiver retry ladders into a non-quiescing lockstep.

Transport completion is ordered behind durable CAR response handling for the
same query. Every node surface uses one bounded scheduling policy with explicit
classes. PushLog, DocSync, BranchableSync, and decoded sync gossip occupy the
ownership-admission worker set. CAR requests use a distinct fixed
recovery-serving set, so serialized durable registration cannot consume the
provider capacity needed to discharge an acknowledged receiver obligation.
Received blocks, replies, and terminal results use the fixed completion set,
while lightweight peer state drains inline. Each bounded request class returns
the same actionable overload result rather than creating a queue or waiting for
a slot. An Iroh `BitswapComplete` cannot overtake storage of the response that
made the query successful or remain queued behind request-worker saturation.
Exact-CID owners may reap once every requested CID is durable; partial responses
retain the transport-completion drain fence.
Completion registration is also race-free in the other direction: Iroh may
finish a failed query before `sync_blocks` returns its query ID to the fetch
task. The bounded completion tracker latches that result, and registration
consumes it exactly once instead of burning the 30-second watchdog. This is the
Rust analogue of Go's blockservice session, which owns request and completion
inside one call and therefore has no separate edge-triggered subscription.

Every Rust node surface uses the same bounded transport-event dispatcher, with
the existing transport channel as its sole resident queue. There is no second
priority/backlog queue and no entrypoint-local concurrency policy. Sync-request
classification applies the fixed ownership-admission, recovery-serving, and
completion bounds and their actionable overflow; no class borrows another
class's permit. Completion work
is also bounded, with no second resident queue: its upstream producers are the
already-bounded receiver fetch/reply paths, and the transport channel retains
backpressure if all completion owners are occupied. Searchable-
encryption artifacts and management operations retain their existing event
paths and are not modeled as DAG-recovery owners in stage 3. On libp2p, a
full event channel no longer blocks the host from servicing response commands:
the event forwarder services commands while waiting for channel capacity. This
breaks the bounded-channel cycle in which a handler waited for the host while
the host waited for that handler to drain another event.

The receiver's process queue remains the DAG ownership boundary: a CAR response
atomically claims its root and contained CIDs without waiting, and an
overlapping arrival coalesces behind the current owner. Completion preserves a
third, explicit `deferred` disposition for this local contention: it releases
the current fetch lease and returns the durable root to the one paced receiver
retry clock without consuming provider-failure attempts or incrementing
terminal exhaustion. Only the storage owner may publish success. This is the bounded Rust
refinement of Go's per-root `processQueue`: duplicate ingest cannot retain
dispatcher workers while waiting for the owner that needs that transport to
make progress.
The completion edge is emitted once, after the bounded storage-conflict retry
finishes; an intermediate retriable attempt is not a provider failure and
cannot start a second recovery path.
PushLog requests use the same non-waiting root claim. A duplicate may success-
ack only after observing an existing durable registration or terminal merge;
otherwise it receives an actionable in-flight nack and the sender's durable
marker clock re-offers it. Explicit replay does not get a hidden waiter lane.
The dispatcher saturation regression fills every admission worker and every
bounded rejection slot with deliberately stalled sinks, proves later tokens are
closed rather than spawning detached writes, and proves a terminal event still
drains. A second regression fills the admission set and proves a CAR request
still reaches its recovery-serving reserve; that reserve has its own bounded,
actionable overflow fence. The converse regression stalls durable completion
work and proves inbound CAR serving still proceeds. It also holds the event
channel open after completion and requires live
scheduler counts to return to zero. The libp2p host regression fills the real
256-event channel and proves a response command is serviced before event
capacity is released; completed command tasks are reaped by that same host loop
instead of retained until shutdown. The CAR regression proves a
duplicate neither writes nor reports success before the root owner's durable
storage.

Go parity has two distinct identity patterns. Direct Go PushLog streams replace
`SenderID` with the authenticated connection peer, derive that ID from the
included public key, clear `Signature`, re-encode canonical CBOR, and verify.
Go pubsub relies on native signed gossipsub instead of signing the PushLog
payload, but `go-libp2p-pubsub-rpc` currently forwards `ReceivedFrom` (the last
relay), not the signed original `GetFrom()` author. Stage 3 does not treat that
hop as the content author. The Iroh endpoint ID is the public key and
`OriginSignature` is cleared for canonical encoding, so the original hint is
verified independently. Durable recovery prefers that verified origin when it
is transport-routable. A relayed hint whose verified origin is not routable is
dropped; the authenticated propagation hop is never promoted into a durable
provider merely because it forwarded gossip. Provider rotation may add a peer
only after positive evidence for a CID on the missing frontier. The additive-field Go fixture
proves existing Go decoders continue to accept the core head hint without
confusing `SourcePeerID` with authenticated Go message metadata.

Go's block-serving filter also resolves an unknown requester's Defra identity
through a challenge/response protocol, verifies that the returned token is
bound to the local peer ID, and then evaluates document ACP. Rust libp2p has
the same resolver, while Iroh previously installed `AnonymousResolver`; that
made a complete signed origin return only universally readable metadata to a
relayed subscriber. The Iroh resolver uses its authenticated QUIC endpoint as
the peer binding and the same audience-bound Defra identity token before ACP
evaluation. Commit signatures remain content-authorship evidence; they do not
replace this requester identity binding. Verified endpoint-to-DID bindings use
a bounded, expiring positive cache, matching the established libp2p behavior
without serializing every CAR request behind a fresh network challenge.
Concurrent misses for one endpoint share one bounded in-flight challenge;
failed resolutions are shared only by those waiters and are never cached.
As in Go, durable replicator authorization short-circuits before this fallback:
a configured peer never waits for an unrelated reverse identity challenge to
receive its CAR. This ordering is a liveness boundary, because the CAR response
stream and the identity challenge otherwise share the same transport timeout.
