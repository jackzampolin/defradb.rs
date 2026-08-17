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
- `pending` is the receiver's bounded durable want queue.
- `serveAuth` records the volatile exact-root CAR grant installed before a
  hint. It is always lost on sender restart.
- `serveScopes` records the replication or collection-subscription policy from
  which exact-root authority can be reconstructed. In the green policy that
  policy is persisted locally or re-established by the configured subscriber
  after reconnect. A fetch may use either the live grant or derive an exact
  root capability from the root's classified collection and `serveScopes`.
- `flights[root]` counts CAR fetch owners for an exact root.
- `drained` records whether a fetch owner retained its productive CAR stream
  through transport completion instead of cancelling after the first block.
- `ProviderMode` distinguishes origin-bound recovery from authenticated-hop
  recovery. Iroh selects the independently verified origin when that endpoint
  is already connected to the receiver; otherwise it records the
  transport-authenticated connected hop. This lets a directly connected hub
  recover from the actual publisher instead of a partial relay, while a sparse
  gossip mesh can still recover hop by hop without pretending every receiver
  has a direct route to the publisher. The red modes retain an unroutable
  origin or an unverified relay.
- `OriginAuthMode` distinguishes an origin bound by native transport metadata
  or an endpoint-key signature from an unsigned peer ID copied out of the
  gossip payload. A signed CRDT block proves content authorship, not that the
  named endpoint currently holds and can serve the linked DAG.
- `FetchMode` distinguishes bounded rooted discovery followed by exact
  selective-CAR recovery from both extremes exposed by fleet evidence: an
  unbounded recursive historical walk, and exact-CID requests that have no
  routable provider after sender demotion removes per-CID advertisements.

`Update` records sender durability before exposing the live hint. `ReHintMarker`
has no stored version to replay and therefore must read `localV[scope]`.
`ProcessMerged` and `ProcessRegister` are the only success-ack actions.
`ProcessNack` leaves sender ownership intact. `ClaimFetch` represents every
retry-expiry, connect-expedite, and partial-progress trigger after coalescing
through the same #1123 clock claim. `Crash` clears in-memory fetch owners and,
only in the volatile red policies, receiver registrations or CAR-serving
authority. In particular, a collection-topic PushLog does not require an
outbound replicator record on the source: the receiver's configured collection
subscription is the restart-reconstructible serving scope.

`ClaimMerge` distinguishes batching from concurrency: one receiver merge
writer may claim several ready roots and commit them together, but two
independent writers may not overlap. This matches Go's explicit P2P merge
serialization while retaining Rust's ordered batch transaction optimization.
Frontend and transport adapters are not allowed to select a different merge
ownership policy.

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
- `INV_SingleMergeWriter`: one receiver-owned writer may hold a batch of ready
  roots, but independent merge writers never overlap.
- `INV_FetchOwnerDrainsResponse`: first-block progress cannot truncate the CAR
  response that the sole fetch owner needs to complete the DAG.
- `INV_ReceiverQueueBounded`: durable want registrations never exceed `Cap`.
- `INV_OnePendingHeadPerScope`: one sender retains only its current durable
  receiver obligation for a document or collection scope; a newer causal head
  atomically supersedes that sender's older root.
- `INV_PendingServiceable`: every success-acked pending root retains a
  restart-safe CAR-serving path.
- `INV_PendingHasRoutableProvider`: every success-acked root retains either the
  native authenticated publisher or an authenticated connected Iroh hop.
- `INV_PendingHasAuthenticatedProvider`: a receiver never transfers durable
  ownership to a provider selected from an unsigned payload claim or an
  unverified relay.
- `INV_KnownFrontierUsesSelective`: once the head block exposes the missing
  frontier, receiver work requests those exact CIDs instead of first walking
  and serializing the entire historical DAG.
- `INV_SenderMarkersOnly`: sender durable state contains no CID/version or
  payload delivery record.
- `INV_MarkersReplayable`: both document and collection scopes inhabit the same
  marker/rederive protocol.
- `LIVE_EventualCurrency`: under fair hint delivery and receiver dispatch, every
  scope eventually stays current despite drops, nacks, and one restart.

The register-then-ack deviation from Go remains honest because `pending` is
durable in the green policy. An optimistic acknowledgement for an in-flight
fetch owner is deliberately not expressible.

## Configurations

| Configuration | Verdict | Boundary isolated |
| --- | --- | --- |
| `MC_SyncOwnership_Green.cfg` | GREEN | marker+rederive, durable registration, one fetch owner, current-head ack guard |
| `MC_SyncOwnership_Green_IrohHop.cfg` | GREEN | signed origin envelope plus transport-authenticated connected-hop recovery on a sparse Iroh mesh |
| `MC_SyncOwnership_Red_DocOnlyMarkers.cfg` | RED | a collection update is dropped after no collection marker was recorded |
| `MC_SyncOwnership_Red_PayloadLedger.cfg` | RED | current-main CID/payload-valued sender durability violates marker-only ownership |
| `MC_SyncOwnership_Red_VolatileRegistration.cfg` | RED | restart destroys the only state behind a success ack |
| `MC_SyncOwnership_Red_DuplicateFetch.cfg` | RED | two triggers claim the same root concurrently |
| `MC_SyncOwnership_Red_StaleAckClears.cfg` | RED | an old ack clears the marker for a newer head |
| `MC_SyncOwnership_Red_VolatileServeAuthority.cfg` | RED | sender restart loses the CAR authority behind a success-acked pending root |
| `MC_SyncOwnership_Red_RelayOnlyProvider.cfg` | RED | an unverified payload relay is recorded instead of a transport-authenticated recovery hop |
| `MC_SyncOwnership_Red_UnroutableOrigin.cfg` | RED | the recorded publisher has no direct-or-relayed CAR route, so durable receiver ownership cannot complete |
| `MC_SyncOwnership_Red_UnsignedIrohOrigin.cfg` | RED | an Iroh relay accepts an unsigned payload origin as if transport-authenticated |
| `MC_SyncOwnership_Red_CancelOnProgress.cfg` | RED | the receiver cancels a productive CAR stream after its first block, stranding descendants |
| `MC_SyncOwnership_Red_RecursiveFirst.cfg` | RED | a known missing frontier is delayed behind a recursive historical CAR walk |
| `MC_SyncOwnership_Red_EveryRoot.cfg` | RED | successive current heads from one sender/scope accumulate obsolete durable roots |
| `MC_SyncOwnership_Red_ParallelMerge.cfg` | RED | frontend-selected parallel merge workers overlap independent receiver writers |

Each red configuration checks only type safety plus the property it is meant to
violate, keeping its counterexample attributable.

## Runtime conformance fence

The deterministic ownership A/B integration test holds topology, logical DAG,
admission bounds, and transport constant while switching only sender delivery
shape. Both arms run a real receiving coordinator through pending registration,
CAR pull, merge, and quiescence. The current-policy arm demonstrates the
dependency-PushLog feedback: a field-head obligation consumes the fixed slot,
the composite head receives the actionable capacity nack, and the receiver
does not reach the source's current head on the first wave. The frozen sender
then retains its logical-head marker, handles the nack, retries/re-offers the
root after the field obligation drains, and is required to reach the same final
state. The target arm witnesses one hint, restart-safe rooted selective-CAR
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

`issue1154_repro` adds the restart/liveness fence that the small A/B cannot: it
success-acks hundreds of durable receiver obligations, restarts the hub with a
one-root admission bound, and requires every current document to merge through
the preserved receiver clock. This catches both dormant reconnect schedules
and response-completion latency that can otherwise serialize the sender and
receiver retry ladders into a non-quiescing lockstep.

Go parity has two distinct identity patterns. Direct Go PushLog streams replace
`SenderID` with the authenticated connection peer, derive that ID from the
included public key, clear `Signature`, re-encode canonical CBOR, and verify.
Go pubsub relies on native signed gossipsub instead of signing the PushLog
payload, but `go-libp2p-pubsub-rpc` currently forwards `ReceivedFrom` (the last
relay), not the signed original `GetFrom()` author. Stage 3 does not treat that
hop as the content author. The Iroh endpoint ID is the public key and
`OriginSignature` is cleared for canonical encoding, so the original hint is
verified independently. Durable recovery prefers that verified origin when it
is present in the receiver's transport-connected set; otherwise it is bound to
Iroh's separately authenticated `delivered_from` hop, which is necessarily
connected and can complete the root hop by hop in a sparse mesh. The
additive-field Go fixture
proves existing Go decoders continue to accept the core head hint without
confusing `SourcePeerID` with authenticated Go message metadata.
