# PushCoalescing — latest-head retirement (TLA+ design)

Models issue **#1102**'s safety/liveness boundary across the live outbound queue and
the durable retry marker for document and collection scopes. A head can already be
active when a newer local head arrives;
that old send may finish, but its failure must not recreate a stale retry obligation.

The model uses monotonically increasing natural numbers for `(priority, CID)` head
versions. The Rust implementation uses the real block priority with `Cid::cmp` as a
deterministic tie-break across every layer. `LatestOnly` atomically removes queued and
persisted predecessors before installing the new head. `CurrentOnly` checks the version again on
failure, so superseded active work is retired instead of persisted.

The green configuration proves:

- `INV_OneLiveHead`: at most one queued head per `(scope, peer)`.
- `INV_OnePersistedHead`: at most one durable retry head per `(scope, peer)`.
- `INV_NoStaleRetry`: a persisted retry is always the newest known head.
- `INV_NewestRetained`: retirement never drops the newest obligation; it is queued,
  active, persisted, or already acknowledged.

`MC_PushCoalescing_Red_AppendEvery.cfg` demonstrates the live-queue failure, while
`MC_PushCoalescing_Red_StaleRetry.cfg` demonstrates the active-send race that recreates
a superseded persisted retry.

The model makes `Fail` and its current-version check atomic. The runtime check and
peerstore write are separated by an asynchronous recorder channel: a newer-head
observation can be processed while no record exists, followed by an already-checked
older failure that creates one stale retry. This narrow interleaving is not covered by
`INV_NoStaleRetry`. Stage 3 closes it with a per-peer serialized marker transition and
an in-process head-ack fence. The model's `persisted` head is a ghost witness for which
head dirtied a scope; Rust persists only the presence marker and rederives current heads.

The model intentionally excludes payload encoding and elapsed time. Each admitted job
contains one head block, while exponential backoff is a deterministic pure function
covered by unit tests.

The runtime's update coalescers use a 250 ms trailing-edge quiet period with a hard
1 second maximum delay. The quiet period absorbs short sequential HTTP bursts; the
maximum forces progress for a continuously written scope and bounds the lifetime
of follower tasks. This deliberately accepts up to 250 ms of latency for an isolated
fan-out or gossip obligation. Transactional document push and gossip are awaited in
order inside the detached broadcaster task, so their independent quiet periods can
stack to about 500 ms before gossip is published. Collection commits use an empty
document ID and therefore share one collection-scoped coalescing key rather than
bypassing the current-head fence. A dropped leader removes its window and wakes
followers to re-admit the latest buffered payload, so cancellation cannot wedge the
scope key.

Durable document and collection records are presence-only dirty markers registered
before send. They share `/rep/retry/id/{peer}` and the exact Go ladder (30 seconds,
then 1, 2, 4, 8, 16, and 32 minutes capped). A retry rederives current heads, and a
success clears a marker only while the serialized scope/head attempt remains current.

If a locally produced head cannot be decoded to obtain its priority, it uses a
CID-scoped volatile obligation key and bypasses scope coalescing; an unproven version
is never retired as an older scope head. The CID is not persisted.

Latest-head retirement assumes that a later locally produced scope head subsumes its
earlier linear predecessor. Concurrent sibling heads that must both remain heads do
not enter this live update path: replay and field/KMS DAG paths bypass the outbound
backlog, collection commits use separate collection-scoped markers, and
`rebroadcast_on_merge` is false at every production construction site. Enabling that
flag, or adding another live producer of non-subsuming document siblings, requires
distinct obligation keys or extending the model from one total-order head to an
antichain.
