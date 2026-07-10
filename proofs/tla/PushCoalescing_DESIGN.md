# PushCoalescing — latest-head retirement (TLA+ design)

Models issue **#1102**'s safety/liveness boundary across the live outbound queue and
the durable retry ledger. A head can already be active when a newer local head arrives;
that old send may finish, but its failure must not recreate a stale retry obligation.

The model uses monotonically increasing natural numbers for `(priority, CID)` head
versions. The Rust implementation uses the real block priority with CID bytes as a
deterministic tie-break. `LatestOnly` atomically removes queued and persisted
predecessors before installing the new head. `CurrentOnly` checks the version again on
failure, so superseded active work is retired instead of persisted.

The green configuration proves:

- `INV_OneLiveHead`: at most one queued head per `(document, peer)`.
- `INV_OnePersistedHead`: at most one durable retry head per `(document, peer)`.
- `INV_NoStaleRetry`: a persisted retry is always the newest known head.
- `INV_NewestRetained`: retirement never drops the newest obligation; it is queued,
  active, persisted, or already acknowledged.

`MC_PushCoalescing_Red_AppendEvery.cfg` demonstrates the live-queue failure, while
`MC_PushCoalescing_Red_StaleRetry.cfg` demonstrates the active-send race that recreates
a superseded persisted retry.

The model intentionally excludes payload encoding and elapsed time. Shared encode-cache
lifetime is Rust ownership (`Weak<CID> -> Arc<entry>`, so the last peer drops the entry),
and exponential backoff is a deterministic pure function covered by unit tests.

The runtime's update coalescers use a 250 ms trailing-edge quiet period with a hard
1 second maximum delay. The quiet period absorbs short sequential HTTP bursts; the
maximum forces progress for a continuously written document and bounds the lifetime
of follower tasks. This deliberately accepts up to 250 ms of latency for an isolated
fan-out or gossip obligation. Transactional document push and gossip are awaited in
order, while collection commits use an empty document ID and therefore a separate
CID-scoped backlog key.

Dormant durable records are volatile-send watermarks, not successful acknowledgements.
They are promoted to immediately due pending retries when a process starts, because the
in-memory send they represented cannot survive a restart. A live failure activates its
retry with the first deterministic jittered interval (15–30 seconds for the 30-second
backoff cap); the in-memory backlog owns the immediate attempt.

Latest-head retirement assumes that a later document head subsumes its earlier linear
predecessor. Concurrent sibling heads that must both remain heads do not enter this
live document-update path: replay and field/KMS DAG paths bypass the outbound backlog,
and collection commits are CID-scoped rather than document-scoped. If a future live
producer can emit non-subsuming document siblings, it must use distinct obligation keys
or extend the model from one total-order head to an antichain.
