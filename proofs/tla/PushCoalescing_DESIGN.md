# PushCoalescing — latest-head retirement (TLA+ design)

Models issue **#1102**'s safety/liveness boundary across the live outbound queue and
the durable retry ledger. A head can already be active when a newer local head arrives;
that old send may finish, but its failure must not recreate a stale retry obligation.

The model uses monotonically increasing natural numbers for `(priority, CID)` head
versions. The Rust implementation uses the real block priority with `Cid::cmp` as a
deterministic tie-break across every layer. `LatestOnly` atomically removes queued and
persisted predecessors before installing the new head. `CurrentOnly` checks the version again on
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

The model makes `Fail` and its current-version check atomic. The runtime check and
peerstore write are separated by an asynchronous recorder channel: a newer-head
observation can be processed while no record exists, followed by an already-checked
older failure that creates one stale retry. This narrow interleaving is not covered by
`INV_NoStaleRetry`. It is bounded self-healing work rather than silent loss because
`retry_doc` re-reads and sends the document's current heads, and the version-guarded
completion removes only the record that was actually attempted.

The model intentionally excludes payload encoding and elapsed time. Shared encode-cache
lifetime is Rust ownership (`Weak<CID> -> Arc<entry>`, so the last peer drops the entry),
and exponential backoff is a deterministic pure function covered by unit tests.

The runtime's update coalescers use a 250 ms trailing-edge quiet period with a hard
1 second maximum delay. The quiet period absorbs short sequential HTTP bursts; the
maximum forces progress for a continuously written document and bounds the lifetime
of follower tasks. This deliberately accepts up to 250 ms of latency for an isolated
fan-out or gossip obligation. Transactional document push and gossip are awaited in
order inside the detached broadcaster task, so their independent quiet periods can
stack to about 500 ms before gossip is published. Collection commits use an empty
document ID and therefore a separate CID-scoped backlog key. A dropped leader removes
its window and wakes followers to re-admit the latest buffered payload, so cancellation
cannot wedge the document key.

Dormant durable records are volatile-send watermarks, not successful acknowledgements.
They are promoted to immediately due pending retries when a process starts, because the
in-memory send they represented cannot survive a restart. A live failure activates its
retry with the first deterministic jittered interval (15–30 seconds for the 30-second
backoff cap); the in-memory backlog owns the immediate attempt.

The live retry sweep removes only the peer scheduling marker when every document record
is dormant; it deliberately retains those watermarks while their in-memory sends may
still be queued or active. This closes the enqueue-to-crash window without causing
2-second sweep churn: restart promotion recreates the peer marker. Collection commits
have empty document IDs and are not admitted to this document retry ledger because its
replay operation requires a document whose current heads can be re-read.

For an equal `(priority, CID)` version, full-DAG delivery is stronger than root-only
delivery. A full-DAG arrival therefore queues behind an active root-only send rather
than coalescing into it. If a locally produced head cannot be decoded to obtain its
priority, it uses a CID-scoped obligation key and bypasses both update coalescers; an
unproven version is never retired as an older document head. Replicator fan-out keys
also separate filter-bearing pushes from pushes without current document JSON. This
prevents an older snapshot from authorizing a newer document-less DAG while retaining
coalescing within each class and preserving both delivery obligations.

Latest-head retirement assumes that a later document head subsumes its earlier linear
predecessor. Concurrent sibling heads that must both remain heads do not enter this
live document-update path: replay and field/KMS DAG paths bypass the outbound backlog,
collection commits are CID-scoped rather than document-scoped, and
`rebroadcast_on_merge` is false at every production construction site. Enabling that
flag, or adding another live producer of non-subsuming document siblings, requires
distinct obligation keys or extending the model from one total-order head to an
antichain.
