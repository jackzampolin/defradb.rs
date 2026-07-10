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
