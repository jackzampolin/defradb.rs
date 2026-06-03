# Survey: `crates/events/`

## Purpose
In-process pub/sub event bus for DefraDB subscriptions. A `Bus` trait
(`publish`/`subscribe`/`unsubscribe`/`close`/`is_closed`) with three impls:
`ChannelBus` (tokio mpsc fan-out, the real one), `NoOpBus`, and a wasm stub
`Subscription`. Publishers emit `Message`s (Update / Merge / MergeComplete /
ReplicatorCompleted / TopicPeerEvent / SEArtifactReceived / Acp* ); subscribers
filter by `EventName` (with `WildCard`). HTTP GraphQL SSE and FFI subscriptions
build on this stream by re-running a scoped query per live update. No cursor,
replay, global sequence, or durable change log — live-only.

## State machines
- **Bus open/closed** (explicit `AtomicBool`): `Open -> Closed` (one-way). Publish
  after close drops; subscribe after close returns a dead channel. Single flag,
  no cross-node dimension.
- **Subscriber lifecycle** (implicit): `subscribe -> active -> {unsubscribe | drop |
  channel-closed -> lazy GC on next publish}`. GC happens opportunistically inside
  `publish` under a read->write lock upgrade.
- **Bounded-channel drop/resync protocol** (implicit, the only interesting one):
  `try_send` is non-blocking; on `Full` the message is *dropped* and a per-sub
  `dropped_count` atomic is bumped. Consumers must call `check_and_reset_dropped()`
  and resync from the DB when non-zero, or they silently miss updates. This is the
  deliberate divergence from Go's *blocking* bus (Go never drops; Rust trades
  liveness-of-publisher for at-most-once-with-loss-signal).

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| Drop-signal soundness | TLA+ | a dropped message always leaves `dropped_count > 0` until observed; consumer that resyncs-on-nonzero never permanently misses an update | no | low |
| EventName.matches wildcard | Lean | `matches` is reflexive + symmetric; WildCard matches all | no | low |

`matches` is a 3-line reflexive/symmetric check already exercised by unit tests —
not proof-worthy. The drop/resync property is genuinely a safety invariant, but it
is single-process channel plumbing: the *correctness* of "resync recovers consistent
state" lives in the consumers (HTTP SSE re-query, FFI poll) and in the
replication/convergence machinery, which the existing **convergence**, **replicator**,
and **commits** slices already cover. The bus itself only guarantees the drop is
*counted*, which is a one-atomic invariant covered by `test_channel_bus_buffer_overflow`.

## Verdict
**Plumbing.** `model_worthy: false`. The crate is an in-process fan-out over bounded
mpsc channels with a one-way closed flag and a drop-counter. No concurrency hazard
beyond a lock and atomics already exercised by in-crate tokio tests, and no
distributed/algebraic law that needs proof. The properties that matter for
subscriptions (does a subscriber eventually converge after drops, does ACP gate
which events a peer sees) are owned by other crates and already modeled
(convergence, replicator, commits, acp).
