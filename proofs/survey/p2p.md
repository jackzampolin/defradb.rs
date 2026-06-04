# p2p — formal-modelability survey

## Purpose
Peer-to-peer networking for DefraDB: transport-agnostic (`P2PTransport`) layer over
libp2p and Iroh that synchronizes CRDT blocks between nodes. Responsibilities: swarm/host
management, gossipsub pubsub, two-stream request/response protocols (pushlog, doc-sync,
identity, SE-query, branchable), Bitswap block fetch, replicator config, the DAG sync
coordinator, KMS pubsub key transport, and the explicit-replay capability gate.

## State machines
- **Sync coordinator / DAG fetcher** — ancestry-walk-before-merge replication. (modeled)
- **Replicator lifecycle** — Active/Inactive + backfill/live/resume delivery. (modeled)
- **Explicit-replay capability** (`explicit_replay.rs`) — authorizer-signed token bound to
  (source_peer, target_peer, collection) with TTL cap + verifier-local revocation deny-list.
  Gates encrypted-data replay; wired into pushlog/merge handlers. NOT modeled.
- **Request correlators** (`se_correlator.rs`, `pubsub_rpc/correlator.rs`) — Arc<Mutex<HashMap>>
  + Drop-cleanup guards. Plumbing; unit-tested.
- **Per-peer token-bucket rate limiter** (`sync/rate_limiter.rs`) — standard algorithm, plumbing.
- **Two-stream runner / host swarm** — libp2p stream mux + connection mgmt. Plumbing/IO.

## Candidates

| name | kind | property | already-modeled | priority |
|---|---|---|---|---|
| explicit-replay capability gate | TLA+ | unforgeable: no peer obtains an `ExplicitReplayAuthorization` for a (source,target,collection) it was not issued one for; replay to wrong target/collection rejected; a compromised authorizer cannot mint capabilities exceeding `MAX_CAPABILITY_TTL`; a revoked capability is never re-accepted after `revoke_capability` | no | high |
| capability revocation consistency | TLA+ | once revoked in a verifier's registry, every subsequent `verify_*` for that token denies; revocation is monotone (no un-revoke) under concurrent verify/revoke | no (Acp models tuple revocation, not capability tokens) | medium |
| rate-limiter fairness/liveness | TLA+ | a peer obeying the refill rate is never starved; a flooding peer is bounded to capacity+rate | no | low |

## Verdict
**Model-worthy.** The crate's replication/convergence/KMS/auth/commits/integrity concerns are
already covered by existing TLA+ slices. The one genuinely un-modeled security state machine is
the **explicit-replay capability** lifecycle: a signed, peer/collection-bound, TTL-capped,
revocable token on the encrypted-replay path. The Auth slice models the HTTP management gate, not
this token's adversary properties (forgery, cross-target replay, TTL escalation, post-issue
revocation), so an explicit-replay slice is the high-priority addition. Everything else
(correlators, rate limiter, swarm/stream plumbing) is glue covered by integration/unit tests.
