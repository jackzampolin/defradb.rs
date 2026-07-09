---- MODULE PushBacklog ----
\* Outbound PushLog backlog admission on the pusher (#1099), abstracting
\* crates/p2p/src/sync/coordinator/broadcast.rs (the per-(write, peer) fan-out feeding
\* the push_semaphore) and SyncShutdownHandle's JoinHandle retention.
\*
\* THE MECHANISM (current main): broadcast materializes PushLog request batches and
\* spawns one tokio task per (write, peer) BEFORE the task acquires the 8-permit
\* push_semaphore. Waiting futures retain their payload buffers, so resident work grows
\* with total arrivals, not with the permit count. SyncShutdownHandle retains every
\* completed JoinHandle until shutdown. There is no per-peer fairness: a nonresponsive
\* peer's sends (30s timeout each) can occupy all permits and starve healthy peers.
\*
\* THE FIX: a bounded queue (item cap AND byte cap) admits compact jobs before anything
\* is spawned; a FIXED pool of Workers tasks drains it; per-peer scheduling with a
\* per-peer active cap PerPeerCap < Workers; queue-full is an explicit outcome
\* (reject -> durable retry ledger; an identical (peer, cid) coalesces into the queued
\* job); only the Workers worker handles are retained.
\*
\* Four knobs, each isolating one failure of current main:
\*   AdmissionMode = "BoundedQueue"  - arrivals are admitted against the item/byte caps,
\*                                     coalesced, or rejected to the retry ledger. [GREEN]
\*                 = "SpawnPerItem"  - every arrival immediately becomes resident spawned
\*                                     work waiting on the semaphore; no cap. [RED]
\*   ReleaseMode   = "Release"       - a completing job returns its worker slot. [GREEN]
\*                 = "Leak"          - it does not; capacity decays. [RED]
\*   HandleMode    = "FixedWorkers"  - only the fixed pool's handles are retained. [GREEN]
\*                 = "RetainAll"     - every completed job's handle is retained and never
\*                                     pruned (SyncShutdownHandle today). [RED]
\*   FairnessMode  = "PerPeerCap"    - at most PerPeerCap active jobs per peer. [GREEN]
\*                 = "Unfair"        - no per-peer cap; the slow peer's jobs may hold
\*                                     every worker forever. [RED]
\*
\* Abstractions: `backlog` is admitted-but-not-active work per peer - the compact bounded
\* queue under BoundedQueue, the spawned-waiting futures (each retaining its payload
\* buffer) under SpawnPerItem; the residency bound is the same question in both. Payload
\* size is a per-peer constant Weight (one heavy peer exercises the byte cap). Slow
\* peers' sends never complete - stuck-forever is the cleanest starvation adversary, and
\* permit CONSERVATION (freeWorkers + active = Workers) is an accounting identity that a
\* stuck holder does not disturb, so the two properties stay independent; send timeouts
\* that would eventually fail a slow send are a liveness relaxation the model omits.
\* Rejected arrivals are terminal ledger entries here - the re-push loop they feed is
\* Replicator.tla's subject. Round-robin ORDER is not modeled; the per-peer cap alone
\* carries the starvation-freedom property.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Peers,         \* target peers of the fan-out
  SlowPeers,     \* peers whose active sends never complete
  Weight,        \* [Peers -> Nat \ {0}] payload bytes of one job for that peer
  MaxArrivals,   \* total (write, peer) arrivals; >> QueueCap to expose unboundedness
  QueueCap,      \* queue item cap
  ByteCap,       \* queue byte cap
  Workers,       \* fixed worker-pool size (the push_semaphore permits)
  PerPeerCap,    \* per-peer active cap, < Workers
  AdmissionMode, \* "BoundedQueue" | "SpawnPerItem"
  ReleaseMode,   \* "Release" | "Leak"
  HandleMode,    \* "FixedWorkers" | "RetainAll"
  FairnessMode   \* "PerPeerCap" | "Unfair"

ASSUME Peers # {} /\ SlowPeers \subseteq Peers /\ SlowPeers # Peers
ASSUME Weight \in [Peers -> Nat] /\ \A p \in Peers : Weight[p] >= 1
ASSUME QueueCap \in Nat /\ QueueCap >= 1
ASSUME ByteCap \in Nat /\ ByteCap >= 1
ASSUME MaxArrivals \in Nat /\ MaxArrivals > QueueCap
ASSUME Workers \in Nat /\ Workers >= 1
ASSUME PerPeerCap \in Nat /\ PerPeerCap >= 1 /\ PerPeerCap < Workers
ASSUME AdmissionMode \in {"BoundedQueue", "SpawnPerItem"}
ASSUME ReleaseMode \in {"Release", "Leak"}
ASSUME HandleMode \in {"FixedWorkers", "RetainAll"}
ASSUME FairnessMode \in {"PerPeerCap", "Unfair"}

VARIABLES
  arrivals,    \* arrivals so far
  backlog,     \* [Peers -> Nat] admitted jobs not yet on a worker (queued or
               \* spawned-waiting; each retains Weight[p] payload bytes)
  active,      \* [Peers -> Nat] jobs currently holding a worker slot
  freeWorkers, \* unheld worker slots
  sent,        \* completed sends
  coalesced,   \* arrivals folded into an already-queued job for the same (peer, cid)
  rejected,    \* arrivals rejected at a full queue, recorded in the retry ledger
  handles      \* retained JoinHandles

vars == <<arrivals, backlog, active, freeWorkers, sent, coalesced, rejected, handles>>

RECURSIVE SumOver(_, _)
SumOver(f, S) ==
  IF S = {} THEN 0
  ELSE LET p == CHOOSE x \in S : TRUE
       IN f[p] + SumOver(f, S \ {p})

BacklogItems == SumOver(backlog, Peers)
BacklogBytes == SumOver([p \in Peers |-> backlog[p] * Weight[p]], Peers)
ActiveJobs   == SumOver(active, Peers)

HasRoom(p) ==
  /\ BacklogItems < QueueCap
  /\ BacklogBytes + Weight[p] <= ByteCap

TypeOK ==
  /\ arrivals \in 0..MaxArrivals
  /\ backlog \in [Peers -> 0..MaxArrivals]
  /\ active \in [Peers -> 0..Workers]
  /\ freeWorkers \in 0..Workers
  /\ sent \in 0..MaxArrivals
  /\ coalesced \in 0..MaxArrivals
  /\ rejected \in 0..MaxArrivals
  /\ handles \in Workers..(Workers + MaxArrivals)

Init ==
  /\ arrivals = 0
  /\ backlog = [p \in Peers |-> 0]
  /\ active = [p \in Peers |-> 0]
  /\ freeWorkers = Workers
  /\ sent = 0
  /\ coalesced = 0
  /\ rejected = 0
  /\ handles = Workers

\* A (write, peer) arrival is admitted as resident work. Under BoundedQueue only within
\* the item and byte caps; under SpawnPerItem unconditionally - the spawned future
\* retains its payload buffer while waiting on the semaphore. [RED path when unbounded]
Enqueue(p) ==
  /\ arrivals < MaxArrivals
  /\ (AdmissionMode = "SpawnPerItem" \/ HasRoom(p))
  /\ arrivals' = arrivals + 1
  /\ backlog' = [backlog EXCEPT ![p] = @ + 1]
  /\ UNCHANGED <<active, freeWorkers, sent, coalesced, rejected, handles>>

\* An arrival for a (peer, cid) already queued folds into the existing job instead of
\* consuming a slot. Coalescing into an ACTIVE job is not possible - its send has begun.
Coalesce(p) ==
  /\ AdmissionMode = "BoundedQueue"
  /\ arrivals < MaxArrivals
  /\ backlog[p] > 0
  /\ arrivals' = arrivals + 1
  /\ coalesced' = coalesced + 1
  /\ UNCHANGED <<backlog, active, freeWorkers, sent, rejected, handles>>

\* Queue full (either cap): the arrival is rejected EXPLICITLY and lands in the durable
\* retry ledger - never silently dropped.
Reject(p) ==
  /\ AdmissionMode = "BoundedQueue"
  /\ arrivals < MaxArrivals
  /\ ~HasRoom(p)
  /\ arrivals' = arrivals + 1
  /\ rejected' = rejected + 1
  /\ UNCHANGED <<backlog, active, freeWorkers, sent, coalesced, handles>>

\* A worker takes a backlog job (acquires a permit). Under PerPeerCap a peer may hold
\* at most PerPeerCap workers; under Unfair nothing stops one peer taking them all.
StartJob(p) ==
  /\ backlog[p] > 0
  /\ freeWorkers > 0
  /\ (FairnessMode = "PerPeerCap" => active[p] < PerPeerCap)
  /\ backlog' = [backlog EXCEPT ![p] = @ - 1]
  /\ active' = [active EXCEPT ![p] = @ + 1]
  /\ freeWorkers' = freeWorkers - 1
  /\ UNCHANGED <<arrivals, sent, coalesced, rejected, handles>>

\* A healthy peer's send finishes. Slow peers' sends never complete (see header).
\* Leak: the worker slot is not returned. RetainAll: the completed job's JoinHandle
\* is retained forever (SyncShutdownHandle).
Complete(p) ==
  /\ p \notin SlowPeers
  /\ active[p] > 0
  /\ active' = [active EXCEPT ![p] = @ - 1]
  /\ sent' = sent + 1
  /\ freeWorkers' = IF ReleaseMode = "Release" THEN freeWorkers + 1 ELSE freeWorkers
  /\ handles' = IF HandleMode = "RetainAll" THEN handles + 1 ELSE handles
  /\ UNCHANGED <<arrivals, backlog, coalesced, rejected>>

Next ==
  \E p \in Peers :
    \/ Enqueue(p)
    \/ Coalesce(p)
    \/ Reject(p)
    \/ StartJob(p)
    \/ Complete(p)

\* Arrivals exhausted and all HEALTHY work drained: stutter so TLC does not flag
\* deadlock on a finished schedule. Slow peers' stuck jobs legitimately remain.
Done ==
  /\ arrivals = MaxArrivals
  /\ \A p \in Peers \ SlowPeers : backlog[p] = 0 /\ active[p] = 0
Terminating == Done /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Terminating]_vars

\* Weak fairness on the scheduler: startable jobs get started, running healthy sends
\* finish. Arrivals need no fairness - liveness is per admitted job.
FairSpec ==
  Spec /\ \A p \in Peers :
    /\ WF_vars(StartJob(p))
    /\ WF_vars(Complete(p))

INV_TypeOK == TypeOK

\* THE #1099 RESIDENCY INVARIANT: resident not-yet-active work is bounded by the queue
\* caps regardless of total arrivals. SpawnPerItem violates this as soon as arrivals
\* outrun the workers; BoundedQueue preserves it in every reachable state.
INV_QueueBounded ==
  /\ BacklogItems <= QueueCap
  /\ BacklogBytes <= ByteCap

\* Worker slots are conserved: free + held = pool size. Leak violates this on the
\* first completion.
INV_PermitConservation == freeWorkers + ActiveJobs = Workers

\* Only the fixed pool's handles are retained. RetainAll violates this on the first
\* completion; unpruned handles are the resident-memory twin of the spawn flood.
INV_HandlesBounded == handles <= Workers

\* No arrival is silently dropped: each is queued, active, sent, coalesced into a
\* queued job, or rejected into the retry ledger.
INV_NoSilentLoss ==
  arrivals = BacklogItems + ActiveJobs + sent + coalesced + rejected

\* Liveness (GREEN, under FairSpec): every admitted job for a healthy peer is
\* eventually sent. Unfair violates it - the slow peer's stuck sends hold every worker
\* while a healthy job waits forever; PerPeerCap < Workers always leaves a worker
\* reachable for healthy peers.
LIVE_HealthyProgress ==
  \A p \in Peers \ SlowPeers :
    (backlog[p] + active[p] > 0) ~> (backlog[p] + active[p] = 0)
====
