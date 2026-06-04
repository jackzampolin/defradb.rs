---- MODULE TxnRegistry ----
\* Stale-transaction cleanup race in the Rust db transaction registry.
\*
\* Abstracts crates/db/src/txn_registry.rs `DbTransactionRegistry::cleanup_stale_transactions`
\* and the concurrent `get`/`get_ctx` touch path (anchors in TxnRegistry_DESIGN.md).
\*
\* THE RACE. A periodic sweep evicts transactions idle longer than `max_idle_age`. It runs
\* in two phases:
\*   1. Collect candidates under the registry READ lock: every ctx whose idle_for(now) >
\*      max_idle_age (txn_registry.rs:275-285).
\*   2. For each candidate, under the registry WRITE lock, RE-CHECK idle_for and remove only
\*      if still idle and still the same Arc (txn_registry.rs:300-318).
\* Meanwhile a request `get`s a txn and calls ctx.touch(), resetting its idle clock to 0
\* (txn_registry.rs:192-199, 766-784; txn_context.rs:65-75). The touch happens while holding
\* the registry READ lock, so it is mutually exclusive with the write-locked remove.
\*
\* THE HAZARD the model exposes: a NAIVE sweep that removes purely on the phase-1 verdict
\* (no write-locked re-check) can evict a txn that a request touched between collect and
\* remove -> a still-live transaction is lost. The real code's write-locked re-check closes it.
\*
\* INDEPENDENT ORACLE. "Live at removal" is ground truth derived from the actual touch
\* history and the logical clock -- NOT from the sweep's own re-check decision. A txn is live
\* at the instant it is removed iff its true last_seen is within max_idle_age of that instant
\* (a request touched it recently). The headline invariant says the sweep never removes a
\* live txn. Because the oracle is the real idle gap (not "the sweep decided it was stale"),
\* a sweep that agrees with itself cannot pass vacuously: the buggy sweep DOES remove a txn
\* whose oracle-idle gap is 0, and TLC catches it.
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Txns,        \* finite set of transaction ids in the registry
  MaxIdle,     \* Nat: max_idle_age threshold (a txn is stale iff idle > MaxIdle)
  MaxTime,     \* Nat: clock bound (keeps the state space finite)
  Recheck      \* "WriteLocked" (real code) | "None" (buggy naive sweep)

ASSUME Txns # {}
ASSUME MaxIdle \in Nat
ASSUME MaxTime \in Nat /\ MaxTime > MaxIdle
ASSUME Recheck \in {"WriteLocked", "None"}

VARIABLES
  clock,        \* Nat: logical time. Touch/sweep actions advance it.
  lastSeen,     \* [Txns -> Nat]  true last-touch time (ctx.last_request_seen). GROUND TRUTH.
  present,      \* [Txns -> BOOLEAN] is the txn still in the registry map?
  rlock,        \* SUBSET Txns currently being touched under the read lock (read-lock holders)
  wlock,        \* {} or {t}: the single write-lock holder (sweep removing candidate t)
  candidates,   \* SUBSET Txns: phase-1 collected stale candidates not yet processed
  collectAt,    \* [Txns -> Nat]  lastSeen snapshot captured at phase-1 collect time
  removedLive   \* BOOLEAN: latch set TRUE if the sweep ever removed a live txn (oracle hit)

vars == <<clock, lastSeen, present, rlock, wlock, candidates, collectAt, removedLive>>

\* idle_for(now) = now - last_request_seen   (txn_context.rs:86-88)
IdleFor(t, now) == now - lastSeen[t]
IsStale(t, now) == IdleFor(t, now) > MaxIdle

\* Lock discipline: read lock (touches) and write lock (removes) are mutually exclusive,
\* exactly as std::sync::RwLock gives. A touch may proceed only if no write lock is held;
\* a write-lock acquire may proceed only if no read lock is held.
NoWriteLock == wlock = {}
NoReadLock  == rlock = {}

TypeOK ==
  /\ clock \in 0..MaxTime
  /\ lastSeen \in [Txns -> 0..MaxTime]
  /\ present  \in [Txns -> BOOLEAN]
  /\ rlock \subseteq Txns
  /\ wlock \subseteq Txns /\ Cardinality(wlock) <= 1
  /\ candidates \subseteq Txns
  /\ collectAt \in [Txns -> 0..MaxTime]
  /\ removedLive \in BOOLEAN

Init ==
  /\ clock = 0
  /\ lastSeen = [t \in Txns |-> 0]
  /\ present  = [t \in Txns |-> TRUE]
  /\ rlock = {}
  /\ wlock = {}
  /\ candidates = {}
  /\ collectAt = [t \in Txns |-> 0]
  /\ removedLive = FALSE

\* ---- Time ----
\* Pure passage of time. Allowed only when no lock is held mid-operation, so that a
\* touch/remove pair stays atomic w.r.t. the clock the way the real locked sections are.
Tick ==
  /\ clock < MaxTime
  /\ NoReadLock /\ NoWriteLock
  /\ clock' = clock + 1
  /\ UNCHANGED <<lastSeen, present, rlock, wlock, candidates, collectAt, removedLive>>

\* ---- get / get_ctx touch (txn_registry.rs:192-213, 766-784) ----
\* Acquire the read lock, refresh last_request_seen to now, release. Only present txns can
\* be looked up (a removed txn returns NotFound; nothing to touch).
TouchAcquire(t) ==
  /\ present[t]
  /\ NoWriteLock
  /\ rlock' = rlock \cup {t}
  /\ UNCHANGED <<clock, lastSeen, present, wlock, candidates, collectAt, removedLive>>

TouchRelease(t) ==
  /\ t \in rlock
  /\ lastSeen' = [lastSeen EXCEPT ![t] = clock]   \* ctx.touch(): *last_request_seen = now
  /\ rlock' = rlock \ {t}
  /\ UNCHANGED <<clock, present, wlock, candidates, collectAt, removedLive>>

\* ---- Sweep phase 1: collect stale candidates under the read lock (txn_registry.rs:275-285)
\* Snapshot lastSeen at collect time into collectAt for each candidate.
Collect ==
  /\ candidates = {}            \* one sweep at a time (the background task is single)
  /\ NoWriteLock
  /\ LET stale == {t \in Txns : present[t] /\ IsStale(t, clock)} IN
       /\ stale # {}
       /\ candidates' = stale
       /\ collectAt' = [t \in Txns |-> IF t \in stale THEN lastSeen[t] ELSE collectAt[t]]
  /\ UNCHANGED <<clock, lastSeen, present, rlock, wlock, removedLive>>

\* ---- Sweep phase 2: per-candidate remove under the WRITE lock (txn_registry.rs:300-318) ----
\* Recheck == "WriteLocked": real code. Remove only if still present AND still idle by the
\*   write-locked re-check `current.idle_for(Instant::now()) > max_idle_age`.
\* Recheck == "None": buggy naive sweep. Remove on the phase-1 verdict alone (no re-check).
\* The oracle (removedLive) is set from the TRUE idle gap at removal time, independent of
\* whichever predicate the sweep used.
RemoveDecision(t) ==
  CASE Recheck = "WriteLocked" -> IsStale(t, clock)   \* re-evaluate idle_for now, write-locked
    [] Recheck = "None"        -> TRUE                \* trust phase-1; remove unconditionally

ProcessCandidate(t) ==
  /\ t \in candidates
  /\ NoReadLock                 \* write-lock acquire waits out all read-lock touches
  /\ wlock = {}
  /\ candidates' = candidates \ {t}
  /\ IF present[t] /\ RemoveDecision(t)
       THEN \* remove t from the registry map
         /\ present' = [present EXCEPT ![t] = FALSE]
         \* ORACLE: was t actually live (recently touched) at this removal instant?
         /\ removedLive' = (removedLive \/ ~IsStale(t, clock))
       ELSE
         /\ UNCHANGED <<present, removedLive>>
  /\ UNCHANGED <<clock, lastSeen, rlock, wlock, collectAt>>

Next ==
  \/ Tick
  \/ \E t \in Txns : TouchAcquire(t)
  \/ \E t \in Txns : TouchRelease(t)
  \/ Collect
  \/ \E t \in Txns : ProcessCandidate(t)

Spec == Init /\ [][Next]_vars

\* =====================================================================
\* Invariants
\* =====================================================================
INV_TypeOK == TypeOK

\* HEADLINE (independent oracle): the sweep never evicts a still-live transaction.
\* A txn is "live at removal" iff its TRUE idle gap (clock - lastSeen) was <= MaxIdle at the
\* instant of removal. removedLive latches if that ever happened. Ground truth, not the
\* sweep's verdict.
INV_NoLiveEvicted == ~removedLive

\* Sanity: the locks really are mutually exclusive in every reachable state. If this ever
\* failed, the read/write exclusion the proof leans on would be unsound.
INV_LockExclusion == NoReadLock \/ NoWriteLock
====
