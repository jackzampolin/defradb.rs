---- MODULE SyncOwnership ----
\* Target sync-ownership model for #1116: PushLog = idempotent head HINT, the
\* receiver owns completion through a bounded durable want-queue, the sender
\* keeps marker-plus-rederive state only (Go reference: internal/db/p2p
\* replicator.go /rep/retry/{id,doc} markers + rederive-heads-at-retry;
\* Rust target: crates/storage/src/stores/peerstore.rs value shrink + new
\* /rep/retry/col scope, crates/p2p pending-DAG registry reframed as the
\* receiver's own fetch queue).
\*
\* THE OWNERSHIP TRANSFER (heart of the design): a local update writes a durable
\* scope MARKER on the sender; the hint (current head, rederived at send time)
\* travels; the ack transfers the obligation to the receiver's DURABLE pending
\* registration; the receiver's paced fetch completes it. At every reachable
\* state the newest version of every scope is tracked somewhere - marker, hint
\* in flight, durable registration - or already merged. No silent loss.
\*
\* Four knobs, each isolating one failure of a wrong ownership model:
\*   SenderMode  = "MarkerRederive" - markers for every scope (docs AND
\*                 collection commits). [GREEN]
\*               = "DocKeyedOnly"   - current main: collection-commit (doc-less)
\*                 work cannot enter the document-keyed ledger
\*                 (push_worker.rs:84-93 warn-and-drop, peerstore.rs:199-203);
\*                 a dropped or nacked collection hint is permanently lost
\*                 (#1113). [RED]
\*   RegisterMode = "Durable"  - registration survives receiver crash
\*                  (pending_store.rs, #1099). [GREEN]
\*                = "Volatile" - the ack clears the sender marker while the
\*                  only remaining obligation dies with the process. [RED]
\*   FlightMode  = "SingleFlight" - at most one active fetch per scope
\*                 (ProcessQueue, queue.rs:12-22; Go processQueue). [GREEN]
\*               = "Dup"          - duplicate hints spawn overlapping fetches
\*                 (the defra-agent#630 same-CID parade). [RED]
\*   AckGuardMode = "HeadCurrent" - an ack clears the marker only if the acked
\*                  version is still the scope's newest head (Go re-reads heads;
\*                  Rust complete_retry_document version guard). [GREEN]
\*                = "Unguarded"   - an ack for a superseded head clears the
\*                  marker for the newer one. [RED]
\*
\* Abstractions: one sender, one receiver (peer identity dropped - every peer
\* edge is this model). Heads are monotone naturals per scope; "rederive at
\* send time" = a (re)hint always carries the CURRENT version, never a stored
\* one. Fetch reliability, provider rotation, and pacing clocks are
\* Convergence.tla / #1095 / #1112 territory - here fetch completion is fair
\* and the model owns WHO holds the obligation, not how fast it drains.
\* Re-hints are unconstrained (any dirty scope, any time): hint idempotence is
\* exercised by construction rather than asserted separately.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Docs,         \* document scopes
  Cols,         \* branchable-collection (doc-less commit) scopes
  MaxV,         \* max local updates per scope
  Cap,          \* receiver want-queue capacity (max_pending_dags)
  SenderMode,   \* "MarkerRederive" | "DocKeyedOnly"
  RegisterMode, \* "Durable" | "Volatile"
  FlightMode,   \* "SingleFlight" | "Dup"
  AckGuardMode  \* "HeadCurrent" | "Unguarded"

Scopes == Docs \cup Cols

ASSUME Docs # {} /\ Cols # {} /\ Docs \cap Cols = {}
ASSUME MaxV \in Nat /\ MaxV >= 1
ASSUME Cap \in Nat /\ Cap >= 1
ASSUME SenderMode \in {"MarkerRederive", "DocKeyedOnly"}
ASSUME RegisterMode \in {"Durable", "Volatile"}
ASSUME FlightMode \in {"SingleFlight", "Dup"}
ASSUME AckGuardMode \in {"HeadCurrent", "Unguarded"}

VARIABLES
  localV,   \* [Scopes -> 0..MaxV] newest local head version at the sender
  dirty,    \* SUBSET Scopes - durable sender markers (/rep/retry/{doc,col})
  inflight, \* SUBSET Scopes x 1..MaxV - hints sent, not yet processed
  pending,  \* SUBSET Scopes x 1..MaxV - receiver want-queue registrations
  flights,  \* [Scopes -> 0..2] active fetches per scope
  mergedV,  \* [Scopes -> 0..MaxV] newest merged version at the receiver
  crashed   \* BOOLEAN - one-shot receiver crash consumed

vars == <<localV, dirty, inflight, pending, flights, mergedV, crashed>>

Hints == Scopes \X (1..MaxV)

TypeOK ==
  /\ localV \in [Scopes -> 0..MaxV]
  /\ dirty \subseteq Scopes
  /\ inflight \subseteq Hints
  /\ pending \subseteq Hints
  /\ Cardinality(pending) <= Cap
  /\ flights \in [Scopes -> 0..2]
  /\ mergedV \in [Scopes -> 0..MaxV]
  /\ crashed \in BOOLEAN

Init ==
  /\ localV = [s \in Scopes |-> 0]
  /\ dirty = {}
  /\ inflight = {}
  /\ pending = {}
  /\ flights = [s \in Scopes |-> 0]
  /\ mergedV = [s \in Scopes |-> 0]
  /\ crashed = FALSE

\* The durable marker write. DocKeyedOnly cannot record collection scopes -
\* the document retry ledger refuses empty doc IDs (peerstore.rs:199-203).
RecordMarker(s) ==
  IF SenderMode = "DocKeyedOnly" /\ s \in Cols THEN dirty ELSE dirty \cup {s}

\* Ack consumption on the sender: clear the marker only if the acked version
\* is still the scope's newest head (rederive semantics make any older ack a
\* no-op for the obligation). Unguarded clears regardless - the stale-clear.
AckClear(s, v) ==
  IF AckGuardMode = "HeadCurrent" /\ v # localV[s] THEN dirty ELSE dirty \ {s}

\* Local update: bump the head, write the durable marker, enqueue the live
\* hint (the in-memory hint queue collapsed into the send).
Update(s) ==
  /\ localV[s] < MaxV
  /\ localV' = [localV EXCEPT ![s] = @ + 1]
  /\ dirty' = RecordMarker(s)
  /\ inflight' = inflight \cup {<<s, localV[s] + 1>>}
  /\ UNCHANGED <<pending, flights, mergedV, crashed>>

\* Scheduled re-hint from the marker ladder: REDERIVES the current head.
\* Unconstrained repetition = hint idempotence is part of the green theorem.
ReHint(s) ==
  /\ s \in dirty
  /\ localV[s] >= 1
  /\ inflight' = inflight \cup {<<s, localV[s]>>}
  /\ UNCHANGED <<localV, dirty, pending, flights, mergedV, crashed>>

\* Network loss of a hint. Harmless while the marker (or a registration) holds
\* the obligation.
DropHint(s, v) ==
  /\ <<s, v>> \in inflight
  /\ inflight' = inflight \ {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, pending, flights, mergedV, crashed>>

\* Receiver fast path: the hinted head is already merged (is_merged check,
\* pushlog.rs:196-225; Go p2p.go:638-645). Ack only - no receiver state.
ProcessMerged(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] >= v
  /\ inflight' = inflight \ {<<s, v>>}
  /\ dirty' = AckClear(s, v)
  /\ UNCHANGED <<localV, pending, flights, mergedV, crashed>>

\* Receiver registers the DAG in its durable want-queue and acks: the
\* obligation transfers. Re-registration of an already-pending head is the
\* idempotent replace-in-place (insert_pending_dag).
ProcessRegister(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] < v
  /\ (<<s, v>> \in pending \/ Cardinality(pending) < Cap)
  /\ pending' = pending \cup {<<s, v>>}
  /\ inflight' = inflight \ {<<s, v>>}
  /\ dirty' = AckClear(s, v)
  /\ UNCHANGED <<localV, flights, mergedV, crashed>>

\* Want-queue full: nack (RATE_LIMITED_MESSAGE). Receiver pacing, not sender
\* admission - the marker survives and the ladder re-hints later.
ProcessNack(s, v) ==
  /\ <<s, v>> \in inflight
  /\ mergedV[s] < v
  /\ <<s, v>> \notin pending
  /\ Cardinality(pending) >= Cap
  /\ inflight' = inflight \ {<<s, v>>}
  /\ UNCHANGED <<localV, dirty, pending, flights, mergedV, crashed>>

\* Receiver-paced pull begins for a registered scope. SingleFlight admits one
\* active fetch per scope (ProcessQueue); Dup lets duplicates overlap.
StartFetch(s) ==
  /\ \E v \in 1..MaxV : <<s, v>> \in pending
  /\ flights[s] < (IF FlightMode = "Dup" THEN 2 ELSE 1)
  /\ flights' = [flights EXCEPT ![s] = @ + 1]
  /\ UNCHANGED <<localV, dirty, inflight, pending, mergedV, crashed>>

\* Pull completes: DAG fetched and merged; the registration retires.
CompleteFetch(s, v) ==
  /\ <<s, v>> \in pending
  /\ flights[s] > 0
  /\ mergedV' = [mergedV EXCEPT ![s] = IF v > @ THEN v ELSE @]
  /\ pending' = pending \ {<<s, v>>}
  /\ flights' = [flights EXCEPT ![s] = @ - 1]
  /\ UNCHANGED <<localV, dirty, inflight, crashed>>

\* One-shot receiver crash+restart: in-memory fetch state dies; the want-queue
\* survives iff registrations are durable (#1099). Sender state is another
\* process and is untouched; hints in the network are untouched.
Crash ==
  /\ ~crashed
  /\ crashed' = TRUE
  /\ flights' = [s \in Scopes |-> 0]
  /\ pending' = IF RegisterMode = "Durable" THEN pending ELSE {}
  /\ UNCHANGED <<localV, dirty, inflight, mergedV>>

\* Every scope current everywhere and fully updated: stutter so TLC does not
\* flag deadlock on a finished schedule.
Done == \A s \in Scopes : localV[s] = MaxV /\ mergedV[s] = MaxV
Terminating == Done /\ UNCHANGED vars

Next ==
  \/ \E s \in Scopes : Update(s) \/ ReHint(s) \/ StartFetch(s)
  \/ \E s \in Scopes, v \in 1..MaxV :
       DropHint(s, v) \/ ProcessMerged(s, v) \/ ProcessRegister(s, v)
       \/ ProcessNack(s, v) \/ CompleteFetch(s, v)
  \/ Crash
  \/ Terminating

Spec == Init /\ [][Next]_vars

\* Fairness: the marker ladder keeps re-hinting, fetches keep starting and
\* completing (weak - continuously enabled once registered), and a hint that
\* is sent infinitely often is eventually processed (strong - DropHint keeps
\* disabling the arrival actions, so weak fairness would let the network eat
\* every hint forever).
FairSpec ==
  Spec /\ \A s \in Scopes :
    /\ WF_vars(ReHint(s))
    /\ WF_vars(StartFetch(s))
    /\ SF_vars(\E v \in 1..MaxV : CompleteFetch(s, v))
    /\ SF_vars(\E v \in 1..MaxV : ProcessMerged(s, v) \/ ProcessRegister(s, v))

INV_TypeOK == TypeOK

\* THE #1116 INVARIANT: the newest version of every scope is merged, or the
\* obligation for it is held somewhere - durable sender marker, hint in
\* flight, or durable receiver registration. No ownership model may reach a
\* state where a behind scope is tracked by nobody.
INV_ObligationConservation ==
  \A s \in Scopes :
    mergedV[s] < localV[s] =>
      \/ s \in dirty
      \/ <<s, localV[s]>> \in inflight
      \/ <<s, localV[s]>> \in pending

\* Per-scope single-flight (#1115 kernel guard; Go processQueue).
INV_SingleFlight == \A s \in Scopes : flights[s] <= 1

\* The receiver's want-queue is its own bounded pacing state.
INV_ReceiverQueueBounded == Cardinality(pending) <= Cap

\* Sender durable state is scope markers only - no versions, CIDs, or
\* payloads. Holds by construction (dirty \subseteq Scopes); stated so the
\* green run records that markers alone SUFFICE for conservation + liveness.
INV_SenderMarkersOnly == dirty \subseteq Scopes

\* Liveness (GREEN, under FairSpec): every scope eventually converges to its
\* newest head AND stays converged - receiver-paced, crash included, re-hints
\* free. (<>[] rather than <>: Init trivially satisfies mergedV = localV, so
\* plain <> would be vacuous; updates are bounded by MaxV, so eventual
\* permanent currency is the honest phrasing.)
LIVE_EventualCurrency == <>[](\A s \in Scopes : mergedV[s] = localV[s])
====
