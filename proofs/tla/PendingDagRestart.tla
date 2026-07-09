---- MODULE PendingDagRestart ----
\* Pending-DAG registration durability across a hub restart (#1099), companion to
\* PushLogAdmission.tla: that module fixes the hub's REPLY decision at capacity (#1088
\* W1, kept here as HubOverflowNack); this one asks what survives a hub CRASH after an
\* honest success reply. Abstracts crates/p2p/src/sync/manager/process/pushlog.rs
\* (insert_pending_dag) plus the pusher's persisted retry ladder.
\*
\* THE MECHANISM: an inbound PushLog with missing links is registered in the hub's
\* bounded in-memory pending-DAG map and success-acked; the pusher then deletes its
\* persisted retry record (terminal - it never re-pushes unless an unrelated later
\* update arrives). Registrations are process-local today: a hub crash after the ack
\* leaves the pusher holding nothing to retry and the hub holding nothing to complete.
\* Silent, permanent loss - the restart-shaped twin of #1088 M1.
\*
\* THE FIX: registrations are also persisted at admission time; on restart the durable
\* set is restored into the pending map and re-driven to completion.
\*
\* One knob:
\*   RecoveryMode = "Persist"      - HubAdmit persists alongside the in-memory
\*                                   registration; Restore re-loads after a crash. [GREEN]
\*                = "ProcessLocal" - current behavior: registrations die with the
\*                                   process. [RED]
\*
\* Abstractions: as in PushLogAdmission, docs are the unit and "the pusher" is the
\* per-doc ack/retry record; the pusher process survives the hub crash. The crash is
\* one-shot and atomic with restart (the hub is immediately back up; a down window adds
\* states without adding behaviors relevant to durability). Under Persist the admission
\* capacity check runs against the durable set - the superset of the in-memory map in
\* the crash window - so the Cap bound survives restore.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Docs,         \* pushed documents (each = one head push contending for admission)
  Cap,          \* pending-DAG capacity (SyncConfig::max_pending_dags)
  RecoveryMode  \* "Persist" | "ProcessLocal"

ASSUME Docs # {}
ASSUME Cap \in Nat /\ Cap >= 1
ASSUME RecoveryMode \in {"Persist", "ProcessLocal"}

PusherStates == {"unsent", "inflight", "ackedSuccess", "retryQueued"}

VARIABLES
  pending,    \* SUBSET Docs - in-memory pending-DAG registrations (lost on crash)
  persisted,  \* SUBSET Docs - durable registrations (always {} under ProcessLocal)
  merged,     \* SUBSET Docs - merged on the hub (durable)
  pusher,     \* [Docs -> PusherStates]
  crashed     \* BOOLEAN - the one-shot hub crash has happened

vars == <<pending, persisted, merged, pusher, crashed>>

\* The set the admission capacity check runs against: the durable ledger under Persist
\* (equal to pending except in the crash window), the in-memory map otherwise.
Registered == IF RecoveryMode = "Persist" THEN persisted ELSE pending

TypeOK ==
  /\ pending \subseteq Docs
  /\ persisted \subseteq Docs
  /\ merged \subseteq Docs
  /\ pending \cap merged = {}
  /\ Cardinality(pending) <= Cap
  /\ Cardinality(persisted) <= Cap
  /\ pusher \in [Docs -> PusherStates]
  /\ crashed \in BOOLEAN

Init ==
  /\ pending = {}
  /\ persisted = {}
  /\ merged = {}
  /\ pusher = [d \in Docs |-> "unsent"]
  /\ crashed = FALSE

\* Pusher sends (or re-sends from its persisted retry ladder) the doc's head push.
Send(d) ==
  /\ pusher[d] \in {"unsent", "retryQueued"}
  /\ pusher' = [pusher EXCEPT ![d] = "inflight"]
  /\ UNCHANGED <<pending, persisted, merged, crashed>>

\* The pushed DAG is complete on arrival: merge, drop any registration, reply success.
HubComplete(d) ==
  /\ pusher[d] = "inflight"
  /\ merged' = merged \cup {d}
  /\ pending' = pending \ {d}
  /\ persisted' = persisted \ {d}
  /\ pusher' = [pusher EXCEPT ![d] = "ackedSuccess"]
  /\ UNCHANGED crashed

\* Missing links and a free slot: register pending - durably too under Persist - and
\* reply success. The ack is honest exactly as long as the registration outlives the
\* process (the point of this module).
HubAdmit(d) ==
  /\ pusher[d] = "inflight"
  /\ d \notin merged
  /\ (Cardinality(Registered) < Cap \/ d \in Registered)
  /\ pending' = pending \cup {d}
  /\ persisted' = IF RecoveryMode = "Persist" THEN persisted \cup {d} ELSE persisted
  /\ pusher' = [pusher EXCEPT ![d] = "ackedSuccess"]
  /\ UNCHANGED <<merged, crashed>>

\* Capacity overflow nacks (RATE_LIMITED_MESSAGE, the #1088 W1 behavior): the pusher
\* keeps its retry record and re-pushes.
HubOverflowNack(d) ==
  /\ pusher[d] = "inflight"
  /\ d \notin merged
  /\ d \notin Registered
  /\ Cardinality(Registered) >= Cap
  /\ pusher' = [pusher EXCEPT ![d] = "retryQueued"]
  /\ UNCHANGED <<pending, persisted, merged, crashed>>

\* Bitswap completes a registered DAG: merge and clear both registrations.
Resolve(d) ==
  /\ d \in pending
  /\ pending' = pending \ {d}
  /\ persisted' = persisted \ {d}
  /\ merged' = merged \cup {d}
  /\ UNCHANGED <<pusher, crashed>>

\* The hub process dies and restarts (at most once): the in-memory pending map is
\* gone; the datastore - merged docs and any persisted registrations - survives. The
\* pusher is another process and keeps its records.
Crash ==
  /\ ~crashed
  /\ crashed' = TRUE
  /\ pending' = {}
  /\ UNCHANGED <<persisted, merged, pusher>>

\* Restart recovery under Persist: reload the durable registrations into the pending
\* map so Bitswap re-drives them. ProcessLocal has nothing to restore.
Restore ==
  /\ crashed
  /\ RecoveryMode = "Persist"
  /\ pending # persisted
  /\ pending' = persisted
  /\ UNCHANGED <<persisted, merged, pusher, crashed>>

Next ==
  \/ \E d \in Docs :
       \/ Send(d)
       \/ HubComplete(d)
       \/ HubAdmit(d)
       \/ HubOverflowNack(d)
       \/ Resolve(d)
  \/ Crash
  \/ Restore

\* All docs merged: stutter so TLC does not flag deadlock on a finished schedule.
\* (Under ProcessLocal a schedule can also WEDGE unmerged after the crash - every
\* pusher record acked, nothing pending, nothing re-pushable - but the invariant
\* below catches it at the crash step, earlier and crisper.)
Done == merged = Docs
Terminating == Done /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Terminating]_vars

\* Weak fairness per doc as in PushLogAdmission, plus Restore: recovery is part of
\* the scheduler, the crash itself is not (it stays optional).
FairSpec ==
  Spec
  /\ \A d \in Docs :
       /\ WF_vars(Send(d))
       /\ WF_vars(Resolve(d))
       /\ WF_vars(HubAdmit(d))
       /\ WF_vars(HubComplete(d))
  /\ WF_vars(Restore)

INV_TypeOK == TypeOK

\* THE #1099 RESTART INVARIANT: a success ack is always backed by hub state that can
\* still complete the doc - merged, registered in memory, or durably persisted.
\* ProcessLocal violates this at the crash step; Persist preserves it in every
\* reachable state, crash window included.
INV_AckBacked ==
  \A d \in Docs : pusher[d] = "ackedSuccess" => d \in merged \cup pending \cup persisted

\* Liveness (GREEN, under FairSpec): even with a crash, every pushed doc eventually
\* merges - restore re-drives what the ack promised.
EventuallyAllMerged == <>(merged = Docs)
====
