---- MODULE InteractiveTxnCounter ----
\* Guard-acquisition LIFECYCLE of the interactive / explicit-transaction counter mutator,
\* abstracting crates/db/src/doc_mutator.rs (DbDocMutator), crates/db/src/txn.rs (DbTxn
\* pending counter deltas + commit) and crates/db/src/doc_write_queue.rs (acquire /
\* acquire_batch_gate). This is the #1044 TARGET design (NOT yet implemented in full — see
\* InteractiveTxnCounter_DESIGN.md). Companion to MergeQueue.tla, which models the per-doc
\* serialization itself; this slice isolates WHEN the interactive txn takes the
\* process-wide batch_gate over its user-controlled lifetime.
\*
\* THE BUG this catches (#1041 review HIGH, fixed by #1044): defradb.rs serializes the
\* guard-ACQUISITION PHASE of multi-doc acquirers with a single PROCESS-WIDE batch_gate, so
\* incremental acquirers cannot deadlock against the sorted ones. Multi-doc acquirers that
\* know their doc set UPFRONT (batch merge, create_many) take the gate, grab per-doc guards
\* in SORTED order, then RELEASE the gate — a bounded hold. The OLD interactive path
\* (DbDocMutator on DbTxn) discovers its docs INCREMENTALLY and so held the gate (and its
\* per-doc guards) for the WHOLE user-controlled transaction — which can sit IDLE between
\* requests up to the ~600s idle reaper (txn_registry.rs DEFAULT_TRANSACTION_IDLE_TIMEOUT).
\* An abandoned interactive counter txn therefore stalled every other gate acquirer
\* node-wide.
\*
\* THE FIX (#1044, GREEN): the interactive txn holds NO gate and NO per-doc guard while
\* active/idle; it only RECORDS its pending counter deltas on DbTxn. At COMMIT it performs
\* a single atomic "finalize": briefly take the gate, acquire its touched-counter-doc
\* guards in SORTED order, do the RMW, then release — exactly like try_batch_merge /
\* create_many's bounded acquire phase. The gate is held ONLY during the bounded commit
\* action, never across an idle state.
\*
\* One knob selects the new vs. old gate lifecycle:
\*   InteractiveGate = "AtCommitOnly"   - #1044 fix: gate taken only inside the atomic
\*                                        finalize/commit action; never while active/idle [GREEN]
\*                   = "AcrossLifetime" - #1041 old path: gate taken on first counter write
\*                                        and HELD across active/idle until commit         [RED]
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  Docs,            \* finite, ORDERED set of document ids (sorted-acquire domain)
  ITouched,        \* SUBSET Docs — counter docs the interactive txn touches (>=1)
  BTouched,        \* SUBSET Docs — counter docs the batch acquirer (create_many) touches (>=1)
  MergeDoc,        \* a single doc the merge worker contends on
  InteractiveGate  \* "AtCommitOnly" | "AcrossLifetime"

ASSUME Docs # {}
ASSUME ITouched \subseteq Docs /\ ITouched # {}
ASSUME BTouched \subseteq Docs /\ BTouched # {}
ASSUME MergeDoc \in Docs
ASSUME InteractiveGate \in {"AtCommitOnly", "AcrossLifetime"}

NoOwner == "none"
\* Sentinel lock holders, one per actor (a doc guard records WHO holds it).
ITok == "itxn"     \* interactive txn
BTok == "batch"    \* batch / create_many acquirer
MTok == "merge"    \* single-doc merge worker

VARIABLES
  gate,        \* {NoOwner} \cup {ITok, BTok}  — process-wide batch_gate holder (merge never holds it here)
  lockOwner,   \* [Docs -> {NoOwner, ITok, BTok, MTok}]  — per-doc DocWriteQueue guard holder
  iPhase,      \* interactive txn phase
  iHeld,       \* SUBSET Docs — per-doc guards the interactive finalize currently holds
  bPhase,      \* batch acquirer phase
  bHeld,       \* SUBSET Docs — per-doc guards the batch acquirer currently holds
  mPhase,      \* merge worker phase
  iInCrit,     \* [Docs -> BOOLEAN] interactive RMW in the doc critical section
  mInCrit      \* [Docs -> BOOLEAN] merge RMW in the doc critical section

vars == <<gate, lockOwner, iPhase, iHeld, bPhase, bHeld, mPhase, iInCrit, mInCrit>>

\* Interactive lifecycle: active <-> idle (holds nothing under GREEN), then the atomic
\* finalize sequence (acquire-sorted-guards -> rmw -> commit/release), then committed.
IPhases == {"active", "idle", "finalize", "committed"}
\* Batch acquirer: idle -> gate -> sorted-acquire -> rmw -> release.
BPhases == {"idle", "acquiring", "rmw", "done"}
\* Merge worker: a single-doc acquire -> crit -> done (no gate; per-doc guard only).
MPhases == {"idle", "crit", "done"}

TypeOK ==
  /\ gate \in {NoOwner, ITok, BTok}
  /\ lockOwner \in [Docs -> {NoOwner, ITok, BTok, MTok}]
  /\ iPhase \in IPhases
  /\ iHeld \subseteq Docs
  /\ bPhase \in BPhases
  /\ bHeld \subseteq Docs
  /\ mPhase \in MPhases
  /\ iInCrit \in [Docs -> BOOLEAN]
  /\ mInCrit \in [Docs -> BOOLEAN]

Init ==
  /\ gate = NoOwner
  /\ lockOwner = [d \in Docs |-> NoOwner]
  /\ iPhase = "active"
  /\ iHeld = {}
  /\ bPhase = "idle"
  /\ bHeld = {}
  /\ mPhase = "idle"
  /\ iInCrit = [d \in Docs |-> FALSE]
  /\ mInCrit = [d \in Docs |-> FALSE]

\* The smallest still-unacquired touched doc whose guard is free, given an acquirer's
\* target set and what it already holds. Sorted acquisition: a doc may be taken only after
\* every smaller touched doc is already held. Docs are naturals so the total order used by
\* BOTH multi-doc acquirers (interactive finalize, batch) is the same `<` — the common
\* total order that makes sorted acquisition deadlock-free (matches Rust's BTreeSet/sorted
\* Vec<doc_id> acquire order in try_batch_merge / create_many / the #1044 finalize).
Remaining(target, held) == target \ held
\* Precondition: rem # {}. The min remaining doc under the shared total order.
MinDoc(rem) == CHOOSE x \in rem : \A y \in rem : x <= y

\* =====================================================================================
\* INTERACTIVE TXN  (DbDocMutator over DbTxn)
\* =====================================================================================

\* active <-> idle: user-controlled think time. Under GREEN the txn holds NOTHING here; the
\* RED knob (AcrossLifetime) grabs the gate on the first counter write and keeps it.
IGoIdle ==
  /\ iPhase = "active"
  /\ IF InteractiveGate = "AcrossLifetime" /\ gate = NoOwner
       THEN gate' = ITok      \* old path: gate held across the whole lifetime
       ELSE gate' = gate
  /\ iPhase' = "idle"
  /\ UNCHANGED <<lockOwner, iHeld, bPhase, bHeld, mPhase, iInCrit, mInCrit>>

IGoActive ==
  /\ iPhase = "idle"
  /\ iPhase' = "active"
  /\ UNCHANGED <<gate, lockOwner, iHeld, bPhase, bHeld, mPhase, iInCrit, mInCrit>>

\* COMMIT begins: enter the atomic finalize. GREEN takes the gate HERE (bounded);
\* AcrossLifetime already holds it.
IBeginFinalize ==
  /\ iPhase \in {"active", "idle"}
  /\ IF InteractiveGate = "AtCommitOnly"
       THEN gate = NoOwner /\ gate' = ITok
       ELSE gate' = gate        \* already ITok under AcrossLifetime
  /\ iPhase' = "finalize"
  /\ UNCHANGED <<lockOwner, iHeld, bPhase, bHeld, mPhase, iInCrit, mInCrit>>

\* finalize: acquire touched-counter-doc guards in SORTED order while holding the gate.
IAcquire ==
  /\ iPhase = "finalize"
  /\ Remaining(ITouched, iHeld) # {}
  /\ LET nd == MinDoc(Remaining(ITouched, iHeld)) IN
       /\ lockOwner[nd] = NoOwner
       /\ lockOwner' = [lockOwner EXCEPT ![nd] = ITok]
       /\ iHeld' = iHeld \cup {nd}
  /\ UNCHANGED <<gate, iPhase, bPhase, bHeld, mPhase, iInCrit, mInCrit>>

\* finalize: once all touched guards are held, ENTER the RMW critical section on each
\* touched doc and release the gate (the bounded gate hold ends here; guards still held).
\* The per-doc guard is what serializes the RMW vs a same-doc merge — the merge cannot be
\* in the critical section on a doc whose guard the interactive txn holds.
IFinalizeCommit ==
  /\ iPhase = "finalize"
  /\ iHeld = ITouched
  /\ iInCrit' = [d \in Docs |-> IF d \in ITouched THEN TRUE ELSE iInCrit[d]]
  /\ gate' = NoOwner
  /\ iPhase' = "committed"
  /\ UNCHANGED <<lockOwner, iHeld, bPhase, bHeld, mPhase, mInCrit>>

\* The RMW critical section is exited and the per-doc guards released (commit durable).
\* Separate step so the INV_NoLocalMergeInterleave window (guard held <=> iInCrit) is
\* observable, and so release-after-durable-commit is explicit.
IExitCrit ==
  /\ iPhase = "committed"
  /\ \E d \in ITouched : iInCrit[d]
  /\ iInCrit' = [d \in Docs |-> IF d \in ITouched THEN FALSE ELSE iInCrit[d]]
  /\ lockOwner' = [d \in Docs |-> IF d \in ITouched THEN NoOwner ELSE lockOwner[d]]
  /\ iHeld' = {}
  /\ UNCHANGED <<gate, iPhase, bPhase, bHeld, mPhase, mInCrit>>

\* =====================================================================================
\* BATCH / create_many ACQUIRER  (knows its doc set upfront; sorted acquire under gate)
\* =====================================================================================
BTakeGate ==
  /\ bPhase = "idle"
  /\ gate = NoOwner
  /\ gate' = BTok
  /\ bPhase' = "acquiring"
  /\ UNCHANGED <<lockOwner, iPhase, iHeld, bHeld, mPhase, iInCrit, mInCrit>>

BAcquire ==
  /\ bPhase = "acquiring"
  /\ Remaining(BTouched, bHeld) # {}
  /\ LET nd == MinDoc(Remaining(BTouched, bHeld)) IN
       /\ lockOwner[nd] = NoOwner
       /\ lockOwner' = [lockOwner EXCEPT ![nd] = BTok]
       /\ bHeld' = bHeld \cup {nd}
  /\ UNCHANGED <<gate, iPhase, iHeld, bPhase, mPhase, iInCrit, mInCrit>>

BRmw ==
  /\ bPhase = "acquiring"
  /\ bHeld = BTouched
  /\ bPhase' = "rmw"
  /\ gate' = NoOwner          \* gate released after the bounded acquire phase
  /\ UNCHANGED <<lockOwner, iPhase, iHeld, bHeld, mPhase, iInCrit, mInCrit>>

BRelease ==
  /\ bPhase = "rmw"
  /\ lockOwner' = [d \in Docs |-> IF d \in BTouched THEN NoOwner ELSE lockOwner[d]]
  /\ bHeld' = {}
  /\ bPhase' = "done"
  /\ UNCHANGED <<gate, iPhase, iHeld, mPhase, iInCrit, mInCrit>>

\* =====================================================================================
\* SINGLE-DOC MERGE  (per-doc guard only; no gate)
\* =====================================================================================
MAcquire ==
  /\ mPhase = "idle"
  /\ lockOwner[MergeDoc] = NoOwner
  /\ lockOwner' = [lockOwner EXCEPT ![MergeDoc] = MTok]
  /\ mInCrit' = [mInCrit EXCEPT ![MergeDoc] = TRUE]
  /\ mPhase' = "crit"
  /\ UNCHANGED <<gate, iPhase, iHeld, bPhase, bHeld, iInCrit>>

MRelease ==
  /\ mPhase = "crit"
  /\ lockOwner' = [lockOwner EXCEPT ![MergeDoc] = NoOwner]
  /\ mInCrit' = [mInCrit EXCEPT ![MergeDoc] = FALSE]
  /\ mPhase' = "done"
  /\ UNCHANGED <<gate, iPhase, iHeld, bPhase, bHeld, iInCrit>>

Next ==
  \/ IGoIdle \/ IGoActive \/ IBeginFinalize \/ IAcquire \/ IFinalizeCommit \/ IExitCrit
  \/ BTakeGate \/ BAcquire \/ BRmw \/ BRelease
  \/ MAcquire \/ MRelease

\* All actors terminated: stutter so TLC does not flag deadlock on a finished schedule.
Done ==
  /\ iPhase = "committed" /\ ~(\E d \in ITouched : iInCrit[d])
  /\ bPhase = "done"
  /\ mPhase = "done"
Terminating == Done /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Terminating]_vars

\* =====================================================================================
\* INVARIANTS (safety)
\* =====================================================================================
INV_TypeOK == TypeOK

\* (1) Deadlock-freedom is checked by TLC's CHECK_DEADLOCK (no deadlock state reachable
\*     under GREEN). The sorted-acquire order shared by the interactive finalize and the
\*     batch acquirer is what makes it deadlock-free without the gate covering the whole
\*     interactive lifetime.

\* (2) Bounded gate hold: the process-wide gate is NEVER held while the interactive actor
\*     is in an active/idle (non-finalize, non-committed) state. GREEN (AtCommitOnly) takes
\*     the gate only inside finalize; RED (AcrossLifetime) grabs it on IGoIdle and holds it
\*     across active/idle -> VIOLATED (anchors non-vacuity and reproduces the old design).
INV_GateBoundedHold ==
  (gate = ITok) => (iPhase \in {"finalize"})

\* (3) No local-vs-merge interleave (carried from MergeQueue.tla): a local interactive RMW
\*     and a merge are never both in the per-doc critical section on the same doc. The
\*     per-doc guard (sorted-acquired at commit) is what excludes them.
INV_NoLocalMergeInterleave ==
  \A d \in Docs : ~(iInCrit[d] /\ mInCrit[d])

\* (4) Sanity: a per-doc guard has at most one holder at a time (single-owner lock).
\*     (Implied by the actions; carried as a TypeOK-adjacent witness of the lock model.)
INV_SingleGuardOwner ==
  \A d \in Docs :
    LET owners == {t \in {ITok, BTok, MTok} : lockOwner[d] = t} IN
    Cardinality(owners) <= 1
====
