---- MODULE InteractiveTxnCounter ----
\* Guard-acquisition LIFECYCLE of the interactive / explicit-transaction counter mutator,
\* abstracting crates/db/src/write/doc.rs (DbDocMutator), crates/db/src/txn/mod.rs (DbTxn
\* pending counter deltas + commit) and crates/db/src/write/queue.rs (acquire /
\* acquire_batch_gate). This is the #1044 design, IMPLEMENTED in the tree (commit-time
\* finalize in txn/registry/lifecycle.rs::finalize_and_commit / apply_counter_ops_at_finalize — see
\* InteractiveTxnCounter_DESIGN.md). Companion to MergeQueue.tla, which models the per-doc
\* serialization itself; this slice isolates WHEN the interactive txn takes the
\* process-wide batch_gate over its user-controlled lifetime.
\*
\* THE MECHANISM (#1041 review HIGH, refined by #1044): defradb.rs serializes the
\* guard-ACQUISITION PHASE of every multi-doc acquirer with a single PROCESS-WIDE
\* batch_gate. The gate is the DEADLOCK-FREEDOM mechanism: it is held only BRIEFLY, during
\* a bounded acquisition phase, so two multi-doc acquirers never acquire their per-doc
\* guards CONCURRENTLY — the gate serializes the whole acquire phase, so no two acquirers
\* can each hold one guard and wait on the other (circular wait is structurally impossible).
\*
\* WHY THE GATE (and not sorted order): SORTED acquisition is NOT the deadlock-freedom
\* mechanism. Because the gate serializes the acquisition phase, two multi-doc acquirers
\* never run concurrently and sorted order never gets a chance to matter — sorted order is
\* incidental/defensive GIVEN the gate. The gate is REQUIRED because some acquirers are
\* IRREDUCIBLY INCREMENTAL: BatchMutator (and the interactive txn before #1044) DISCOVER
\* their docs one mutation at a time and CANNOT pre-sort their doc set, so a sorted-order
\* discipline is simply not available to them. The gate is what protects the system from
\* these incremental acquirers. (Go has NO such gate because Go NEVER holds more than one
\* per-doc lock at a time — its isolation is pure storage-txn isolation; the Rust gate is a
\* compensator for Rust holding multiple per-doc guards at once.)
\*
\* Multi-doc acquirers that know their doc set UPFRONT (batch merge, create_many) take the
\* gate, grab their per-doc guards, then RELEASE the gate — a bounded hold. The OLD
\* interactive path (DbDocMutator on DbTxn) discovers its docs INCREMENTALLY and so held
\* the gate (and its per-doc guards) for the WHOLE user-controlled transaction — which can
\* sit IDLE between requests up to the ~600s idle reaper (txn/registry/mod.rs
\* DEFAULT_TRANSACTION_IDLE_TIMEOUT). An abandoned interactive counter txn therefore
\* stalled every other gate acquirer node-wide.
\*
\* THE #1044 FIX (GREEN): the interactive txn holds NO gate and NO per-doc guard while
\* active/idle; it only RECORDS its pending counter deltas on DbTxn. At COMMIT it performs
\* a single atomic "finalize": briefly take the gate, acquire its touched-counter-doc
\* guards, do the RMW, then release — exactly like try_batch_merge / create_many's bounded
\* acquire phase. The gate is held ONLY during the bounded commit action (a BOUNDED
\* gate-holder), never across an idle state.
\*
\* Two knobs:
\*   GateMode        = "On"  - multi-doc acquirers take the gate (real system). [GREEN]
\*                   = "Off" - acquirers SKIP the gate -> acquire concurrently. With the gate
\*                             removed, the arbitrary-order incremental batch actor and the
\*                             interactive finalize can grab docs in opposite orders -> a
\*                             circular-wait DEADLOCK. Proves the gate is load-bearing. [RED]
\*   InteractiveGate = "AtCommitOnly"   - #1044 fix: gate taken only inside the atomic
\*                                        finalize/commit action; never while active/idle [GREEN]
\*                   = "AcrossLifetime" - #1041 old path: gate taken on first counter write
\*                                        and HELD across active/idle until commit         [RED]
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  Docs,            \* finite set of document ids
  ITouched,        \* SUBSET Docs — counter docs the interactive txn touches (>=1)
  BTouched,        \* SUBSET Docs — counter docs the incremental batch acquirer (BatchMutator) touches (>=1)
  MergeDoc,        \* a single doc the merge worker contends on
  GateMode,        \* "On" | "Off" — whether multi-doc acquirers take the process-wide gate
  InteractiveGate  \* "AtCommitOnly" | "AcrossLifetime"

ASSUME Docs # {}
ASSUME ITouched \subseteq Docs /\ ITouched # {}
ASSUME BTouched \subseteq Docs /\ BTouched # {}
ASSUME MergeDoc \in Docs
ASSUME GateMode \in {"On", "Off"}
ASSUME InteractiveGate \in {"AtCommitOnly", "AcrossLifetime"}

NoOwner == "none"
\* Sentinel lock holders, one per actor (a doc guard records WHO holds it).
ITok == "itxn"     \* interactive txn
BTok == "batch"    \* BatchMutator (incremental); try_batch_merge/create_many are sorted-upfront, same gate
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
\* Batch acquirer: idle -> gate -> arbitrary-order acquire -> rmw -> release.
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

\* The still-unacquired touched docs, given an acquirer's target set and what it holds.
Remaining(target, held) == target \ held
\* The min remaining doc under `<`. Used by the interactive finalize as a DEFENSIVE/incidental
\* order; it is NOT the deadlock-freedom mechanism (the gate is). When the gate is On, the
\* batch acquirer cannot run concurrently with the finalize, so this order never decides
\* anything; when the gate is Off, this fixed order vs. the batch's arbitrary order is what
\* lets a circular wait form — exposing that the gate, not the order, prevents deadlock.
MinDoc(rem) == CHOOSE x \in rem : \A y \in rem : x <= y

\* =====================================================================================
\* INTERACTIVE TXN  (DbDocMutator over DbTxn)
\* =====================================================================================

\* active <-> idle: user-controlled think time. Under GREEN the txn holds NOTHING here; the
\* RED knob (AcrossLifetime) grabs the gate on the first counter write and HOLDS it across
\* the whole idle lifetime (faithfully modeling the #1041 old design): the gate must be
\* acquired here (gate=NoOwner -> ITok) and is then held into idle and finalize.
IGoIdle ==
  /\ iPhase = "active"
  /\ IF InteractiveGate = "AcrossLifetime"
       THEN gate = NoOwner /\ gate' = ITok   \* old path: take the gate, hold it across idle
       ELSE gate' = gate                      \* GREEN: hold nothing while active/idle
  /\ iPhase' = "idle"
  /\ UNCHANGED <<lockOwner, iHeld, bPhase, bHeld, mPhase, iInCrit, mInCrit>>

IGoActive ==
  /\ iPhase = "idle"
  /\ iPhase' = "active"
  /\ UNCHANGED <<gate, lockOwner, iHeld, bPhase, bHeld, mPhase, iInCrit, mInCrit>>

\* COMMIT begins: enter the atomic finalize.
\*   - AtCommitOnly + GateMode="On": take the gate HERE (the bounded #1044 hold).
\*   - AcrossLifetime (GateMode="On"): the gate is ALREADY held (from IGoIdle); require it.
\*   - GateMode="Off": skip the gate entirely (finalize acquires concurrently with batch).
IBeginFinalize ==
  /\ iPhase \in {"active", "idle"}
  /\ IF GateMode = "On"
       THEN IF InteractiveGate = "AtCommitOnly"
              THEN gate = NoOwner /\ gate' = ITok
              ELSE gate = ITok /\ gate' = gate   \* already ITok under AcrossLifetime
       ELSE gate' = gate                          \* GateMode="Off": no gate
  /\ iPhase' = "finalize"
  /\ UNCHANGED <<lockOwner, iHeld, bPhase, bHeld, mPhase, iInCrit, mInCrit>>

\* finalize: acquire touched-counter-doc guards. The order is MinDoc (defensive/incidental);
\* with the gate On nothing else acquires concurrently so the order is moot. With the gate
\* Off, this fixed order vs. the batch actor's arbitrary order is what admits a circular wait.
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
\* INCREMENTAL BATCH ACQUIRER = BatchMutator  (IRREDUCIBLY INCREMENTAL: discovers docs one at a
\* time and CANNOT pre-sort -> acquires in ARBITRARY order; this is the actor the gate protects
\* against. try_batch_merge / create_many sort+dedup UPFRONT, so they are NOT this actor — they
\* are sorted acquirers protected by the same gate.)
\* =====================================================================================
\* GateMode="On": take the gate before acquiring (real BatchMutator). GateMode="Off": skip it.
BTakeGate ==
  /\ bPhase = "idle"
  /\ IF GateMode = "On" THEN gate = NoOwner /\ gate' = BTok ELSE gate' = gate
  /\ bPhase' = "acquiring"
  /\ UNCHANGED <<lockOwner, iPhase, iHeld, bHeld, mPhase, iInCrit, mInCrit>>

\* ARBITRARY-order acquisition: a free remaining doc is chosen non-deterministically,
\* modeling BatchMutator's irreducibly-incremental discovery (it cannot acquire in a sorted
\* order it does not yet know). This is precisely why a sorted discipline is unavailable and
\* the gate is the actual deadlock-freedom mechanism.
BAcquire ==
  /\ bPhase = "acquiring"
  /\ \E nd \in Remaining(BTouched, bHeld) :
       /\ lockOwner[nd] = NoOwner
       /\ lockOwner' = [lockOwner EXCEPT ![nd] = BTok]
       /\ bHeld' = bHeld \cup {nd}
  /\ UNCHANGED <<gate, iPhase, iHeld, bPhase, mPhase, iInCrit, mInCrit>>

BRmw ==
  /\ bPhase = "acquiring"
  /\ bHeld = BTouched
  /\ bPhase' = "rmw"
  /\ gate' = NoOwner          \* gate released after the bounded acquire phase (no-op if Off)
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

\* (1) Deadlock-freedom is checked by TLC's CHECK_DEADLOCK. The BOUNDED PROCESS-WIDE GATE
\*     is the mechanism: with GateMode="On" the gate serializes the acquisition phase of the
\*     multi-doc acquirers (interactive finalize, batch), so they never acquire concurrently
\*     and no circular wait can form -> no deadlock (GREEN). With GateMode="Off" the
\*     arbitrary-order incremental batch actor and the finalize acquire concurrently in
\*     opposite orders -> circular-wait DEADLOCK that TLC detects (Red_NoGate), proving the
\*     gate — not the (incidental) MinDoc order — is what prevents deadlock.

\* (2) Bounded gate hold: the process-wide gate is NEVER held while the interactive actor
\*     is in an active/idle (non-finalize, non-committed) state. GREEN (AtCommitOnly) takes
\*     the gate only inside finalize; RED (AcrossLifetime) grabs it on IGoIdle and holds it
\*     across active/idle -> VIOLATED (anchors non-vacuity and reproduces the old design).
INV_GateBoundedHold ==
  (gate = ITok) => (iPhase \in {"finalize"})

\* (3) No local-vs-merge interleave (carried from MergeQueue.tla): a local interactive RMW
\*     and a merge are never both in the per-doc critical section on the same doc. The
\*     per-doc guard (acquired at commit) is what excludes them.
INV_NoLocalMergeInterleave ==
  \A d \in Docs : ~(iInCrit[d] /\ mInCrit[d])

\* (4) Sanity: a per-doc guard has at most one holder at a time (single-owner lock).
\*     (Implied by the actions; carried as a TypeOK-adjacent witness of the lock model.)
INV_SingleGuardOwner ==
  \A d \in Docs :
    LET owners == {t \in {ITok, BTok, MTok} : lockOwner[d] = t} IN
    Cardinality(owners) <= 1
====
