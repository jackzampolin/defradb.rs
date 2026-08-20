---- MODULE MergeQueue ----
\* Per-document write serialization + bounded conflict-retry, abstracting
\* crates/db/src/write/queue.rs (DocWriteQueue, owned by the DB and shared by
\* BOTH the local-write path and the db-merge merge handler — #1021) and
\* crates/db-merge/src/merge_handler/batch.rs (merge_blocks_individually retry loop).
\* (Was crates/db-merge/src/merge_handler/queue.rs before #1021 unified local
\* writes and merges onto one per-doc lock.) Anchors are in MergeQueue_DESIGN.md.
\*
\* The property: one P2P merge writer owns the shared mutable index transaction at a
\* time, while the per-doc async mutex still serializes local writes with merges on the
\* same document. The writer may apply an ordered multi-document batch. The bounded
\* (MaxRetries) txn-conflict retry loop loses and duplicates no block; retry exhaustion
\* fails CLOSED (the block is reported failed and stays re-deliverable, never silently
\* marked done).
\*
\* INDEPENDENT ORACLE.  Correctness is judged from two ground-truth ledgers that are NOT
\* the mechanism's own accept/skip decision:
\*   appliedCount[b] - how many times block b's delta was actually committed into doc state
\*                     (incremented only inside a committing txn that found b un-applied).
\*   marked[b]       - whether the caller recorded b as "done" (added to the merged-set in
\*                     process_merge_batch: Ok(Merged) or Ok(terminal Skip) -> mark;
\*                     Err -> NOT marked -> block remains re-deliverable).
\* The headline safety invariants relate these ledgers; the mutex/retry mechanism cannot
\* fake them by agreeing with itself.
\*
\* Three knobs select correct mechanism vs. adversary variant:
\*   LockMode = "GlobalMerge" - one receiver merge writer plus per-doc guards       [GREEN]
\*            = "PerDoc" - pre-stage-3: different-doc P2P writers overlap           [RED]
\*            = "None"    - bug: no per-doc mutex; same-doc merges run concurrently  [RED]
\*   FailMode = "Closed"  - real Rust: exhausted retries -> Err -> NOT marked done   [GREEN]
\*            = "Open"     - Go merge.go bug: exhausted retries -> return nil ->
\*                           caller treats block as done -> silent drop              [RED]
\*   UserWriteMode = "PerDoc" - #1021 fix: a local user-write ALSO acquires the shared
\*                              per-doc DocWriteQueue guard (update_impl/create_impl take
\*                              the SAME lock the merge handler takes). A local write and a
\*                              same-doc merge are MUTUALLY EXCLUDED in the critical section,
\*                              not merely conflict-retried.                         [GREEN]
\*                 = "LockFree" - pre-#1021 adversary: the local-write path does NOT take the
\*                              merge lock; it only bumps docVer and is reconciled by the
\*                              txn-conflict retry loop (the conflict-detection story).  This
\*                              alone does NOT serialize the store RMW the counter fix needs.
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  Blocks,      \* finite set of merge-block ids (re-deliveries share a doc; see Dup)
  Docs,        \* finite set of document ids
  BlockDoc,    \* [Blocks -> Docs]  which document each block targets
  Dup,         \* [Blocks -> Blocks] block b is a duplicate delivery of Dup[b] (or itself)
  MaxRetries,  \* retry budget (MAX_MERGE_RETRIES = 5; node.go MaxTxnRetries = 5)
  MaxUserWrites, \* bound on adversarial concurrent local user-writes per doc
  LockMode,    \* "GlobalMerge" | "PerDoc" | "None"
  FailMode,    \* "Closed" | "Open"
  UserWriteMode \* "PerDoc" | "LockFree"  — does a local user-write take the shared guard?

ASSUME Blocks # {} /\ Docs # {}
ASSUME BlockDoc \in [Blocks -> Docs]
ASSUME Dup \in [Blocks -> Blocks]
\* A duplicate must target the same document as its original (same CID => same doc).
ASSUME \A b \in Blocks : BlockDoc[Dup[b]] = BlockDoc[b]
ASSUME MaxRetries \in Nat /\ MaxRetries >= 1
ASSUME MaxUserWrites \in Nat
ASSUME LockMode \in {"GlobalMerge", "PerDoc", "None"}
ASSUME FailMode \in {"Closed", "Open"}
ASSUME UserWriteMode \in {"PerDoc", "LockFree"}

NoOwner == "none"
\* Sentinel lock holder for a local user-write inside the shared per-doc guard.
UserTok == "user"

VARIABLES
  pc,          \* [Blocks -> phase]  per-worker control state
  attempt,     \* [Blocks -> Nat]    attempts consumed in the retry loop
  readVer,     \* [Blocks -> Nat]    docVer snapshotted at the START of the current attempt
  seenMerged,  \* [Blocks -> BOOLEAN] is_merged(cid) snapshotted at the START of the attempt
  lockOwner,   \* [Docs -> Blocks \cup {NoOwner, UserTok}]  per-doc async mutex holder
  docVer,      \* [Docs -> Nat]      bumped by each local user-write (drives txn conflict)
  userWrites,  \* [Docs -> Nat]      adversarial user-writes spent (bounded by MaxUserWrites)
  applied,     \* [Blocks -> Nat]    ORACLE: times this block's effect hit doc state
  docState,    \* [Docs -> SUBSET Blocks]  the merged-set: original ids applied to each doc
  marked,      \* [Blocks -> BOOLEAN]  ORACLE: caller recorded the block as "done"
  inCrit,      \* [Docs -> SUBSET Blocks]  merge workers currently inside the critical section
  uwInCrit     \* [Docs -> BOOLEAN]  is a local user-write currently inside the critical section
               \*                    (only reachable when UserWriteMode="PerDoc")

vars == <<pc, attempt, readVer, seenMerged, lockOwner, docVer, userWrites,
          applied, docState, marked, inCrit, uwInCrit>>

Phases == {"start", "crit", "done"}

\* The canonical identity a block applies under (its de-duplicated original).
Orig(b) == Dup[b]

TypeOK ==
  /\ pc        \in [Blocks -> Phases]
  /\ attempt   \in [Blocks -> 0..MaxRetries]
  /\ readVer   \in [Blocks -> Nat]
  /\ seenMerged\in [Blocks -> BOOLEAN]
  /\ lockOwner \in [Docs -> Blocks \cup {NoOwner, UserTok}]
  /\ docVer    \in [Docs -> Nat]
  /\ userWrites\in [Docs -> 0..MaxUserWrites]
  /\ applied   \in [Blocks -> Nat]
  /\ docState  \in [Docs -> SUBSET Blocks]
  /\ marked    \in [Blocks -> BOOLEAN]
  /\ inCrit    \in [Docs -> SUBSET Blocks]
  /\ uwInCrit  \in [Docs -> BOOLEAN]

Init ==
  /\ pc         = [b \in Blocks |-> "start"]
  /\ attempt    = [b \in Blocks |-> 0]
  /\ readVer    = [b \in Blocks |-> 0]
  /\ seenMerged = [b \in Blocks |-> FALSE]
  /\ lockOwner  = [d \in Docs   |-> NoOwner]
  /\ docVer     = [d \in Docs   |-> 0]
  /\ userWrites = [d \in Docs   |-> 0]
  /\ applied    = [b \in Blocks |-> 0]
  /\ docState   = [d \in Docs   |-> {}]
  /\ marked     = [b \in Blocks |-> FALSE]
  /\ inCrit     = [d \in Docs   |-> {}]
  /\ uwInCrit   = [d \in Docs   |-> FALSE]

\* ---- Mutex semantics, parameterized by LockMode -------------------------------------
\* PerDoc: acquire blocks while another worker holds the doc's lock (queue.rs acquire()).
\* None:   no lock; any number of same-doc workers can enter the critical section.
CanAcquire(b) ==
  LET d == BlockDoc[b] IN
  CASE LockMode = "GlobalMerge" ->
         lockOwner[d] = NoOwner /\ \A other \in Docs : inCrit[other] = {}
    [] LockMode = "PerDoc" -> lockOwner[d] = NoOwner
    [] OTHER -> TRUE

\* ---- Adversary (pre-#1021, conflict-retry path): a LOCK-FREE local user-write ---------
\* Models "a user updates a document while a merge is in progress" (Go merge.go comment):
\* it bumps docVer, which makes any in-flight merge attempt (whose readVer is now stale)
\* conflict at commit. It does NOT take the merge lock, so it can fire even while a merge
\* holds the doc's critical section — the conflict-detection realization of user-vs-merge
\* safety. This is the pre-#1021 behavior (the local-write path bypassed the shared guard,
\* the bug behind the #1021 counter clobber). Reachable only when UserWriteMode="LockFree".
UserWrite(d) ==
  /\ UserWriteMode = "LockFree"
  /\ userWrites[d] < MaxUserWrites
  /\ docVer'     = [docVer     EXCEPT ![d] = @ + 1]
  /\ userWrites' = [userWrites EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<pc, attempt, readVer, seenMerged, lockOwner, applied, docState, marked,
                 inCrit, uwInCrit>>

\* ---- #1021 fix: a local user-write that ACQUIRES the shared per-doc guard -------------
\* update_impl/create_impl take the SAME per-doc DocWriteQueue guard the merge handler
\* takes (crates/db/src/write/queue.rs, shared by both paths). The write is performed
\* INSIDE the critical section and the guard is released afterwards, so a local write and a
\* same-doc merge are mutually excluded — never interleaved in the critical section. The
\* merge worker's CanAcquire already refuses while lockOwner = UserTok, and vice-versa.
\* Modeled as acquire -> (write) -> release so the serialization is observable in uwInCrit.
UserWriteAcquire(d) ==
  /\ UserWriteMode = "PerDoc"
  /\ userWrites[d] < MaxUserWrites
  /\ ~uwInCrit[d]
  /\ IF LockMode \in {"GlobalMerge", "PerDoc"} THEN lockOwner[d] = NoOwner ELSE TRUE
  /\ lockOwner' = [lockOwner EXCEPT
       ![d] = IF LockMode \in {"GlobalMerge", "PerDoc"} THEN UserTok ELSE @]
  /\ uwInCrit'  = [uwInCrit  EXCEPT ![d] = TRUE]
  /\ UNCHANGED <<pc, attempt, readVer, seenMerged, docVer, userWrites,
                 applied, docState, marked, inCrit>>

UserWriteRelease(d) ==
  /\ UserWriteMode = "PerDoc"
  /\ uwInCrit[d]
  /\ docVer'     = [docVer     EXCEPT ![d] = @ + 1]
  /\ userWrites' = [userWrites EXCEPT ![d] = @ + 1]
  /\ lockOwner'  = [lockOwner  EXCEPT
       ![d] = IF LockMode \in {"GlobalMerge", "PerDoc"} THEN NoOwner ELSE @]
  /\ uwInCrit'   = [uwInCrit   EXCEPT ![d] = FALSE]
  /\ UNCHANGED <<pc, attempt, readVer, seenMerged, applied, docState, marked, inCrit>>

\* ---- Worker: acquire the per-doc lock, enter the critical section, snapshot the txn ---
\* At txn start the worker snapshots BOTH the doc version (for conflict detection) and the
\* is_merged(cid) membership (the idempotency guard). The is_merged read is taken at the
\* START of the attempt, NOT at commit -- this is the real race window: two concurrent
\* same-doc txns can each snapshot "not merged" before either commits its merged-marker.
Acquire(b) ==
  LET d == BlockDoc[b] IN
  /\ pc[b] = "start"
  /\ CanAcquire(b)
  /\ pc'         = [pc         EXCEPT ![b] = "crit"]
  /\ lockOwner'  = [lockOwner  EXCEPT
       ![d] = IF LockMode \in {"GlobalMerge", "PerDoc"} THEN b ELSE @]
  /\ inCrit'     = [inCrit     EXCEPT ![d] = @ \cup {b}]
  /\ readVer'    = [readVer    EXCEPT ![b] = docVer[d]]
  /\ seenMerged' = [seenMerged EXCEPT ![b] = (Orig(b) \in docState[d])]
  /\ attempt'    = [attempt    EXCEPT ![b] = 1]
  /\ UNCHANGED <<docVer, userWrites, applied, docState, marked, uwInCrit>>

\* A merge attempt commits iff the doc version it snapshotted is still current
\* (no intervening user-write) -- i.e. no txn conflict. Mirrors executeMerge's txn:
\* read heads at snapshot, commit; storage SSI aborts with ErrTxnConflict on a stale read.
\* NOTE (modeling boundary): docVer is bumped only by user-writes, not by a concurrent
\* same-doc merge. The system deliberately does not rely on SSI to detect merge-vs-merge
\* conflicts on the merged-set marker -- the per-doc mutex is what serializes them. So in
\* the "None" (no-mutex) variant nothing re-orders or aborts the racing merges; the
\* is_merged snapshot is the only idempotency guard, and it is stale.
NoConflict(b) == docVer[BlockDoc[b]] = readVer[b]

\* Already-applied guard, evaluated against the SNAPSHOT taken at attempt start
\* (composite.rs / counter.rs is_merged(cid) -> terminal_skip("already merged")).
AlreadyApplied(b) == seenMerged[b]

\* COMMIT: attempt succeeds (no conflict). Either the block was already applied
\* (terminal skip, idempotent) or it applies now. Release lock, mark done.
Commit(b) ==
  LET d == BlockDoc[b] IN
  /\ pc[b] = "crit"
  /\ NoConflict(b)
  /\ pc'        = [pc     EXCEPT ![b] = "done"]
  /\ marked'    = [marked EXCEPT ![b] = TRUE]
  /\ lockOwner' = [lockOwner EXCEPT
       ![d] = IF LockMode \in {"GlobalMerge", "PerDoc"} THEN NoOwner ELSE @]
  /\ inCrit'    = [inCrit EXCEPT ![d] = @ \ {b}]
  /\ IF AlreadyApplied(b)
       THEN \* terminal skip "already merged": no second application
         UNCHANGED <<applied, docState>>
       ELSE \* first application: write the delta into doc state, bump the merged-set
         /\ applied'  = [applied  EXCEPT ![Orig(b)] = @ + 1]
         /\ docState' = [docState EXCEPT ![d] = @ \cup {Orig(b)}]
  /\ UNCHANGED <<attempt, readVer, seenMerged, docVer, userWrites, uwInCrit>>

\* RETRY: attempt hit a txn conflict (stale snapshot) and budget remains. Re-snapshot
\* docVer and loop. The lock is HELD across retries (the retry loop is inside the guard
\* in merge_blocks_individually), so a same-doc worker still cannot interleave.
Retry(b) ==
  LET d == BlockDoc[b] IN
  /\ pc[b] = "crit"
  /\ ~NoConflict(b)
  /\ attempt[b] < MaxRetries
  /\ attempt'    = [attempt    EXCEPT ![b] = @ + 1]
  /\ readVer'    = [readVer    EXCEPT ![b] = docVer[d]]
  /\ seenMerged' = [seenMerged EXCEPT ![b] = (Orig(b) \in docState[d])]
  /\ UNCHANGED <<pc, lockOwner, docVer, userWrites, applied, docState, marked, inCrit, uwInCrit>>

\* EXHAUST: conflict persists and the retry budget is spent. This is the fail-open vs
\* fail-closed fork:
\*   Closed (Rust): final_result is Err(txn_conflict) -> process_merge_batch pushes
\*                  ReplicationResult::Failed, CID NOT added to merged_cids -> NOT marked.
\*   Open   (Go):   Merge() falls through the loop to `return nil` -> caller treats the
\*                  block as merged -> marked done though never applied (silent drop).
\* Either way the lock is released and the worker terminates; nothing is applied.
Exhaust(b) ==
  LET d == BlockDoc[b] IN
  /\ pc[b] = "crit"
  /\ ~NoConflict(b)
  /\ attempt[b] = MaxRetries
  /\ pc'        = [pc EXCEPT ![b] = "done"]
  /\ lockOwner' = [lockOwner EXCEPT
       ![d] = IF LockMode \in {"GlobalMerge", "PerDoc"} THEN NoOwner ELSE @]
  /\ inCrit'    = [inCrit EXCEPT ![d] = @ \ {b}]
  /\ marked'    = [marked EXCEPT ![b] = (FailMode = "Open")]
  /\ UNCHANGED <<attempt, readVer, seenMerged, docVer, userWrites, applied, docState, uwInCrit>>

Next ==
  \/ \E b \in Blocks : Acquire(b)
  \/ \E b \in Blocks : Commit(b)
  \/ \E b \in Blocks : Retry(b)
  \/ \E b \in Blocks : Exhaust(b)
  \/ \E d \in Docs   : UserWrite(d)
  \/ \E d \in Docs   : UserWriteAcquire(d)
  \/ \E d \in Docs   : UserWriteRelease(d)

\* Stutter once every merge worker has terminated AND no user-write is mid-critical-section,
\* so TLC does not report deadlock on a finished schedule.
Done == (\A b \in Blocks : pc[b] = "done") /\ (\A d \in Docs : ~uwInCrit[d])
Terminating == Done /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Terminating]_vars

\* =====================================================================================
\* INVARIANTS (safety)
\* =====================================================================================
INV_TypeOK == TypeOK

\* ---- Serialization: at most one occupant per doc inside its critical section --------
\* The occupants of a doc's critical section are the merge workers in inCrit[d] PLUS a
\* local user-write (uwInCrit[d]) when UserWriteMode="PerDoc". This is the direct property
\* of the shared per-doc guard (MergeQueue.acquire / DocWriteQueue). RED under
\* LockMode = "None".
DocOccupants(d) == Cardinality(inCrit[d]) + (IF uwInCrit[d] THEN 1 ELSE 0)

INV_SameDocSerialized ==
  \A d \in Docs : DocOccupants(d) <= 1

\* A batch is one writer even when it contains several roots. MergeQueue models
\* individual block workers, so this invariant rules out overlapping workers;
\* SyncOwnership models the multi-root claim as one owner explicitly.
INV_SingleMergeWriter ==
  Cardinality(UNION {inCrit[d] : d \in Docs}) <= 1

\* Anti-vacuity witness for the per-document lock model.  A configuration
\* using LockMode="PerDoc" must be able to reach two active documents, proving
\* same-document serialization is not merely an alias for a global lock.
TwoDocsActive ==
  \E d1, d2 \in Docs :
    /\ d1 # d2
    /\ inCrit[d1] # {}
    /\ inCrit[d2] # {}

NoCrossDocParallel == ~TwoDocsActive

\* ---- Shared-guard mutual exclusion: a local user-write and a merge are NEVER both in
\* the critical section on the SAME doc (#1021). This is the property the counter fix
\* actually relies on — that a local counter RMW and a same-doc merge RMW cannot
\* interleave inside the store. It is falsified by REMOVING the shared guard:
\* LockMode="None" with UserWriteMode="PerDoc" lets a local write and a same-doc merge
\* both enter the critical section (counterexample inCrit=[d1|->{"b1"}],
\* uwInCrit=[d1|->TRUE]; RED-anchored by MC_MergeQueue_Red_LocalMergeInterleave). It is
\* NOT falsified by UserWriteMode="LockFree": there UserWriteAcquire is disabled so
\* uwInCrit is never set TRUE and the invariant is VACUOUSLY true. The #1021 GREEN config
\* sets LockMode="PerDoc" with UserWriteMode="PerDoc" and this must hold.
INV_NoLocalMergeInterleave ==
  \A d \in Docs : ~(uwInCrit[d] /\ inCrit[d] # {})

\* ---- No double-apply: an original block's delta is committed at most once ------------
\* Independent oracle: the apply ledger, not the skip decision. A duplicate delivery
\* (Dup) of an already-merged block must hit the is_merged guard and skip. Without the
\* per-doc mutex two same-doc workers (e.g. the original and its duplicate) can both pass
\* the un-applied check and both commit. RED under LockMode = "None" with a duplicate.
INV_NoDoubleApply ==
  \A b \in Blocks : applied[b] <= 1

\* ---- No silent drop: a marked-done block is never lost --------------------------------
\* Ground truth: a block counts as DELIVERED (its delta reached doc state) iff its
\* de-duplicated original is in docState. The mechanism may legitimately mark a block done
\* in two cases: it was applied, or its original was already applied (idempotent terminal
\* skip). Marking done WITHOUT either is a silent drop. RED under FailMode = "Open".
Delivered(b) == Orig(b) \in docState[BlockDoc[b]]

INV_NoSilentDrop ==
  \A b \in Blocks : marked[b] => Delivered(b)

\* ---- Conservation: every terminated block is either delivered or still re-deliverable -
\* A done block that is neither delivered nor still un-marked (hence re-fetchable) is lost.
\* Combined with INV_NoSilentDrop this states the no-loss guarantee directly.
INV_NoLoss ==
  \A b \in Blocks : (pc[b] = "done") => (Delivered(b) \/ ~marked[b])
====
