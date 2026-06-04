---- MODULE MergeQueue ----
\* Per-document merge-queue serialization + bounded conflict-retry, abstracting
\* crates/db-merge/src/merge_handler/queue.rs (MergeQueue) and
\* crates/db-merge/src/merge_handler/batch.rs (merge_blocks_individually retry loop).
\* Anchors are in MergeQueue_DESIGN.md.
\*
\* The property: the per-doc async mutex serializes same-document merges while letting
\* different documents run in parallel; the bounded (MaxRetries) txn-conflict retry loop
\* loses and duplicates no block; retry exhaustion fails CLOSED (the block is reported
\* failed and stays re-deliverable, never silently marked done).
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
\* Two knobs select correct mechanism vs. adversary variant:
\*   LockMode = "PerDoc" - real code: MergeQueue.acquire(doc) before the retry loop  [GREEN]
\*            = "None"    - bug: no per-doc mutex; same-doc merges run concurrently  [RED]
\*   FailMode = "Closed"  - real Rust: exhausted retries -> Err -> NOT marked done   [GREEN]
\*            = "Open"     - Go merge.go bug: exhausted retries -> return nil ->
\*                           caller treats block as done -> silent drop              [RED]
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  Blocks,      \* finite set of merge-block ids (re-deliveries share a doc; see Dup)
  Docs,        \* finite set of document ids
  BlockDoc,    \* [Blocks -> Docs]  which document each block targets
  Dup,         \* [Blocks -> Blocks] block b is a duplicate delivery of Dup[b] (or itself)
  MaxRetries,  \* retry budget (MAX_MERGE_RETRIES = 5; node.go MaxTxnRetries = 5)
  MaxUserWrites, \* bound on adversarial concurrent local user-writes per doc
  LockMode,    \* "PerDoc" | "None"
  FailMode     \* "Closed" | "Open"

ASSUME Blocks # {} /\ Docs # {}
ASSUME BlockDoc \in [Blocks -> Docs]
ASSUME Dup \in [Blocks -> Blocks]
\* A duplicate must target the same document as its original (same CID => same doc).
ASSUME \A b \in Blocks : BlockDoc[Dup[b]] = BlockDoc[b]
ASSUME MaxRetries \in Nat /\ MaxRetries >= 1
ASSUME MaxUserWrites \in Nat
ASSUME LockMode \in {"PerDoc", "None"}
ASSUME FailMode \in {"Closed", "Open"}

NoOwner == "none"

VARIABLES
  pc,          \* [Blocks -> phase]  per-worker control state
  attempt,     \* [Blocks -> Nat]    attempts consumed in the retry loop
  readVer,     \* [Blocks -> Nat]    docVer snapshotted at the START of the current attempt
  seenMerged,  \* [Blocks -> BOOLEAN] is_merged(cid) snapshotted at the START of the attempt
  lockOwner,   \* [Docs -> Blocks \cup {NoOwner}]  per-doc async mutex holder
  docVer,      \* [Docs -> Nat]      bumped by each local user-write (drives txn conflict)
  userWrites,  \* [Docs -> Nat]      adversarial user-writes spent (bounded by MaxUserWrites)
  applied,     \* [Blocks -> Nat]    ORACLE: times this block's effect hit doc state
  docState,    \* [Docs -> SUBSET Blocks]  the merged-set: original ids applied to each doc
  marked,      \* [Blocks -> BOOLEAN]  ORACLE: caller recorded the block as "done"
  inCrit       \* [Docs -> SUBSET Blocks]  workers currently inside the per-doc critical section

vars == <<pc, attempt, readVer, seenMerged, lockOwner, docVer, userWrites,
          applied, docState, marked, inCrit>>

Phases == {"start", "crit", "done"}

\* The canonical identity a block applies under (its de-duplicated original).
Orig(b) == Dup[b]

TypeOK ==
  /\ pc        \in [Blocks -> Phases]
  /\ attempt   \in [Blocks -> 0..MaxRetries]
  /\ readVer   \in [Blocks -> Nat]
  /\ seenMerged\in [Blocks -> BOOLEAN]
  /\ lockOwner \in [Docs -> Blocks \cup {NoOwner}]
  /\ docVer    \in [Docs -> Nat]
  /\ userWrites\in [Docs -> 0..MaxUserWrites]
  /\ applied   \in [Blocks -> Nat]
  /\ docState  \in [Docs -> SUBSET Blocks]
  /\ marked    \in [Blocks -> BOOLEAN]
  /\ inCrit    \in [Docs -> SUBSET Blocks]

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

\* ---- Mutex semantics, parameterized by LockMode -------------------------------------
\* PerDoc: acquire blocks while another worker holds the doc's lock (queue.rs acquire()).
\* None:   no lock; any number of same-doc workers can enter the critical section.
CanAcquire(b) ==
  LET d == BlockDoc[b] IN
  IF LockMode = "PerDoc" THEN lockOwner[d] = NoOwner ELSE TRUE

\* ---- Adversary: a local user-write to a doc, concurrent with merges -----------------
\* Models "a user updates a document while a merge is in progress" (Go merge.go comment):
\* it bumps docVer, which makes any in-flight merge attempt (whose readVer is now stale)
\* conflict at commit. Permitted regardless of the merge mutex (the mutex serializes
\* merge-vs-merge, not user-vs-merge).
UserWrite(d) ==
  /\ userWrites[d] < MaxUserWrites
  /\ docVer'     = [docVer     EXCEPT ![d] = @ + 1]
  /\ userWrites' = [userWrites EXCEPT ![d] = @ + 1]
  /\ UNCHANGED <<pc, attempt, readVer, seenMerged, lockOwner, applied, docState, marked, inCrit>>

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
  /\ lockOwner'  = [lockOwner  EXCEPT ![d] = IF LockMode = "PerDoc" THEN b ELSE @]
  /\ inCrit'     = [inCrit     EXCEPT ![d] = @ \cup {b}]
  /\ readVer'    = [readVer    EXCEPT ![b] = docVer[d]]
  /\ seenMerged' = [seenMerged EXCEPT ![b] = (Orig(b) \in docState[d])]
  /\ attempt'    = [attempt    EXCEPT ![b] = 1]
  /\ UNCHANGED <<docVer, userWrites, applied, docState, marked>>

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
  /\ lockOwner' = [lockOwner EXCEPT ![d] = IF LockMode = "PerDoc" THEN NoOwner ELSE @]
  /\ inCrit'    = [inCrit EXCEPT ![d] = @ \ {b}]
  /\ IF AlreadyApplied(b)
       THEN \* terminal skip "already merged": no second application
         UNCHANGED <<applied, docState>>
       ELSE \* first application: write the delta into doc state, bump the merged-set
         /\ applied'  = [applied  EXCEPT ![Orig(b)] = @ + 1]
         /\ docState' = [docState EXCEPT ![d] = @ \cup {Orig(b)}]
  /\ UNCHANGED <<attempt, readVer, seenMerged, docVer, userWrites>>

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
  /\ UNCHANGED <<pc, lockOwner, docVer, userWrites, applied, docState, marked, inCrit>>

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
  /\ lockOwner' = [lockOwner EXCEPT ![d] = IF LockMode = "PerDoc" THEN NoOwner ELSE @]
  /\ inCrit'    = [inCrit EXCEPT ![d] = @ \ {b}]
  /\ marked'    = [marked EXCEPT ![b] = (FailMode = "Open")]
  /\ UNCHANGED <<attempt, readVer, seenMerged, docVer, userWrites, applied, docState>>

Next ==
  \/ \E b \in Blocks : Acquire(b)
  \/ \E b \in Blocks : Commit(b)
  \/ \E b \in Blocks : Retry(b)
  \/ \E b \in Blocks : Exhaust(b)
  \/ \E d \in Docs   : UserWrite(d)

\* Stutter once every worker has terminated, so TLC does not report deadlock on a
\* finished schedule.
Done == \A b \in Blocks : pc[b] = "done"
Terminating == Done /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Terminating]_vars

\* =====================================================================================
\* INVARIANTS (safety)
\* =====================================================================================
INV_TypeOK == TypeOK

\* ---- Serialization: at most one worker per doc inside its critical section ----------
\* This is the direct property of MergeQueue.acquire. RED under LockMode = "None".
INV_SameDocSerialized ==
  \A d \in Docs : Cardinality(inCrit[d]) <= 1

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

\* =====================================================================================
\* VACUITY GUARD (used only as a NEGATED probe; see MC_MergeQueue_CrossDocParallel).
\* If two different-doc workers can be in their critical sections simultaneously, this
\* predicate is reachable; asserting it as an invariant forces TLC to exhibit the witness
\* as a counterexample, proving the lock does not serialize across documents.
\* =====================================================================================
TwoDocsActive ==
  \E d1, d2 \in Docs :
    /\ d1 # d2
    /\ inCrit[d1] # {}
    /\ inCrit[d2] # {}

NoCrossDocParallel == ~TwoDocsActive
====
