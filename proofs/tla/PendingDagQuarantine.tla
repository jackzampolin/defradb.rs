---- MODULE PendingDagQuarantine ----
\* Terminal-failure disposition for pending-DAG roots (#1128), companion to
\* PendingDagRestart.tla (does a registration SURVIVE a hub restart?) and
\* SyncOwnership.tla (who OWNS a registration's completion). Those models ask
\* whether a registration survives; this one asks what happens when its
\* merge is retried and DETERMINISTICALLY REJECTED on the block's own
\* content (e.g. a unique-index violation) rather than failing transiently.
\* Abstracts crates/db-merge/src/merge_handler/composite.rs
\* (UniqueConstraintViolation -> MergeOutcome::Rejected, composite.rs:261-284;
\* MergeOutcome::Rejected itself at crates/p2p/src/sync/merge.rs:89-97),
\* crates/p2p/src/sync/replication/handlers.rs (Ok(MergeOutcome::Rejected)
\* -> quarantine_pending_dag, handlers.rs:353-362) and
\* crates/p2p/src/sync/manager/process/pending_dag.rs
\* (quarantine_pending_dag's write-quarantine-record-BEFORE-delete-live-record
\* ordering, pending_dag.rs:838-880, and the resync sweep's is_quarantined
\* skip that keeps a quarantined root out of re-registration,
\* pending_dag.rs:712-723).
\*
\* THE MECHANISM: a registered pending-DAG root is re-driven by a sweep
\* (retry clock, resync, peer reconnect - the trigger is irrelevant here).
\* The re-drive's merge attempt has three possible content-determined
\* outcomes: MERGED (success), a transient failure that leaves the root
\* registered for another sweep, or a deterministic REJECTION. Poison docs
\* (model: a doc whose content will never merge, e.g. a unique-index
\* collision with data already committed) are rejected on every attempt.
\* Sound docs merge, possibly after one transient hiccup first (network
\* blip, lock contention - anything NOT a function of the block's content).
\*
\* One knob, three settings - each isolates one way to get the disposition
\* wrong:
\*   QuarantineMode = "Quarantine"          - Rejected -> durable quarantine
\*                     record, root never re-driven again; a transient
\*                     failure leaves the root registered (retried). [GREEN]
\*                   = "RetryForever"        - today's bug: Rejected is
\*                     treated as a retryable skip, so the doomed root is
\*                     swept forever. [RED 1]
\*                   = "QuarantineTransient" - the forbidden overcorrection:
\*                     a SOUND doc's transient failure ALSO quarantines it,
\*                     so a doc that would have merged on retry never gets
\*                     the chance. [RED 2]
\*
\* Abstractions: as in PendingDagRestart, the sweep trigger (retry clock
\* tick, resync sweep, peer reconnect) is irrelevant - only the content-
\* determined outcome matters. "Registered" collapses PendingDagRestart's
\* in-memory/durable distinction (that durability question is already
\* fenced there); here every registered root is assumed durable and this
\* model owns only the terminal-vs-retryable disposition. One optional
\* transient failure per sound doc, before it succeeds, is enough to give
\* QuarantineTransient something to bite without needing an unbounded retry
\* ladder. attempts is a saturating counter (capped at MaxAttempts) rather
\* than an unbounded one: RetryForever's bug is "sweeps forever, never
\* disposes" - saturating keeps the state space finite while the
\* never-disposes behavior still shows up as an infinite sweep loop under
\* fairness (LIVE_PoisonQuiesces never closes).
EXTENDS Naturals, FiniteSets

CONSTANTS
  SoundDocs,     \* docs that merge (possibly after one transient hiccup)
  PoisonDocs,    \* docs whose content is deterministically rejected, always
  MaxAttempts,   \* saturating cap on the attempts counter (state-space bound)
  QuarantineMode \* "Quarantine" | "RetryForever" | "QuarantineTransient"

Docs == SoundDocs \cup PoisonDocs

ASSUME SoundDocs # {} /\ PoisonDocs # {} /\ SoundDocs \cap PoisonDocs = {}
ASSUME MaxAttempts \in Nat /\ MaxAttempts >= 1
ASSUME QuarantineMode \in {"Quarantine", "RetryForever", "QuarantineTransient"}

Status == {"registered", "quarantined", "merged"}

VARIABLES
  status,       \* [Docs -> Status]
  attempts,     \* [Docs -> 0..MaxAttempts] - saturating sweep counter
  transientDone \* [SoundDocs -> BOOLEAN] - has this doc already used its one
                \* optional transient failure?

vars == <<status, attempts, transientDone>>

TypeOK ==
  /\ status \in [Docs -> Status]
  /\ attempts \in [Docs -> 0..MaxAttempts]
  /\ transientDone \in [SoundDocs -> BOOLEAN]

Init ==
  /\ status = [d \in Docs |-> "registered"]
  /\ attempts = [d \in Docs |-> 0]
  /\ transientDone = [s \in SoundDocs |-> FALSE]

Bumped(d) == IF attempts[d] < MaxAttempts THEN attempts[d] + 1 ELSE attempts[d]

\* A poison doc's sweep is always a rejection (composite.rs:261-284: the
\* unique-index check is a function of already-committed data, not of retry
\* timing - every replay reaches the same verdict). Quarantine and
\* QuarantineTransient both dispose it correctly (handlers.rs:353-362 ->
\* pending_dag.rs:838-880); RetryForever leaves it registered forever -
\* the #1128 bug this model exists to catch.
SweepPoisonReject(p) ==
  /\ p \in PoisonDocs
  /\ status[p] = "registered"
  /\ attempts' = [attempts EXCEPT ![p] = Bumped(p)]
  /\ status' = [status EXCEPT ![p] =
       IF QuarantineMode = "RetryForever" THEN "registered" ELSE "quarantined"]
  /\ UNCHANGED transientDone

\* A sound doc's one optional transient failure (network blip, lock
\* contention - NOT a function of content, unlike SweepPoisonReject).
\* QuarantineTransient's mistake: it cannot tell this apart from a real
\* content rejection and quarantines anyway, stranding a doc that would
\* have merged. Quarantine and RetryForever both correctly leave it
\* registered for the next sweep, which - transientDone now TRUE - must
\* succeed (SweepSoundTransientFail can never fire twice for the same doc).
SweepSoundTransientFail(s) ==
  /\ s \in SoundDocs
  /\ status[s] = "registered"
  /\ ~transientDone[s]
  /\ attempts' = [attempts EXCEPT ![s] = Bumped(s)]
  /\ transientDone' = [transientDone EXCEPT ![s] = TRUE]
  /\ status' = [status EXCEPT ![s] =
       IF QuarantineMode = "QuarantineTransient" THEN "quarantined" ELSE "registered"]

\* A sound doc's sweep succeeds - enabled every sweep, not gated on the one
\* optional transient failure having already happened (a doc may merge on
\* its very first attempt).
SweepSoundSucceed(s) ==
  /\ s \in SoundDocs
  /\ status[s] = "registered"
  /\ attempts' = [attempts EXCEPT ![s] = Bumped(s)]
  /\ status' = [status EXCEPT ![s] = "merged"]
  /\ UNCHANGED transientDone

\* Every doc disposed (merged or quarantined): stutter so TLC does not flag
\* deadlock on a finished schedule. Under RetryForever a poison doc never
\* reaches this state - SweepPoisonReject stays enabled forever instead,
\* which is exactly the bug LIVE_PoisonQuiesces is built to catch.
Done == \A d \in Docs : status[d] \in {"quarantined", "merged"}
Terminating == Done /\ UNCHANGED vars

Next ==
  \/ \E p \in PoisonDocs : SweepPoisonReject(p)
  \/ \E s \in SoundDocs : SweepSoundTransientFail(s) \/ SweepSoundSucceed(s)

Spec == Init /\ [][Next \/ Terminating]_vars

\* Weak fairness on the DISPOSITIVE action per doc only - a sound doc's
\* optional transient failure is never required to fire (it is optional by
\* construction: a doc may go straight to merged). This is the minimal
\* fairness under which GREEN's two liveness properties are real: without
\* WF(SweepPoisonReject) a fair trace could simply never sweep a poison doc;
\* without WF(SweepSoundSucceed) a fair trace could dwell forever between a
\* transient failure and the guaranteed-success sweep that follows it
\* (verified: the no-fairness probe on the GREEN config fails both
\* liveness properties - see the DESIGN doc's anti-vacuity note).
FairSpec ==
  Spec
  /\ \A p \in PoisonDocs : WF_vars(SweepPoisonReject(p))
  /\ \A s \in SoundDocs : WF_vars(SweepSoundSucceed(s))

INV_TypeOK == TypeOK

\* THE #1128 LEDGER INVARIANT: every doc is registered, quarantined, or
\* merged - never a fourth, untracked state. Holds by construction in every
\* config (status \in [Docs -> Status] already enforces it); stated because
\* it is the property the quarantine record exists to preserve at the
\* granularity that matters - a deterministically-failing merge stays
\* accounted for. Neither RED config loses a doc from the ledger; they get
\* the DISPOSITION wrong (a liveness failure), not the accounting.
INV_NoSilentDrop == \A d \in Docs : status[d] \in {"registered", "quarantined", "merged"}

\* Liveness (GREEN, under FairSpec): every sound doc eventually merges and
\* stays merged. Violated by QuarantineTransient (RED 2): a doc quarantined
\* on its transient failure never merges - <>[] never closes.
LIVE_SoundEventuallyMerged == <>[](\A s \in SoundDocs : status[s] = "merged")

\* Liveness (GREEN, under FairSpec): every poison doc eventually quarantines
\* and stays quarantined - the #1128 fix's headline promise: a
\* deterministically failing merge is disposed of, not retried indefinitely.
\* Violated by RetryForever (RED 1): SweepPoisonReject stays enabled and
\* keeps firing (attempts saturated at MaxAttempts closes the state space)
\* without status ever reaching "quarantined" - the loop IS the bug.
LIVE_PoisonQuiesces == <>[](\A p \in PoisonDocs : status[p] = "quarantined")
====
