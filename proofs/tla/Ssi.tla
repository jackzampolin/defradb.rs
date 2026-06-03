---- MODULE Ssi ----
\* SSI snapshot-isolation serializability for the Rust storage `ConflictTracker`.
\*
\* Abstracts crates/storage/src/backends/shared.rs `ConflictTracker::check_and_record`
\* (anchors in Ssi_DESIGN.md). Concurrently committing transactions, each opened at a
\* snapshot version, are accepted or aborted by the tracker. The headline invariant is an
\* INDEPENDENT oracle: the multiversion serialization graph (MVSG) over the accepted
\* commits must be acyclic (conflict-serializability). The mechanism cannot fake green by
\* "agreeing with itself" because the oracle is computed from the textbook MVSG, not from
\* the tracker's accept/abort decision.
\*
\* SSIMode selects the conflict test:
\*   "Full"         - real code: ww + rw_A (committed read hit my write)
\*                                   + rw_B (committed write hit my read)   [GREEN]
\*   "WWOnly"       - bug: only write-write conflicts -> plain SI, write-skew slips through [RED]
\*   "NoSnapFilter" - drop the `commit_ver > read_version` guard (probe)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  Txns,          \* finite set of transaction ids
  Keys,          \* finite set of keys
  Reads,         \* [Txns -> SUBSET Keys]   read set of each txn (anchor: read_set)
  Writes,        \* [Txns -> SUBSET Keys]   write set of each txn (anchor: pending.keys())
  SSIMode        \* "Full" | "WWOnly" | "NoSnapFilter"

ASSUME Txns # {}
ASSUME Reads  \in [Txns -> SUBSET Keys]
ASSUME Writes \in [Txns -> SUBSET Keys]
ASSUME SSIMode \in {"Full", "WWOnly", "NoSnapFilter"}

VARIABLES
  version,   \* global monotonic commit-version counter (AtomicU64)
  snap,      \* [Txns -> Nat]  read_version captured at Begin
  status,    \* [Txns -> {"idle","active","committed","aborted"}]
  cver,      \* [Txns -> Nat]  commit version (0 = not committed)
  log        \* Seq of records [t |-> , ver |-> , w |-> , r |-> ] (the committed Vec)

vars == <<version, snap, status, cver, log>>

Records == [t: Txns, ver: Nat, w: SUBSET Keys, r: SUBSET Keys]

TypeOK ==
  /\ version \in Nat
  /\ snap   \in [Txns -> Nat]
  /\ status \in [Txns -> {"idle","active","committed","aborted"}]
  /\ cver   \in [Txns -> Nat]
  /\ \A i \in 1..Len(log) : log[i] \in Records

Init ==
  /\ version = 0
  /\ snap   = [t \in Txns |-> 0]
  /\ status = [t \in Txns |-> "idle"]
  /\ cver   = [t \in Txns |-> 0]
  /\ log    = << >>

ReadOnly(t) == Writes[t] = {}

\* ---- The tracker's conflict test, parameterized by SSIMode ----
\* Mirrors check_and_record: scan committed records, restricted to those committed AFTER my
\* snapshot, and test the relevant intersections (shared.rs:291-307).
SnapVisible(rec, t) ==
  IF SSIMode = "NoSnapFilter" THEN TRUE ELSE rec.ver > snap[t]

WWHit(rec, t)  == (rec.w \cap Writes[t]) # {}              \* committed_writes.contains(write_key)
RWAHit(rec, t) == (rec.r \cap Writes[t]) # {}              \* committed_reads.conflicts_key(write_key)
RWBHit(rec, t) == (rec.w \cap Reads[t])  # {}              \* read_set.conflicts_key(committed_write)

RecConflicts(rec, t) ==
  /\ SnapVisible(rec, t)
  /\ \/ WWHit(rec, t)
     \/ (SSIMode # "WWOnly" /\ RWAHit(rec, t))
     \/ (SSIMode # "WWOnly" /\ RWBHit(rec, t))

Conflicts(t) ==
  \E i \in 1..Len(log) : RecConflicts(log[i], t)

\* ---- Actions ----
Begin(t) ==
  /\ status[t] = "idle"
  /\ status' = [status EXCEPT ![t] = "active"]
  /\ snap'   = [snap   EXCEPT ![t] = version]
  /\ UNCHANGED <<version, cver, log>>

\* check_and_record: read-only (empty write set) always accepted (shared.rs:284).
Commit(t) ==
  /\ status[t] = "active"
  /\ IF ReadOnly(t) \/ ~Conflicts(t)
       THEN \* accept: assign new version, append record, bump global version
         /\ version' = version + 1
         /\ cver'    = [cver   EXCEPT ![t] = version + 1]
         /\ status'  = [status EXCEPT ![t] = "committed"]
         /\ log'     = Append(log,
                         [t |-> t, ver |-> version + 1, w |-> Writes[t], r |-> Reads[t]])
       ELSE \* abort: TxnConflict
         /\ status' = [status EXCEPT ![t] = "aborted"]
         /\ UNCHANGED <<version, cver, log>>
  /\ UNCHANGED snap

Next ==
  \/ \E t \in Txns : Begin(t)
  \/ \E t \in Txns : Commit(t)

\* Stutter when every txn has reached a terminal state, so TLC does not flag deadlock
\* on completed schedules.
Done == \A t \in Txns : status[t] \in {"committed","aborted"}
Terminating == Done /\ UNCHANGED vars

Spec == Init /\ [][Next \/ Terminating]_vars

\* =====================================================================
\* The oracle: multiversion serialization graph (MVSG) over committed txns.
\* =====================================================================
Committed == {t \in Txns : status[t] = "committed"}

\* Last committer of key k at or before snapshot version v (the version a reader saw).
\* Returns a txn id or the sentinel "INIT" (the initial empty store).
WritersOfBefore(k, v) ==
  {t \in Committed : k \in Writes[t] /\ cver[t] <= v}

\* a -> b edges in the MVSG (a, b committed, a # b):
\*  ww : shared written key, commit order a before b.
\*  wr : b read key k from a's installed version (a is last committer <= snap[b]).
\*  rw : a read key k that b later overwrote w.r.t a's snapshot (anti-dependency / skew).
WWEdge(a, b) ==
  /\ (Writes[a] \cap Writes[b]) # {}
  /\ cver[a] < cver[b]

WREdge(a, b) ==
  \E k \in Reads[b] :
    /\ k \in Writes[a]
    /\ cver[a] <= snap[b]
    /\ \A c \in Committed :
         (k \in Writes[c] /\ cver[c] <= snap[b]) => cver[c] <= cver[a]

RWEdge(a, b) ==
  \E k \in (Reads[a] \cap Writes[b]) :
    cver[b] > snap[a]    \* b's write was NOT visible to a's snapshot read

MVSGEdge(a, b) ==
  /\ a \in Committed /\ b \in Committed /\ a # b
  /\ (WWEdge(a, b) \/ WREdge(a, b) \/ RWEdge(a, b))

\* Transitive closure over the committed set, then acyclicity = no self-reach.
RECURSIVE Reach(_, _, _)
Reach(a, b, visited) ==
  \/ MVSGEdge(a, b)
  \/ \E m \in (Committed \ visited) :
        /\ MVSGEdge(a, m)
        /\ Reach(m, b, visited \cup {m})

HasCycle == \E a \in Committed : Reach(a, a, {a})

\* ---- Invariants ----
INV_TypeOK == TypeOK

\* Headline: every accepted (committed) schedule is conflict-serializable.
INV_Serializable == ~HasCycle

\* Sanity: distinct, monotone commit versions, all <= the global counter.
INV_MonotoneCommit ==
  /\ \A t \in Committed : cver[t] <= version /\ cver[t] > 0
  /\ \A a, b \in Committed : (a # b) => cver[a] # cver[b]
====
