---- MODULE SsiRange ----
\* SSI range/scan carve-out SOUNDNESS for the Rust storage `ConflictTracker`.
\*
\* Abstracts crates/storage/src/backends/shared.rs:
\*   - `ReadSet::record_iter_options`        (shared.rs:195-207): records a range read,
\*     EXCEPT it drops the range entirely when `is_document_collection_scan_prefix` holds.
\*   - `is_document_collection_scan_prefix`  (shared.rs:214-223): the carve-out heuristic;
\*     true for `d/d/`, `/d/`, `d/del/`, `/del/` full-collection document scans.
\*   - `ConflictTracker::check_and_record`   (shared.rs:275-324): the SSI commit gate, which
\*     consults the (carved) read-set via `read_set.conflicts_key(committed_write)`.
\*
\* This slice does NOT re-prove the core SSI engine (that is the committed `Ssi` module). It
\* isolates ONE question: does suppressing range rw-conflicts for document-scan prefixes drop
\* only FALSE POSITIVES, or can a too-aggressive carve-out swallow a GENUINE range write-skew?
\*
\* INDEPENDENT ORACLE. The MVSG acyclicity invariant is computed from each txn's TRUE read
\* footprint (TrueReads: point keys UNION every key its range reads actually cover, regardless
\* of whether the carve-out recorded the range). The carve-out is the MECHANISM under test; it
\* never feeds the oracle. So GREEN cannot be vacuous: if the mechanism accepts a schedule that
\* is non-serializable under the txns' real reads, the oracle's MVSG has a cycle and TLC fails.
\*
\* CarveMode selects the heuristic under test:
\*   "Correct"        - real code: carve ONLY DocScan ranges (d/d, /d). IndexRange (d/i FK
\*                      range reads) is fully tracked -> genuine range write-skews are caught. [GREEN]
\*   "TooAggressive"  - bug: carve EVERY range read, including IndexRange. A genuine FK-index
\*                      range rw-conflict is suppressed; the write-skew commits.            [RED]
\*   "NoCarve"        - the maximally conservative engine: never carve. Always safe (more
\*                      aborts, never fewer). Probe / liveness baseline.                    [GREEN]
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
  Txns,        \* finite set of transaction ids
  Keys,        \* finite set of point/range-member keys (the shared keyspace)
  PointReads,  \* [Txns -> SUBSET Keys]  keys read by point get/has (record_key, shared.rs:191)
  RangeKeys,   \* [Txns -> SUBSET Keys]  TRUE keys a txn's range read covers (ground truth)
  RangeKind,   \* [Txns -> {"None","DocScan","IndexRange"}]  prefix class of the range read
  Writes,      \* [Txns -> SUBSET Keys]  keys written (pending.keys())
  CarveMode    \* "Correct" | "TooAggressive" | "NoCarve"

ASSUME Txns # {}
ASSUME PointReads \in [Txns -> SUBSET Keys]
ASSUME RangeKeys  \in [Txns -> SUBSET Keys]
ASSUME RangeKind  \in [Txns -> {"None","DocScan","IndexRange"}]
ASSUME Writes     \in [Txns -> SUBSET Keys]
ASSUME CarveMode  \in {"Correct","TooAggressive","NoCarve"}
\* A txn with no range read has empty RangeKeys, and vice-versa (well-formedness).
ASSUME \A t \in Txns : (RangeKind[t] = "None") <=> (RangeKeys[t] = {})

VARIABLES
  version,   \* global monotonic commit-version counter (AtomicU64, shared.rs:250)
  snap,      \* [Txns -> Nat]  read_version captured at Begin (current_version, shared.rs:267)
  status,    \* [Txns -> {"idle","active","committed","aborted"}]
  cver,      \* [Txns -> Nat]  commit version (0 = not committed)
  log        \* Seq of committed records (the committed Vec, shared.rs:253)

vars == <<version, snap, status, cver, log>>

\* A committed record stores the version, the write set, and the RECORDED read set
\* (point reads + range keys MINUS whatever the carve-out dropped). This is exactly what
\* `check_and_record` stores and later consults -- the mechanism's view, not ground truth.
Records == [t: Txns, ver: Nat, w: SUBSET Keys, rr: SUBSET Keys]

\* ---------------------------------------------------------------------------
\* The carve-out: is_document_collection_scan_prefix (shared.rs:214-223), lifted to the
\* range-kind abstraction. DocScan == the d/d, /d, d/del, /del prefixes that the real
\* predicate matches; IndexRange == FK index range reads (d/i) that it must NOT match.
\* ---------------------------------------------------------------------------
IsCarved(kind) ==
  CASE CarveMode = "NoCarve"       -> FALSE
    [] CarveMode = "Correct"       -> (kind = "DocScan")
    [] CarveMode = "TooAggressive" -> (kind \in {"DocScan","IndexRange"})

\* RecordedReads(t): what record_key + record_iter_options actually store for txn t.
\* Point reads are always stored; the range's keys are stored UNLESS the carve-out fires.
RecordedReads(t) ==
  PointReads[t] \cup (IF IsCarved(RangeKind[t]) THEN {} ELSE RangeKeys[t])

\* TrueReads(t): the ground-truth read footprint -- everything the txn really observed.
\* THE ORACLE USES THIS. The carve-out cannot touch it.
TrueReads(t) == PointReads[t] \cup RangeKeys[t]

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

\* ---- The tracker's conflict test (shared.rs:291-307), over the RECORDED read-sets. ----
\* Scan committed records newer than my snapshot; abort on any ww / rw_A / rw_B hit. The
\* range carve-out has already shrunk both my recorded reads (RecordedReads(t)) and each
\* committed record's stored reads (rec.rr), so a carved range simply never participates.
WWHit(rec, t)  == (rec.w  \cap Writes[t])          # {}   \* committed_writes.contains(write_key)
RWAHit(rec, t) == (rec.rr \cap Writes[t])          # {}   \* committed_reads.conflicts_key(write_key)
RWBHit(rec, t) == (rec.w  \cap RecordedReads(t))   # {}   \* read_set.conflicts_key(committed_write)

RecConflicts(rec, t) ==
  /\ rec.ver > snap[t]            \* `if *commit_ver > read_version` (shared.rs:292)
  /\ (WWHit(rec, t) \/ RWAHit(rec, t) \/ RWBHit(rec, t))

Conflicts(t) ==
  \E i \in 1..Len(log) : RecConflicts(log[i], t)

\* ---- Actions ----
Begin(t) ==
  /\ status[t] = "idle"
  /\ status' = [status EXCEPT ![t] = "active"]
  /\ snap'   = [snap   EXCEPT ![t] = version]
  /\ UNCHANGED <<version, cver, log>>

\* check_and_record: empty write set always accepted (shared.rs:284).
Commit(t) ==
  /\ status[t] = "active"
  /\ IF ReadOnly(t) \/ ~Conflicts(t)
       THEN
         /\ version' = version + 1
         /\ cver'    = [cver   EXCEPT ![t] = version + 1]
         /\ status'  = [status EXCEPT ![t] = "committed"]
         /\ log'     = Append(log,
                         [t |-> t, ver |-> version + 1,
                          w |-> Writes[t], rr |-> RecordedReads(t)])
       ELSE
         /\ status' = [status EXCEPT ![t] = "aborted"]
         /\ UNCHANGED <<version, cver, log>>
  /\ UNCHANGED snap

Next ==
  \/ \E t \in Txns : Begin(t)
  \/ \E t \in Txns : Commit(t)

Done == \A t \in Txns : status[t] \in {"committed","aborted"}
Terminating == Done /\ UNCHANGED vars
Spec == Init /\ [][Next \/ Terminating]_vars

\* =====================================================================
\* The oracle: MVSG over committed txns, built from TRUE reads (range + point).
\* Identical edge definitions to the Ssi slice, but every Reads[x] is replaced by
\* TrueReads(x) -- so a carved-away range still contributes its anti-dependency edges.
\* =====================================================================
Committed == {t \in Txns : status[t] = "committed"}

WWEdge(a, b) ==
  /\ (Writes[a] \cap Writes[b]) # {}
  /\ cver[a] < cver[b]

WREdge(a, b) ==
  \E k \in TrueReads(b) :
    /\ k \in Writes[a]
    /\ cver[a] <= snap[b]
    /\ \A c \in Committed :
         (k \in Writes[c] /\ cver[c] <= snap[b]) => cver[c] <= cver[a]

\* Anti-dependency: a read key k (possibly via a RANGE read) that b later overwrote, where
\* b's write was not visible to a's snapshot. This is the write-skew edge the carve-out can
\* wrongly suppress in the mechanism -- but never here, because TrueReads ignores the carve.
RWEdge(a, b) ==
  \E k \in (TrueReads(a) \cap Writes[b]) :
    cver[b] > snap[a]

MVSGEdge(a, b) ==
  /\ a \in Committed /\ b \in Committed /\ a # b
  /\ (WWEdge(a, b) \/ WREdge(a, b) \/ RWEdge(a, b))

RECURSIVE Reach(_, _, _)
Reach(a, b, visited) ==
  \/ MVSGEdge(a, b)
  \/ \E m \in (Committed \ visited) :
        /\ MVSGEdge(a, m)
        /\ Reach(m, b, visited \cup {m})

HasCycle == \E a \in Committed : Reach(a, a, {a})

\* ---- Invariants ----
INV_TypeOK == TypeOK

\* Headline: every accepted schedule is serializable under the TRUE read footprint.
INV_Serializable == ~HasCycle

\* Sanity: distinct, monotone commit versions, all <= the global counter.
INV_MonotoneCommit ==
  /\ \A t \in Committed : cver[t] <= version /\ cver[t] > 0
  /\ \A a, b \in Committed : (a # b) => cver[a] # cver[b]

\* Soundness witness (the "drops only false positives" half, stated affirmatively).
\* A carve-out is UNSOUND iff there exist two committed txns where the mechanism's recorded
\* reads miss a real rw-conflict that the true reads contain AND that closes an MVSG cycle.
\* INV_Serializable already detects exactly this; this auxiliary makes the dropped-edge
\* explicit for diagnostics: a carved range key that is overwritten by a later committer.
DroppedRangeRWConflict ==
  \E a, b \in Committed :
    /\ a # b
    /\ IsCarved(RangeKind[a])
    /\ \E k \in (RangeKeys[a] \cap Writes[b]) :
         /\ cver[b] > snap[a]
         /\ k \notin RecordedReads(a)   \* the carve-out actually hid this read from the gate
====
