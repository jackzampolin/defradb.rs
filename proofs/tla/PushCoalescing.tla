---- MODULE PushCoalescing ----
\* Latest-head retirement for outbound PushLog work (#1102).
\*
\* One document or collection scope per peer is sufficient to expose the race: head 1 may become
\* active, head 2 arrives and retires every queued/persisted predecessor, then
\* head 1 fails.  The failure path must observe that it is stale and must not
\* recreate a persisted retry for head 1. `persisted` is a ghost witness for
\* the head that dirtied a scope; Rust stores only a presence marker.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Peers,
  MaxHead,
  QueueMode,  \* "LatestOnly" | "AppendEvery"
  RetryMode   \* "CurrentOnly" | "ReplayEvery"

ASSUME Peers # {}
ASSUME MaxHead \in Nat /\ MaxHead >= 2
ASSUME QueueMode \in {"LatestOnly", "AppendEvery"}
ASSUME RetryMode \in {"CurrentOnly", "ReplayEvery"}

VARIABLES nextHead, latest, queued, active, persisted, delivered, retired

vars == <<nextHead, latest, queued, active, persisted, delivered, retired>>
Heads == 1..MaxHead

TypeOK ==
  /\ nextHead \in 1..(MaxHead + 1)
  /\ latest \in [Peers -> 0..MaxHead]
  /\ queued \in [Peers -> SUBSET Heads]
  /\ active \in [Peers -> SUBSET Heads]
  /\ persisted \in [Peers -> SUBSET Heads]
  /\ delivered \in [Peers -> 0..MaxHead]
  /\ retired \in 0..(MaxHead * Cardinality(Peers) * 3)

Init ==
  /\ nextHead = 1
  /\ latest = [p \in Peers |-> 0]
  /\ queued = [p \in Peers |-> {}]
  /\ active = [p \in Peers |-> {}]
  /\ persisted = [p \in Peers |-> {}]
  /\ delivered = [p \in Peers |-> 0]
  /\ retired = 0

\* A newer local head is offered to one peer.  LatestOnly atomically retires
\* queued and persisted predecessors before installing the new obligation.
Enqueue(p) ==
  /\ nextHead <= MaxHead
  /\ LET h == nextHead
         old == Cardinality(queued[p]) + Cardinality(persisted[p])
     IN /\ latest' = [latest EXCEPT ![p] = h]
        /\ queued' = [queued EXCEPT ![p] =
             IF QueueMode = "LatestOnly" THEN {h} ELSE @ \cup {h}]
        /\ persisted' = [persisted EXCEPT ![p] =
             IF QueueMode = "LatestOnly" THEN {} ELSE @]
        /\ retired' = retired + IF QueueMode = "LatestOnly" THEN old ELSE 0
  /\ nextHead' = nextHead + 1
  /\ UNCHANGED <<active, delivered>>

Start(p, h) ==
  /\ h \in queued[p]
  /\ queued' = [queued EXCEPT ![p] = @ \ {h}]
  /\ active' = [active EXCEPT ![p] = @ \cup {h}]
  /\ UNCHANGED <<nextHead, latest, persisted, delivered, retired>>

\* A superseded active send may finish failing after a newer enqueue.  The
\* CurrentOnly path retires it; ReplayEvery recreates the stale persisted head.
Fail(p, h) ==
  /\ h \in active[p]
  /\ active' = [active EXCEPT ![p] = @ \ {h}]
  /\ persisted' = [persisted EXCEPT ![p] =
       IF RetryMode = "CurrentOnly" /\ h = latest[p] THEN {h} ELSE
       IF RetryMode = "ReplayEvery" THEN @ \cup {h} ELSE @]
  /\ retired' = retired + IF RetryMode = "CurrentOnly" /\ h < latest[p] THEN 1 ELSE 0
  /\ UNCHANGED <<nextHead, latest, queued, delivered>>

Retry(p, h) ==
  /\ h \in persisted[p]
  /\ persisted' = [persisted EXCEPT ![p] = @ \ {h}]
  /\ active' = [active EXCEPT ![p] = @ \cup {h}]
  /\ UNCHANGED <<nextHead, latest, queued, delivered, retired>>

Ack(p, h) ==
  /\ h \in active[p]
  /\ active' = [active EXCEPT ![p] = @ \ {h}]
  /\ delivered' = [delivered EXCEPT ![p] = IF @ >= h THEN @ ELSE h]
  /\ UNCHANGED <<nextHead, latest, queued, persisted, retired>>

Next ==
  \/ \E p \in Peers : Enqueue(p)
  \/ \E p \in Peers, h \in Heads :
       Start(p, h) \/ Fail(p, h) \/ Retry(p, h) \/ Ack(p, h)

Spec == Init /\ [][Next]_vars

INV_TypeOK == TypeOK

\* Safety: neither in-memory admission nor durable retry contains two heads
\* for one (scope, peer).
INV_OneLiveHead == \A p \in Peers : Cardinality(queued[p]) <= 1
INV_OnePersistedHead == \A p \in Peers : Cardinality(persisted[p]) <= 1

\* A persisted retry is always the newest head known for that peer.
INV_NoStaleRetry ==
  \A p \in Peers : persisted[p] \subseteq {latest[p]}

\* Liveness accounting: retirement never destroys the newest obligation.  It
\* remains queued, active, persisted, or has already been acknowledged.
INV_NewestRetained ==
  \A p \in Peers :
    latest[p] = 0 \/ latest[p] \in (queued[p] \cup active[p] \cup persisted[p])
                  \/ delivered[p] >= latest[p]
====
