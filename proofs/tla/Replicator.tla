---- MODULE Replicator ----
\* Directional replicator lifecycle over a single source -> target edge.
\*
\* This model sits above the DAG/block substrate: each document has one or
\* more head blocks, RequiredBlocks(d) is the transitive head history, and
\* target merge is parent-guarded. The difference under test is whether a
\* reconnect recomputes the target gap and requeues missing documents.
EXTENDS FiniteSets

CONSTANTS
  Docs,
  Blocks,
  Heads,
  Parents,
  Doc,
  InitialDocs,
  LiveDocs,
  InitialReceived,
  InitialConnected,
  Mode             \* "Naive" | "Resumable"

PhaseNames == {"Disconnected", "Connecting", "Backfill", "Live", "Backoff"}

RECURSIVE AncestorsOf(_)
AncestorsOf(b) == Parents[b] \cup UNION { AncestorsOf(p) : p \in Parents[b] }

DocHeads(d) == { h \in Heads : Doc[h] = d }
RequiredBlocks(d) == UNION { {h} \cup AncestorsOf(h) : h \in DocHeads(d) }

ParentClosed(s) == \A b \in s : Parents[b] \subseteq s

ASSUME Docs # {}
ASSUME Blocks # {}
ASSUME Heads \subseteq Blocks
ASSUME Parents \in [Blocks -> SUBSET Blocks]
ASSUME Doc \in [Blocks -> Docs]
ASSUME InitialDocs \subseteq Docs
ASSUME LiveDocs \subseteq Docs
ASSUME InitialDocs \cap LiveDocs = {}
ASSUME InitialReceived \subseteq Blocks
ASSUME ParentClosed(InitialReceived)
ASSUME InitialConnected \in BOOLEAN
ASSUME Mode \in {"Naive", "Resumable"}
ASSUME \A d \in InitialDocs \cup LiveDocs : DocHeads(d) # {}
ASSUME \A b \in Blocks : \A p \in Parents[b] : Doc[p] = Doc[b]

VARIABLES
  phase,
  connected,
  knownDocs,
  liveCreated,
  received,
  merged,
  queue,
  inflight,
  backfillStarted

vars ==
  << phase, connected, knownDocs, liveCreated, received, merged,
     queue, inflight, backfillStarted >>

TargetCompleteFor(d) == RequiredBlocks(d) \subseteq merged
MissingDocs == { d \in knownDocs : ~TargetCompleteFor(d) }

TypeOK ==
  /\ phase \in PhaseNames
  /\ connected \in BOOLEAN
  /\ knownDocs \subseteq InitialDocs \cup LiveDocs
  /\ InitialDocs \subseteq knownDocs
  /\ liveCreated \subseteq LiveDocs
  /\ liveCreated \subseteq knownDocs
  /\ received \subseteq Blocks
  /\ merged \subseteq received
  /\ ParentClosed(merged)
  /\ queue \subseteq knownDocs
  /\ inflight \subseteq knownDocs
  /\ queue \cap inflight = {}
  /\ backfillStarted \in BOOLEAN

Init ==
  /\ phase = IF InitialConnected THEN "Connecting" ELSE "Disconnected"
  /\ connected = InitialConnected
  /\ knownDocs = InitialDocs
  /\ liveCreated = {}
  /\ received = InitialReceived
  /\ merged = InitialReceived
  /\ queue = {}
  /\ inflight = {}
  /\ backfillStarted = FALSE

Reconnect ==
  /\ ~connected
  /\ connected' = TRUE
  /\ phase' = "Connecting"
  /\ UNCHANGED << knownDocs, liveCreated, received, merged,
                  queue, inflight, backfillStarted >>

Disconnect ==
  /\ connected
  /\ connected' = FALSE
  /\ phase' = "Backoff"
  \* Transport failure stops in-flight ordered sends. In the resumable model,
  \* reconnect requeues MissingDocs; in the naive model, the dropped in-flight
  \* doc can be forgotten permanently.
  /\ inflight' = {}
  /\ UNCHANGED << knownDocs, liveCreated, received, merged,
                  queue, backfillStarted >>

BeginBackfill ==
  /\ connected
  /\ phase = "Connecting"
  /\ phase' = "Backfill"
  /\ queue' =
       queue \cup
         (IF Mode = "Resumable" \/ ~backfillStarted THEN MissingDocs ELSE {})
  /\ backfillStarted' = TRUE
  /\ UNCHANGED << connected, knownDocs, liveCreated, received, merged, inflight >>

EnterLive ==
  /\ connected
  /\ phase = "Backfill"
  /\ queue = {}
  /\ inflight = {}
  /\ phase' = "Live"
  /\ UNCHANGED << connected, knownDocs, liveCreated, received, merged,
                  queue, inflight, backfillStarted >>

CreateLiveDoc(d) ==
  /\ connected
  /\ phase = "Live"
  /\ d \in LiveDocs
  /\ d \notin knownDocs
  /\ knownDocs' = knownDocs \cup {d}
  /\ liveCreated' = liveCreated \cup {d}
  /\ queue' = queue \cup {d}
  /\ UNCHANGED << phase, connected, received, merged, inflight, backfillStarted >>

StartPush(d) ==
  /\ connected
  /\ phase \in {"Backfill", "Live"}
  /\ d \in queue
  /\ d \notin inflight
  /\ queue' = queue \ {d}
  /\ inflight' = inflight \cup {d}
  /\ UNCHANGED << phase, connected, knownDocs, liveCreated,
                  received, merged, backfillStarted >>

ReceiveBlock(d, b) ==
  /\ connected
  /\ d \in inflight
  /\ b \in RequiredBlocks(d)
  /\ b \notin received
  /\ received' = received \cup {b}
  /\ UNCHANGED << phase, connected, knownDocs, liveCreated,
                  merged, queue, inflight, backfillStarted >>

MergeBlock(b) ==
  /\ b \in received
  /\ b \notin merged
  /\ Parents[b] \subseteq merged
  /\ merged' = merged \cup {b}
  /\ UNCHANGED << phase, connected, knownDocs, liveCreated,
                  received, queue, inflight, backfillStarted >>

FinishDoc(d) ==
  /\ d \in inflight
  /\ TargetCompleteFor(d)
  /\ inflight' = inflight \ {d}
  /\ UNCHANGED << phase, connected, knownDocs, liveCreated,
                  received, merged, queue, backfillStarted >>

Next ==
  \/ Reconnect
  \/ Disconnect
  \/ BeginBackfill
  \/ EnterLive
  \/ \E d \in LiveDocs : CreateLiveDoc(d)
  \/ \E d \in Docs : StartPush(d)
  \/ \E d \in Docs, b \in Blocks : ReceiveBlock(d, b)
  \/ \E b \in Blocks : MergeBlock(b)
  \/ \E d \in Docs : FinishDoc(d)

Fairness ==
  /\ WF_vars(BeginBackfill)
  /\ WF_vars(EnterLive)
  /\ \A d \in LiveDocs : WF_vars(CreateLiveDoc(d))
  /\ \A d \in Docs : WF_vars(StartPush(d))
  /\ \A d \in Docs, b \in Blocks : WF_vars(ReceiveBlock(d, b))
  /\ \A b \in Blocks : WF_vars(MergeBlock(b))
  /\ \A d \in Docs : WF_vars(FinishDoc(d))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---- Properties ----

INV_TargetMergeClosed ==
  /\ merged \subseteq received
  /\ ParentClosed(merged)

BackfillDone == \A d \in InitialDocs : TargetCompleteFor(d)
LiveDone == \A d \in liveCreated : TargetCompleteFor(d)
NoKnownLoss == \A d \in knownDocs : TargetCompleteFor(d)

EventuallyConnected == <>[]connected

INV_BackfillComplete == EventuallyConnected => <>[]BackfillDone
INV_LiveDelivery == EventuallyConnected => <>[]LiveDone
INV_NoLoss == EventuallyConnected => <>[]NoKnownLoss
====
