---- MODULE Commits ----
\* ACP-on-commits safety model.
\*
\* A protected document's materialized value and its CRDT commit blocks both
\* carry the document content. Therefore read authorization must be enforced on
\* the regular User path, the _commits path, and P2P commit-block delivery.
EXTENDS FiniteSets

CONSTANTS
  Readers,
  Docs,
  Blocks,
  DocOfBlock,
  Grant,
  InitialBlockHolders,
  UserGateMode,        \* "ACP" | "Open"
  CommitsGateMode,     \* "ACP" | "Open"
  ReplicationGateMode  \* "ACP" | "Open"

GateModes == {"ACP", "Open"}

BlocksOf(d) == {b \in Blocks : DocOfBlock[b] = d}
Authorized(r, d) == r \in Grant[d]

GateAllows(mode, r, d) ==
  CASE mode = "ACP"  -> Authorized(r, d)
    [] mode = "Open" -> TRUE
    [] OTHER         -> FALSE

ASSUME Readers # {}
ASSUME Docs # {}
ASSUME Blocks # {}
ASSUME DocOfBlock \in [Blocks -> Docs]
ASSUME Grant \in [Docs -> SUBSET Readers]
ASSUME InitialBlockHolders \in [Readers -> SUBSET Blocks]
ASSUME UserGateMode \in GateModes
ASSUME CommitsGateMode \in GateModes
ASSUME ReplicationGateMode \in GateModes
ASSUME \A d \in Docs : BlocksOf(d) # {}
\* The model starts before an unauthorized peer has received protected blocks.
ASSUME \A r \in Readers, d \in Docs :
         ~Authorized(r, d) => InitialBlockHolders[r] \cap BlocksOf(d) = {}

VARIABLES
  userContent,
  commitBlocks,
  heldBlocks,
  receivedBlocks

vars == <<userContent, commitBlocks, heldBlocks, receivedBlocks>>

TypeOK ==
  /\ userContent \in [Readers -> SUBSET Docs]
  /\ commitBlocks \in [Readers -> SUBSET Blocks]
  /\ heldBlocks \in [Readers -> SUBSET Blocks]
  /\ receivedBlocks \in [Readers -> SUBSET Blocks]

Init ==
  /\ userContent = [r \in Readers |-> {}]
  /\ commitBlocks = [r \in Readers |-> {}]
  /\ heldBlocks = InitialBlockHolders
  /\ receivedBlocks = [r \in Readers |-> {}]

\* User path: the materialized document query.
UserRead(r, d) ==
  /\ d \notin userContent[r]
  /\ GateAllows(UserGateMode, r, d)
  /\ userContent' = [userContent EXCEPT ![r] = @ \cup {d}]
  /\ UNCHANGED <<commitBlocks, heldBlocks, receivedBlocks>>

\* Commits path: the _commits system collection returns raw CRDT delta blocks.
CommitsRead(r, d) ==
  /\ ~(BlocksOf(d) \subseteq commitBlocks[r])
  /\ GateAllows(CommitsGateMode, r, d)
  /\ commitBlocks' = [commitBlocks EXCEPT ![r] = @ \cup BlocksOf(d)]
  /\ UNCHANGED <<userContent, heldBlocks, receivedBlocks>>

\* Replication path: a peer receives a commit block from another peer.
ReplicateBlock(src, dst, b) ==
  /\ src # dst
  /\ b \in heldBlocks[src]
  /\ b \notin heldBlocks[dst]
  /\ GateAllows(ReplicationGateMode, dst, DocOfBlock[b])
  /\ heldBlocks' = [heldBlocks EXCEPT ![dst] = @ \cup {b}]
  /\ receivedBlocks' = [receivedBlocks EXCEPT ![dst] = @ \cup {b}]
  /\ UNCHANGED <<userContent, commitBlocks>>

Next ==
  \/ \E r \in Readers, d \in Docs : UserRead(r, d)
  \/ \E r \in Readers, d \in Docs : CommitsRead(r, d)
  \/ \E src \in Readers, dst \in Readers, b \in Blocks :
       ReplicateBlock(src, dst, b)

Spec == Init /\ [][Next]_vars

\* ---- Properties ----

ContentViaUser(r, d) == d \in userContent[r]
ContentViaLocalCommits(r, d) ==
  \E b \in commitBlocks[r] : DocOfBlock[b] = d
ContentViaReplication(r, d) ==
  \E b \in receivedBlocks[r] : DocOfBlock[b] = d

ObtainedContent(r, d) ==
  \/ ContentViaUser(r, d)
  \/ ContentViaLocalCommits(r, d)
  \/ ContentViaReplication(r, d)

\* Safety: if r is not in the ACP grant set for d, r obtains d's content via
\* neither the User path, nor the _commits path, nor replicated commit blocks.
INV_BothPathsGated ==
  \A r \in Readers, d \in Docs :
    ~Authorized(r, d) => ~ObtainedContent(r, d)
====
