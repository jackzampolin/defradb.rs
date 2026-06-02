---- MODULE Claim ----
\* Multi-instance AgentRequest claim model.
\*
\* A claim block is represented by the instance that authored it. Each instance's
\* local merged view is the set of claim blocks it has seen. The local CRDT result
\* is an LWW winner over that set, abstracted by ClaimRank (claimed_at plus a
\* deterministic tie-break). Execute is a historical side effect: once an instance
\* starts work, later convergence cannot remove it from executed.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Instances, DIDs, DidOf, RequestDID, ClaimRank, ReplicationPeers

Contenders == { i \in Instances : DidOf[i] = RequestDID }

ASSUME DidOf \in [Instances -> DIDs]
ASSUME RequestDID \in DIDs
ASSUME Contenders # {}
ASSUME ClaimRank \in [Instances -> Nat]
ASSUME \A i \in Instances : \A j \in Instances :
         i # j => ClaimRank[i] # ClaimRank[j]
ASSUME ReplicationPeers \in [Instances -> SUBSET Instances]

VARIABLES claims, seen, executed
vars == <<claims, seen, executed>>

TypeOK ==
  /\ claims \in [Contenders -> BOOLEAN]
  /\ seen \in [Contenders -> SUBSET Contenders]
  /\ executed \subseteq Contenders

Init ==
  /\ claims = [i \in Contenders |-> FALSE]
  /\ seen = [i \in Contenders |-> {}]
  /\ executed = {}

Winner(S) ==
  CHOOSE i \in S : \A j \in S : ClaimRank[j] <= ClaimRank[i]

LocalClaimer(i) ==
  IF seen[i] = {} THEN "none" ELSE Winner(seen[i])

LocalPending(i) == seen[i] = {}

\* CRDT-CAS: the update can be authored if this instance's local view still says
\* status=pending. The successful local mutation immediately makes the author
\* believe it is claimed.
Claim(i) ==
  /\ LocalPending(i)
  /\ ~claims[i]
  /\ claims' = [claims EXCEPT ![i] = TRUE]
  /\ seen' = [seen EXCEPT ![i] = @ \cup {i}]
  /\ UNCHANGED executed

\* Delivery of an authored claim block. ReplicationPeers is the only place where
\* unfiltered, DID-filtered, or split same-DID delivery differs.
Deliver(src, dst) ==
  /\ claims[src]
  /\ dst \in ReplicationPeers[src]
  /\ src \notin seen[dst]
  /\ seen' = [seen EXCEPT ![dst] = @ \cup {src}]
  /\ UNCHANGED <<claims, executed>>

\* begin_execution: enabled once the instance's local merged view says it is the
\* current claimer. This records "started work", not the eventual merged status.
Execute(i) ==
  /\ i \notin executed
  /\ LocalClaimer(i) = i
  /\ executed' = executed \cup {i}
  /\ UNCHANGED <<claims, seen>>

Next ==
  \/ \E i \in Contenders : Claim(i)
  \/ \E src \in Contenders, dst \in Contenders : Deliver(src, dst)
  \/ \E i \in Contenders : Execute(i)

Fairness ==
  \A src \in Contenders, dst \in Contenders : WF_vars(Deliver(src, dst))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---- Properties ----

SameDIDCommonReplication ==
  \A src \in Contenders, dst \in Contenders : dst \in ReplicationPeers[src]

AnyClaim == \E i \in Contenders : claims[i]

ConvergedSameDIDViews ==
  \A i \in Contenders, j \in Contenders : seen[i] = seen[j]

ClaimedViews == { i \in Contenders : LocalClaimer(i) # "none" }
WinnerValues == { LocalClaimer(i) : i \in ClaimedViews }

EventualClaimUniqueState ==
  /\ ConvergedSameDIDViews
  /\ (~AnyClaim \/ Cardinality(WinnerValues) = 1)

\* Temporal property: after claim blocks that can replicate have converged, every
\* same-DID instance agrees on exactly one LWW claimer if any claim exists.
INV_EventualClaimUnique == <>[]EventualClaimUniqueState

\* Safety property expected to fail under concurrent local CAS: two instances can
\* start work before either receives the other's claim block.
INV_ExecutionUnique == Cardinality(executed) <= 1

\* Meta-property for the neutral case: when filtering keeps the complete same-DID
\* contention set mutually replicating, the eventual claim verdict is preserved.
INV_FilterNeutral ==
  SameDIDCommonReplication => INV_EventualClaimUnique
====
