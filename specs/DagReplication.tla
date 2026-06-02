---- MODULE DagReplication ----
\* Parametric Merkle-DAG replication core. Generalizes the M1Convergence spike
\* with documents, an owner/DID subscription filter, key mutability, and a
\* pluggable fetch policy. Every parameter is a CONSTANT so scenario wrappers
\* (MC_S2/S3/S4) can EXTEND this module and override via their .cfg files.
EXTENDS Naturals, FiniteSets

CONSTANTS
  Nodes, DIDs, DidOf, Blocks, Doc, Parents, Heads, Creator,
  OwnerWrite, CreateOwner, RelRef, FilterScope,
  KeyMutability,   \* "Immutable" | "Mutable" — documentation knob for the wrappers;
                   \* the actual behavioral difference is realized by the OwnerWrite data.
  FetchPolicy

\* The set of documents present in the DAG (Doc : Blocks -> Docs).
Docs == { Doc[b] : b \in Blocks }

\* Terminates because Parents is acyclic (a DAG); base case is Parents[b] = {}.
RECURSIVE AncestorsOf(_)
AncestorsOf(b) == Parents[b] \cup UNION { AncestorsOf(p) : p \in Parents[b] }

\* OwnerView's CHOOSE assumes a unique LATEST owner-write exists, which holds iff a
\* doc's owner-write blocks form a chain under ancestry (no concurrent reassignment
\* fork). Enforce it on the constants so a bad cfg fails cleanly at startup rather
\* than producing a cryptic CHOOSE-from-empty error mid-run. Lift this if concurrent
\* key reassignment is ever modelled.
ASSUME \A d \in Docs :
         LET w == { b \in Blocks : Doc[b] = d /\ OwnerWrite[b] # "none" }
         IN  \A b1 \in w, b2 \in w :
               b1 = b2 \/ b1 \in AncestorsOf(b2) \/ b2 \in AncestorsOf(b1)

VARIABLES have, merged, wanted
vars == <<have, merged, wanted>>

\* A node's current local view of a doc's owner: the LATEST OwnerWrite among the
\* doc's blocks it has merged, else the doc's create-block owner. The latest write
\* is the one that is a DESCENDANT of every other write (reachable from it via
\* ancestry), i.e. the most recent reassignment.
OwnerView(n, d) ==
  LET writes == { b \in merged[n] : Doc[b] = d /\ OwnerWrite[b] # "none" }
  IN  IF writes = {} THEN CreateOwner[d]
      ELSE OwnerWrite[ CHOOSE b \in writes :
                         \A c \in writes : c \in AncestorsOf(b) \/ b = c ]

Subscribed(n, d) ==
  CASE FilterScope = "None"     -> TRUE
    [] FilterScope = "WholeDoc" -> OwnerView(n, d) = DidOf[n]
    [] FilterScope = "SubDoc"   -> OwnerView(n, d) = DidOf[n]  \* doc-level part; field-grain filter added in Task 6

TypeOK ==
  /\ have   \in [Nodes -> SUBSET Blocks]
  /\ merged \in [Nodes -> SUBSET Blocks]
  /\ wanted \in [Nodes -> SUBSET Blocks]

Init ==
  /\ have   = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ merged = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ wanted = [n \in Nodes |-> {}]

HasProvider(b) == \E m \in Nodes : b \in have[m]

\* SENDER-SIDE FILTER: a head is announced to n only if the SENDER's view of the
\* doc's owner matches n's DID. The bandwidth win; with a mutable key it is also
\* the split-ownership trap (a reassigned doc's new head stops reaching the old owner).
Announce(m, n, h) ==
  /\ h \in Heads
  /\ h \in merged[m]
  /\ h \notin merged[n] /\ h \notin wanted[n]
  /\ \/ FilterScope = "None"
     \/ OwnerView(m, Doc[h]) = DidOf[n]
  /\ wanted' = [wanted EXCEPT ![n] = @ \cup {h}]
  /\ UNCHANGED <<have, merged>>

FetchTarget(n, b) ==
  CASE FetchPolicy = "Naive"     -> b \in wanted[n]
    [] FetchPolicy = "FullWalkA" -> \E h \in wanted[n] : b = h \/ b \in AncestorsOf(h)

Fetch(n, b) ==
  /\ FetchTarget(n, b)
  /\ b \notin have[n]
  /\ HasProvider(b)
  /\ have' = [have EXCEPT ![n] = @ \cup {b}]
  /\ UNCHANGED <<merged, wanted>>

Merge(n, b) ==
  /\ b \in have[n]
  /\ b \notin merged[n]
  /\ Parents[b] \subseteq merged[n]
  /\ merged' = [merged EXCEPT ![n] = @ \cup {b}]
  /\ wanted' = [wanted EXCEPT ![n] = @ \ {b}]
  /\ UNCHANGED have

Next ==
  \/ \E m \in Nodes, n \in Nodes, h \in Heads  : Announce(m, n, h)
  \/ \E n \in Nodes, b \in Blocks : Fetch(n, b)
  \/ \E n \in Nodes, b \in Blocks : Merge(n, b)

Fairness ==
  /\ \A m \in Nodes, n \in Nodes, h \in Heads  : WF_vars(Announce(m, n, h))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(Fetch(n, b))
  /\ \A n \in Nodes, b \in Blocks : WF_vars(Merge(n, b))

Spec == Init /\ [][Next]_vars /\ Fairness

\* ---- Properties ----

\* Blocks a node is supposed to converge on: those whose doc it is subscribed to.
SubBlocks(n) == { b \in Blocks : Subscribed(n, Doc[b]) }

\* Causal closure: a node never merges a block before all its parents.
INV_DagComplete    == \A n \in Nodes : \A b \in merged[n] : Parents[b] \subseteq merged[n]

\* Liveness (declared as a PROPERTY in the MC cfgs): every node eventually merges
\* every block of every doc it is subscribed to, and stays converged.
INV_SubsetConverge == <>[](\A n \in Nodes : SubBlocks(n) \subseteq merged[n])

\* RelRef safety: a non-creator node only ever fetches/merges blocks of documents
\* it is subscribed to. Combined with INV_SubsetConverge in a RelRef scenario, this
\* shows a subscribed doc converges WITHOUT the filtered ref-target doc's blocks
\* ever reaching the subscriber. The Creator is exempt: it authored every block and
\* starts holding all of them.
INV_RelRefSafe ==
  \A n \in Nodes :
    n = Creator \/ \A b \in (have[n] \cup merged[n]) : Subscribed(n, Doc[b])

\* No split ownership: among nodes that are subscribed to a doc and hold one of its
\* blocks, at most one distinct DID is "actionable". A mutable-key reassignment that
\* strands the old owner shows up as two actionable owners.
ActiveNodes(d) == { m \in Nodes : Subscribed(m, d) /\ \E b \in merged[m] : Doc[b] = d }
ActionableOwners(d) == { DidOf[n] : n \in ActiveNodes(d) }
INV_NoSplitOwnership == \A d \in Docs : Cardinality(ActionableOwners(d)) <= 1
====
