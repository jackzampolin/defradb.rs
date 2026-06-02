---- MODULE M1Convergence ----
\* M1 spike: minimal Merkle-DAG replication on the Go #2721 "dual-branch" DAG
\* (b0 = create; b1,b2 = concurrent children of b0). Gossip announces only heads;
\* a node walks full causal ancestry and merges under a parent-guard.
\* FetchPolicy is the knob: "FullWalkA" (this cfg, green) vs "Naive" (M1Naive.cfg,
\* reproduces #2721 by never merging). Checks INV_DagComplete + Converge.
EXTENDS FiniteSets

Blocks  == {"b0", "b1", "b2"}
Heads   == {"b1", "b2"}
Parents == [b \in Blocks |->
              CASE b = "b1" -> {"b0"}
                [] b = "b2" -> {"b0"}
                [] OTHER    -> {}]
Nodes   == {"n1", "n2"}
Creator == "n1"

CONSTANT FetchPolicy

\* Terminates because Parents is acyclic (a DAG); base case is Parents[b] = {}.
RECURSIVE AncestorsOf(_)
AncestorsOf(b) == Parents[b] \cup UNION { AncestorsOf(p) : p \in Parents[b] }

VARIABLES have, merged, wanted
vars == <<have, merged, wanted>>

TypeOK ==
  /\ have   \in [Nodes -> SUBSET Blocks]
  /\ merged \in [Nodes -> SUBSET Blocks]
  /\ wanted \in [Nodes -> SUBSET Blocks]

Init ==
  /\ have   = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ merged = [n \in Nodes |-> IF n = Creator THEN Blocks ELSE {}]
  /\ wanted = [n \in Nodes |-> {}]

Announce(m, n, h) ==
  /\ h \in Heads
  /\ h \in merged[m]
  /\ h \notin merged[n] /\ h \notin wanted[n]
  /\ wanted' = [wanted EXCEPT ![n] = @ \cup {h}]
  /\ UNCHANGED <<have, merged>>

HasProvider(b) == \E m \in Nodes : b \in have[m]

FetchA(n, b) ==
  /\ \E h \in wanted[n] : b = h \/ b \in AncestorsOf(h)
  /\ b \notin have[n]
  /\ HasProvider(b)
  /\ have' = [have EXCEPT ![n] = @ \cup {b}]
  /\ UNCHANGED <<merged, wanted>>

FetchNaive(n, b) ==
  /\ b \in wanted[n]
  /\ b \notin have[n]
  /\ HasProvider(b)
  /\ have' = [have EXCEPT ![n] = @ \cup {b}]
  /\ UNCHANGED <<merged, wanted>>

Fetch(n, b) == IF FetchPolicy = "Naive" THEN FetchNaive(n, b) ELSE FetchA(n, b)

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
INV_DagComplete == \A n \in Nodes : \A b \in merged[n] : Parents[b] \subseteq merged[n]
Converge        == <>[](\A n \in Nodes : merged[n] = Blocks)
====
