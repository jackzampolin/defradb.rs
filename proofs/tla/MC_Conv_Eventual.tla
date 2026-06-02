---- MODULE MC_Conv_Eventual ----
EXTENDS Convergence

\* Two-node, three-block DAG: b1 and b2 are accepted concurrent heads over b0.
\* n1 starts with the full accepted history; n2 starts partitioned and empty.
mcNodes   == {"n1", "n2"}
mcBlocks  == {"b0", "b1", "b2"}
mcParents == [b \in mcBlocks |->
                CASE b = "b1" -> {"b0"}
                  [] b = "b2" -> {"b0"}
                  [] OTHER    -> {}]
mcHeads   == {"b1", "b2"}
mcInitialConnected == {}
====
