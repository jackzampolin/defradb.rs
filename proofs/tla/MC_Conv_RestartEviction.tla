---- MODULE MC_Conv_RestartEviction ----
EXTENDS Convergence

\* Same DAG as MC_Conv_Eventual, but the cfg enables one arbitrary restart per
\* node and keeps MaxSynced at 1 so FIFO eviction is exercised.
mcNodes   == {"n1", "n2"}
mcBlocks  == {"b0", "b1", "b2"}
mcParents == [b \in mcBlocks |->
                CASE b = "b1" -> {"b0"}
                  [] b = "b2" -> {"b0"}
                  [] OTHER    -> {}]
mcHeads   == {"b1", "b2"}
mcInitialConnected == {}
====
