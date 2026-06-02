---- MODULE MC_Conv_NoHeadRediscovery ----
EXTENDS Convergence

\* Red scenario: transport can reconnect, but no fair DocSync/BranchableSync
\* head rediscovery occurs after the partition. n2 never learns the accepted
\* heads, so eventual connectivity alone is not enough.
mcNodes   == {"n1", "n2"}
mcBlocks  == {"b0", "b1", "b2"}
mcParents == [b \in mcBlocks |->
                CASE b = "b1" -> {"b0"}
                  [] b = "b2" -> {"b0"}
                  [] OTHER    -> {}]
mcHeads   == {"b1", "b2"}
mcInitialConnected == {}
====
