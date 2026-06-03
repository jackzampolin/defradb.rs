---- MODULE MC_Acp_StaleCache_Red ----
EXTENDS Acp
\* RED: a positive access-decision cache survives revocation propagation.

mcNodes == {"owner", "replica"}
mcTuples == {"doc1#reader@alice"}
mcInitialAuthority == mcTuples
mcInitiallyKnown == [n \in mcNodes |-> mcTuples]
mcInitialCache == [n \in mcNodes |-> IF n = "replica" THEN mcTuples ELSE {}]
mcGrantable == {}
mcRevocable == mcTuples
====
