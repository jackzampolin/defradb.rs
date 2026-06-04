---- MODULE MC_Acp_Green ----
EXTENDS Acp
\* GREEN: a cached positive decision is present before revoke, but propagation
\* invalidates the object cache and removes the tuple from each local view.

mcNodes == {"owner", "replica"}
mcTuples == {"doc1#reader@alice"}
mcInitialAuthority == mcTuples
mcInitiallyKnown == [n \in mcNodes |-> mcTuples]
mcInitialCache == [n \in mcNodes |-> IF n = "replica" THEN mcTuples ELSE {}]
mcGrantable == {}
mcRevocable == mcTuples
====
