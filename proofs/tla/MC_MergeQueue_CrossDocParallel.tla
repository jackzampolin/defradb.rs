---- MODULE MC_MergeQueue_CrossDocParallel ----
EXTENDS MergeQueue
\* RED: the pre-stage-3 PerDoc-only receiver policy permits b1 on d1 and b3
\* on d2 inside independent merge critical sections at once. Those transactions
\* still mutate shared index/ownership keyspaces, so the overlap violates the
\* receiver's single P2P merge-writer boundary.

mcBlocks  == {"b1", "b3"}
mcDocs    == {"d1", "d2"}

mcBlockDoc == [b \in mcBlocks |->
                CASE b = "b3" -> "d2" [] OTHER -> "d1" ]
mcDup == [b \in mcBlocks |-> b]
====
