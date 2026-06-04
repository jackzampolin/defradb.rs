---- MODULE MC_MergeQueue_CrossDocParallel ----
EXTENDS MergeQueue
\* ANTI-VACUITY PROBE (expected RED = counterexample is the witness).
\* With the correct PerDoc mutex, we assert NoCrossDocParallel ( = no two different-doc
\* workers are simultaneously in their critical sections). TLC MUST find a counterexample:
\* b1 on d1 and b3 on d2 both inside the critical section at once. That counterexample is
\* the proof that the per-doc mutex permits CROSS-document parallelism -- it serializes
\* same-doc merges WITHOUT globally serializing everything. If this probe ever held
\* (no counterexample), the GREEN serialization result would be suspect (the lock would be
\* a global lock and same-doc serialization would be trivially true).

mcBlocks  == {"b1", "b3"}
mcDocs    == {"d1", "d2"}

mcBlockDoc == [b \in mcBlocks |->
                CASE b = "b3" -> "d2" [] OTHER -> "d1" ]
mcDup == [b \in mcBlocks |-> b]
====
