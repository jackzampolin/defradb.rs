---- MODULE MC_MergeQueue_Red_NoMutex ----
EXTENDS MergeQueue
\* RED: drop the per-doc mutex (LockMode="None"). Same shape as the GREEN case -- b2 is a
\* duplicate delivery of b1 on doc d1. Without serialization, b1 and its duplicate b2 can
\* both enter the critical section on d1, both read docState without the other's write,
\* both pass the is_merged guard, and both commit -> b1's delta is applied TWICE.
\* Violates INV_NoDoubleApply (and INV_SameDocSerialized). Fail-closed is kept so the
\* counterexample is attributable purely to the missing mutex, not to the exhaustion fork.

mcBlocks  == {"b1", "b2", "b3"}
mcDocs    == {"d1", "d2"}

mcBlockDoc == [b \in mcBlocks |->
                CASE b = "b3" -> "d2" [] OTHER -> "d1" ]

mcDup == [b \in mcBlocks |->
            CASE b = "b2" -> "b1" [] OTHER -> b ]
====
