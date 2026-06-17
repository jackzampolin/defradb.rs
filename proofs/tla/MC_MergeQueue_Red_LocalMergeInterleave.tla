---- MODULE MC_MergeQueue_Red_LocalMergeInterleave ----
EXTENDS MergeQueue
\* RED (non-vacuity anchor for INV_NoLocalMergeInterleave). Remove the SHARED guard
\* (LockMode="None") while keeping the #1021 shared-guard local-write path
\* (UserWriteMode="PerDoc"). With no real lock backing the guard, a local user-write
\* enters the critical section on d1 (uwInCrit[d1]=TRUE) while merge worker b1 is also in
\* it (inCrit[d1]={"b1"}) -> INV_NoLocalMergeInterleave VIOLATED
\* (counterexample inCrit=[d1|->{"b1"}], uwInCrit=[d1|->TRUE]). This is the property the
\* #1021 counter fix relies on: it fails precisely when the shared guard is removed, NOT
\* under UserWriteMode="LockFree" (where uwInCrit is never set TRUE, so the invariant is
\* VACUOUSLY true). Same b1/b2(dup)/b3 shape as the GREEN case.

mcBlocks  == {"b1", "b2", "b3"}
mcDocs    == {"d1", "d2"}

mcBlockDoc == [b \in mcBlocks |->
                CASE b = "b3" -> "d2" [] OTHER -> "d1" ]

mcDup == [b \in mcBlocks |->
            CASE b = "b2" -> "b1" [] OTHER -> b ]
====
