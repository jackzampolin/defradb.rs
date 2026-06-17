---- MODULE MC_MergeQueue_Green ----
EXTENDS MergeQueue
\* GREEN: correct mechanism -- per-doc async mutex (LockMode="PerDoc") + fail-closed
\* exhaustion (FailMode="Closed") + the #1021 shared guard on local writes
\* (UserWriteMode="PerDoc": a local user-write acquires the SAME per-doc guard the merge
\* takes). Three workers over two docs, including a DUPLICATE delivery (b2 is a re-delivery
\* of b1 on doc d1) to exercise the is_merged idempotency guard, plus local user-writes
\* that take the shared guard and perform their write inside the critical section. Every
\* safety invariant -- including INV_NoLocalMergeInterleave (no local-write-vs-merge
\* interleave on one doc, the property the counter fix relies on) -- must hold. That
\* invariant is falsified only by REMOVING the shared guard (LockMode="None" with
\* UserWriteMode="PerDoc"; RED-anchored by MC_MergeQueue_Red_LocalMergeInterleave), NOT by
\* lock-free user-writes (UserWriteMode="LockFree" never sets uwInCrit, so it is vacuous
\* there).

mcBlocks  == {"b1", "b2", "b3"}
mcDocs    == {"d1", "d2"}

\* b1, b2 -> d1 (b2 duplicates b1);  b3 -> d2.
mcBlockDoc == [b \in mcBlocks |->
                CASE b = "b3" -> "d2" [] OTHER -> "d1" ]

\* b2 is a duplicate delivery of b1's block (same CID); others are their own originals.
mcDup == [b \in mcBlocks |->
            CASE b = "b2" -> "b1" [] OTHER -> b ]
====
