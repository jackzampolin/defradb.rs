---- MODULE MC_MergeQueue_Green ----
EXTENDS MergeQueue
\* GREEN: correct mechanism -- per-doc async mutex (LockMode="PerDoc") + fail-closed
\* exhaustion (FailMode="Closed"). Three workers over two docs, including a DUPLICATE
\* delivery (b2 is a re-delivery of b1 on doc d1) to exercise the is_merged idempotency
\* guard, plus an adversary that can issue concurrent user-writes to drive txn conflicts
\* and retries. Every safety invariant must hold over all interleavings.

mcBlocks  == {"b1", "b2", "b3"}
mcDocs    == {"d1", "d2"}

\* b1, b2 -> d1 (b2 duplicates b1);  b3 -> d2.
mcBlockDoc == [b \in mcBlocks |->
                CASE b = "b3" -> "d2" [] OTHER -> "d1" ]

\* b2 is a duplicate delivery of b1's block (same CID); others are their own originals.
mcDup == [b \in mcBlocks |->
            CASE b = "b2" -> "b1" [] OTHER -> b ]
====
