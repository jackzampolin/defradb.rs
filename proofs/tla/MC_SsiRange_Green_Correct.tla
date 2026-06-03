---- MODULE MC_SsiRange_Green_Correct ----
EXTENDS SsiRange
\* GREEN. The correct carve-out (DocScan only) over a genuine INDEX-RANGE write-skew.
\* ta, tb each range-read the SAME index range {kx,ky} (an FK-index scan, kind="IndexRange",
\* NOT carved), then write a DISJOINT key inside that range. Because IndexRange is tracked,
\* check_and_record's rw_B test (committed write hit my recorded range) aborts the second
\* committer. Every accepted schedule stays MVSG-acyclic under the true reads.
\*
\* This is the exact false-positive-vs-real distinction: the carve-out must NOT swallow this.

mcTxns  == {"ta", "tb"}
mcKeys  == {"kx", "ky"}

mcPointReads == [t \in mcTxns |-> {}]
mcRangeKeys  == [t \in mcTxns |-> {"kx", "ky"}]   \* both scan the same index range
mcRangeKind  == [t \in mcTxns |-> "IndexRange"]   \* FK index range read -> never carved
mcWrites     == [t \in mcTxns |->
                  CASE t = "ta" -> {"kx"}
                    [] OTHER    -> {"ky"} ]
====
