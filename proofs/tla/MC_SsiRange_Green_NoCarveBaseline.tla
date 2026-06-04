---- MODULE MC_SsiRange_Green_NoCarveBaseline ----
EXTENDS SsiRange
\* GREEN baseline. The maximally conservative engine: NEVER carve. Same index-range
\* write-skew shape as the Red/Correct pair. With no carve-out, the full range read is tracked
\* and rw_B aborts the second committer -- always serializable. This proves the ORACLE is not
\* trivially cyclic (it stays acyclic when the mechanism is conservative), so the RED config's
\* cycle is caused by the carve-out, not by the oracle or the shape alone.

mcTxns  == {"ta", "tb"}
mcKeys  == {"kx", "ky"}

mcPointReads == [t \in mcTxns |-> {}]
mcRangeKeys  == [t \in mcTxns |-> {"kx", "ky"}]
mcRangeKind  == [t \in mcTxns |-> "IndexRange"]
mcWrites     == [t \in mcTxns |->
                  CASE t = "ta" -> {"kx"}
                    [] OTHER    -> {"ky"} ]
====
