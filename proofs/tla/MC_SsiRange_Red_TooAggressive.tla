---- MODULE MC_SsiRange_Red_TooAggressive ----
EXTENDS SsiRange
\* RED. SAME txn shape as MC_SsiRange_Green_Correct (the only change is CarveMode below).
\* The too-aggressive carve-out also suppresses IndexRange reads, so both ta and tb have
\* EMPTY recorded read-sets. check_and_record sees no rw conflict (writes are disjoint), so
\* BOTH commit -- a genuine range write-skew. The oracle, built from TRUE reads {kx,ky},
\* finds the anti-dependency 2-cycle ta <-> tb and INV_Serializable is violated.
\* This proves the carve-out has teeth: making it swallow real index-range conflicts is unsound.

mcTxns  == {"ta", "tb"}
mcKeys  == {"kx", "ky"}

mcPointReads == [t \in mcTxns |-> {}]
mcRangeKeys  == [t \in mcTxns |-> {"kx", "ky"}]
mcRangeKind  == [t \in mcTxns |-> "IndexRange"]
mcWrites     == [t \in mcTxns |->
                  CASE t = "ta" -> {"kx"}
                    [] OTHER    -> {"ky"} ]
====
