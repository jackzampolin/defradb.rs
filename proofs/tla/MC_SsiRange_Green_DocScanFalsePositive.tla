---- MODULE MC_SsiRange_Green_DocScanFalsePositive ----
EXTENDS SsiRange
\* GREEN. The carve-out's ACTUAL justified domain: a full-collection document scan whose
\* writers insert into a DISJOINT keyspace (an unrelated insert -- another collection, or the
\* FK index), so no writer's key lies inside the scanned doc range. Here the suppressed range
\* conflict is a TRUE FALSE POSITIVE: there is no real anti-dependency, and carving it changes
\* nothing about serializability. Both txns commit and the schedule is MVSG-acyclic.
\*
\* Contrast with MC_SsiRange_Probe_DocScanSkew (same kind=DocScan, but writers write INSIDE
\* the scan range -> real skew -> RED). The difference is the write target's keyspace, which is
\* exactly the real-code precondition: document-collection scans do not conflict with unrelated
\* inserts (shared.rs:216-218). This config witnesses the "drops only false positives" claim.

mcTxns  == {"ta", "tb"}
\* kscan = a key inside the doc-scan range; kwa, kwb = inserts into a DISJOINT keyspace.
mcKeys  == {"kscan", "kwa", "kwb"}

mcPointReads == [t \in mcTxns |-> {}]
mcRangeKeys  == [t \in mcTxns |-> {"kscan"}]      \* both scan the same doc collection
mcRangeKind  == [t \in mcTxns |-> "DocScan"]      \* carved
mcWrites     == [t \in mcTxns |->
                  CASE t = "ta" -> {"kwa"}        \* unrelated insert, NOT in scan range
                    [] OTHER    -> {"kwb"} ]       \* unrelated insert, NOT in scan range
====
