---- MODULE MC_SsiRange_Probe_DocScanSkew ----
EXTENDS SsiRange
\* PROBE / adversarial soundness check of the CORRECT carve-out itself.
\* Identical write-skew shape, but the range read is now a DOC SCAN (kind="DocScan"), which
\* the correct carve-out DOES suppress. If a document-collection scan can be the read-leg of a
\* genuine write-skew, then even the "correct" carve-out is unsound and the oracle (true reads)
\* finds an MVSG cycle -> INV_Serializable violated (the carve-out's premise would be FALSE).
\*
\* This is the load-bearing soundness question: the carve-out's justification (shared.rs:216-218)
\* is that full document-collection scans do not conflict with unrelated inserts. Here the two
\* writers write keys that lie INSIDE each other's doc-scan range -- the worst case for the
\* premise. Verdict reported per honest observation; see SsiRange_DESIGN.md "Probe" for analysis.

mcTxns  == {"ta", "tb"}
mcKeys  == {"kx", "ky"}

mcPointReads == [t \in mcTxns |-> {}]
mcRangeKeys  == [t \in mcTxns |-> {"kx", "ky"}]
mcRangeKind  == [t \in mcTxns |-> "DocScan"]      \* d/d, /d full-collection scan -> carved
mcWrites     == [t \in mcTxns |->
                  CASE t = "ta" -> {"kx"}
                    [] OTHER    -> {"ky"} ]
====
