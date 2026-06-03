---- MODULE MC_Ssi_Probe_NoSnapFilter ----
EXTENDS Ssi
\* PROBE: drop the `commit_ver > read_version` guard (shared.rs:292). Same write-skew
\* shape as the green config. This config probes whether removing the snapshot filter
\* changes safety. The full ww+rw_A+rw_B test still aborts the conflicting txn, so safety
\* (INV_Serializable) is expected to HOLD here -- the guard governs over-aborting / liveness,
\* not the serializability safety property. Reported per honest observed verdict.

mcTxns  == {"ta", "tb"}
mcKeys  == {"kx", "ky"}

mcReads  == [t \in mcTxns |-> {"kx", "ky"}]
mcWrites == [t \in mcTxns |->
              CASE t = "ta" -> {"kx"}
                [] OTHER    -> {"ky"} ]
====
