---- MODULE MC_Ssi_Green ----
EXTENDS Ssi
\* GREEN: full SSI test (ww + rw_A + rw_B). Three txns over two keys, including the
\* classic write-skew pair (ta, tb) plus a lost-update pair on kx. Every accepted
\* schedule must be MVSG-acyclic.

mcTxns  == {"ta", "tb", "tc"}
mcKeys  == {"kx", "ky"}

\* ta, tb: write-skew shape (each reads both keys, writes one).
\* tc: a lost-update probe on kx (reads and writes kx).
mcReads  == [t \in mcTxns |->
              CASE t = "ta" -> {"kx", "ky"}
                [] t = "tb" -> {"kx", "ky"}
                [] OTHER    -> {"kx"} ]
mcWrites == [t \in mcTxns |->
              CASE t = "ta" -> {"kx"}
                [] t = "tb" -> {"ky"}
                [] OTHER    -> {"kx"} ]
====
