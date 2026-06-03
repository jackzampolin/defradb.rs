---- MODULE MC_Ssi_Red_WriteSkew ----
EXTENDS Ssi
\* RED: ww-only conflict test (plain snapshot isolation). The write-skew pair (ta, tb)
\* each reads {kx,ky} and writes a DISJOINT key, so there is no write-write conflict and
\* both commit -- but each invalidated the other's read predicate. The MVSG over the two
\* committed txns has an anti-dependency cycle ta <-> tb, so INV_Serializable is violated.
\* This is the exact regression the rw_A/rw_B disjuncts in check_and_record prevent.

mcTxns  == {"ta", "tb"}
mcKeys  == {"kx", "ky"}

mcReads  == [t \in mcTxns |-> {"kx", "ky"}]
mcWrites == [t \in mcTxns |->
              CASE t = "ta" -> {"kx"}
                [] OTHER    -> {"ky"} ]
====
