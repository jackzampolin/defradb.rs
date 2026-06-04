---- MODULE MC_DeferredAcp_Common ----
EXTENDS DeferredAcp
\* Shared concrete instance for the DeferredAcp scenarios: two concurrent txns over one
\* document with two candidate owners plus the anonymous principal. The document starts
\* committed-Registered to u1, so it is genuinely protected: any over-grant to u2/anon is a
\* real fail-closed violation, and any cross-txn leak is observable.

mcTxns   == {"tA", "tB"}
mcDocs   == {"d1"}
mcIdents == {"u1", "u2"}
mcAnon   == "anon"

\* d1 starts Registered to u1 (protected). Register/Unregister projections can flip it.
mcInitCommitted == [d \in mcDocs |-> Reg("u1")]
====
