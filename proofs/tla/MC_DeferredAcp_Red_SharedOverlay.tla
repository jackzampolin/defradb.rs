---- MODULE MC_DeferredAcp_Red_SharedOverlay ----
EXTENDS MC_DeferredAcp_Common
\* RED (isolation bug): one global DeferredAcpMutations shared by both txns. Txn tB reads
\* doc d1 and the overlay sees tA's UNCOMMITTED projection (e.g. tA projected Unregistered,
\* opening d1) -> tB is granted access its OWN commit would never produce. INV_NoCrossTxnLeak
\* / INV_FailClosedActive violated with a concrete two-txn counterexample.
====
