---- MODULE MC_TxnRegistry_Red_NaiveSweep ----
EXTENDS TxnRegistry
\* RED: a naive sweep that removes on the phase-1 collect verdict alone (no write-locked
\* re-check). The counterexample: t1 is collected as stale, a concurrent get() touches t1
\* (refreshing its idle clock to 0), then the sweep removes t1 anyway -- evicting a now-live
\* transaction. INV_NoLiveEvicted is violated, proving the invariant has teeth and that the
\* real code's write-locked re-check is load-bearing.
mcTxns == {"t1", "t2"}
====
