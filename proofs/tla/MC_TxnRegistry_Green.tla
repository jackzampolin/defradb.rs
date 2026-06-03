---- MODULE MC_TxnRegistry_Green ----
EXTENDS TxnRegistry
\* GREEN: the real code's write-locked re-check. Two transactions, small clock. Over every
\* interleaving of touch (read lock) and remove (write lock), no live txn is ever evicted.
mcTxns == {"t1", "t2"}
====
