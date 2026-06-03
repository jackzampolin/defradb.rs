---- MODULE MC_DeferredAcp_Red_RollbackHooks ----
EXTENDS MC_DeferredAcp_Common
\* RED (atomicity bug): rollback fires the buffered ACP hooks anyway (as if on_success_async
\* ran on discard). A txn that projects d1 Unregistered then ROLLS BACK leaves committed ACP
\* opened -> a later read (or the surviving committed grant) lets a non-owner through though
\* the txn aborted. INV_RollbackNoOp violated (committed[d1] changed on a doc no live/committed
\* txn touched), and the opened state is caught by INV_FailClosedAfterCommit on a sibling read.
====
