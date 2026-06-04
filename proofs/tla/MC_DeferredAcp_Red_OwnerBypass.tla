---- MODULE MC_DeferredAcp_Red_OwnerBypass ----
EXTENDS MC_DeferredAcp_Common
\* RED (fail-OPEN projection bug): the overlay grants a projected Registered{owner} to ANY
\* authenticated identity, dropping the `did == owner` check in check_doc_access_with_overlay.
\* Txn tA projects d1 Registered{u1}; reader u2 (a stranger) is granted -- access the
\* committed state (Registered{u1}) denies. INV_FailClosedActive violated: the in-flight
\* projection grants what the committed state would deny.
====
