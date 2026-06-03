---- MODULE MC_DeferredAcp_Green ----
EXTENDS MC_DeferredAcp_Common
\* GREEN: the correct mechanism -- per-txn isolated projection, rollback runs no hooks,
\* strict owner-only check on a projected Registered. Every safety invariant must hold over
\* all interleavings of two concurrent txns (register / unregister / read / commit /
\* rollback).
====
