# Finding 29: Session 3 — Authorization Model & Access Checks Summary

**Session: Stream 3, Session 3**
**Focus: Two-tier authorization model, access check coverage, trust boundaries**

## Architecture

The P2P layer implements a two-tier access control model:

1. **Collection-level (P2P Layer)**: `SyncCoordinator.check_access()` verifies peer is a registered replicator for the target collection
2. **Document-level (Merge Layer)**: Database merge handler checks ACP permissions for the document creator

The collection-level check uses `AccessMode` (Open vs Controlled) and `ReplicatorRegistry` (HashMap-based peer-per-collection authorization).

## Critical Finding

**The entire collection-level access control system is dead code (Finding 20).** `AccessMode::Controlled` is never activated in production — all constructors and the FFI entry point hardcode `AccessMode::Open`. This means every `check_access()` call returns `Ok(())` unconditionally.

## Findings Summary

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| 20 | AccessMode::Controlled never activated — access control is dead code | CRITICAL | New |
| 21 | DocSync, BranchableSync, CAR fetch have NO access checks | HIGH | New |
| 22 | Bitswap serves blocks without collection-level checks | MEDIUM | New |
| 23 | GossipSub checks relay peer, not message originator | MEDIUM | New |
| 24 | GossipSub topic names leak collection IDs | LOW | New |
| 25 | Replicator management is admin-only (no self-registration) | GREEN | New |
| 26 | PushLog access check ordering is correct (before CID parsing) | GREEN | New |
| 27 | Collection ID matching is exact — no wildcards or inheritance | GREEN | New |
| 28 | Registry RwLock is atomic — no internal TOCTOU | GREEN | New |

**Severity Distribution:** 1 CRITICAL, 1 HIGH, 2 MEDIUM, 1 LOW, 4 GREEN

## Trust Boundary Analysis

```
Untrusted (P2P Network)
    │
    ├── PushLog Request ──→ check_access() ──→ CID parse ──→ process
    ├── GossipSub Msg  ──→ check_access() ──→ CID parse ──→ process
    ├── DocSync Request ──→ [NO CHECK] ──→ return heads          ← GAP
    ├── BranchableSync  ──→ [NO CHECK] ──→ return collection heads ← GAP
    ├── CAR Fetch       ──→ [NO CHECK] ──→ serve entire DAG      ← GAP
    └── Bitswap Get     ──→ [NO CHECK] ──→ serve any block       ← BY DESIGN
    │
    └── check_access() currently always returns Ok(()) ← DEAD CODE
    │
Trusted (Local Admin)
    ├── HTTP API ──→ NAC permission check ──→ add/remove replicator ✓
    └── CLI      ──→ local access ──→ add/remove replicator        ✓
```

## Key Takeaways

1. **The access control plumbing exists but is not wired up.** The code for `check_access`, `ReplicatorRegistry`, and `AccessMode::Controlled` is correct — it just needs to be activated.

2. **Three protocol handlers were never integrated with access control.** DocSync, BranchableSync, and CAR fetch were likely added after the initial access control design and never received `check_access()` calls.

3. **The admin control plane is properly separated from the data plane.** Replicator management requires local admin access; no P2P message can modify the registry.

4. **Even with fixes, the model has inherent limitations.** GossipSub relay-based bypass (Finding 23) and Bitswap CID-guessing (Finding 22) are architectural limitations that require defense-in-depth at the merge layer.

## Relationship to Session 2 Findings

- Finding 12 (two-stream no signature verification) compounds with Finding 21 — DocSync/BranchableSync requests arrive via two-stream and have neither signature verification NOR access checks
- Finding 17 (GossipSub no application-level signature) relates to Finding 23 — without verifying the message originator's signature, checking `propagation_source` is the only option

## Next Sessions

- **Session 4**: Replication protocol security — message validation, CID parsing, DAG depth bombs
- **Session 5**: Resource limits — rate limiting, connection limits, message sizes
