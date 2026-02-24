# lru 0.12.5: Unsound IterMut (RUSTSEC-2026-0002)

**Severity:** Medium
**Category:** Memory safety — Unsoundness
**Status:** Vulnerable — no fix available yet

## Summary

`lru` v0.12.5 has a soundness bug in its `IterMut` implementation that violates Stacked Borrows by invalidating an internal pointer. This means safe Rust code using `IterMut` on an LRU cache could trigger undefined behavior.

## Affected Crate(s)

- `lru` v0.12.5 (direct workspace dependency and transitive via libp2p)

## Dependency Chain

```
lru 0.12.5
├── libp2p-swarm 0.44.2 (via libp2p 0.53.2)
├── libp2p-identify 0.44.2
└── blockstore 0.5.0 (direct usage)
```

## Details

- **Advisory ID:** RUSTSEC-2026-0002
- **Fix Available:** Not yet — no safe upgrade path at time of writing
- **Impact:** `IterMut` violates Stacked Borrows rules, potentially causing undefined behavior. Under MIRI, this would be flagged as UB. In practice, current compilers may not exploit this, but future compiler optimizations could.
- **Usage in defradb.rs:** The `blockstore` crate uses `lru` for block caching, and `libp2p-swarm` uses it internally for connection management.

## Risk Assessment

Medium risk. The soundness bug is in `IterMut` specifically — if the codebase or libp2p only uses immutable iteration or direct lookups (get/put), the bug may not be triggered. However, this is undefined behavior in principle and could manifest under different optimization levels or compiler versions.

## Remediation

- Monitor RUSTSEC-2026-0002 for a patched version
- Audit `blockstore` crate's usage of `lru` — does it use `iter_mut()`?
- Consider switching to `quick_cache` or `moka` as alternatives that don't have this issue
- For libp2p's internal usage: requires libp2p upgrade
