# Cross-Stream Integration Gaps

- **Severity:** MEDIUM
- **Category:** Architecture / Cross-Cutting Concerns
- **Status:** Confirmed — multiple gaps between audit streams compound risk

## Summary

This final audit session identified integration gaps where findings from different audit streams interact to create risks greater than any individual finding. These compound vulnerabilities arise when weaknesses at one layer amplify weaknesses at another.

## Details

### Gap 1: FFI Panic + No catch_unwind + Downstream Panics

**Streams:** S7 (Finding 00) + S6 (Finding 11) + S5 (Finding 02)

The FFI boundary has no `catch_unwind` (S7-00). The DAG traversal has no depth limit (S6-11) and can cause stack overflow on fixed-size tokio worker threads. The filter evaluation has no recursion limit (S5-02). Any of these panic paths, when reached via an FFI call, produce undefined behavior.

**Chain:** HTTP API → GraphQL → Deep query → Stack overflow → Panic → FFI UB

### Gap 2: ACP Bypass + P2P No Auth + Debug Dump

**Streams:** S2 (Findings 01, 16, 18) + S3 (Findings 12, 20, 22) + S4 (Finding 37)

ACP bypasses via `_commits` queries (S2-01) allow reading document data. The dump endpoint has no authentication (S4-37). P2P merge has no signature verification (S2-18). AccessMode::Controlled is dead code (S3-20). Bitswap has no collection access checks (S3-22). These combine: a peer can read via Bitswap, forge via unsigned merge, and dump via HTTP — all bypassing the ACP model.

### Gap 3: Resource Exhaustion Chain

**Streams:** S3 (Findings 00, 01, 30, 42) + S5 (Findings 00, 01, 02, 05, 32)

No HTTP body size limit (S5-01) → No GraphQL depth limit (S5-00) → No query timeout (S5-05) → No filter recursion limit (S5-02) → No rate limiting (S5-32). On the P2P side: no message size limit (S3-00) → no connection limits (S3-01) → unbounded task spawning (S3-30) → no per-peer rate limiting (S3-42). An attacker with network access can chain these at any layer for amplified denial-of-service.

### Gap 4: SE Pipeline Incomplete + ACP Bypass

**Streams:** S1 (Finding 10) + S2 (Finding 04) + S6 (Findings 34, 37)

SE tag UTF-8 handling diverges from Go (S1-10). Encrypted search bypasses ACP (S2-04). SE receiver is not implemented (S6-34). SE query evaluation is not integrated (S6-37). The searchable encryption subsystem is non-functional end-to-end in Rust, and the parts that do work bypass access control.

### Gap 5: Dependency CVEs + No CI Scanning

**Streams:** S7 (Findings 21-23, 29, 43)

ring 0.16.20 has an AES panic CVE (S7-21). wasmtime has 3 CVEs (S7-22). lru has unsound IterMut (S7-23). No cargo-deny config exists (S7-29). No cargo-audit step in CI (S7-43). New CVEs accumulate without detection, and existing ones remain unpatched without automated alerting.

### Gap 6: FFI Test Coverage Not on Main + No Negative Testing

**Streams:** S7 (Findings 50, 51, 52)

The comprehensive FFI test suite exists only on `jack/ffi-rust-compat` (S7-50). No negative testing exercises adversarial inputs (S7-51). No stress testing exercises handle lifecycle under load (S7-52). Regressions in FFI correctness are undetectable on `main`, and adversarial inputs have never been tested.

## Remediation

These integration gaps are addressed by the prioritized remediation in AUDIT-SUMMARY.md. The key insight is that Phase 1 remediations (HTTP body size limit, GraphQL depth limit, query timeout, catch_unwind, dump authentication) break the most dangerous chains at their most leveraged points.

## Test Gap

No integration tests exercise cross-cutting attack scenarios. Each subsystem is tested in isolation, but the compound vulnerabilities from chaining subsystem weaknesses are never exercised.
