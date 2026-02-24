# Stream 05: Input Validation & Injection — Complete Summary

**Sessions**: 4 of 4 (Complete)
**Date**: 2026-02-21
**Scope**: GraphQL parsing, HTTP handling, filesystem operations, schema validation, multiaddr, error handling, storage keys, WASM sandbox, rate limiting

## All Findings

| # | Title | Severity | Session | Status |
|---|-------|----------|---------|--------|
| 00 | GraphQL Parser Has No Depth or Complexity Limits | HIGH | S1 | Confirmed |
| 01 | No Explicit HTTP Request Body Size Limit | HIGH | S1 | Confirmed |
| 02 | Filter Logical Operators Allow Unbounded Recursion | MEDIUM | S1 | Confirmed |
| 03 | SDL Schema Endpoint Accepts Unbounded Input | MEDIUM | S1 | Confirmed |
| 04 | Fragment Width Amplification (Non-Cyclic) | LOW | S1 | Confirmed |
| 05 | No Query Timeout or Cost Budget | MEDIUM | S1 | Confirmed |
| 06 | SSE Subscription Has No Connection or Resource Limits | MEDIUM | S1 | Confirmed |
| 07 | *(Session 1 Summary)* | — | S1 | — |
| 08 | WASM Lens Module Path Traversal via `file://` Prefix | MEDIUM | S2 | Confirmed |
| 09 | FFI Backup Export Writes to Arbitrary Filesystem Path | MEDIUM | S2 | Confirmed |
| 10 | CLI File Reading Operations Have No Size Limit | LOW | S2 | Confirmed |
| 11 | HTTP Handlers Do Not Accept Filesystem Paths | GREEN | S2 | Not Vulnerable |
| 12 | No `canonicalize()` or Symlink Resolution on User Paths | LOW | S2 | Confirmed |
| 13 | Data Directory Created Without Permission Hardening | LOW | S2 | Confirmed |
| 14 | Dump and Purge Commands Are HTTP-Only | GREEN | S2 | Not Vulnerable |
| 15 | Lens WASM Path Traversal Reachable via HTTP API | HIGH | S2 | Confirmed |
| 16 | Null Byte Path Handling in Rust | GREEN | S2 | Not Vulnerable |
| 17 | *(Session 2 Summary)* | — | S2 | — |
| 18 | Unknown Directives Silently Accepted | LOW | S3 | Confirmed |
| 19 | Multiaddr SSRF — No Private IP Blocklist | MEDIUM | S3 | Confirmed |
| 20 | Error Messages Echo User Input | LOW | S3 | Confirmed |
| 21 | GraphQL Introspection Always Enabled | MEDIUM | S3 | Confirmed |
| 22 | Schema No Field Drop Migration Guard | LOW | S3 | Confirmed |
| 23 | Content-Type Not Enforced on Schema Endpoint | LOW | S3 | Confirmed |
| 24 | Identifiers Accept Unbounded Length | LOW | S3 | Confirmed |
| 25 | Error Responses Safe — JSON Content-Type | GREEN | S3 | Not Vulnerable |
| 26 | Schema Not Replicated via P2P | GREEN | S3 | Not Vulnerable |
| 27 | Directive Args Not Stored or Evaluated | GREEN | S3 | Not Vulnerable |
| 28 | Circular References Properly Detected | GREEN | S3 | Not Vulnerable |
| 29 | *(Session 3 Summary)* | — | S3 | — |
| 30 | Storage Key Construction Verified Injection-Proof | GREEN | S4 | Verified Safe |
| 31 | WASM Sandbox Has No Memory, CPU, or Syscall Restrictions | HIGH | S4 | Confirmed |
| 32 | No HTTP Rate Limiting, Request Timeout, or Connection Limits | MEDIUM | S4 | Confirmed |
| 33 | Lens Transform Output Not Validated Against Schema | MEDIUM | S4 | Confirmed |
| 34 | No Size Limit on WASM Module Binaries | LOW | S4 | Confirmed |
| 35 | String-Based Keys Use `/` Separator Without Escaping | LOW | S4 | Mitigated |
| 36 | WASM Transform Execution Blocks Tokio Worker Thread | MEDIUM | S4 | Confirmed |
| 37 | *(Session 4 Summary)* | — | S4 | — |

## Severity Distribution

| Severity | Count | Findings |
|----------|-------|----------|
| **HIGH** | 4 | 00, 01, 15, 31 |
| **MEDIUM** | 11 | 02, 03, 05, 06, 08, 09, 19, 21, 32, 33, 36 |
| **LOW** | 10 | 04, 10, 12, 13, 18, 20, 22, 23, 24, 34, 35 |
| **GREEN** | 7 | 11, 14, 16, 25, 26, 27, 28, 30 |

**Total**: 32 findings (4 HIGH, 11 MEDIUM, 10 LOW, 7 GREEN)

## Thematic Analysis

### Theme 1: Resource Exhaustion (DoS Surface) — Critical

The most systemic issue across all sessions is the absence of resource limits at multiple layers:

```
HTTP Layer:     No body size limit (01), no rate limit (32), no connection limit (32)
GraphQL Layer:  No depth limit (00), no recursion limit (02), no width limit (04)
Query Layer:    No timeout (05), no cost budget (05)
WASM Layer:     No memory limit (31), no CPU limit (31), no output limit (31)
SSE Layer:      No subscription limit (06)
```

Each layer amplifies the one below it. A single attacker can:
1. Open unlimited connections (32)
2. Send unlimited requests at unlimited size (01, 32)
3. Each request triggers unbounded query parsing (00, 02, 04)
4. Each query runs without timeout (05)
5. If lens transforms are involved, WASM executes without limits (31, 36)

**Priority**: This is the highest-priority remediation theme. Adding a request timeout (05) and body size limit (01) would provide immediate relief at minimal cost.

### Theme 2: Path Traversal via Lens — High

Findings 08, 15 form a cohesive vulnerability: the lens WASM module path accepts arbitrary filesystem paths via the HTTP API. This is a remote arbitrary file read (or at minimum, file existence oracle) with no authentication required when NAC is disabled (the default).

**Priority**: P0. Block `file://` paths from the HTTP lens endpoint immediately.

### Theme 3: WASM Sandbox Gaps — High

Findings 31, 33, 34, 36 expose that the wasmtime sandbox is structurally sound (no WASI access = correct) but operationally dangerous (no resource limits). A single malicious lens transform can:
- Allocate unbounded memory (31)
- Run an infinite loop blocking a tokio thread (31, 36)
- Produce unlimited output documents (31, 33)
- Accept arbitrarily large module binaries (34)
- Return arbitrary JSON that bypasses schema validation (33)

**Priority**: P1. Add fuel metering, memory limits, and `spawn_blocking()`.

### Theme 4: Storage Layer — Strong

The storage key construction (finding 30) is well-designed with three defense layers. String-based keys in headstore/peerstore lack escaping (finding 35) but are mitigated by upstream validation. The namespace isolation mechanism prevents cross-store data leakage. This is the strongest area of the codebase from an input validation perspective.

### Theme 5: Schema/API Validation — Adequate with Gaps

- `validate_identifier()` is correctly implemented and prevents injection (30)
- Fragment cycle detection works correctly (noted in S1 summary)
- Circular type reference detection uses Tarjan's algorithm (28)
- Error responses use JSON Content-Type, preventing XSS (25)
- But: no identifier length limits (24), no introspection toggle (21), no Content-Type enforcement (23)

### Theme 6: Network Input Validation — Moderate Gaps

- Multiaddr validation is superficial — no private IP blocklist (19)
- No P2P schema injection risk — schemas not replicated (26)
- Documented in P2P stream: no per-peer rate limiting, no message size limits

## Overall Security Posture Assessment

### Strengths

1. **Storage key design** is injection-proof by construction
2. **Namespace isolation** prevents cross-store data access
3. **Rust type system** prevents entire classes of vulnerabilities (null bytes, buffer overflows, use-after-free)
4. **Fragment cycle detection** is correct
5. **Error handling** uses proper JSON responses, no XSS surface
6. **WASM WASI isolation** is correctly configured (no capabilities granted)
7. **NAC permission model** provides authorization when enabled
8. **Schema not replicated via P2P** eliminates a significant attack vector
9. **Input validation** (`validate_identifier`, `validate_doc_id`, `validate_multiaddr`) exists and is well-tested

### Weaknesses

1. **No resource limits anywhere** — HTTP, GraphQL, WASM all lack bounds
2. **Lens path traversal** is remotely exploitable via HTTP
3. **WASM sandbox lacks resource controls** despite correct capability isolation
4. **Rate limiting completely absent** from the HTTP stack
5. **Query timeout missing** — slow queries hold connections indefinitely
6. **Introspection always enabled** — schema enumerable by any user

### Risk Rating

For a **development/testnet deployment**: MEDIUM risk. The DoS surface is significant but exploitation requires network access to the API.

For a **production/public-facing deployment**: HIGH risk. The absence of rate limiting, request timeouts, and body size limits, combined with the WASM sandbox gaps, creates a realistic DoS attack surface. The lens path traversal is a direct confidentiality violation.

## Prioritized Remediation Roadmap

### Immediate (P0 — Do Now)

| Action | Finding | Effort | Impact |
|--------|---------|--------|--------|
| Block `file://` lens paths from HTTP | 15 | 30 min | Eliminates remote file read |
| Add `DefaultBodyLimit` to HTTP router | 01 | 10 min | Caps all request sizes |
| Add `TimeoutLayer` to HTTP router | 05, 32 | 15 min | Prevents indefinite queries |

### Short Term (P1 — This Sprint)

| Action | Finding | Effort | Impact |
|--------|---------|--------|--------|
| Add wasmtime fuel metering | 31 | 2 hours | Prevents infinite WASM loops |
| Add wasmtime memory limits | 31 | 1 hour | Prevents WASM OOM |
| Move WASM to `spawn_blocking()` | 36 | 1 hour | Protects tokio threads |
| Add GraphQL depth counter | 00 | 2 hours | Prevents depth bombs |
| Add filter recursion limit | 02 | 1 hour | Prevents filter DoS |
| Add `ConcurrencyLimitLayer` | 32 | 15 min | Caps concurrent requests |

### Medium Term (P2 — This Month)

| Action | Finding | Effort | Impact |
|--------|---------|--------|--------|
| Add multiaddr IP blocklist | 19 | 4 hours | Prevents SSRF |
| Add introspection toggle | 21 | 2 hours | Hides schema from public |
| Add WASM module size limit | 34 | 30 min | Prevents compile-time DoS |
| Add WASM output document cap | 31, 33 | 30 min | Prevents output amplification |
| Add identifier length limit | 24 | 30 min | Prevents key bloat |
| Validate lens transform output | 33 | 4 hours | Ensures schema compliance |
| Canonicalize FFI backup paths | 09 | 1 hour | Prevents path traversal |

### Long Term (P3 — Before 1.0)

| Action | Finding | Effort | Impact |
|--------|---------|--------|--------|
| Per-IP rate limiting | 32 | 8 hours | Full DoS protection |
| GraphQL query cost analysis | 05 | 16 hours | Prevents complex query DoS |
| SDL schema size limits | 03 | 2 hours | Prevents schema bloat |
| SSE subscription limits | 06 | 4 hours | Prevents subscription flood |
| Content-Type enforcement | 23 | 2 hours | Defensive hardening |
| Schema migration guards | 22 | 4 hours | Prevents accidental data loss |
| Data directory permissions | 13 | 1 hour | Defense-in-depth |
| Defense-in-depth key assertions | 35 | 2 hours | Assert no separator in key components |
