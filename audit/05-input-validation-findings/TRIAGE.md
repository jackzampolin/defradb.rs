# Input Validation Findings -- Triage Report

**Stream**: 05 - Input Validation
**Date**: 2026-02-21
**Findings Reviewed**: 37 (excluding session summaries)

---

## 1. Findings Table

Sorted by severity (HIGH first, GREEN last).

| # | Severity | Title | Status | One-Line Summary |
|---|----------|-------|--------|------------------|
| 00 | HIGH | GraphQL Parser Has No Depth or Complexity Limits | CONFIRMED | Recursive descent parser accepts unlimited depth/width queries, enabling OOM and stack overflow via width bombs and depth bombs before the planner's `MAX_NESTING_DEPTH=10` check. |
| 01 | HIGH | No Explicit HTTP Request Body Size Limit | CONFIRMED | Schema endpoint (`String` extractor) and backup import (`Bytes` extractor) have no body size limit; Axum's `Json` 2MB default is implicit and not applied globally. |
| 15 | HIGH | Lens WASM Path Traversal Reachable via HTTP API | CONFIRMED | Remote attacker can read arbitrary server files by sending a crafted `Path` field in `POST /api/v0/lens/set`; the path flows unvalidated to `Module::from_file()`. |
| 31 | HIGH | WASM Sandbox Has No Memory, CPU, or Syscall Restrictions | CONFIRMED | wasmtime Engine uses defaults with no `StoreLimiter`, no fuel metering, no execution timeout; a malicious WASM module can OOM or infinite-loop the node. |
| 02 | MEDIUM | Filter Logical Operators Allow Unbounded Recursion | CONFIRMED | `_and`/`_or`/`_not` filters can nest to arbitrary depth with no recursion limit in parser or evaluator, causing stack overflow or CPU exhaustion per document. |
| 03 | MEDIUM | SDL Schema Endpoint Accepts Unbounded Input | CONFIRMED | `POST /api/v0/schema` reads raw `String` body with no size limit; no type count or field count limits on parsed SDL; 10,000 types cause O(n^2) validation. |
| 05 | MEDIUM | No Query Timeout or Cost Budget | CONFIRMED | No per-query timeout, no cost estimation, no concurrent query limit; a single expensive query can block a tokio worker thread indefinitely. |
| 06 | MEDIUM | SSE Subscription Has No Connection or Resource Limits | CONFIRMED | SSE connections live indefinitely, re-execute queries per database event, no connection count/duration/idle limits; enables CPU amplification via many subscriptions. |
| 08 | MEDIUM | WASM Lens Module Path Traversal via `file://` Prefix | CONFIRMED | Lens module loader strips `file://` and passes the remainder to `Module::from_file()` / `fs::read()` with no path traversal validation. |
| 09 | MEDIUM | FFI Backup Export Writes to Arbitrary Filesystem Path | CONFIRMED | FFI `basic_export` writes to an arbitrary `filepath` from JSON config with no validation or directory confinement; FFI `basic_import` reads from arbitrary path. |
| 19 | MEDIUM | Multiaddr SSRF -- No Private IP Blocklist | CONFIRMED | `validate_multiaddr()` only checks for leading `/`; no IP range validation at any layer; authenticated attacker can probe internal networks and cloud metadata. |
| 21 | MEDIUM | GraphQL Introspection Always Enabled | CONFIRMED | `__schema`/`__type` queries always enabled without authentication when NAC is off; reveals complete type system including ACP-protected collection names. |
| 32 | MEDIUM | No HTTP Rate Limiting, Request Timeout, or Connection Limits | CONFIRMED | HTTP server has no `RateLimitLayer`, `TimeoutLayer`, or `ConcurrencyLimitLayer`; attacker can exhaust resources via high-volume requests or idle connections. |
| 33 | MEDIUM | Lens Transform Output Not Validated Against Schema | CONFIRMED | WASM transform output is trusted completely; a malicious transform can return wrong types, extra fields, modified `_docID`, or unbounded output without validation. |
| 36 | MEDIUM | WASM Transform Execution Blocks Tokio Worker Thread | CONFIRMED | Synchronous `TypedFunc::call()` runs on tokio async thread pool; a slow WASM module blocks async capacity for HTTP, P2P, and all other operations. |
| 04 | LOW | Fragment Width Amplification (Non-Cyclic) | CONFIRMED | Non-cyclic fragment spreads amplify query width linearly; cycle detection is correct; amplification is equivalent to a direct width bomb with no additional leverage. |
| 10 | LOW | CLI File Reading Operations Have No Size Limit | CONFIRMED | Multiple CLI commands use `fs::read_to_string()` with no file size limit; user-controlled paths to device files could hang or OOM the process. |
| 12 | LOW | No `canonicalize()` or Symlink Resolution on User-Controlled Paths | CONFIRMED | No production code resolves symlinks before filesystem operations; symlinks could redirect reads/writes to unexpected locations in FFI/lens paths. |
| 13 | LOW | Data Directory Created Without Permission Hardening | CONFIRMED | Data directory created with default permissions (0755) instead of 0700; config files world-readable; Go DefraDB uses 0700. |
| 18 | LOW | Unknown SDL Directives Silently Accepted | CONFIRMED | Parser accepts arbitrary directives (e.g., `@malicious`) with a warning instead of an error; directive arguments are discarded, not evaluated. |
| 20 | LOW | Error Messages Echo User Input Unsanitized | CONFIRMED | Filepaths, multiaddrs, and serde error details reflected in JSON error responses; XSS mitigated by JSON Content-Type, but useful for fingerprinting. |
| 22 | LOW | Schema Migration -- No Field Drop or Type Change Guard | CONFIRMED | Schema versioning tracks `previous_version` but does not validate backward compatibility; field drops and type changes are silently accepted. |
| 23 | LOW | Content-Type Not Enforced on Schema Endpoint | CONFIRMED | Schema endpoint accepts any Content-Type; body always parsed as SDL regardless; risk only if WAF/proxy makes routing decisions on Content-Type. |
| 24 | LOW | Identifiers Accept Unbounded Length | CONFIRMED | `validate_identifier()` enforces character set but no max length; million-character collection names flow into storage keys and introspection. |
| 34 | LOW | No Size Limit on WASM Module Binaries | CONFIRMED | `Module::from_file()` and `Module::new()` accept arbitrarily large WASM modules; multi-GB files cause OOM during compilation. |
| 35 | LOW | String-Based Keys Use `/` Separator Without Escaping | CONFIRMED | Headstore, peerstore, and systemstore keys use `format!()` with `/` separators and no escaping; currently safe due to upstream input validation. |
| 11 | GREEN | HTTP Handlers Do Not Accept Filesystem Paths | NOT VULNERABLE | No HTTP endpoint accepts a filesystem path from remote clients; all filesystem operations confined to CLI/FFI. Exception: lens `Path` field (see #08/#15). |
| 14 | GREEN | Dump and Purge Commands Are HTTP-Only | NOT VULNERABLE | Dump outputs to stdout, purge operates via HTTP API on own data; no path traversal risk. |
| 16 | GREEN | Null Byte Path Handling | NOT VULNERABLE | Rust's `CString` conversion rejects interior null bytes with an error; null byte injection is not possible. |
| 25 | GREEN | Error Responses Safe -- JSON Content-Type Prevents XSS | CONFIRMED SAFE | All HTTP error responses use `Content-Type: application/json`; CRLF injection prevented by Rust string handling and Axum `HeaderValue` validation. |
| 26 | GREEN | Schema Not Replicated via P2P | CONFIRMED SAFE | Schemas are not replicated between peers; P2P syncs documents/blocks only; no malicious schema injection via P2P. |
| 27 | GREEN | Directive Arguments Not Stored or Evaluated | CONFIRMED SAFE | All directive arguments are type-checked and consumed; unknown arguments are discarded; no eval/exec path exists. |
| 28 | GREEN | Circular Type References Properly Detected | CONFIRMED SAFE | Tarjan's SCC algorithm correctly detects all cycle patterns; no infinite loop risk from circular schema definitions. |
| 30 | GREEN | Storage Key Construction Verified Injection-Proof | CONFIRMED SAFE | Three-layer defense (namespace prefix, varint encoding, identifier validation) makes key injection effectively impossible. |

---

## 2. Themes

### Theme A: Parser and Query DoS (Findings 00, 02, 04, 05)

The GraphQL parser and query engine lack resource limits at multiple levels. The parser accepts unlimited depth, width, and filter recursion. The planner's `MAX_NESTING_DEPTH=10` is a partial mitigation that only covers join depth, not parser-level allocation, filter recursion, or width bombs. Once past the parser, queries execute without timeout or cost budget. These findings combine: an attacker can craft a query that is expensive at parsing time (width bomb), expensive at evaluation time (filter recursion), and holds a worker thread indefinitely (no timeout).

**Findings**: 00, 02, 04, 05

### Theme B: HTTP Server Hardening (Findings 01, 03, 06, 32)

The HTTP server lacks fundamental DoS protection. No global body size limit, no request timeout, no connection limit, no rate limiting. The schema endpoint is particularly exposed because it uses a `String` extractor (no Axum default limit) and accepts unbounded SDL. SSE subscriptions compound the problem by holding connections indefinitely with per-event query re-execution. These are all straightforward to fix with standard `tower` middleware layers.

**Findings**: 01, 03, 06, 32

### Theme C: Filesystem Safety and Path Traversal (Findings 08, 09, 12, 15)

The lens WASM module loader and FFI backup paths have no path traversal protection. Finding 15 elevates finding 08 from MEDIUM to HIGH by demonstrating the path traversal is reachable via HTTP. The FFI backup path writes to arbitrary filesystem locations. No production code uses `canonicalize()` to resolve symlinks. The HTTP handler layer itself is clean (finding 11 GREEN), but the lens `Path` field creates a bridge from HTTP to filesystem.

**Findings**: 08, 09, 12, 15, (11 GREEN, 14 GREEN, 16 GREEN)

### Theme D: WASM Sandbox Gaps (Findings 31, 33, 34, 36)

The WASM runtime is missing four categories of protection: memory limits (unbounded `memory.grow()`), CPU limits (no fuel metering), output validation (transforms return untyped JSON trusted completely), and thread isolation (synchronous wasmtime calls block tokio workers). The combination means a single malicious lens module can OOM the node, infinite-loop a worker thread, or corrupt data by returning invalid documents. The only positive: WASI capabilities are not granted, so WASM modules cannot access the host filesystem/network.

**Findings**: 31, 33, 34, 36

### Theme E: Information Disclosure and Input Echo (Findings 19, 20, 21, 23, 24)

Several endpoints leak implementation details or accept overly permissive input. Multiaddr SSRF enables internal network probing. Introspection reveals the complete type system without authentication. Error messages echo user input (mitigated by JSON Content-Type). Identifiers have no length limit. Content-Type is not enforced on schema endpoints. Individually these are low-to-medium severity, but together they provide an attacker with extensive reconnaissance capability.

**Findings**: 19, 20, 21, 23, 24

### Theme F: Storage and Schema Safety (Findings 22, 30, 35, 18, 26, 27, 28)

The storage key construction is sound (finding 30 GREEN). String-based keys in headstore/peerstore rely on upstream validation rather than self-defense (finding 35 LOW). Schema migration lacks backward compatibility guards (finding 22 LOW). Unknown directives are silently accepted (finding 18 LOW). Schemas are not replicated via P2P (finding 26 GREEN). Directive arguments are safely consumed (finding 27 GREEN). Circular references are properly detected (finding 28 GREEN). Overall, this area is in good shape.

**Findings**: 18, 22, 26, 27, 28, 30, 35

---

## 3. Actionable vs Informational

### Must Fix (1.0 Blockers)

These findings represent confirmed vulnerabilities exploitable by remote attackers with no or minimal authentication:

| # | Title | Why 1.0 Blocker |
|---|-------|-----------------|
| 15 | Lens WASM Path Traversal via HTTP API | Remote arbitrary file read via unauthenticated HTTP endpoint (when NAC disabled). Immediate confidentiality impact. |
| 00 | GraphQL No Depth or Complexity Limits | Remote OOM/crash via single HTTP request. No authentication required for public collection queries. |
| 01 | No HTTP Body Size Limit | Remote OOM via multi-GB POST to schema endpoint. No authentication required (NAC off by default). |
| 31 | WASM Sandbox No Memory/CPU Limits | Any lens module (local or remote-configured) can OOM or infinite-loop the node. |

### Should Fix (Pre-1.0)

These findings have real exploit potential but require either partial authentication or produce availability (not confidentiality/integrity) impact:

| # | Title | Why Pre-1.0 |
|---|-------|-------------|
| 05 | No Query Timeout or Cost Budget | Single expensive query blocks worker thread indefinitely. Easy DoS for any client. |
| 32 | No HTTP Rate Limiting or Connection Limits | Volume-based DoS with no mitigation. Standard infrastructure hardening. |
| 02 | Filter Recursion Unbounded | Stack overflow from crafted filter. Bypasses planner depth check. |
| 08 | WASM Lens Path Traversal (base finding) | Core vulnerability behind finding 15. Fix at the source in `load_module()`. |
| 09 | FFI Backup Arbitrary Path Write | FFI callers can write to arbitrary filesystem paths. |
| 19 | Multiaddr SSRF No IP Blocklist | Internal network probing via P2P connect endpoints. |
| 33 | Lens Transform Output No Validation | Data integrity risk from untrusted WASM output. |
| 36 | WASM Transform Blocks Tokio Thread | Async capacity degradation from synchronous WASM execution. |
| 06 | SSE Subscription No Limits | Connection exhaustion and CPU amplification via subscriptions. |
| 03 | SDL Schema No Size Limits | OOM via unbounded schema input. Partially addressed by fixing #01. |
| 21 | GraphQL Introspection Always Enabled | Schema enumeration without authentication. |

### Accept Risk / Backlog

These findings are real but low-impact, require local access, or represent design trade-offs:

| # | Title | Rationale |
|---|-------|-----------|
| 04 | Fragment Width Amplification | Linear amplification only; equivalent to direct width bomb. Addressed by fixing #00. |
| 10 | CLI File Read No Size Limit | CLI user already has local access. Robustness issue, not security. |
| 12 | No Symlink Resolution | Requires local filesystem manipulation. Low priority. |
| 13 | Data Directory No Permission Hardening | Local-only. Match Go's 0700. Low effort fix. |
| 18 | Unknown Directives Silently Accepted | Directives are discarded. Forward-compatibility design. |
| 20 | Error Messages Echo User Input | Mitigated by JSON Content-Type. No XSS. Fingerprinting risk only. |
| 22 | Schema No Field Drop Guard | Data integrity, not security. Matches Go behavior. |
| 23 | Content-Type Not Enforced on Schema | No practical impact unless WAF routing depends on Content-Type. |
| 24 | Identifier No Length Limit | Easy fix but low impact. Partially addressed by body size limits (#01). |
| 34 | WASM No Module Size Limit | Partially addressed by body size limits (#01) and WASM sandbox fixes (#31). |
| 35 | String Key Separator No Escaping | Mitigated by upstream validation. Defense-in-depth improvement. |

### No Action (GREEN)

Confirmed safe -- no changes needed:

| # | Title | Why Safe |
|---|-------|----------|
| 11 | HTTP Handlers No Filesystem Exposure | HTTP layer correctly isolates filesystem ops to CLI/FFI. |
| 14 | Dump and Purge Commands Safe | HTTP-only, stdout output, `--force` guard. |
| 16 | Null Byte Path Handling | Rust CString rejects interior nulls. Language-level protection. |
| 25 | Error Response Safe JSON Content-Type | All errors return `application/json`. XSS not possible. |
| 26 | Schema Not Replicated via P2P | P2P protocol has no schema message type. |
| 27 | Directive Arguments Not Stored or Evaluated | Type-checked consumption, no eval path. |
| 28 | Circular References Properly Detected | Tarjan's SCC algorithm handles all cycle patterns. |
| 30 | Storage Key Construction Injection-Proof | Three-layer defense (namespace, varint, identifier validation). |

---

## 4. Recommended Fix Order

The ordering prioritizes: (1) remote exploitability without authentication, (2) fix effort relative to risk reduction, (3) dependencies between findings.

### Phase 1: Remote Code/File Access (Week 1)

**Fix #15 + #08: Lens Path Traversal**
- Reject `file://` paths via HTTP entirely (only accept inline module bytes)
- Add path validation and `canonicalize()` in `load_module()`
- Strip `path` field from P2P-received lens configs
- This is the highest-severity finding: remote arbitrary file read, no auth required when NAC is off
- Estimated effort: 1-2 days

### Phase 2: HTTP Layer Hardening (Week 1)

**Fix #01: Add DefaultBodyLimit middleware**
- Add `DefaultBodyLimit::max(256KB)` globally
- Add per-route overrides for schema (1MB) and backup import (100MB via streaming)
- This single change partially mitigates #03, #24, and #34
- Estimated effort: 0.5 days

**Fix #32: Add Timeout and Connection Limits**
- Add `TimeoutLayer::new(60s)` and `ConcurrencyLimitLayer::new(1000)`
- This partially mitigates #05 and #06
- Estimated effort: 0.5 days

### Phase 3: Query Engine Hardening (Week 2)

**Fix #00: Add Parser Depth and Width Limits**
- Add depth counter to `parse_selection_set()` and `parse_selection_to_selects()`
- Add total field counter across fragment expansions
- Add query byte size check before `graphql_parser::parse_query()`
- This also addresses #04
- Estimated effort: 1-2 days

**Fix #02: Add Filter Recursion Limit**
- Add depth parameter to `eval_conditions()` and `matches_scalar_value()`
- Add width limit to `_and`/`_or` arrays
- Estimated effort: 0.5 days

**Fix #05: Add Query Timeout**
- Wrap `execute_with_context()` in `tokio::time::timeout(30s)`
- Estimated effort: 0.5 days

### Phase 4: WASM Sandbox Hardening (Week 2-3)

**Fix #31: WASM Resource Limits**
- Configure `StoreLimitsBuilder` with 64MB memory limit
- Enable fuel metering with `Config::consume_fuel(true)`
- Add epoch deadline for wall-clock timeout
- This is the most impactful WASM fix
- Estimated effort: 1 day

**Fix #36: Move WASM to Blocking Thread Pool**
- Replace `tokio::spawn()` with `tokio::task::spawn_blocking()` for WASM execution
- Prevents async capacity degradation
- Estimated effort: 0.5 days

**Fix #33: Validate WASM Transform Output**
- Validate output field types against destination schema
- Assert `_docID` preservation
- Add output document count cap
- Estimated effort: 1-2 days

### Phase 5: Network and Remaining Fixes (Week 3)

**Fix #19: Multiaddr IP Blocklist**
- Add private IP range rejection to `validate_multiaddr()`
- Block 127.0.0.0/8, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 169.254.0.0/16, ::1
- Estimated effort: 0.5 days

**Fix #09: FFI Backup Path Validation**
- Add path traversal checks and directory confinement to FFI backup paths
- Estimated effort: 0.5 days

**Fix #06: SSE Connection Limits**
- Add connection counter, max duration (1hr), idle timeout (5min)
- Estimated effort: 0.5 days

**Fix #21: Introspection Toggle**
- Add `introspection_enabled` config option (default: true for Go compat)
- Estimated effort: 0.5 days

### Phase 6: Backlog (Post-1.0 or As Capacity Allows)

- #13: Data directory permissions (0700) -- 15 minutes
- #24: Identifier length limit (256 chars) -- 15 minutes
- #20: Remove user input from error messages -- 30 minutes
- #34: WASM module size limit -- 15 minutes (if not already covered by #01)
- #35: Debug assertions on key separator components -- 30 minutes
- #03: Schema type/field count limits -- 30 minutes (if not already covered by #01)
- #12: Add `canonicalize()` to remaining file paths -- 30 minutes
- #23: Content-Type enforcement on schema endpoint -- 15 minutes
- #22: Schema migration compatibility guard -- 1 day (design decision)
- #18: Optional strict mode for unknown directives -- 30 minutes

---

## Summary Statistics

| Severity | Count | Actionable | Green/Info |
|----------|-------|------------|------------|
| HIGH | 4 | 4 | 0 |
| MEDIUM | 11 | 11 | 0 |
| LOW | 11 | 11 | 0 |
| GREEN/INFO | 7 | 0 | 7 |
| **Total** | **33** | **26** | **7** |

**Critical path to 1.0**: Phases 1-3 (lens path traversal, HTTP hardening, query limits) should be completed before any public-facing deployment. Phase 4 (WASM sandbox) is required before lens transforms are used with untrusted modules. Phase 5 items are important hardening but not strict blockers if the node runs in a trusted network.
