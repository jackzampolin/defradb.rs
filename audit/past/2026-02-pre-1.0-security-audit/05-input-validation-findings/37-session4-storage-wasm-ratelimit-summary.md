# Session 4 Summary: Storage Keys, WASM Sandbox, Rate Limiting

## Session Scope

Final session of the Input Validation & Injection audit stream. Validated the storage key construction layer, audited the WASM sandbox for lens transforms, and documented the absence of HTTP rate limiting.

## Findings

| # | Title | Severity | Status |
|---|-------|----------|--------|
| 30 | Storage Key Construction Verified Injection-Proof | GREEN | Safe |
| 31 | WASM Sandbox Has No Memory, CPU, or Syscall Restrictions | HIGH | Confirmed |
| 32 | No HTTP Rate Limiting, Request Timeout, or Connection Limits | MEDIUM | Confirmed |
| 33 | Lens Transform Output Not Validated Against Schema | MEDIUM | Confirmed |
| 34 | No Size Limit on WASM Module Binaries | LOW | Confirmed |
| 35 | String-Based Keys Use `/` Separator Without Escaping | LOW | Mitigated |
| 36 | WASM Transform Execution Blocks Tokio Worker Thread | MEDIUM | Confirmed |

## Key Conclusions

### Storage Keys: Strong (Verified)

The storage key layer is well-designed with three complementary defense mechanisms:

1. **Namespace byte prefixes** (single-byte `d/b/h/s/p/e/a`) provide hard isolation between stores. Iterators are always scoped to a namespace prefix, preventing cross-namespace data leakage.

2. **CockroachDB-style varint encoding** for integer components (collection IDs, index IDs) maintains sort order and cannot produce separator bytes. The encoding is self-delimiting.

3. **Input validation** (`validate_identifier()`) restricts collection/field names to `[A-Za-z_][A-Za-z0-9_]*`, preventing separator injection at the API boundary.

4. **String encoding** for index keys uses null-byte escaping (`0x00 0xFF` escape, `0x00 0x00` terminator), preventing embedded null bytes from creating ambiguous keys.

One minor concern (finding 35): headstore, peerstore, and systemstore keys use unescaped string formatting with `/` separators. This is currently safe because all inputs are validated upstream, but lacks defense-in-depth.

### WASM Sandbox: Weak (Needs Remediation)

The wasmtime integration has correct WASI isolation (no filesystem, network, or syscall access) but lacks all resource controls:

- **No memory limits** — WASM modules can grow memory unboundedly
- **No CPU limits** — No fuel metering, infinite loops block forever
- **No execution timeout** — No wall-clock limit on transform duration
- **No output cap** — Transform output loop has no iteration limit
- **Blocks tokio threads** — Synchronous WASM calls run on async worker threads

This is the highest-severity cluster in this session. A single malicious lens transform can DoS the entire node.

### Rate Limiting: Absent (Known Gap)

No rate limiting, request timeout, or connection limit middleware exists in the HTTP stack. Combined with the expensive query endpoints identified in Sessions 1-3 (GraphQL depth amplification, filter recursion, fragment width amplification), this creates a significant DoS surface for public-facing deployments.
