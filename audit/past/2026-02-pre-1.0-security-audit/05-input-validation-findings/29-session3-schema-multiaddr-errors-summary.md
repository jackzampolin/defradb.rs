# Session 3 Summary: Schema Validation, Multiaddr, Error Leakage

**Date**: 2026-02-21
**Scope**: SDL directive handling, multiaddr validation, error message information disclosure, GraphQL introspection, schema migration integrity

## Findings Overview

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| 18 | [Unknown Directives Silently Accepted](18-unknown-directives-silently-accepted.md) | LOW | Deliberate forward-compat, but no strict mode |
| 19 | [Multiaddr SSRF — No Private IP Blocklist](19-multiaddr-ssrf-no-ip-blocklist.md) | MEDIUM | No IP validation at any layer |
| 20 | [Error Messages Echo User Input](20-error-messages-echo-user-input.md) | LOW | Filepath, serde details reflected |
| 21 | [GraphQL Introspection Always Enabled](21-graphql-introspection-always-enabled.md) | MEDIUM | Full schema enumerable by any reader |
| 22 | [Schema No Field Drop Migration Guard](22-schema-no-field-drop-migration-guard.md) | LOW | No backward-compat validation |
| 23 | [Content-Type Not Enforced on Schema Endpoint](23-content-type-not-enforced-on-schema-endpoint.md) | LOW | Raw String body, any Content-Type |
| 24 | [Identifiers Accept Unbounded Length](24-identifier-no-length-limit.md) | LOW | No max length on collection/field names |
| 25 | [Error Responses Safe — JSON Content-Type](25-error-response-safe-json-content-type.md) | INFO | GREEN — XSS/CRLF mitigated |
| 26 | [Schema Not Replicated via P2P](26-schema-not-replicated-via-p2p.md) | INFO | GREEN — no P2P schema injection |
| 27 | [Directive Args Not Stored or Evaluated](27-directive-args-not-stored-or-evaluated.md) | INFO | GREEN — no code execution risk |
| 28 | [Circular References Properly Detected](28-circular-references-properly-detected.md) | INFO | GREEN — Tarjan's SCC algorithm |

## Severity Distribution

- **MEDIUM**: 2 (multiaddr SSRF, introspection)
- **LOW**: 5 (directives, error echo, migration, content-type, identifier length)
- **INFO/GREEN**: 4 (JSON safety, P2P schema, directive safety, cycle detection)

## Key Findings

### 1. Multiaddr SSRF (MEDIUM)

The `validate_multiaddr()` function only checks `starts_with('/')`. No IP range validation exists at any layer — HTTP handler, P2P adapter, or libp2p. An authenticated user with P2P permissions can probe localhost, cloud metadata (169.254.169.254), and private networks (10.x, 172.16.x, 192.168.x).

**Remediation**: Add `is_private_ip()` check to `validate_multiaddr()` and add max length (1024 bytes).

### 2. GraphQL Introspection (MEDIUM)

Introspection is always enabled. Any user with `DocumentRead` permission (or anyone when NAC is disabled) can enumerate all collection names, field types, and the complete type system — including ACP-protected collection names.

**Remediation**: Add configurable introspection toggle.

### 3. Strong Positives

- **Error responses are JSON** — Content-Type prevents XSS
- **Schemas not replicated via P2P** — no peer injection vector
- **Directive arguments are safe** — type-checked, not evaluated as code
- **Circular references detected** — Tarjan's algorithm prevents infinite loops
- **Policy validation is thorough** — rejects path traversal, null bytes, path separators

## Prioritized Remediation

### Immediate (Day 1)
1. Remove filepath from backup error message (finding #20) — 5 min fix
2. Add max length to `validate_multiaddr()` — 10 min fix

### Short Term (Week 1)
3. Add private IP blocklist to multiaddr validation (finding #19)
4. Add `MAX_IDENTIFIER_LENGTH = 256` (finding #24)
5. Add introspection toggle configuration (finding #21)

### Medium Term (Sprint)
6. Add strict mode for SDL directive validation (finding #18)
7. Add schema migration backward-compatibility checks (finding #22)
8. Add Content-Type enforcement on text body endpoints (finding #23)

## Checklist Results

| Check | Result |
|-------|--------|
| Unknown directives rejected? | No — silently accepted with warning |
| SDL size limits? | No — covered in finding #03 |
| Circular type references? | Safe — Tarjan's SCC detection |
| Schema migration validated? | No — field drops allowed |
| Multiaddr IP validation? | No — SSRF possible |
| Error message leakage? | Minor — filepath and serde details |
| XSS via errors? | Safe — JSON Content-Type |
| CRLF injection? | Safe — Rust/Axum prevent |
| Introspection gated? | No — always enabled |
| P2P schema injection? | Safe — schemas not replicated |
| Content-Type enforced? | No on text body endpoints |
| Identifier length limited? | No |

## Next Session

Session 4 should cover: Storage key construction (binary encoding — expected safe), WASM lens isolation (memory/CPU limits, syscall restrictions), and rate limiting (none exists).
