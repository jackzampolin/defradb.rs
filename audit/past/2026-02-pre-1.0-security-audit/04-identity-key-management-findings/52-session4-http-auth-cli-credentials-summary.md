# Session 4 Summary: HTTP Authentication Middleware & CLI Credential Handling

## Scope

Deep audit of the HTTP identity extraction layer and CLI credential handling:
- Bearer token parsing and edge cases
- Host header audience validation
- Anonymous access paths
- Error response information leakage
- CORS interaction with authentication
- Middleware ordering and coverage
- CLI credential safety (key generation, storage, transmission)
- Client-side auth header construction

## Files Audited

| File | Lines | Focus |
|------|-------|-------|
| `crates/http/src/identity_extractor.rs` | 1-409 | Bearer parsing, Host extraction, JWT verification, FromRequestParts |
| `crates/http/src/server.rs` | 1-508 | CORS configuration, middleware layering |
| `crates/http/src/nac_guard.rs` | 1-89 | NAC permission checks, anonymous fallback |
| `crates/http/src/validation.rs` | 1-222 | Input validation utilities |
| `crates/http/src/error.rs` | 1-95 | Error types, 401/403 semantics |
| `crates/http/src/query_context.rs` | 1-131 | Signing config, DAC bypass context |
| `crates/http/src/router/routes.rs` | 1-259 | All route registrations |
| `crates/http/src/handlers/utility.rs` | 1-106 | Dump, purge endpoints |
| `crates/http/src/handlers/backup.rs` | 1-341 | Backup export/import |
| `crates/http/src/handlers/collections.rs` | 1-498 | Collection CRUD |
| `crates/http/src/handlers/documents.rs` | 1-216 | Document CRUD |
| `crates/http/src/handlers/graphql/query.rs` | 1-354 | GraphQL query/mutation/subscription |
| `crates/http/tests/identity_extractor_tests.rs` | 1-298 | Extractor unit tests |
| `crates/cli/src/commands/keyring_cmd.rs` | 1-284 | Key generation, export, import |
| `crates/cli/src/commands/identity.rs` | 1-413 | Identity new, export, import, delete |
| `crates/cli/src/commands/client/mod.rs` | 1-283 | Auth token generation, client context |
| `crates/cli/src/commands/client/http_client/mod.rs` | 1-381 | HTTP client, auth header attachment |
| `crates/cli/src/commands/mod.rs` | 1-107 | Keyring opening, secret loading |

## Findings

### High Severity (1)
| # | Finding | Status |
|---|---------|--------|
| 37 | Debug dump endpoint has no identity or NAC check | Confirmed |

### Medium Severity (6)
| # | Finding | Status |
|---|---------|--------|
| 36 | Empty Bearer token treated as anonymous | Confirmed (Go compat) |
| 40 | CORS allows wildcard origin with auth header | Confirmed (safe, Go compat) |
| 41 | No X-Forwarded-Host support for audience validation | Confirmed |
| 42 | Private key passed as CLI argument visible in process table | Confirmed |
| 45 | Identity extraction is per-handler, not global middleware | Confirmed |
| 51 | Key type ambiguity for 32-byte keys | Confirmed (Go compat) |

### Low Severity (5)
| # | Finding | Status |
|---|---------|--------|
| 35 | Bearer prefix incomplete case-insensitivity | Confirmed (Go compat) |
| 38 | 403 error response leaks failure reason | Confirmed |
| 44 | WebSocket endpoint registered without auth | Confirmed (returns 501) |
| 48 | keyring export prints raw key hex to stdout | Expected behavior |
| 50 | Multiple Authorization headers: first wins | Framework behavior |

### Info (3)
| # | Finding | Status |
|---|---------|--------|
| 39 | 403 not 401 for invalid credentials | Intentional Go compat |
| 46 | Host header audience exact match, no port normalization | Green |
| 49 | Identity extraction before body read | Green |

### Also Confirmed (from prior sessions)
| # | Finding | Status |
|---|---------|--------|
| 27 | Private key printed to stdout (identity new) | Additional context in 43 |
| 47 | keyring import accepts key on CLI argument | Confirmed |

## Architecture Assessment

### Strengths
1. **Identity extraction before body read** — Axum's `FromRequestParts` guarantees auth is checked before body consumption, preventing pre-auth DoS
2. **Host header audience binding** — tokens are bound to a specific host, preventing token reuse across different nodes
3. **Missing host + token = reject** — correctly prevents audience bypass by omitting Host header
4. **NAC wildcard DID fallback** — anonymous requests check wildcard permissions before rejecting
5. **Auth token generation** — 15-minute expiration, audience binding, proper scheme stripping
6. **Keyring integration** — `--identity-name` provides a safe alternative to passing keys on CLI

### Weaknesses
1. **No global auth middleware** — each handler must opt-in to identity extraction; forgetting it creates an unauthenticated endpoint (demonstrated by dump endpoint)
2. **Error message detail** — 403 responses distinguish failure modes, aiding attacker enumeration
3. **CLI credential exposure** — Go-compatible `--identity` flag exposes keys in process table
4. **No proxy header support** — reverse proxy deployments break audience validation
5. **32-byte key ambiguity** — length-based key type detection can't distinguish secp256k1 from secp256r1

### Test Coverage
The identity extractor has good unit test coverage (14 tests) covering:
- No auth → anonymous
- Empty bearer → anonymous
- Non-bearer auth → error
- Valid token → DID extracted
- Lowercase bearer → works
- Invalid token → error
- Missing host with token → error
- Missing host without token → ok
- Wrong host → audience mismatch
- Leading/trailing whitespace → trimmed
- BEARER (uppercase) → rejected

Missing tests:
- Multiple Authorization headers
- IPv6 Host header
- Empty Host header with token
- Specific error response body content validation
