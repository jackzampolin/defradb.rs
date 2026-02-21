# Audit Stream 5: Input Validation & Injection

## Scope

All external input surfaces and their validation. Audit covers:
- GraphQL query parsing (depth bombs, complexity attacks, introspection)
- HTTP API input sanitization (headers, body, query params)
- CLI argument handling (path traversal, shell injection)
- Schema/SDL parsing and validation
- Storage key construction (key injection)
- Lens transform inputs
- Index query inputs

## Key Questions

- Is there a query depth/complexity limit on GraphQL?
- Can a crafted GraphQL query cause OOM or CPU exhaustion?
- Are HTTP headers properly validated (Content-Type, Auth headers)?
- Can storage keys be crafted to access other collections' data?
- Are file paths in backup/restore sanitized against traversal?
- Is there any string interpolation into queries or key paths?
- Are error messages leaking internal state?

## Crates of Interest

- `query/` (GraphQL parsing and planning)
- `http/` (API request handling)
- `cli/` (argument parsing)
- `schema/` (SDL validation)
- `storage/` (key construction)
- `db/` (backup/restore paths)

## Session 1 Findings (2026-02-21): GraphQL Parser & HTTP Body

| # | Finding | Severity | File |
|---|---------|----------|------|
| 00 | [GraphQL No Depth/Complexity Limits](05-input-validation-findings/00-graphql-no-depth-complexity-limits.md) | HIGH | Updated — parser unbounded, planner has MAX_NESTING_DEPTH=10 |
| 01 | [No HTTP Body Size Limit](05-input-validation-findings/01-no-http-body-size-limit.md) | HIGH | Updated — schema endpoint uses String (no limit), confirmed endpoint-by-endpoint |
| 02 | [Filter Recursion Unbounded](05-input-validation-findings/02-filter-recursion-unbounded.md) | MEDIUM | NEW — `_and`/`_or`/`_not` nest arbitrarily in evaluator |
| 03 | [SDL Schema No Size Limits](05-input-validation-findings/03-sdl-schema-no-size-limits.md) | MEDIUM | NEW — unbounded String body, no type count limit |
| 04 | [Fragment Width Amplification](05-input-validation-findings/04-fragment-width-amplification.md) | LOW | NEW — cycle detection works, width amp is theoretical |
| 05 | [No Query Timeout or Cost Budget](05-input-validation-findings/05-no-query-timeout-or-cost-budget.md) | MEDIUM | NEW — no timeout, no concurrent limit, no rate limit |
| 06 | [SSE Subscription No Limits](05-input-validation-findings/06-sse-subscription-no-limits.md) | MEDIUM | NEW — indefinite connections, per-event query re-execution |
| 07 | [Session 1 Summary](05-input-validation-findings/07-session1-graphql-http-summary.md) | — | Session summary with prioritized remediation |

## Recon Findings

### Surface Area
- **HTTP API**: 60+ endpoints across 26 handler files
- **GraphQL parser**: 8 files (parser.rs 600+ LOC, filters, ordering, mutations, aggregates)
- **CLI**: 50+ command files
- **Schema**: 13 files
- **Storage keys**: 8 key type files
- **Total input surfaces**: 115+

### HTTP API Endpoints
- GraphQL: POST/GET `/api/v0/graphql`, WebSocket
- Collections: CRUD on `/collections/{name}/{docID}`
- Transactions, P2P (15+ routes), ACP/NAC (8 routes)
- Backup: export/import, Schema, Index, Lens, Batch signing, Views

### Validation Approach by Area
- **Identifiers**: STRONG - `validate_identifier()` enforces ASCII letters/underscores/digits
- **Storage keys**: STRONG - Binary varint encoding, 0x00 separators, **injection-proof**
- **HTTP auth**: STRONG - JWT verified, audience checked
- **GraphQL parsing**: MODERATE - Fragment cycle detection, but no depth/complexity limits
- **File paths**: WEAK - PathBuf accepts `../`, no symlink validation
- **Lens/WASM**: WEAK - `file://` prefix stripping without traversal checks

### Red Flags
- **HIGH: No GraphQL depth/complexity limits** - Deeply nested queries accepted without bound
- **HIGH: No HTTP body size limit** - Axum defaults may apply but not explicitly configured
- **MEDIUM: File path traversal** - CLI backup/restore accepts arbitrary paths, no sanitization
- **MEDIUM: WASM module path** - `strip_prefix("file://")` without traversal validation
- **MEDIUM: SDL parsing unbounded** - graphql-parser used directly, no size limits
- **LOW: Backup import** - 100MB limit exists but not configurable
- **LOW: Error messages** echo user input (collection names) - acceptable for GraphQL but review
- **LOW: No rate limiting** in HTTP handler layer

### Green Strengths
- Storage layer completely injection-proof (binary keys)
- Strong identifier validation
- JWT auth is solid
- Backup HTTP endpoint returns body (doesn't write to server filesystem)

## Session 2 Findings (2026-02-21): Filesystem Operations

| # | Finding | Severity | File |
|---|---------|----------|------|
| 08 | [WASM Lens Module Path Traversal via `file://`](05-input-validation-findings/08-wasm-lens-path-traversal.md) | MEDIUM | `lens/src/wasm.rs`, `ffi/src/lens.rs` — no validation after stripping file:// |
| 09 | [FFI Backup Export Writes to Arbitrary Path](05-input-validation-findings/09-ffi-backup-arbitrary-path-write.md) | MEDIUM | `ffi/src/backup/export.rs` — fs::write on attacker-controlled filepath |
| 10 | [CLI File Reading No Size Limit](05-input-validation-findings/10-cli-file-read-no-size-limit.md) | LOW | Multiple CLI commands — fs::read_to_string unbounded |
| 11 | [HTTP Handlers No Filesystem Exposure](05-input-validation-findings/11-http-handlers-no-filesystem-exposure.md) | INFO | GREEN — no HTTP endpoint accepts filesystem paths |
| 12 | [No canonicalize() or Symlink Resolution](05-input-validation-findings/12-no-canonicalize-or-symlink-resolution.md) | LOW | No production code resolves symlinks on user paths |
| 13 | [Data Directory No Permission Hardening](05-input-validation-findings/13-data-directory-no-permission-hardening.md) | LOW | `config/mod.rs` — create_dir_all with default 0755 |
| 14 | [Dump and Purge Safe HTTP-Only](05-input-validation-findings/14-dump-purge-safe-http-only.md) | INFO | GREEN — HTTP-only, no filesystem path parameters |
| 15 | [Lens Path Traversal Reachable via HTTP API](05-input-validation-findings/15-lens-path-reachable-via-http-api.md) | HIGH | HTTP `/api/v0/lens/set` → lens path → arbitrary file read |
| 16 | [Null Byte Path Handling](05-input-validation-findings/16-null-byte-path-handling.md) | INFO | GREEN — Rust rejects interior null bytes |
| 17 | [Session 2 Summary](05-input-validation-findings/17-session2-filesystem-ops-summary.md) | — | Session summary with prioritized remediation |

## Session 3 Findings (2026-02-21): Schema Validation, Multiaddr, Error Leakage

| # | Finding | Severity | File |
|---|---------|----------|------|
| 18 | [Unknown Directives Silently Accepted](05-input-validation-findings/18-unknown-directives-silently-accepted.md) | LOW | `sdl_parse/fields.rs` — forward-compat, no strict mode |
| 19 | [Multiaddr SSRF — No Private IP Blocklist](05-input-validation-findings/19-multiaddr-ssrf-no-ip-blocklist.md) | MEDIUM | `validation.rs` → `p2p/address.rs` → libp2p — zero IP validation |
| 20 | [Error Messages Echo User Input](05-input-validation-findings/20-error-messages-echo-user-input.md) | LOW | `backup.rs`, `error.rs`, `query.rs` — filepath/serde details reflected |
| 21 | [GraphQL Introspection Always Enabled](05-input-validation-findings/21-graphql-introspection-always-enabled.md) | MEDIUM | `runner/introspection/` — full schema enumerable by any reader |
| 22 | [Schema No Field Drop Migration Guard](05-input-validation-findings/22-schema-no-field-drop-migration-guard.md) | LOW | `schema/collection.rs` — no backward-compat validation |
| 23 | [Content-Type Not Enforced on Schema Endpoint](05-input-validation-findings/23-content-type-not-enforced-on-schema-endpoint.md) | LOW | `handlers/schema.rs` — accepts any Content-Type |
| 24 | [Identifiers Accept Unbounded Length](05-input-validation-findings/24-identifier-no-length-limit.md) | LOW | `validation.rs` — no max length on names |
| 25 | [Error Responses Safe — JSON Content-Type](05-input-validation-findings/25-error-response-safe-json-content-type.md) | INFO | GREEN — XSS/CRLF mitigated by application/json |
| 26 | [Schema Not Replicated via P2P](05-input-validation-findings/26-schema-not-replicated-via-p2p.md) | INFO | GREEN — no P2P schema injection vector |
| 27 | [Directive Args Not Stored or Evaluated](05-input-validation-findings/27-directive-args-not-stored-or-evaluated.md) | INFO | GREEN — type-checked, never executed |
| 28 | [Circular References Properly Detected](05-input-validation-findings/28-circular-references-properly-detected.md) | INFO | GREEN — Tarjan's SCC algorithm |
| 29 | [Session 3 Summary](05-input-validation-findings/29-session3-schema-multiaddr-errors-summary.md) | — | Session summary with prioritized remediation |

## Estimated Scope

**MEDIUM: 3-5 sessions**

### Session 1: GraphQL Parser Limits + HTTP Body (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/query/src/query_parse/parser.rs` | 127-194, 311-318 | Recursive `parse_selection_to_selects()`, no depth limit |
| `crates/query/src/query_parse/filters.rs` | all | Nested filters recursion |
| `crates/query/src/query_parse/mutations.rs` | 303+ | `parse_document_input()` recursive field parsing |
| `crates/query/src/sdl_parse/parser.rs` | 131-138 | SDL `parse_with_warnings()` - no size limits |
| `crates/http/src/server.rs` | 343-403 | Router: only TraceLayer + CorsLayer, **no DefaultBodyLimit** |
| `crates/http/src/handlers/graphql/query.rs` | 96-111, 128-162 | GraphQL handlers, Json extractor |
| `crates/http/src/handlers/backup.rs` | 30-34, 168-173 | Backup import: 100MB limit (only endpoint with one) |

**Checklist**: Depth bomb test, width bomb test, fragment explosion, SDL 10K types, 1GB body, streaming chunked

### Session 2: Filesystem Operations (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/cli/src/commands/client/backup.rs` | 61-107 | **No path validation** - `fs::write`/`fs::read_to_string` on arbitrary PathBuf |
| `crates/cli/src/commands/client/schema.rs` | all | Multiple Vec<PathBuf> files, no traversal checks |
| `crates/cli/src/commands/client/query.rs` | 69 | `fs::read_to_string(path)` - no validation |
| `crates/cli/src/commands/client/mod.rs` | 60-66 | `get_data_from_args()` utility |
| `crates/lens/src/wasm.rs` | 68-89 | **file:// prefix strip without validation** |
| `crates/ffi/src/lens.rs` | 27-30 | Same file:// vulnerability in FFI |

**Checklist**: `../` traversal, absolute path, symlink, `/dev/zero`, Unicode traversal, WASM from arbitrary path

### Session 3: Schema Validation + P2P Multiaddr + Error Leakage (MEDIUM)

| File | Lines | Focus |
|------|-------|-------|
| `crates/query/src/sdl_parse/directives.rs` | all | Custom directives without whitelist |
| `crates/http/src/validation.rs` | 46-63 | `validate_multiaddr()` - only checks starts with `/` |
| `crates/query/src/query_parse/parser.rs` | 78-100 | Error messages echo collection/field names |
| `crates/http/src/handlers/backup.rs` | 188-193 | Error echoes filepath |

**Checklist**: Unknown directives, null bytes in multiaddr, SSRF via internal addresses, CRLF in error messages

### Session 4: Storage Keys + Lens Isolation + Rate Limiting (LOW)

| File | Focus |
|------|-------|
| `crates/storage/src/keys/` | Binary varint encoding - SAFE (injection-proof) |
| `crates/lens/src/wasm.rs` | 100+ | wasmtime sandbox, memory/CPU limits |
| `crates/http/src/server.rs` | No rate limiting middleware |
| `crates/http/src/nac_guard.rs` | Checks permissions, not rate |

**Checklist**: Storage keys confirmed safe, WASM syscall restrictions, infinite loop timeout, no rate limiting
