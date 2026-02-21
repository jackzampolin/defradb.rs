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
