# 17: Session 2 Summary — Filesystem Operations Audit

## Session Overview

Deep-dive into every code path that reads from or writes to the filesystem based on user-controlled input.

## Findings Summary

| # | Title | Severity | Status |
|---|-------|----------|--------|
| 08 | WASM Lens Module Path Traversal via `file://` Prefix | MEDIUM | Confirmed |
| 09 | FFI Backup Export Writes to Arbitrary Filesystem Path | MEDIUM | Confirmed |
| 10 | CLI File Reading Operations Have No Size Limit | LOW | Confirmed |
| 11 | HTTP Handlers Do Not Accept Filesystem Paths | INFO | Not Vulnerable |
| 12 | No `canonicalize()` or Symlink Resolution on User Paths | LOW | Confirmed |
| 13 | Data Directory Created Without Permission Hardening | LOW | Confirmed |
| 14 | Dump and Purge Commands Are HTTP-Only | INFO | Not Vulnerable |
| 15 | Lens WASM Path Traversal Reachable via HTTP API | HIGH | Confirmed |
| 16 | Null Byte Path Handling in Rust | INFO | Not Vulnerable |

## Critical Finding: Remote File Read via Lens HTTP API

**Finding 15 is the highest-severity issue.** The lens path traversal (finding 08) is reachable by remote attackers through the HTTP API, making it a remote arbitrary file read vulnerability. The attack requires:

1. Network access to the DefraDB HTTP API (default: `localhost:9181`)
2. NAC disabled (default) OR valid `MigrationSet` permission
3. Send `POST /api/v0/lens/set` with `"Path": "file://../../../etc/passwd"`

The file content cannot be returned to the attacker directly (it fails WASM validation), but:
- Error messages may leak file existence and partial content
- In the FFI path, file bytes are fully read before validation
- P2P schema replication can amplify the attack across nodes

## Architecture Assessment

### Positive Patterns

1. **HTTP layer is clean**: The HTTP backup handler explicitly rejects filepath-based operations and returns/accepts data in the response/request body. This is the correct design.
2. **ACP path traversal prevention**: The `crates/acp/` module has thorough path traversal validation for storage key construction, with tests. This shows the team is aware of the pattern.
3. **Keyring path validation**: `KeyName::new()` rejects `..` in key names, preventing path traversal in keyring operations.
4. **Null byte safety**: Rust's type system prevents null byte injection by design.

### Gaps

1. **No shared path validation utility**: Each filesystem operation does its own path handling (or doesn't). A central `validate_path()` function would prevent the inconsistency.
2. **WASM module path is a `String`**: `LensModule.path` is a plain `String` with no structural validation at parse time. It should be a validated type.
3. **File size limits absent**: No CLI command checks file size before reading. The HTTP layer has `MAX_IMPORT_SIZE` but the CLI does not.
4. **Directory permissions**: Default `create_dir_all` permissions are too permissive compared to Go DefraDB.

## Prioritized Remediation

1. **P0 — Block remote lens path traversal**: In the HTTP lens handler, reject configs with `Path` containing `..` or `file://` scheme. Accept only `Module` bytes via HTTP.
2. **P1 — Validate lens paths at parse time**: Add validation to `LensModule` deserialization for `..`, absolute paths, and `file://` scheme.
3. **P1 — Canonicalize FFI backup paths**: Validate and canonicalize paths in the FFI backup export/import functions.
4. **P2 — File size limits**: Add metadata check before `read_to_string` in CLI commands.
5. **P2 — Directory permissions**: Set 0700 on data directory, 0600 on config file.
6. **P3 — P2P lens path stripping**: When deserializing lens configs from P2P, strip path fields.

## Checklist Results

| Check | Result |
|-------|--------|
| WASM module path traversal | VULNERABLE (findings 08, 15) |
| FFI lens path | Same vulnerability via FFI boundary |
| Backup path handling (CLI) | Low risk (user-controlled) |
| Backup path handling (HTTP) | SAFE (data in body, filepath rejected) |
| Backup path handling (FFI) | MEDIUM risk (arbitrary write) |
| Schema file loading | Low risk (CLI-only, no size limit) |
| Query file loading | Low risk (CLI-only, no size limit) |
| Data directory security | No permission hardening |
| Dump command | SAFE (HTTP-only, stdout output) |
| Purge command | SAFE (HTTP-only, --force required) |
| Symlink attacks | No resolution anywhere |
| Null bytes in paths | SAFE (Rust rejects interior nulls) |
| HTTP handler path exposure | Only lens Path field flows to filesystem |
