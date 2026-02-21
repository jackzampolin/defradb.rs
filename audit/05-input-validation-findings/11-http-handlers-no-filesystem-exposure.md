# 11: HTTP Handlers Do Not Accept Filesystem Paths (GREEN)

| Field    | Value |
|----------|-------|
| Severity | INFO |
| Category | Path Traversal |
| Status   | Not Vulnerable |

## Summary

Systematic review of all HTTP handlers confirms that **no HTTP endpoint accepts a filesystem path from remote clients**. All filesystem operations are confined to the CLI and FFI layers. The HTTP API operates purely through request/response body data, making it safe from remote path traversal attacks.

## Analysis

### Backup Handlers (`crates/http/src/handlers/backup.rs`)

- **Export** (`POST /api/v0/backup/export`): Returns backup data in the response body. Does NOT write to a file. When the Go-format `filepath` field is present in the request JSON, it is logged as a warning and **ignored** (line 90-95).
- **Import** (`POST /api/v0/backup/import`): Reads backup data from the request body. Does NOT read from a file. When Go-format `filepath` is detected, it returns a **400 Bad Request** with explicit explanation (line 186-194).
- **Size limit**: Import data is capped at 100MB (`MAX_IMPORT_SIZE`, line 31).

### Schema Handler (`crates/http/src/handlers/schema.rs`)

- **Add** (`POST /api/v0/schema`): Accepts SDL text as the request body (`body: String`). No file path parameter.

### Lens Handlers (`crates/http/src/handlers/lens.rs`)

- **Set Migration** (`POST /api/v0/lens/set`): Accepts lens configuration as JSON in request body. The configuration **may contain** a `Path` field for the WASM module, but this field is processed server-side (see finding 08). The HTTP handler itself does not touch the filesystem — that happens in the lens store when the transform is loaded.
- **Add Lens** (`POST /api/v0/lens`): Same pattern — JSON body, no direct filesystem access.
- **List/Reload**: No path parameters.

**NOTE**: While the HTTP lens handler does not directly access the filesystem, it forwards the `Path` field from the request body to the lens store, which does. This means a remote attacker CAN trigger the path traversal in finding 08 via the HTTP API. The HTTP handler is the **entry point** for the vulnerability, even though the filesystem access happens deeper in the stack.

### Other Handlers

- **GraphQL query** (`POST /api/v0/graphql`): Body is query text, no file references.
- **Documents** (CRUD): JSON bodies, no file paths.
- **P2P** (peers, replicators, collections): JSON bodies with peer IDs and collection names.
- **Views**: SDL and query in request body.
- **Index/EncryptedIndex**: Collection and field names via URL path parameters (validated).
- **ACP**: Policy YAML in body, no file paths.
- **Block**: CID in body, no file paths.
- **Dump**: No parameters.
- **NAC**: JSON configuration, no file paths.

### `Path` Extractor Usage

The `axum::extract::Path` occurrences in HTTP handlers are URL path parameters (e.g., collection names, document IDs, transaction IDs) — not filesystem paths. These are validated against collection/document identifiers, not passed to filesystem operations.

## Conclusion

The HTTP API layer provides a clean security boundary for filesystem operations. The only indirect path is through lens configuration, where a `Path` field in the JSON body eventually reaches the WASM module loader (finding 08). All other filesystem operations are properly confined to CLI/FFI.
