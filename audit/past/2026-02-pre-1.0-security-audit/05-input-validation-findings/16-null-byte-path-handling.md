# 16: Null Byte Path Handling in Rust (GREEN)

| Field    | Value |
|----------|-------|
| Severity | INFO |
| Category | Null Byte Injection |
| Status   | Not Vulnerable |

## Summary

Rust's `std::path::PathBuf` and `std::fs` operations are **not vulnerable** to null byte injection. Unlike C strings, Rust paths are byte sequences (on Unix) or UTF-16 sequences (on Windows) with explicit length. Null bytes are not treated as terminators.

## Analysis

### How Null Bytes Work in Rust Paths

In C, `open("file\x00.json", ...)` would truncate to `open("file", ...)` because C strings are null-terminated. In Rust:

```rust
let path = PathBuf::from("file\x00.json");
// path contains the bytes: ['f','i','l','e','\0','.','j','s','o','n']
```

When this path is passed to OS APIs, Rust converts it to a `CString`, which checks for interior null bytes and **returns an error**:

```rust
std::fs::read_to_string("file\x00.json")
// Returns Err(InvalidInput: "interior nul byte found")
```

### Where Paths Enter the System

1. **CLI arguments** (clap `PathBuf`): Command-line arguments cannot contain null bytes on Unix (they're `\0`-terminated by the shell). Clap parses these as `OsString`, which cannot embed nulls from shell arguments.

2. **HTTP request bodies** (JSON strings): JSON strings are UTF-8. A literal null byte (`\u0000`) would be valid JSON but when used as a `PathBuf`, the OS call would reject it with "interior nul byte found".

3. **FFI C strings** (`*const c_char`): `CStr::from_ptr()` stops at the first null byte, but this is the correct behavior for C interop. The path is truncated at null, which is the C caller's expectation.

4. **Deserialized `LensModule.path`** (serde JSON string): Same as HTTP — a JSON `\u0000` would be preserved in the Rust `String`, but `Path::new()` → `CString::new()` would reject it.

### Verification

```rust
// This would fail with "interior nul byte found"
let result = std::fs::read_to_string("file\x00.json");
assert!(result.is_err());
```

## Conclusion

Rust's memory safety and explicit CString conversion prevent null byte injection attacks on filesystem paths. No remediation needed.
