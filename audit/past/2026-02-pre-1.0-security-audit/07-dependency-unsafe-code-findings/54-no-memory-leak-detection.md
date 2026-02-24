# No Memory Leak Detection in CI

- **Severity:** LOW
- **Category:** Test Infrastructure / Memory Safety
- **Status:** Confirmed — no sanitizer or leak detection tooling in CI

## Summary

Neither AddressSanitizer (ASan), LeakSanitizer (LSan), MemorySanitizer (MSan), nor Valgrind is configured for any test target. The FFI boundary involves cross-language memory ownership (Rust allocates CStrings, Go frees via `defra_free_string`), making memory leaks possible if either side fails to free. These would be undetected in the current test infrastructure.

## Details

### Memory Ownership Boundaries

The FFI boundary has two ownership transfer patterns:

1. **Go → Rust:** Go allocates via `C.CString()`, Rust reads, Go frees with `defer C.free()`. If Go fails to free (e.g., early return), the memory leaks.

2. **Rust → Go:** Rust allocates via `CString::into_raw()`, Go reads via `C.GoString()` (copies), Go frees via `C.defra_free_string()`. If Go fails to free, the memory leaks.

On the `jack/ffi-rust-compat` branch, the Go wrapper consistently follows both patterns correctly. But the absence of automated leak detection means regressions would be silent.

### What's Missing

| Tool | Purpose | Configured? |
|------|---------|-------------|
| ASan (AddressSanitizer) | Buffer overflows, use-after-free | No |
| LSan (LeakSanitizer) | Memory leaks | No |
| MSan (MemorySanitizer) | Uninitialized memory reads | No |
| Valgrind | Comprehensive memory analysis | No |
| Miri | Rust-specific UB detection | No |

### Rust-Specific Considerations

- Rust's ownership model prevents most memory leaks in pure Rust code
- The FFI boundary is the primary leak vector since `CString::into_raw()` transfers ownership to C
- `defra_free_string` correctly reconstitutes and drops the CString, but only if called
- `mem::forget` is not used in the FFI crate — no intentional leaks

### Integration with Go's Leak Detection

Go's built-in `goleak` package (uber/goleak) detects goroutine leaks but not C memory leaks. Even if the Go test suite uses `goleak`, it would not detect leaked CStrings from Rust.

## Remediation

1. **Add ASan+LSan to Rust FFI tests:**
   ```bash
   RUSTFLAGS="-Zsanitizer=address" cargo test -p ffi --target x86_64-unknown-linux-gnu
   ```
   (Requires nightly Rust and Linux)

2. **Run Miri on pure Rust tests:**
   ```bash
   cargo +nightly miri test -p ffi
   ```
   (Cannot test `extern "C"` functions directly, but can test internal logic)

3. **Add Valgrind to CI for Go FFI tests:**
   ```bash
   valgrind --leak-check=full go test -tags=rust_ffi ./tests/integration/...
   ```

## Test Gap

No automated detection of memory leaks at the FFI boundary. Manual code review confirms correct patterns on the feature branch, but regressions would be silent.
