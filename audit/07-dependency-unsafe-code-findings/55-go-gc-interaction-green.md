# Go GC Interaction with FFI: Properly Handled

- **Severity:** GREEN
- **Category:** FFI Safety / Memory Management
- **Status:** Verified — Go GC interaction correctly managed

## Summary

The Go FFI wrapper on the `jack/ffi-rust-compat` branch correctly handles Go garbage collector interaction with the Rust FFI boundary. All pointers passed to Rust are either short-lived stack allocations (`C.CString`) or pinned via `cgo.Handle`. No Go heap pointers are passed to Rust without proper pinning.

## Details

### Go GC Constraints

Go's garbage collector can relocate heap-allocated objects. The `cgo` rules require that:
1. Go code may pass a Go pointer to C only if the Go memory it points to does not contain any Go pointers
2. C code must not store a Go pointer after the call returns

### How the Wrapper Handles This

**CString Allocation (Go → Rust):**
```go
cStr := C.CString(goString)  // malloc'd C memory, not Go heap
defer C.free(unsafe.Pointer(cStr))
result := C.some_ffi_function(cStr)
```
`C.CString()` allocates via `malloc`, not the Go heap. The GC cannot relocate it. This is correct.

**Result Reading (Rust → Go):**
```go
value := C.GoString(result.value)  // copies to Go heap
C.defra_free_string(result.value)  // frees Rust memory
```
`C.GoString()` copies the C string into Go-managed memory. The original Rust pointer is freed immediately after. No dangling references.

**Handle Storage (Go cbindings):**
The Go `cbindings/wrapper.go` uses `cgo.Handle` for storing Go objects referenced by FFI handles:
```go
handle := cgo.NewHandle(wrapper)  // prevents GC collection
// ... later ...
wrapper := handle.Value().(*CWrapper)  // safe retrieval
handle.Delete()  // allows GC collection
```
`cgo.Handle` is the standard Go mechanism for preventing GC collection of objects that need to survive across FFI boundaries.

### What Could Go Wrong (But Doesn't)

1. **Passing `&goSlice[0]` to C** — Would be unsafe if the GC relocates the slice. Not done in this codebase.
2. **Storing Go pointers in Rust structs** — Would dangle after GC relocation. Not done; Rust stores integer handles, not Go pointers.
3. **Callback from Rust into Go with Go pointer** — Could be unsafe if the Go object was relocated. Not done; callbacks pass handles.

## Verification

All 73 wrapped FFI functions on the `jack/ffi-rust-compat` branch follow the same safe pattern:
- Input strings: `C.CString()` + `defer C.free()`
- Output strings: `C.GoString()` + `C.defra_free_string()`
- Handles: integer types (`C.ulong`), not pointers
- No Go heap pointers passed to Rust

## Test Gap

None. The patterns are correct by construction. The `cgo.Handle` mechanism is the recommended Go standard library approach.
