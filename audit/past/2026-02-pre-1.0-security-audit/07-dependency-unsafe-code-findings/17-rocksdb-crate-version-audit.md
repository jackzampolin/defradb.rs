# RocksDB Crate Version and Known Issues

**Severity**: Low
**Category**: Dependency Audit
**Status**: Current version, no known CVEs

## Summary

The project uses `rocksdb` crate version **0.22.0**, which is the latest stable release. This crate is a thin Rust wrapper around the C++ RocksDB library and contains extensive unsafe code by necessity (FFI to C++). No known soundness CVEs affect this version.

## Details

### Version

- **Cargo.toml**: `rocksdb = "0.22"` (workspace dependency)
- **Cargo.lock**: `rocksdb 0.22.0`
- **Source**: `registry+https://github.com/rust-lang/crates.io-index`

### The `rocksdb` Crate's Unsafe Profile

The `rocksdb` crate is a C++ FFI wrapper — virtually all of its code is unsafe by definition. It wraps:
- `rocksdb::DB::Open()` → constructor
- `rocksdb::DB::Get()` / `Put()` / `Delete()` → data operations
- `rocksdb::DB::GetSnapshot()` → snapshot creation
- `rocksdb::Iterator` → range scans
- `rocksdb::WriteBatch` → atomic writes

The crate provides safe Rust APIs that handle memory management (via RAII) and lifetime tracking. The defradb.rs project uses these safe APIs, with the sole exception of the `OwnedSnapshot` transmute (Finding #12).

### Types Used

| rocksdb Type | Usage in defradb.rs |
|-------------|-------------------|
| `OptimisticTransactionDB` | Main DB handle, wrapped in `Arc` |
| `SnapshotWithThreadMode` | Snapshot for reads, lifetime-extended via transmute |
| `DBIteratorWithThreadMode` | Used transiently during iterator materialization |
| `WriteBatchWithTransaction` | Atomic commit of pending changes |
| `Options` / `BlockBasedOptions` | Configuration at open time |
| `ReadOptions` / `WriteOptions` | Per-operation configuration |
| `Cache` | LRU block cache |

### Known Issues

- No CVEs listed for `rocksdb` 0.22.0 in RustSec Advisory Database
- The underlying C++ RocksDB library version is determined by the `librocksdb-sys` crate, which bundles the C++ source and compiles it during build
- RocksDB C++ has had historical issues with data corruption under specific compaction configurations, but these are not Rust-specific

### Feature Flags

The project uses `rocksdb` with default features (no explicit feature selection in Cargo.toml). The backend is gated behind the `rocksdb` feature flag in the storage crate.

## Remediation

None needed. Keep the dependency updated as new versions are released.

## Test Gap

- No fuzz testing against the RocksDB backend specifically
- Integration tests exercise the RocksDB backend when run with `--features rocksdb`
