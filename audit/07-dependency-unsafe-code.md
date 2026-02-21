# Audit Stream 7: Dependency & Unsafe Code Audit

## Scope

Supply chain security and unsafe code usage. Audit covers:
- `cargo audit` for known CVEs in dependencies
- `unsafe` block inventory with justification review
- FFI boundary safety (Go FFI, C FFI)
- Dependency tree analysis (transitive risk)
- Build script security (`build.rs` files)
- Feature flag combinations and their security implications
- Minimum supported Rust version and compiler guarantees

## Key Questions

- Are there any known CVEs in our dependency tree?
- Is every `unsafe` block justified and sound?
- Are FFI boundaries properly validating inputs/outputs?
- Are there dependencies with broad permissions (network, fs) that shouldn't have them?
- Do any build scripts execute external commands?
- Are there yanked or unmaintained dependencies?
- Is `#[deny(unsafe_code)]` used where appropriate?

## Crates of Interest

- All crates (dependency scan)
- `storage/` (rocksdb FFI)
- Any crate with `unsafe` blocks
- `Cargo.toml` / `Cargo.lock` (dependency analysis)

## Recon Findings

### Unsafe Block Inventory: 522 total occurrences
- **FFI crate**: 434 (83%) - 84 `pub unsafe extern "C"` functions, all justified
- **db crate**: 26 - Mostly naming convention ("unsafe_policy_transition") + ~6 actual blocks
- **query crate**: 5 - Fat pointer reconstruction for DocFetcher trait objects
- **storage/rocksdb**: 3 - Lifetime transmute for OwnedSnapshot (Send/Sync)
- **integration-test**: 1 - Child process handling

### FFI Boundaries
- 84 C FFI entry points across 22 modules in `crates/ffi/src/`
- Build tool: `cbindgen 0.28` generates C headers
- Architecture: Rust FFI <-> C Header (defra.h) <-> Go (CGO)
- Pointer model: Opaque handles (usize), not raw struct pointers

### Build Scripts: 3 (all safe)
- `orbis/build.rs` - Protobuf compilation (tonic)
- `defra-version/build.rs` - Git metadata (read-only)
- `db/src/block_builder/build.rs` - Misidentified (not actual build.rs)

### Dependency Profile
- **Direct workspace deps**: 60
- **Transitive dependencies**: 898 packages (from Cargo.lock)
- **Native/C bindings**: rocksdb 0.22 (optional, C++ FFI)
- **Network-facing**: axum 0.7, hyper 1.0, libp2p 0.53, tonic 0.12
- **Crypto**: ed25519-dalek 2.1, k256 0.13, aes-gcm 0.10, sha2 0.10 (all modern)
- **Storage**: redb 2.4 (default, pure Rust), fjall 3 (pure Rust), rocksdb 0.22 (optional C++)

### Feature Flags
- Storage: `redb` (default) / `fjall` / `rocksdb` / `leveldb` (WASM only)
- Runtime: `p2p` + `native` (default), can disable P2P for headless
- `vendored-openssl` optional for CLI

### `#![deny(unsafe_code)]`: NOT USED (acceptable given FFI crate)

### Red Flags: NONE
- Unsafe well-concentrated and justified (83% in FFI boundary)
- No risky build scripts
- Default storage is pure Rust (redb)
- All crypto deps are modern and audited

### Yellow Flags
- **LOW: RocksDB** - Optional C++ dep with platform-specific compilation
- **LOW: FFI pointer contracts** - Require discipline from Go caller side
- **LOW: Feature interaction matrix** - 4 backends + optional P2P untested in all combos
- **LOW: Fat pointer transmute** in query crate (safe but in hot path)

## Estimated Scope

**MEDIUM: 3-5 sessions**

### Session 1: FFI Boundary Review (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/ffi/src/types.rs` | 1-250 | `FfiResult`, `sanitize_to_cstring()`, `defra_free_string()`, `c_str_to_string()` |
| `crates/ffi/src/node.rs` | 25-150 | `new_node()` pointer validation, raw slice creation for signing_private_key |
| `crates/ffi/src/txn/lifecycle.rs` | 63, 96 | `commit_txn()`/`rollback_txn()` - `require_c_str()` safety |
| `crates/ffi/src/query/exec.rs` | 39-80 | `exec_request()` pointer validation, DID parsing |
| `crates/ffi/src/batch.rs` | 25-100 | Batch signing FFI, identity resolution |
| `crates/ffi/src/helpers.rs` | 1-36 | `require_c_str()` null check, registry lookups |
| `crates/ffi/src/state/registry.rs` | 1-100 | Handle allocation (starts at 1), RwLock, TOCTOU prevention |

**Checklist**: Null pointer before deref, CString ownership transfer, handle reuse prevention, concurrent access safety

### Session 2: Storage & RocksDB Unsafe Code (MEDIUM)

| File | Lines | Focus |
|------|-------|-------|
| `crates/storage/src/backends/rocksdb/transaction.rs` | 15-56 | **OwnedSnapshot transmute** (`&'_` to `'static`), Arc<DB> lifetime guarantee |
| `crates/storage/src/backends/rocksdb/transaction.rs` | 189-254 | Iterator lifetime safety with transmute |
| `crates/query/src/runner/fetcher.rs` | 32-75 | **FetcherWrapper fat pointer** - `*const dyn DocFetcher` split/reconstruct |

**Checklist**: Transmute soundness, Send/Sync safety, fat pointer layout stability, lifetime enforcement

### Session 3: Dependency Audit & CVE Scan (HIGH)

Key dependencies to check:
- rocksdb 0.22.0 (C++ FFI), libp2p 0.53.2 (network-facing), axum (HTTP)
- k256 0.13.4, ed25519-dalek 2.2.0, aes-gcm 0.10.3 (crypto)
- 898 transitive dependencies total

**Checklist**: `cargo audit`, yanked crates, unmaintained crates, version discrepancies (Cargo.toml vs Cargo.lock), MSRV 1.82 compatibility

### Session 4: Build Scripts & Feature Flags (LOW)

| File | Focus |
|------|-------|
| `crates/orbis/build.rs` | tonic proto compilation (safe) |
| `crates/defra-version/build.rs` | Git metadata via Command::new("git") |

**Checklist**: Proto file safety, git command not injectable, feature exclusivity (storage backends), vendored-openssl scope

### Session 5: FFI Integration Regression (MEDIUM)

Test matrix across storage backends (redb, rocksdb, fjall, memory) x FFI lifecycle tests.

**Checklist**: Memory leaks in error paths, handle cleanup on node_close, concurrent query FetcherWrapper safety
