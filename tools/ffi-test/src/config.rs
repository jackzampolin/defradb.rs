use std::path::PathBuf;

/// Rust worktree prefix
pub const RUST_WORKTREE_PREFIX: &str = "defradb.rs";

/// Go worktree prefix
pub const GO_WORKTREE_PREFIX: &str = "defradb";

/// Environment variable naming the Go DefraDB checkout
pub const GO_REPO_ENV: &str = "DEFRADB_GO_REPO";

/// Report retention count per branch+package
pub const REPORT_RETENTION_COUNT: usize = 10;

/// Get the reports directory
pub fn reports_dir() -> PathBuf {
    dirs::home_dir()
        .expect("Could not determine home directory")
        .join(".defra-ffi-reports")
        .join("runs")
}

/// FFI crate path relative to Rust worktree root
pub const FFI_CRATE_PATH: &str = "crates/ffi";

/// cbindgen config path relative to Rust worktree root
pub const CBINDGEN_CONFIG: &str = "crates/ffi/cbindgen.toml";

/// Header destination relative to Go worktree root
pub const HEADER_DESTINATION: &str = "tests/clients/rustffi/defra.h";

/// FFI library name (without lib prefix or extension)
pub const FFI_LIB_NAME: &str = "ffi";

/// Library destination relative to Go worktree root
pub const LIBRARY_DESTINATION: &str = "tests/clients/rustffi";

/// Expected library name in Go worktree (without lib prefix or extension)
pub const GO_FFI_LIB_NAME: &str = "defra_ffi";
