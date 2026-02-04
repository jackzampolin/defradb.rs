use std::path::PathBuf;

/// Base path for Go defradb repository
pub const GO_REPO_BASE: &str = "/Users/johnzampolin/go/src/github.com/sourcenetwork";

/// Rust worktree prefix
pub const RUST_WORKTREE_PREFIX: &str = "defradb.rs";

/// Go worktree prefix
pub const GO_WORKTREE_PREFIX: &str = "defradb";

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
