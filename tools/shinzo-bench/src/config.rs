use std::path::{Path, PathBuf};

pub const BASE_DIR: &str = "/tmp/shinzo-test";

pub fn base_dir() -> PathBuf {
    PathBuf::from(BASE_DIR)
}

pub fn defra_data_dir() -> PathBuf {
    base_dir().join("defradb")
}

pub fn defra_log() -> PathBuf {
    base_dir().join("defra.log")
}

pub fn indexer_log() -> PathBuf {
    base_dir().join("indexer.log")
}

pub fn ports_file() -> PathBuf {
    base_dir().join("ports")
}

pub fn pids_file() -> PathBuf {
    base_dir().join("pids")
}

pub fn metrics_file() -> PathBuf {
    base_dir().join("metrics.json")
}

/// Find the defra binary in the workspace.
pub fn defra_bin() -> PathBuf {
    workspace_root().join("target/release/defra")
}

/// Find the workspace root by walking up from the executable location.
pub fn workspace_root() -> PathBuf {
    let exe = std::env::current_exe().unwrap_or_default();
    // Walk up from tools/shinzo-bench/target/... to find Cargo.toml
    let mut dir = exe.parent().unwrap_or(Path::new(".")).to_path_buf();
    for _ in 0..10 {
        if dir.join("Cargo.toml").exists() && dir.join("crates").exists() {
            return dir;
        }
        if let Some(parent) = dir.parent() {
            dir = parent.to_path_buf();
        } else {
            break;
        }
    }
    // Fallback: try current directory
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
}
