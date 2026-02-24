//! WASM lens module build and caching.
//!
//! Builds the `set_default` test lens module for schema migration tests.
//! The module is compiled once per process and cached.

use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;

use anyhow::{Context, Result};

use crate::workspace_root;

static BUILD_DONE: OnceLock<()> = OnceLock::new();

pub struct WasmLens;

impl WasmLens {
    /// Build the set_default WASM lens module (once per process).
    pub fn build() -> Result<()> {
        BUILD_DONE.get_or_init(|| {
            Self::do_build().expect("failed to build set_default WASM lens");
        });
        Ok(())
    }

    fn do_build() -> Result<()> {
        let wasm_path = Self::wasm_file_path();
        if Self::wasm_is_current(&wasm_path) {
            return Ok(());
        }

        let lens_dir = Self::lens_source_dir();

        // Verify wasm32-unknown-unknown target is installed
        let target_check = Command::new("rustup")
            .args(["target", "list", "--installed"])
            .output()
            .context("failed to run rustup")?;
        let installed = String::from_utf8_lossy(&target_check.stdout);
        anyhow::ensure!(
            installed.contains("wasm32-unknown-unknown"),
            "wasm32-unknown-unknown target not installed. Run: rustup target add wasm32-unknown-unknown"
        );

        eprintln!("Building set_default WASM lens module...");
        let status = Command::new("cargo")
            .args([
                "build",
                "--target",
                "wasm32-unknown-unknown",
                "--manifest-path",
            ])
            .arg(lens_dir.join("Cargo.toml"))
            .status()
            .context("failed to run cargo build for WASM lens")?;

        anyhow::ensure!(status.success(), "cargo build failed for WASM lens");

        anyhow::ensure!(
            wasm_path.exists(),
            "WASM file not found after build at {}",
            wasm_path.display()
        );

        Ok(())
    }

    /// Check if the .wasm file exists and is newer than source files.
    fn wasm_is_current(wasm_path: &PathBuf) -> bool {
        let wasm_mtime = match std::fs::metadata(wasm_path) {
            Ok(m) => match m.modified() {
                Ok(t) => t,
                Err(_) => return false,
            },
            Err(_) => return false,
        };

        let lens_dir = Self::lens_source_dir();
        let sources = [lens_dir.join("Cargo.toml"), lens_dir.join("src/lib.rs")];

        for src in &sources {
            if let Ok(meta) = std::fs::metadata(src) {
                if let Ok(mtime) = meta.modified() {
                    if mtime > wasm_mtime {
                        return false;
                    }
                }
            }
        }

        true
    }

    /// Return the file:// prefixed path to the compiled WASM module.
    pub fn module_path() -> String {
        format!("file://{}", Self::wasm_file_path().display())
    }

    /// Path to the lens source directory.
    fn lens_source_dir() -> PathBuf {
        workspace_root().join("tools/integration-test/test-lenses/set_default")
    }

    /// Path to the compiled .wasm file.
    fn wasm_file_path() -> PathBuf {
        Self::lens_source_dir().join("target/wasm32-unknown-unknown/debug/set_default_lens.wasm")
    }
}
