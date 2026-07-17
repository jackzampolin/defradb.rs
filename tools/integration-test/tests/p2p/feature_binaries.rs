//! Prepares immutable default and Iroh CLI snapshots before P2P tests run in parallel.
//!
//! The harness builds both feature sets to `target/debug/defra`, so retaining that
//! path would make whichever build finishes last determine every test's features.

use std::path::{Path, PathBuf};
use std::process::Command;

#[ctor::ctor]
fn prepare_feature_binaries() {
    let default = existing_binary("DEFRA_RUST_BINARY");
    let iroh = existing_binary("DEFRA_IROH_BINARY");
    if default.is_some() && iroh.is_some() {
        return;
    }

    let workspace = workspace_root();
    let default = default.unwrap_or_else(|| build_variant(&workspace, &[], "defra-default"));
    let iroh = iroh.unwrap_or_else(|| build_variant(&workspace, &["iroh"], "defra-iroh"));

    std::env::set_var("DEFRA_RUST_BINARY", default);
    std::env::set_var("DEFRA_IROH_BINARY", iroh);
}

fn existing_binary(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| path.is_file())
}

fn build_variant(workspace: &Path, features: &[&str], output_name: &str) -> PathBuf {
    let mut command = Command::new("cargo");
    command.args(["build", "-p", "cli"]);
    if !features.is_empty() {
        command.args(["--features", &features.join(",")]);
    }

    let status = command
        .current_dir(workspace)
        .status()
        .expect("failed to build defra test binary");
    assert!(status.success(), "cargo build -p cli failed");

    let target_dir = workspace.join("target/debug");
    let source = target_dir.join(format!("defra{}", std::env::consts::EXE_SUFFIX));
    let destination = target_dir.join(format!("{output_name}{}", std::env::consts::EXE_SUFFIX));
    let temporary = target_dir.join(format!(
        "{output_name}-{}{}",
        std::process::id(),
        std::env::consts::EXE_SUFFIX
    ));

    std::fs::copy(&source, &temporary).expect("failed to snapshot defra test binary");
    std::fs::rename(&temporary, &destination).unwrap_or_else(|_| {
        let _ = std::fs::remove_file(&destination);
        std::fs::rename(&temporary, &destination).expect("failed to replace defra test binary");
    });
    destination
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}
