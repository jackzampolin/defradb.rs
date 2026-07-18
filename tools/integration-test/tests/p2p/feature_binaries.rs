//! Prepares immutable default and Iroh CLI snapshots before P2P tests run in parallel.
//!
//! The harness builds both feature sets to `target/debug/defra`, so retaining that
//! path would make whichever build finishes last determine every test's features.

use std::path::PathBuf;

use integration_test::{build_cli_variant, workspace_root};

#[ctor::ctor]
fn prepare_feature_binaries() {
    let default = existing_binary("DEFRA_RUST_BINARY");
    let iroh = existing_binary("DEFRA_IROH_BINARY");
    if default.is_some() && iroh.is_some() {
        return;
    }

    let workspace = workspace_root();
    let default = default.unwrap_or_else(|| build_cli_variant(&workspace, &[], "defra-default"));
    let iroh = iroh.unwrap_or_else(|| build_cli_variant(&workspace, &["iroh"], "defra-iroh"));

    std::env::set_var("DEFRA_RUST_BINARY", default);
    std::env::set_var("DEFRA_IROH_BINARY", iroh);
}

fn existing_binary(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|path| path.is_file())
}
