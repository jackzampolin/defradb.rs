//! Shared scaffolding for binary-axis conformance: locate the release artifact
//! under test. Included via `#[path]` from the behavioral test binaries.

use defra_harness::BinarySource;
use std::path::PathBuf;

/// The release artifact under test. Override with `DEFRA_CONFORMANCE_BINARY`
/// (e.g. a downloaded tagged release) to validate a specific shipped binary;
/// otherwise defaults to `target/release/defra`.
pub fn release_binary() -> BinarySource {
    if let Some(path) = std::env::var_os("DEFRA_CONFORMANCE_BINARY") {
        return BinarySource::Path(PathBuf::from(path));
    }
    BinarySource::Path(workspace_root().join("target/release/defra"))
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("proofs/ has a parent (the workspace root)")
        .to_path_buf()
}
