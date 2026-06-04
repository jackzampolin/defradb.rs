//! Shared scaffolding for binary-axis conformance: locate the release artifact
//! under test. Included via `#[path]` from the behavioral test binaries.

use defra_harness::BinarySource;
use std::path::PathBuf;

/// The artifact under test. Resolution order:
/// 1. `DEFRA_CONFORMANCE_BINARY` override (e.g. a downloaded tagged release);
/// 2. `target/release/defra` if present (a real release build);
/// 3. `target/debug/defra` otherwise.
///
/// The debug fallback lets these tests run under a plain `cargo test --workspace`
/// (CI builds only the debug `defra`, never a release one before the test job),
/// while still preferring an optimized release binary when one has been built
/// (e.g. via `proofs/verify-all.sh`).
pub fn release_binary() -> BinarySource {
    if let Some(path) = std::env::var_os("DEFRA_CONFORMANCE_BINARY") {
        return BinarySource::Path(PathBuf::from(path));
    }
    let release = workspace_root().join("target/release/defra");
    if release.exists() {
        return BinarySource::Path(release);
    }
    BinarySource::Path(workspace_root().join("target/debug/defra"))
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("proofs/ has a parent (the workspace root)")
        .to_path_buf()
}
