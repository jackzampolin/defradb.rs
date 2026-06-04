//! Shared scaffolding for binary-axis conformance: locate the release artifact
//! under test. Included via `#[path]` from the behavioral test binaries.

use defra_harness::BinarySource;
use std::path::PathBuf;

/// The artifact under test.
///
/// Defaults to `target/debug/defra` — the binary that `cargo test --workspace`
/// rebuilds fresh for the exact revision under test. We deliberately do NOT
/// prefer `target/release/defra`: a self-hosted CI runner caches `target/`, so a
/// release binary left over from an earlier run can be stale and silently
/// validate old behavior (which produced confusing red CI here). `cargo test`
/// never rebuilds the release binary, but always brings the debug one current.
///
/// Override with `DEFRA_CONFORMANCE_BINARY` to validate a specific shipped
/// artifact (e.g. a downloaded tagged release, or a local release build from
/// `proofs/verify-all.sh`).
pub fn release_binary() -> BinarySource {
    if let Some(path) = std::env::var_os("DEFRA_CONFORMANCE_BINARY") {
        return BinarySource::Path(PathBuf::from(path));
    }
    BinarySource::Path(workspace_root().join("target/debug/defra"))
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("proofs/ has a parent (the workspace root)")
        .to_path_buf()
}
