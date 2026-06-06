//! Shared scaffolding for binary-axis conformance: locate the release artifact
//! under test. Included via `#[path]` from the behavioral test binaries.

use defra_harness::BinarySource;
use std::path::PathBuf;

const CONFORMANCE_RUST_LOG: &str = "info";

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
    let root = workspace_root();
    ensure_harness_uses_workspace(&root);
    ensure_harness_logs_are_visible();

    if let Some(path) = std::env::var_os("DEFRA_CONFORMANCE_BINARY") {
        return BinarySource::Path(PathBuf::from(path));
    }
    BinarySource::Path(root.join("target/debug/defra"))
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("proofs/ has a parent (the workspace root)")
        .to_path_buf()
}

fn ensure_harness_logs_are_visible() {
    let should_set = std::env::var("RUST_LOG")
        .map(|value| !global_filter_allows_info(&value))
        .unwrap_or(true);

    if should_set {
        std::env::set_var("RUST_LOG", CONFORMANCE_RUST_LOG);
    }
}

fn ensure_harness_uses_workspace(root: &std::path::Path) {
    std::env::set_var("DEFRA_WORKSPACE_ROOT", root);
}

fn global_filter_allows_info(value: &str) -> bool {
    value.split(',').any(|directive| {
        let directive = directive.trim();
        if directive.is_empty() || directive.contains('=') {
            return false;
        }

        matches!(
            directive.to_ascii_lowercase().as_str(),
            "info" | "debug" | "trace"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::global_filter_allows_info;

    #[test]
    fn global_rust_log_filter_must_include_info_for_harness_readiness() {
        assert!(global_filter_allows_info("info"));
        assert!(global_filter_allows_info("warn,info"));
        assert!(global_filter_allows_info("debug,cranelift=warn"));

        assert!(!global_filter_allows_info(""));
        assert!(!global_filter_allows_info("warn"));
        assert!(!global_filter_allows_info("error"));
        assert!(!global_filter_allows_info("defra_http::server=info"));
    }
}
