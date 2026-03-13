//! Build-time version info.
//!
//! All constants are set by `build.rs` at compile time. Any binary that
//! depends on defra-node gets these for free.

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_HASH: &str = env!("DEFRA_NODE_GIT_HASH");
pub const GIT_DIRTY: &str = env!("DEFRA_NODE_GIT_DIRTY");
pub const RELEASE_TAG: &str = env!("DEFRA_NODE_RELEASE_TAG");
pub const BUILD_TIME: &str = env!("DEFRA_NODE_BUILD_TIME");
pub const TARGET: &str = env!("DEFRA_NODE_TARGET");
pub const RUSTC: &str = env!("DEFRA_NODE_RUSTC");

fn revision_label() -> String {
    if RELEASE_TAG.is_empty() {
        format!("{GIT_HASH}{GIT_DIRTY}")
    } else {
        format!("{GIT_HASH}{GIT_DIRTY}, tag {RELEASE_TAG}")
    }
}

/// Full version string suitable for `--version` output.
pub fn long() -> String {
    format!(
        "{} ({})\nbuilt {} for {}\n{}",
        VERSION,
        revision_label(),
        BUILD_TIME,
        TARGET,
        RUSTC,
    )
}

/// Short version string (version + commit).
pub fn short() -> String {
    format!("{} ({})", VERSION, revision_label())
}

/// Structured version info for HTTP endpoints / JSON.
pub fn info() -> VersionInfo {
    VersionInfo {
        version: VERSION,
        git_hash: GIT_HASH,
        git_dirty: !GIT_DIRTY.is_empty(),
        release_tag: if RELEASE_TAG.is_empty() {
            None
        } else {
            Some(RELEASE_TAG)
        },
        build_time: BUILD_TIME,
        target: TARGET,
        rustc: RUSTC,
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct VersionInfo {
    pub version: &'static str,
    pub git_hash: &'static str,
    pub git_dirty: bool,
    pub release_tag: Option<&'static str>,
    pub build_time: &'static str,
    pub target: &'static str,
    pub rustc: &'static str,
}
