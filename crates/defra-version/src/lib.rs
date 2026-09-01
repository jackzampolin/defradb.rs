use serde::Serialize;

/// Go upstream commit we last synced with.
pub const GO_COMPAT_COMMIT: &str = "13c46ec9";
/// Go upstream branch.
pub const GO_COMPAT_BRANCH: &str = "develop";
/// Go release tag; empty when CI should build the pinned commit from source.
pub const GO_COMPAT_TAG: &str = "";

/// Go commit carrying the rustffi test client, which the FFI oracle checks out.
///
/// Deliberately independent of `GO_COMPAT_COMMIT`: that pin selects the Go
/// binary the parity job measures against and tracks upstream drift, while this
/// one fixes the Go *test corpus* the FFI oracle runs, so its pass rate stays
/// comparable across baseline bumps. It lives on a dedicated compatibility
/// branch upstream does not merge, and moves only when we retarget the parity
/// claim or fix the client.
pub const GO_FFI_CLIENT_COMMIT: &str = "e7cd3a8de";

/// Go compatibility metadata.
#[derive(Debug, Clone, Serialize)]
pub struct GoCompat {
    pub commit: &'static str,
    pub branch: &'static str,
    pub tag: &'static str,
}

/// Complete version information for defradb.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionInfo {
    pub version: &'static str,
    pub commit: &'static str,
    pub commit_date: &'static str,
    #[serde(rename = "httpAPI")]
    pub http_api: &'static str,
    pub doc_id_versions: &'static str,
    pub net_protocol: &'static str,
    pub rust: String,
    pub go_compat: GoCompat,
}

impl VersionInfo {
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION"),
            commit: env!("GIT_COMMIT"),
            commit_date: env!("BUILD_DATE"),
            http_api: "v0",
            doc_id_versions: "1",
            net_protocol: "/defra/0.0.1",
            rust: format!(
                "{} {}/{}",
                env!("CARGO_PKG_RUST_VERSION"),
                std::env::consts::OS,
                std::env::consts::ARCH,
            ),
            go_compat: GoCompat {
                commit: GO_COMPAT_COMMIT,
                branch: GO_COMPAT_BRANCH,
                tag: GO_COMPAT_TAG,
            },
        }
    }

    /// Short one-liner: `defradb 0.5.0`
    pub fn short(&self) -> String {
        format!("defradb {}", self.version)
    }

    /// Single-line descriptive build string, used as the OTLP
    /// `service.version` resource attribute. Mirrors the shape of Go
    /// DefraDB's version string (`defradb <ver> (<commit8> <date>) built
    /// with ...`) so a collector grouping on `service.version` sees the
    /// same cardinality/format across the two implementations. The commit
    /// is truncated to 8 chars to match Go.
    pub fn descriptive(&self) -> String {
        let commit8: String = self.commit.chars().take(8).collect();
        format!(
            "defradb {} ({} {}) built with {}",
            self.version, commit8, self.commit_date, self.rust
        )
    }

    /// Full human-readable text output.
    pub fn full(&self) -> String {
        let mut out = format!(
            "defradb {} ({} {})\n",
            self.version, self.commit, self.commit_date
        );
        out.push_str(&format!("* HTTP API: {}\n", self.http_api));
        out.push_str(&format!("* P2P multicodec: {}\n", self.net_protocol));
        out.push_str(&format!("* DocID versions: {}\n", self.doc_id_versions));
        out.push_str(&format!("* Rust: {}\n", self.rust));
        let tag_suffix = if self.go_compat.tag.is_empty() {
            String::new()
        } else {
            format!(", {}", self.go_compat.tag)
        };
        out.push_str(&format!(
            "* Go compat: {} ({}{})",
            self.go_compat.commit, self.go_compat.branch, tag_suffix
        ));
        out
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_info_fields_are_populated() {
        let info = VersionInfo::new();
        assert!(!info.version.is_empty());
        assert!(!info.commit.is_empty());
        assert!(!info.rust.is_empty());
        assert_eq!(info.http_api, "v0");
        assert_eq!(info.doc_id_versions, "1");
        assert_eq!(info.net_protocol, "/defra/0.0.1");
    }

    #[test]
    fn short_format() {
        let info = VersionInfo::new();
        assert!(info.short().starts_with("defradb "));
    }

    #[test]
    fn descriptive_format_matches_go_shape() {
        let info = VersionInfo::new();
        let d = info.descriptive();
        // `defradb <ver> (<commit8> <date>) built with <rust>`
        assert!(d.starts_with(&format!("defradb {} (", info.version)));
        assert!(d.contains(") built with "));
        assert!(d.ends_with(&info.rust));
    }

    #[test]
    fn full_format_contains_all_sections() {
        let info = VersionInfo::new();
        let full = info.full();
        assert!(full.contains("HTTP API:"));
        assert!(full.contains("P2P multicodec:"));
        assert!(full.contains("DocID versions:"));
        assert!(full.contains("Rust:"));
        assert!(full.contains("Go compat:"));
    }

    #[test]
    fn json_uses_camel_case_keys() {
        let info = VersionInfo::new();
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("\"commitDate\""));
        assert!(json.contains("\"httpAPI\""));
        assert!(json.contains("\"docIdVersions\""));
        assert!(json.contains("\"netProtocol\""));
        assert!(json.contains("\"goCompat\""));
    }

    #[test]
    fn go_compat_constants_are_set() {
        assert!(!GO_COMPAT_COMMIT.is_empty());
        assert!(!GO_COMPAT_BRANCH.is_empty());
        assert!(!GO_FFI_CLIENT_COMMIT.is_empty());
    }
}
