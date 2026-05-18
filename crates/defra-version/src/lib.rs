use serde::Serialize;

/// Go upstream commit we last synced with.
pub const GO_COMPAT_COMMIT: &str = "6c874754";
/// Go upstream branch.
pub const GO_COMPAT_BRANCH: &str = "develop";
/// Go release tag (empty until Go cuts a release).
pub const GO_COMPAT_TAG: &str = "";

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
    }
}
