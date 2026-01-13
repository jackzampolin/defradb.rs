// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Version command implementation

use clap::Args;
use serde::Serialize;

use crate::error::Result;

/// Version information
#[derive(Debug, Serialize)]
pub struct VersionInfo {
    pub version: String,
    pub commit: String,
    pub build_date: String,
    pub go_version: String, // N/A for Rust impl, kept for compatibility
    pub rust_version: String,
    pub platform: String,
}

impl VersionInfo {
    /// Create version info from compile-time environment
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            commit: option_env!("GIT_COMMIT").unwrap_or("unknown").to_string(),
            build_date: option_env!("BUILD_DATE").unwrap_or("unknown").to_string(),
            go_version: "N/A (Rust implementation)".to_string(),
            rust_version: env!("CARGO_PKG_RUST_VERSION").to_string(),
            platform: format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH),
        }
    }

    /// Format as simple version string
    pub fn short(&self) -> String {
        format!("defradb {}", self.version)
    }

    /// Format as full version string
    pub fn full(&self) -> String {
        let mut output = format!("defradb {}\n", self.version);
        output.push_str(&format!("Commit:       {}\n", self.commit));
        output.push_str(&format!("Build Date:   {}\n", self.build_date));
        output.push_str(&format!("Rust Version: {}\n", self.rust_version));
        output.push_str(&format!("Platform:     {}", self.platform));
        output
    }
}

impl Default for VersionInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Arguments for the version command
#[derive(Args, Debug)]
pub struct VersionArgs {
    /// Version output format. Options are text, json
    #[arg(short, long, default_value = "text")]
    pub format: String,

    /// Display the full version information
    #[arg(long)]
    pub full: bool,
}

impl VersionArgs {
    /// Execute the version command
    pub fn execute(&self) -> Result<()> {
        let info = VersionInfo::new();

        if self.format.to_lowercase() == "json" {
            let json = serde_json::to_string_pretty(&info).unwrap();
            println!("{json}");
        } else if self.full {
            println!("{}", info.full());
        } else {
            println!("{}", info.short());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_info() {
        let info = VersionInfo::new();
        assert!(!info.version.is_empty());
        assert!(!info.platform.is_empty());
    }

    #[test]
    fn test_version_short() {
        let info = VersionInfo::new();
        let short = info.short();
        assert!(short.starts_with("defradb "));
    }

    #[test]
    fn test_version_full() {
        let info = VersionInfo::new();
        let full = info.full();
        assert!(full.contains("defradb"));
        assert!(full.contains("Rust Version:"));
        assert!(full.contains("Platform:"));
    }
}
