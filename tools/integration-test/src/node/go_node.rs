use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

use super::{DefraNode, NodeConfig};

/// A Go DefraDB node backed by the `defradb` binary from PATH.
pub struct GoNode {
    binary_path: PathBuf,
}

impl GoNode {
    /// Create a GoNode using the `defradb` binary from PATH.
    pub fn from_path() -> Self {
        Self {
            binary_path: PathBuf::from("defradb"),
        }
    }

    /// Verify the Go binary is available and version-compatible.
    ///
    /// Compares the Go binary's commit against `GO_COMPAT_COMMIT` from `defra-version`.
    /// Set `DEFRA_SKIP_VERSION_CHECK=1` to bypass the check.
    pub fn check_available() -> Result<()> {
        let output = Command::new("defradb")
            .args(["version", "--format", "json"])
            .output()
            .context("defradb binary not found in PATH")?;

        anyhow::ensure!(
            output.status.success(),
            "defradb version failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let json: serde_json::Value = serde_json::from_slice(&output.stdout)
            .context("failed to parse defradb version JSON")?;

        let go_commit = json["commit"].as_str().unwrap_or("unknown");

        let expected = defra_version::GO_COMPAT_COMMIT;
        if !go_commit.starts_with(expected) {
            if std::env::var("DEFRA_SKIP_VERSION_CHECK").as_deref() == Ok("1") {
                tracing::warn!(
                    expected = expected,
                    actual = go_commit,
                    "Go binary version mismatch (skipped via DEFRA_SKIP_VERSION_CHECK)"
                );
            } else {
                anyhow::bail!(
                    "Go binary version mismatch: expected commit starting with {expected}, got {go_commit}. \
                     Set DEFRA_SKIP_VERSION_CHECK=1 to bypass."
                );
            }
        }

        Ok(())
    }
}

impl DefraNode for GoNode {
    fn command(&self, config: &NodeConfig) -> Command {
        let mut cmd = Command::new(&self.binary_path);

        cmd.arg("--rootdir").arg(&config.rootdir);
        cmd.arg("--url").arg(&config.http_addr);
        cmd.arg("--no-log-color");
        cmd.arg("--log-output").arg("stdout");
        if config.keyring_enabled {
            cmd.env("DEFRA_KEYRING_SECRET", "integration-test-secret");
        } else {
            cmd.arg("--no-keyring");
        }

        cmd.arg("start");
        cmd.arg("--store")
            .arg(config.store.as_deref().unwrap_or("memory"));
        cmd.arg("--no-telemetry");

        if !config.encryption_enabled {
            cmd.arg("--no-encryption");
            cmd.arg("--no-searchable-encryption");
        }
        if !config.signing_enabled {
            cmd.arg("--no-signing");
        }

        if config.p2p_enabled {
            if let Some(ref addr) = config.p2p_addr {
                cmd.arg("--p2paddr").arg(addr);
            }
            for peer in &config.peers {
                cmd.arg("--peers").arg(peer);
            }
        } else {
            cmd.arg("--no-p2p");
        }

        if let Some(ref identity) = config.identity {
            cmd.arg("--identity").arg(identity);
        }

        if let Some(ref acp_type) = config.acp_document_type {
            cmd.arg("--document-acp-type").arg(acp_type);
        }

        if config.nac_enabled {
            cmd.arg("--node-acp-enable");
        }

        if config.development {
            cmd.arg("--development");
        }

        if let Some(timeout) = config.query_timeout {
            cmd.arg("--query-timeout").arg(timeout.to_string());
        }

        cmd
    }

    fn binary_path(&self) -> &Path {
        &self.binary_path
    }
}
