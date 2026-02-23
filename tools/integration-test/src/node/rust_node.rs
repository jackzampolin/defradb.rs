use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

use super::{DefraNode, NodeConfig};
use crate::workspace_root;

/// A Rust DefraDB node backed by the `defra` binary from this workspace.
pub struct RustNode {
    binary_path: PathBuf,
}

impl RustNode {
    /// Point to the debug binary in the workspace target dir.
    pub fn from_workspace() -> Self {
        Self {
            binary_path: workspace_root().join("target/debug/defra"),
        }
    }

    /// Build the Rust binary via cargo (debug mode for fast iteration).
    pub fn build() -> Result<()> {
        let status = Command::new("cargo")
            .args(["build", "-p", "cli"])
            .current_dir(workspace_root())
            .status()
            .context("failed to run cargo build")?;

        anyhow::ensure!(status.success(), "cargo build failed with {}", status);
        Ok(())
    }

    /// Verify the Rust binary is built and returns parseable version info.
    pub fn check_available() -> Result<()> {
        let binary = workspace_root().join("target/debug/defra");

        let output = Command::new(&binary)
            .args(["version", "--format", "json"])
            .output()
            .context("defra binary not found — run `cargo build -p cli` first")?;

        anyhow::ensure!(
            output.status.success(),
            "defra version failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let _json: serde_json::Value =
            serde_json::from_slice(&output.stdout).context("failed to parse defra version JSON")?;

        Ok(())
    }
}

impl DefraNode for RustNode {
    fn command(&self, config: &NodeConfig) -> Command {
        let mut cmd = Command::new(&self.binary_path);

        cmd.arg("--rootdir").arg(&config.rootdir);
        cmd.arg("--url").arg(&config.http_addr);
        cmd.arg("--no-log-color");
        cmd.arg("--log-output").arg("stdout");
        cmd.arg("--no-keyring");

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
            cmd.arg("--acp-document-type").arg(acp_type);
        }

        if config.nac_enabled {
            cmd.arg("--acp-node-enable");
        }

        if let Some(ref addr) = config.source_hub_address {
            cmd.arg("--source-hub-address").arg(addr);
        }
        if let Some(ref addr) = config.source_hub_comet_address {
            cmd.arg("--source-hub-comet-address").arg(addr);
        }
        if let Some(ref chain_id) = config.source_hub_chain_id {
            cmd.arg("--source-hub-chain-id").arg(chain_id);
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
