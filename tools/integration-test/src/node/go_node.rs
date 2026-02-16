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

    /// Verify the Go binary is available.
    pub fn check_available() -> Result<()> {
        let output = Command::new("defradb")
            .arg("version")
            .output()
            .context("defradb binary not found in PATH")?;

        anyhow::ensure!(
            output.status.success(),
            "defradb version failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
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
        cmd.arg("--no-keyring");

        cmd.arg("start");
        cmd.arg("--store").arg("memory");
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

        cmd
    }

    fn binary_path(&self) -> &Path {
        &self.binary_path
    }
}
