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
        cmd.arg("--no-log-color").arg("true");
        cmd.arg("--log-output").arg("stdout");
        cmd.arg("--no-keyring").arg("true");

        cmd.arg("start");
        cmd.arg("--store").arg("memory");
        cmd.arg("--no-telemetry").arg("true");
        cmd.arg("--no-encryption").arg("true");
        cmd.arg("--no-signing").arg("true");
        cmd.arg("--no-searchable-encryption").arg("true");

        if config.p2p_enabled {
            if let Some(ref addr) = config.p2p_addr {
                cmd.arg("--p2paddr").arg(addr);
            }
            for peer in &config.peers {
                cmd.arg("--peers").arg(peer);
            }
        } else {
            cmd.arg("--no-p2p").arg("true");
        }

        if let Some(ref identity) = config.identity {
            cmd.arg("--identity").arg(identity);
        }

        cmd
    }

    fn binary_path(&self) -> &Path {
        &self.binary_path
    }
}
