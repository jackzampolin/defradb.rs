use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use crate::client::DefraClient;
use crate::observe::LogTracker;
use crate::process::ManagedProcess;
use crate::run::TestRunDir;

/// A running node within a test cluster.
pub struct RunningNode {
    pub name: String,
    pub api_url: String,
    pub http_addr: String,
    pub binary_path: PathBuf,
    #[allow(dead_code)]
    pub process: ManagedProcess,
    #[allow(dead_code)]
    pub log_tracker: LogTracker,
    #[allow(dead_code)]
    pub rootdir: PathBuf,
}

/// A cluster of running DefraDB nodes.
///
/// Field order matters: `nodes` is dropped before `run_dir`, ensuring
/// processes are killed before their data directories are removed.
pub struct TestCluster {
    pub nodes: Vec<RunningNode>,
    #[allow(dead_code)]
    run_dir: TestRunDir,
    startup_identity: Option<String>,
}

impl TestCluster {
    pub(crate) fn new(
        nodes: Vec<RunningNode>,
        run_dir: TestRunDir,
        startup_identity: Option<String>,
    ) -> Self {
        Self {
            nodes,
            run_dir,
            startup_identity,
        }
    }

    /// Return the private key hex used to start nodes (if any).
    ///
    /// In NAC mode, Go grants automatic admin access to the startup identity.
    /// Tests must use this identity for admin operations.
    pub fn startup_identity(&self) -> Option<&str> {
        self.startup_identity.as_deref()
    }

    pub fn builder() -> super::builder::TestClusterBuilder {
        super::builder::TestClusterBuilder::new()
    }

    /// Return a CLI-based client for the node at `index`.
    pub fn client(&self, index: usize) -> DefraClient {
        let node = &self.nodes[index];
        DefraClient::new(&node.binary_path, &node.http_addr)
    }

    pub fn api_url(&self, index: usize) -> &str {
        &self.nodes[index].api_url
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Wait for a named log pattern on the node at `index`.
    pub async fn wait_for_log(
        &self,
        index: usize,
        pattern: &str,
        timeout: Duration,
    ) -> Result<String> {
        self.nodes[index]
            .log_tracker
            .wait_for_pattern(pattern, timeout)
            .await
    }
}
