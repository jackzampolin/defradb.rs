use std::path::PathBuf;

use reqwest::Client;

use crate::observe::LogTracker;
use crate::process::ManagedProcess;
use crate::run::TestRunDir;

/// A running node within a test cluster.
pub struct RunningNode {
    pub name: String,
    pub api_url: String,
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
    pub client: Client,
    #[allow(dead_code)]
    run_dir: TestRunDir,
}

impl TestCluster {
    pub(crate) fn new(nodes: Vec<RunningNode>, client: Client, run_dir: TestRunDir) -> Self {
        Self {
            nodes,
            client,
            run_dir,
        }
    }

    pub fn builder() -> super::builder::TestClusterBuilder {
        super::builder::TestClusterBuilder::new()
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
}
