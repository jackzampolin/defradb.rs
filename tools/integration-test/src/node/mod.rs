mod go_node;
mod rust_node;

pub use go_node::GoNode;
pub use rust_node::RustNode;

use std::path::{Path, PathBuf};
use std::process::Command;

/// Configuration for a single DefraDB node.
pub struct NodeConfig {
    pub name: String,
    pub rootdir: PathBuf,
    pub log_dir: PathBuf,
    pub http_addr: String,
    pub p2p_enabled: bool,
    pub p2p_addr: Option<String>,
    pub peers: Vec<String>,
    pub identity: Option<String>,
    pub acp_document_type: Option<String>,
    pub encryption_enabled: bool,
    pub signing_enabled: bool,
}

/// Trait for building a DefraDB command from config.
pub trait DefraNode {
    fn command(&self, config: &NodeConfig) -> Command;
    fn api_url(host: &str, port: u16) -> String
    where
        Self: Sized,
    {
        format!("http://{}:{}", host, port)
    }
    fn prepare(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn binary_path(&self) -> &Path;
}
