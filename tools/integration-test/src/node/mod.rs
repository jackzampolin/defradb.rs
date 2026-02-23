mod go_node;
mod rust_node;

pub use go_node::GoNode;
pub use rust_node::RustNode;

use std::path::{Path, PathBuf};
use std::process::Command;

/// Configuration for a single DefraDB node.
#[derive(Clone)]
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
    pub nac_enabled: bool,
    pub source_hub_address: Option<String>,
    pub source_hub_comet_address: Option<String>,
    pub source_hub_chain_id: Option<String>,
    pub development: bool,
    pub store: Option<String>,
    pub query_timeout: Option<u64>,
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
