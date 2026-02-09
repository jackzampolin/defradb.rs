//! Node identity command implementation

use clap::Args;

use super::ClientContext;
use crate::error::Result;

/// Get the node's identity
#[derive(Args, Debug)]
pub struct NodeIdentityArgs {}

impl NodeIdentityArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}
