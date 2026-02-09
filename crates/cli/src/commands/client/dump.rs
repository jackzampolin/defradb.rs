//! Dump command implementation

use clap::Args;

use super::ClientContext;
use crate::error::Result;

/// Dump the database contents
#[derive(Args, Debug)]
pub struct DumpArgs {}

impl DumpArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}
