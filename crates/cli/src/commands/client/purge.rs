//! Purge command implementation

use clap::Args;

use super::ClientContext;
use crate::error::Result;

/// Purge all database data
#[derive(Args, Debug)]
pub struct PurgeArgs {
    /// Force purge without confirmation (required)
    #[arg(long, short = 'f', required = true)]
    pub force: bool,
}

impl PurgeArgs {
    pub async fn execute(&self, _ctx: &ClientContext) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}
