//! Server dump command implementation

use clap::Args;

use crate::error::Result;

/// Dump server-side data
#[derive(Args, Debug)]
pub struct ServerDumpArgs {}

impl ServerDumpArgs {
    pub fn execute(&self) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}
