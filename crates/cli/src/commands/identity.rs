//! Identity command implementation

use clap::{Args, Subcommand};

use crate::error::Result;

/// Manage identities
#[derive(Args, Debug)]
pub struct IdentityArgs {
    #[command(subcommand)]
    pub command: IdentityCommand,
}

/// Identity subcommands
#[derive(Subcommand, Debug)]
pub enum IdentityCommand {
    /// Generate a new identity
    New(IdentityNewArgs),
}

/// Arguments for identity new command
#[derive(Args, Debug)]
pub struct IdentityNewArgs {
    /// Key type to generate (secp256k1 or ed25519)
    #[arg(long = "type", default_value = "secp256k1")]
    pub key_type: String,
}

impl IdentityArgs {
    pub fn execute(&self) -> Result<()> {
        match &self.command {
            IdentityCommand::New(args) => args.execute(),
        }
    }
}

impl IdentityNewArgs {
    pub fn execute(&self) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}
