//! SDL command implementation

use std::path::PathBuf;

use clap::{Args, Subcommand};

use crate::error::Result;

/// Manage SDL (Schema Definition Language)
#[derive(Args, Debug)]
pub struct SdlArgs {
    #[command(subcommand)]
    pub command: SdlCommand,
}

/// SDL subcommands
#[derive(Subcommand, Debug)]
pub enum SdlCommand {
    /// Generate SDL from input files
    Generate(SdlGenerateArgs),
}

/// Arguments for sdl generate command
#[derive(Args, Debug)]
pub struct SdlGenerateArgs {
    /// Input files
    #[arg(value_name = "FILES")]
    pub files: Vec<PathBuf>,

    /// Output file path
    #[arg(long, short = 'o', default_value = "schema.gen.graphql")]
    pub output: PathBuf,

    /// Overwrite existing output file
    #[arg(long, short = 'y')]
    pub overwrite: bool,

    /// Include searchable encryption directives
    #[arg(long, short = 's')]
    pub include_searchable_encryption: bool,
}

impl SdlArgs {
    pub fn execute(&self) -> Result<()> {
        match &self.command {
            SdlCommand::Generate(args) => args.execute(),
        }
    }
}

impl SdlGenerateArgs {
    pub fn execute(&self) -> Result<()> {
        eprintln!("not yet implemented");
        Ok(())
    }
}
