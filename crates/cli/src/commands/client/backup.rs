//! Backup command implementation for database export/import

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::{validate_identifier, ClientContext};
use crate::error::{Error, Result};

/// Manage database backups
#[derive(Args, Debug)]
pub struct BackupArgs {
    #[command(subcommand)]
    pub command: BackupCommand,
}

/// Backup subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum BackupCommand {
    /// Export database to a file
    Export(BackupExportArgs),
    /// Import database from a file
    Import(BackupImportArgs),
}

/// Arguments for backup export command
#[derive(Args, Debug)]
pub struct BackupExportArgs {
    /// Output file path for the backup
    #[arg(value_name = "FILE")]
    pub file: PathBuf,

    /// Collection(s) to export (can be specified multiple times)
    #[arg(long, short = 'c')]
    pub collections: Vec<String>,

    /// Pretty print the JSON output
    #[arg(long)]
    pub pretty: bool,
}

/// Arguments for backup import command
#[derive(Args, Debug)]
pub struct BackupImportArgs {
    /// Input file path for the backup to restore
    #[arg(value_name = "FILE")]
    pub file: PathBuf,
}

impl BackupArgs {
    /// Execute the backup command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            BackupCommand::Export(args) => args.execute(ctx).await,
            BackupCommand::Import(args) => args.execute(ctx).await,
        }
    }
}

impl BackupExportArgs {
    /// Execute the backup export command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        // Validate collection names before making the request
        for col in &self.collections {
            validate_identifier(col)?;
        }

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        let collections = if self.collections.is_empty() {
            None
        } else {
            Some(self.collections.as_slice())
        };

        let data = client.backup_export(collections, self.pretty).await?;

        tokio::fs::write(&self.file, &data)
            .await
            .map_err(|e| Error::WriteConfig {
                path: self.file.clone(),
                source: e,
            })?;

        println!("Backup exported to: {}", self.file.display());
        Ok(())
    }
}

impl BackupImportArgs {
    /// Execute the backup import command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let data = tokio::fs::read_to_string(&self.file)
            .await
            .map_err(|e| Error::ReadFile {
                path: self.file.clone(),
                source: e,
            })?;

        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);

        client.backup_import(&data).await?;

        println!("Backup imported from: {}", self.file.display());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_export_args() {
        let args = BackupExportArgs {
            file: PathBuf::from("backup.json"),
            collections: vec![],
            pretty: false,
        };
        assert_eq!(args.file, PathBuf::from("backup.json"));
        assert!(args.collections.is_empty());
        assert!(!args.pretty);
    }

    #[test]
    fn test_backup_export_args_with_collections() {
        let args = BackupExportArgs {
            file: PathBuf::from("backup.json"),
            collections: vec!["Users".to_string(), "Posts".to_string()],
            pretty: true,
        };
        assert_eq!(args.collections.len(), 2);
        assert!(args.pretty);
    }

    #[test]
    fn test_backup_import_args() {
        let args = BackupImportArgs {
            file: PathBuf::from("backup.json"),
        };
        assert_eq!(args.file, PathBuf::from("backup.json"));
    }
}
