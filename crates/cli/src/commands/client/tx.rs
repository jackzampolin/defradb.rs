//! Transaction command implementation

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
use super::ClientContext;
use crate::error::Result;

/// Manage transactions
#[derive(Args, Debug)]
pub struct TxArgs {
    #[command(subcommand)]
    pub command: TxCommand,
}

/// Transaction subcommands
#[derive(Subcommand, Debug)]
pub enum TxCommand {
    /// Create a new transaction
    #[command(alias = "create")]
    New(TxCreateArgs),
    /// Commit a transaction
    Commit(TxCommitArgs),
    /// Discard (rollback) a transaction
    Discard(TxDiscardArgs),
}

/// Arguments for tx create command
#[derive(Args, Debug)]
pub struct TxCreateArgs {
    /// Create a read-only transaction
    #[arg(long = "read-only")]
    pub read_only: bool,

    /// Create a concurrent transaction
    #[arg(long)]
    pub concurrent: bool,
}

/// Arguments for tx commit command
#[derive(Args, Debug)]
pub struct TxCommitArgs {
    /// The transaction ID to commit
    #[arg(value_name = "TXN_ID")]
    pub txn_id: String,
}

/// Arguments for tx discard command
#[derive(Args, Debug)]
pub struct TxDiscardArgs {
    /// The transaction ID to discard
    #[arg(value_name = "TXN_ID")]
    pub txn_id: String,
}

impl TxArgs {
    /// Execute the transaction command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        match &self.command {
            TxCommand::New(args) => args.execute(ctx).await,
            TxCommand::Commit(args) => args.execute(ctx).await,
            TxCommand::Discard(args) => args.execute(ctx).await,
        }
    }
}

impl TxCreateArgs {
    /// Execute the tx create command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
        let response = if self.concurrent {
            client.tx_begin_concurrent(self.read_only).await?
        } else {
            client.tx_begin(self.read_only).await?
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "id": response.id }))?
        );
        Ok(())
    }
}

impl TxCommitArgs {
    /// Execute the tx commit command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
        client.tx_commit(&self.txn_id).await?;
        Ok(())
    }
}

impl TxDiscardArgs {
    /// Execute the tx discard command
    pub async fn execute(&self, ctx: &ClientContext) -> Result<()> {
        let client = HttpClient::new(&ctx.url)?
            .with_auth_token(ctx.auth_token.clone())
            .with_verbose(ctx.verbose);
        client.tx_rollback(&self.txn_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tx_create_args_default() {
        let args = TxCreateArgs {
            read_only: false,
            concurrent: false,
        };
        assert!(!args.read_only);
        assert!(!args.concurrent);
    }

    #[test]
    fn test_tx_commit_args() {
        let args = TxCommitArgs {
            txn_id: "test-txn-123".to_string(),
        };
        assert_eq!(args.txn_id, "test-txn-123");
    }

    #[test]
    fn test_tx_discard_args() {
        let args = TxDiscardArgs {
            txn_id: "test-txn-456".to_string(),
        };
        assert_eq!(args.txn_id, "test-txn-456");
    }
}
