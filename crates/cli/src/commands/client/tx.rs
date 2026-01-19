// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Transaction command implementation

use clap::{Args, Subcommand};

use super::http_client::HttpClient;
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
    /// Begin a new transaction
    Begin(TxBeginArgs),
    /// Commit a transaction
    Commit(TxCommitArgs),
    /// Discard (rollback) a transaction
    Discard(TxDiscardArgs),
}

/// Arguments for tx begin command
#[derive(Args, Debug)]
pub struct TxBeginArgs {
    /// Create a read-only transaction
    #[arg(long)]
    pub readonly: bool,
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
    pub async fn execute(&self, url: &str) -> Result<()> {
        match &self.command {
            TxCommand::Begin(args) => args.execute(url).await,
            TxCommand::Commit(args) => args.execute(url).await,
            TxCommand::Discard(args) => args.execute(url).await,
        }
    }
}

impl TxBeginArgs {
    /// Execute the tx begin command
    pub async fn execute(&self, url: &str) -> Result<()> {
        let client = HttpClient::new(url)?;
        let response = client.tx_begin(self.readonly).await?;
        println!("{}", response.txn_id);
        Ok(())
    }
}

impl TxCommitArgs {
    /// Execute the tx commit command
    pub async fn execute(&self, url: &str) -> Result<()> {
        let client = HttpClient::new(url)?;
        let response = client.tx_commit(&self.txn_id).await?;
        println!("{}", response.status);
        Ok(())
    }
}

impl TxDiscardArgs {
    /// Execute the tx discard command
    pub async fn execute(&self, url: &str) -> Result<()> {
        let client = HttpClient::new(url)?;
        let response = client.tx_rollback(&self.txn_id).await?;
        println!("{}", response.status);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tx_begin_args_default() {
        let args = TxBeginArgs { readonly: false };
        assert!(!args.readonly);
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
