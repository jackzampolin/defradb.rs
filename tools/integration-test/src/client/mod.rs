use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use serde_json::Value;

/// CLI-based client for DefraDB.
///
/// Executes commands against a running node using the `client` subcommand tree.
pub struct DefraClient {
    binary_path: PathBuf,
    url: String,
}

impl DefraClient {
    pub fn new(binary_path: impl Into<PathBuf>, url: impl Into<String>) -> Self {
        Self {
            binary_path: binary_path.into(),
            url: url.into(),
        }
    }

    pub fn binary_path(&self) -> &Path {
        &self.binary_path
    }

    fn exec(&self, args: &[&str]) -> Result<String> {
        let output = Command::new(&self.binary_path)
            .arg("--url")
            .arg(&self.url)
            .args(args)
            .output()
            .with_context(|| {
                format!(
                    "failed to exec: {} --url {} {}",
                    self.binary_path.display(),
                    self.url,
                    args.join(" ")
                )
            })?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            Err(anyhow::anyhow!(
                "command failed (exit {}): stderr={}, stdout={}",
                output.status,
                stderr.trim(),
                stdout.trim()
            ))
        }
    }

    /// Deploy a schema via `client schema add '<sdl>'`.
    pub fn schema_add(&self, sdl: &str) -> Result<Value> {
        let out = self.exec(&["client", "schema", "add", sdl])?;
        serde_json::from_str(&out).context("failed to parse schema_add output")
    }

    /// Execute a GraphQL query/mutation via `client query '<gql>'`.
    ///
    /// The CLI already extracts the `data` field and exits non-zero on
    /// GraphQL errors, so the returned value is the data directly.
    pub fn query(&self, gql: &str) -> Result<Value> {
        let out = self.exec(&["client", "query", gql])?;
        serde_json::from_str(&out).context("failed to parse query output")
    }

    /// Create a document via `client collection create --name <n> '<json>'`.
    pub fn collection_create(&self, name: &str, doc: &str) -> Result<Value> {
        let out = self.exec(&["client", "collection", "create", "--name", name, doc])?;
        serde_json::from_str(&out).context("failed to parse collection_create output")
    }

    /// Get a document via `client collection get --name <n> <id>`.
    pub fn collection_get(&self, name: &str, doc_id: &str) -> Result<Value> {
        let out = self.exec(&["client", "collection", "get", "--name", name, doc_id])?;
        serde_json::from_str(&out).context("failed to parse collection_get output")
    }

    /// Delete a document via `client collection delete --name <n> --docID <id>`.
    pub fn collection_delete(&self, name: &str, doc_id: &str) -> Result<String> {
        self.exec(&[
            "client",
            "collection",
            "delete",
            "--name",
            name,
            "--docID",
            doc_id,
        ])
    }

    /// List collections via `client collection list`.
    pub fn collection_list(&self) -> Result<Vec<String>> {
        let out = self.exec(&["client", "collection", "list"])?;
        let val: Value =
            serde_json::from_str(&out).context("failed to parse collection_list output")?;
        let arr = val.as_array().context("collection_list not an array")?;
        Ok(arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect())
    }

    /// Get P2P node info via `client p2p info`.
    pub fn p2p_info(&self) -> Result<Value> {
        let out = self.exec(&["client", "p2p", "info"])?;
        serde_json::from_str(&out).context("failed to parse p2p_info output")
    }

    /// Connect to peers via `client p2p connect <addr>...`.
    pub fn p2p_connect(&self, addrs: &[&str]) -> Result<String> {
        let mut args = vec!["client", "p2p", "connect"];
        args.extend(addrs);
        self.exec(&args)
    }
}
