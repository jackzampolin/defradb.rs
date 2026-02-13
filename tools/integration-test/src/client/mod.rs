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
    /// Normalizes output across Go and Rust CLIs:
    /// - Go wraps in `{"data": ...}` with a header; Rust returns data directly.
    pub fn query(&self, gql: &str) -> Result<Value> {
        let out = self.exec(&["client", "query", gql])?;
        // Go CLI prefixes output with "------ Request Results ------\n"
        let json_str = out.find('{').map(|i| &out[i..]).unwrap_or(&out);
        let val: Value = serde_json::from_str(json_str).context("failed to parse query output")?;
        // Go CLI wraps in {"data": ...}; Rust returns data directly
        if let Some(data) = val.get("data") {
            Ok(data.clone())
        } else {
            Ok(val)
        }
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

    /// Create P2P collections via `client p2p collection create <cols>`.
    pub fn p2p_collection_add(&self, collections: &[&str]) -> Result<String> {
        let cols = collections.join(",");
        self.exec(&["client", "p2p", "collection", "create", &cols])
    }

    /// Create a replicator via `client p2p replicator create -c <cols> <addr>`.
    pub fn p2p_replicator_set(&self, collections: &[&str], addr: &str) -> Result<String> {
        let cols = collections.join(",");
        self.exec(&["client", "p2p", "replicator", "create", "-c", &cols, addr])
    }

    /// Execute a GraphQL query with an identity via `client -i <key> query '<gql>'`.
    pub fn query_with_identity(&self, gql: &str, hex_key: &str) -> Result<Value> {
        let out = self.exec(&["client", "-i", hex_key, "query", gql])?;
        let json_str = out.find('{').map(|i| &out[i..]).unwrap_or(&out);
        let val: Value = serde_json::from_str(json_str).context("failed to parse query output")?;
        if let Some(data) = val.get("data") {
            Ok(data.clone())
        } else {
            Ok(val)
        }
    }

    /// Deploy a schema with identity via `client -i <key> schema add '<sdl>'`.
    pub fn schema_add_with_identity(&self, sdl: &str, hex_key: &str) -> Result<Value> {
        let out = self.exec(&["client", "-i", hex_key, "schema", "add", sdl])?;
        serde_json::from_str(&out).context("failed to parse schema_add output")
    }

    /// Add an ACP policy via `client -i <key> acp document policy add '<yaml>'`.
    pub fn acp_policy_add(&self, policy: &str, hex_key: &str) -> Result<Value> {
        let out = self.exec(&[
            "client", "-i", hex_key, "acp", "document", "policy", "add", policy,
        ])?;
        serde_json::from_str(&out).context("failed to parse acp_policy_add output")
    }

    /// Add an ACP document relationship.
    pub fn acp_relationship_add(
        &self,
        collection: &str,
        doc_id: &str,
        relation: &str,
        actor_did: &str,
        hex_key: &str,
    ) -> Result<Value> {
        let out = self.exec(&[
            "client",
            "-i",
            hex_key,
            "acp",
            "document",
            "relationship",
            "add",
            "-c",
            collection,
            "--docID",
            doc_id,
            "-r",
            relation,
            "-a",
            actor_did,
        ])?;
        serde_json::from_str(&out).context("failed to parse acp_relationship_add output")
    }

    // -- Collection extensions --

    /// Update a document via `client collection update --name <n> --docID <id> --updater '<json>'`.
    pub fn collection_update(&self, name: &str, doc_id: &str, updater: &str) -> Result<Value> {
        let out = self.exec(&[
            "client",
            "collection",
            "update",
            "--name",
            name,
            "--docID",
            doc_id,
            "--updater",
            updater,
        ])?;
        serde_json::from_str(&out).context("failed to parse collection_update output")
    }

    /// Describe a collection via `client collection describe --name <n>`.
    pub fn collection_describe(&self, name: &str) -> Result<Value> {
        let out = self.exec(&["client", "collection", "describe", "--name", name])?;
        serde_json::from_str(&out).context("failed to parse collection_describe output")
    }

    /// List document IDs via `client collection doc-ids --name <n>`.
    pub fn collection_doc_ids(&self, name: &str) -> Result<Value> {
        let out = self.exec(&["client", "collection", "doc-ids", "--name", name])?;
        serde_json::from_str(&out).context("failed to parse collection_doc_ids output")
    }

    /// Truncate a collection via `client collection truncate --name <n>`.
    pub fn collection_truncate(&self, name: &str) -> Result<String> {
        self.exec(&["client", "collection", "truncate", "--name", name])
    }

    // -- Schema extensions --

    /// Describe the full schema via `client schema describe`.
    pub fn schema_describe(&self) -> Result<String> {
        self.exec(&["client", "schema", "describe"])
    }

    // -- Index operations --

    /// Create an index via `client index create <collection> --fields <f> [--name <n>] [--unique]`.
    pub fn index_create(
        &self,
        collection: &str,
        fields: &[&str],
        name: Option<&str>,
        unique: bool,
    ) -> Result<Value> {
        let fields_csv = fields.join(",");
        let mut args = vec![
            "client",
            "index",
            "create",
            collection,
            "--fields",
            &fields_csv,
        ];
        if let Some(n) = name {
            args.push("--name");
            args.push(n);
        }
        if unique {
            args.push("--unique");
        }
        let out = self.exec(&args)?;
        serde_json::from_str(&out).context("failed to parse index_create output")
    }

    /// List indexes via `client index list [collection]`.
    pub fn index_list(&self, collection: Option<&str>) -> Result<Value> {
        let mut args = vec!["client", "index", "list"];
        if let Some(c) = collection {
            args.push(c);
        }
        let out = self.exec(&args)?;
        serde_json::from_str(&out).context("failed to parse index_list output")
    }

    /// Drop an index via `client index drop <collection> <name>`.
    pub fn index_drop(&self, collection: &str, name: &str) -> Result<String> {
        self.exec(&["client", "index", "drop", collection, name])
    }

    // -- Transaction operations --

    /// Create a transaction via `client tx create`.
    pub fn tx_create(&self) -> Result<String> {
        let out = self.exec(&["client", "tx", "create"])?;
        Ok(out.trim().to_string())
    }

    /// Create a concurrent transaction via `client tx create --concurrent`.
    pub fn tx_create_concurrent(&self) -> Result<String> {
        let out = self.exec(&["client", "tx", "create", "--concurrent"])?;
        Ok(out.trim().to_string())
    }

    /// Commit a transaction via `client tx commit <id>`.
    pub fn tx_commit(&self, tx_id: &str) -> Result<String> {
        self.exec(&["client", "tx", "commit", tx_id])
    }

    /// Discard a transaction via `client tx discard <id>`.
    pub fn tx_discard(&self, tx_id: &str) -> Result<String> {
        self.exec(&["client", "tx", "discard", tx_id])
    }

    /// Execute a GraphQL query inside a transaction via `client --tx <id> query '<gql>'`.
    pub fn query_with_tx(&self, gql: &str, tx_id: &str) -> Result<Value> {
        let out = self.exec(&["client", "--tx", tx_id, "query", gql])?;
        let json_str = out.find('{').map(|i| &out[i..]).unwrap_or(&out);
        let val: Value =
            serde_json::from_str(json_str).context("failed to parse query_with_tx output")?;
        if let Some(data) = val.get("data") {
            Ok(data.clone())
        } else {
            Ok(val)
        }
    }

    // -- Backup operations --

    /// Export backup via `client backup export <file> [--collections <c>] [--pretty]`.
    pub fn backup_export(&self, file: &str, collections: &[&str], pretty: bool) -> Result<String> {
        let mut args = vec!["client", "backup", "export", file];
        for c in collections {
            args.push("-c");
            args.push(c);
        }
        if pretty {
            args.push("--pretty");
        }
        self.exec(&args)
    }

    /// Import backup via `client backup import <file>`.
    pub fn backup_import(&self, file: &str) -> Result<String> {
        self.exec(&["client", "backup", "import", file])
    }

    // -- P2P extensions --

    /// List active peers via `client p2p active-peers`.
    pub fn p2p_active_peers(&self) -> Result<Value> {
        let out = self.exec(&["client", "p2p", "active-peers"])?;
        serde_json::from_str(&out).context("failed to parse p2p_active_peers output")
    }

    /// List P2P collections via `client p2p collection list`.
    pub fn p2p_collection_list(&self) -> Result<Value> {
        let out = self.exec(&["client", "p2p", "collection", "list"])?;
        serde_json::from_str(&out).context("failed to parse p2p_collection_list output")
    }

    /// Delete P2P collections via `client p2p collection delete <cols>`.
    pub fn p2p_collection_delete(&self, collections: &[&str]) -> Result<String> {
        let cols = collections.join(",");
        self.exec(&["client", "p2p", "collection", "delete", &cols])
    }

    /// List replicators via `client p2p replicator list`.
    pub fn p2p_replicator_list(&self) -> Result<Value> {
        let out = self.exec(&["client", "p2p", "replicator", "list"])?;
        serde_json::from_str(&out).context("failed to parse p2p_replicator_list output")
    }

    /// Delete a replicator via `client p2p replicator delete -c <cols>`.
    pub fn p2p_replicator_delete(&self, collections: &[&str]) -> Result<String> {
        let cols = collections.join(",");
        self.exec(&["client", "p2p", "replicator", "delete", "-c", &cols])
    }
}
