//! Commits fetcher for _commits system collection queries.
//!
//! This module provides fetching of commit history from the headstore and blockstore.
//! Commits are the CRDT blocks that make up document version history.

use cid::Cid;
use defra_core::block::{Block, CrdtDelta};
use document::Document;
use serde_json::{json, Value as JsonValue};
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use tokio::sync::Mutex as TokioMutex;

use crate::error::{Error, Result};
use crate::txn::DbTxn;

/// Options for commits queries
#[derive(Debug, Clone, Default)]
pub struct CommitsQueryOptions {
    /// Filter by document ID
    pub doc_id: Option<String>,
    /// Filter by specific CID
    pub cid: Option<String>,
    /// Maximum depth to traverse (None = unlimited)
    pub depth: Option<u64>,
    /// Filter by field name
    pub field_name: Option<String>,
}

/// Fetcher for commit history from the merkle DAG
pub struct CommitsFetcher<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
}

impl<S: Store> CommitsFetcher<S> {
    /// Create a new commits fetcher with a shared transaction
    pub fn new(txn: Arc<TokioMutex<Option<DbTxn<S>>>>) -> Self {
        Self { txn }
    }

    /// Fetch all commits matching the given options
    pub async fn fetch_commits(&self, options: &CommitsQueryOptions) -> Result<Vec<Document>> {
        let mut guard = self.txn.lock().await;
        let txn = guard.as_mut().ok_or(Error::TxnNotActive)?;

        // If a specific CID is requested, fetch just that commit
        if let Some(ref cid_str) = options.cid {
            return self.fetch_commit_by_cid(txn, cid_str, options).await;
        }

        // Otherwise, get all heads and traverse
        self.fetch_commits_from_heads(txn, options).await
    }

    /// Fetch a single commit by its CID
    async fn fetch_commit_by_cid(
        &self,
        txn: &mut DbTxn<S>,
        cid_str: &str,
        options: &CommitsQueryOptions,
    ) -> Result<Vec<Document>> {
        tracing::debug!(cid = %cid_str, "Fetching commit by CID");
        let cid = Cid::from_str(cid_str).map_err(|e| {
            tracing::error!(cid = %cid_str, error = %e, "Invalid CID format");
            // Go's CID library is more lenient and parses CIDs that look like valid
            // CIDv1 format but have invalid hashes. Rust's library is stricter.
            // If the string looks like a CIDv1 (starts with known multibase prefix
            // for CIDv1), treat it as "not found" rather than "invalid".
            if Self::looks_like_cidv1(cid_str) {
                Error::Serialization("cid either does not exist or belong to document".to_string())
            } else {
                Error::Serialization(format!("invalid cid: {}", e))
            }
        })?;

        let block = self.load_block(txn, &cid).await?;
        let commit_doc = self.block_to_commit_doc(&cid, &block)?;

        // Verify docID matches if specified
        if let Some(ref expected_doc_id) = options.doc_id {
            if let Some(actual_doc_id) = commit_doc.get("docID").and_then(|v| v.as_str()) {
                if actual_doc_id != expected_doc_id {
                    return Err(Error::Serialization(
                        "cid either does not exist or belong to document".to_string(),
                    ));
                }
            }
        }

        // If depth is specified and > 0, traverse heads
        let mut commits = vec![commit_doc];
        if let Some(depth) = options.depth {
            if depth > 0 {
                self.traverse_depth(txn, &block, depth - 1, &mut commits, options)
                    .await?;
            }
        }

        Ok(commits)
    }

    /// Fetch commits starting from all heads
    async fn fetch_commits_from_heads(
        &self,
        txn: &mut DbTxn<S>,
        options: &CommitsQueryOptions,
    ) -> Result<Vec<Document>> {
        // Get all head CIDs
        let head_cids = self.get_head_cids(txn, options).await?;

        let mut commits = Vec::new();
        let mut visited = HashSet::new();

        for cid in head_cids {
            if visited.contains(&cid.to_string()) {
                continue;
            }

            // BFS traversal with optional depth limit
            let mut queue: VecDeque<(Cid, u64)> = VecDeque::new();
            queue.push_back((cid, 0));

            while let Some((current_cid, current_depth)) = queue.pop_front() {
                let cid_str = current_cid.to_string();
                if visited.contains(&cid_str) {
                    continue;
                }
                visited.insert(cid_str);

                let block = match self.load_block(txn, &current_cid).await {
                    Ok(b) => b,
                    Err(_) => continue, // Skip missing blocks
                };

                // Check field name filter
                if let Some(ref field_filter) = options.field_name {
                    let field_name = self.get_field_name(&block.delta);
                    if field_name.as_deref() != Some(field_filter.as_str()) {
                        continue;
                    }
                }

                let commit_doc = self.block_to_commit_doc(&current_cid, &block)?;
                commits.push(commit_doc);

                // Check depth limit
                // Go's depth semantics: depth=1 means only heads (no traversal),
                // depth=2 means heads + their parents, etc.
                // So we check current_depth + 1 < max_depth to decide if we should
                // traverse to the next level.
                let should_traverse = match options.depth {
                    None => true, // No limit
                    Some(max_depth) => current_depth + 1 < max_depth,
                };

                if should_traverse {
                    // Add heads to queue
                    if let Some(ref heads) = block.heads {
                        for head_cid in heads {
                            queue.push_back((*head_cid, current_depth + 1));
                        }
                    }
                }
            }
        }

        // Sort commits to match Go's ordering: regular fields first (by name), composite (_C) last
        self.sort_commits_go_order(&mut commits);

        Ok(commits)
    }

    /// Sort commits to match Go DefraDB's ordering.
    ///
    /// Go's ordering for commits:
    /// 1. Group by docID first (documents in order they were created/discovered)
    /// 2. Within each document, sort by field ID: regular fields first (by name),
    ///    composite field (_C) last
    ///
    /// Go stores field IDs as numeric short IDs in headstore keys, which gives
    /// lexicographic order: "1" < "2" < "C" (composite comes after regular fields).
    fn sort_commits_go_order(&self, commits: &mut Vec<Document>) {
        commits.sort_by(|a, b| {
            // Primary sort: docID
            let doc_id_a = a.get("docID").and_then(|v| v.as_str()).unwrap_or("");
            let doc_id_b = b.get("docID").and_then(|v| v.as_str()).unwrap_or("");

            if doc_id_a != doc_id_b {
                return doc_id_a.cmp(doc_id_b);
            }

            // Secondary sort: fieldName (composite last)
            let field_a = a.get("fieldName").and_then(|v| v.as_str()).unwrap_or("");
            let field_b = b.get("fieldName").and_then(|v| v.as_str()).unwrap_or("");

            // Composite (_C) should come last within each document
            match (field_a == "_C", field_b == "_C") {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => field_a.cmp(field_b), // Regular fields sorted by name
            }
        });
    }

    /// Traverse depth from a starting block
    async fn traverse_depth(
        &self,
        txn: &mut DbTxn<S>,
        block: &Block,
        remaining_depth: u64,
        commits: &mut Vec<Document>,
        options: &CommitsQueryOptions,
    ) -> Result<()> {
        if remaining_depth == 0 {
            return Ok(());
        }

        if let Some(ref heads) = block.heads {
            for head_cid in heads {
                let head_block = match self.load_block(txn, head_cid).await {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                // Check field name filter
                if let Some(ref field_filter) = options.field_name {
                    let field_name = self.get_field_name(&head_block.delta);
                    if field_name.as_deref() != Some(field_filter.as_str()) {
                        continue;
                    }
                }

                let commit_doc = self.block_to_commit_doc(head_cid, &head_block)?;
                commits.push(commit_doc);

                // Recurse
                Box::pin(self.traverse_depth(
                    txn,
                    &head_block,
                    remaining_depth - 1,
                    commits,
                    options,
                ))
                .await?;
            }
        }

        Ok(())
    }

    /// Get all head CIDs from headstore
    async fn get_head_cids(
        &self,
        txn: &mut DbTxn<S>,
        options: &CommitsQueryOptions,
    ) -> Result<Vec<Cid>> {
        let headstore = txn.headstore()?;

        let prefix = if let Some(ref doc_id) = options.doc_id {
            format!("/d/{}/", doc_id).into_bytes()
        } else {
            // All document heads start with /d/
            b"/d/".to_vec()
        };

        let opts = IterOptions::new().with_prefix(prefix);
        let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;

        let mut cids = Vec::new();

        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            // Parse CID from key: /d/{doc_id}/{field_id}/{cid}
            let key_str = String::from_utf8_lossy(&pair.key);
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() >= 5 {
                // parts: ["", "d", doc_id, field_id, cid]
                if let Ok(cid) = Cid::from_str(parts[4]) {
                    cids.push(cid);
                }
            }
        }

        iter.close().await.map_err(Error::Storage)?;
        Ok(cids)
    }

    /// Load a block from blockstore by CID
    async fn load_block(&self, txn: &mut DbTxn<S>, cid: &Cid) -> Result<Block> {
        let blockstore = txn.blockstore()?;

        let key = cid.to_bytes();
        let data = blockstore
            .get(&key)
            .await
            .map_err(Error::Storage)?
            .ok_or_else(|| {
                Error::Serialization("cid either does not exist or belong to document".to_string())
            })?;

        Block::from_dag_cbor(&data)
            .map_err(|e| Error::Serialization(format!("Failed to decode block: {}", e)))
    }

    /// Convert a block to a commit document
    fn block_to_commit_doc(&self, cid: &Cid, block: &Block) -> Result<Document> {
        let mut map = HashMap::new();

        // cid
        map.insert("cid".to_string(), json!(cid.to_string()));

        // height (priority)
        map.insert("height".to_string(), json!(block.delta.priority() as i64));

        // fieldName
        let field_name = self.get_field_name(&block.delta);
        map.insert(
            "fieldName".to_string(),
            field_name.map(|s| json!(s)).unwrap_or(JsonValue::Null),
        );

        // docID
        let doc_id = self.get_doc_id(&block.delta);
        map.insert(
            "docID".to_string(),
            doc_id.map(|s| json!(s)).unwrap_or(JsonValue::Null),
        );

        // delta (CBOR encoded data as base64)
        let delta_data = self.get_delta_data(&block.delta);
        if let Some(data) = delta_data {
            use base64::Engine;
            map.insert(
                "delta".to_string(),
                json!(base64::engine::general_purpose::STANDARD.encode(&data)),
            );
        } else {
            map.insert("delta".to_string(), JsonValue::Null);
        }

        // collectionVersionId (schemaVersionID)
        let schema_version_id = self.get_schema_version_id(&block.delta);
        map.insert(
            "collectionVersionId".to_string(),
            schema_version_id
                .map(|s| json!(s))
                .unwrap_or(JsonValue::Null),
        );

        // links - array of {cid, fieldName}
        let links: Vec<JsonValue> = block
            .links
            .as_ref()
            .map(|links| {
                links
                    .iter()
                    .map(|link| {
                        json!({
                            "cid": link.link.to_string(),
                            "fieldName": link.name,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        map.insert("links".to_string(), json!(links));

        // heads - array of {cid}
        let heads: Vec<JsonValue> = block
            .heads
            .as_ref()
            .map(|heads| {
                heads
                    .iter()
                    .map(|head_cid| {
                        json!({
                            "cid": head_cid.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        map.insert("heads".to_string(), json!(heads));

        // signature - null for now (handle signature blocks separately)
        map.insert("signature".to_string(), JsonValue::Null);

        Document::from_map(map)
            .map_err(|e| Error::Serialization(format!("Failed to create document: {}", e)))
    }

    /// Get field name from delta
    fn get_field_name(&self, delta: &CrdtDelta) -> Option<String> {
        match delta {
            CrdtDelta::Lww(d) => Some(d.field_name.clone()),
            CrdtDelta::Counter(d) => Some(d.field_name.clone()),
            CrdtDelta::Composite(_) => Some("_C".to_string()), // Composite field marker
            CrdtDelta::Collection(_) => None,                  // Collection commits have no field
            CrdtDelta::FieldDefinition(_) => None,
            CrdtDelta::CollectionDefinition(_) => None,
        }
    }

    /// Get document ID from delta
    fn get_doc_id(&self, delta: &CrdtDelta) -> Option<String> {
        delta
            .doc_id()
            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
    }

    /// Get delta data from delta
    fn get_delta_data(&self, delta: &CrdtDelta) -> Option<Vec<u8>> {
        match delta {
            CrdtDelta::Lww(d) => {
                if d.data.is_empty() {
                    None
                } else {
                    Some(d.data.clone())
                }
            }
            CrdtDelta::Counter(d) => {
                if d.data.is_empty() {
                    None
                } else {
                    Some(d.data.clone())
                }
            }
            CrdtDelta::Composite(_) => None, // Composite has no data
            CrdtDelta::Collection(_) => None,
            CrdtDelta::FieldDefinition(_) => None,
            CrdtDelta::CollectionDefinition(_) => None,
        }
    }

    /// Get schema version ID from delta
    fn get_schema_version_id(&self, delta: &CrdtDelta) -> Option<String> {
        match delta {
            CrdtDelta::Lww(d) => Some(d.schema_version_id.clone()),
            CrdtDelta::Counter(d) => Some(d.schema_version_id.clone()),
            CrdtDelta::Composite(d) => Some(d.schema_version_id.clone()),
            CrdtDelta::Collection(d) => Some(d.schema_version_id.clone()),
            CrdtDelta::FieldDefinition(_) => None,
            CrdtDelta::CollectionDefinition(_) => None,
        }
    }

    /// Check if a string looks like a CIDv1.
    ///
    /// Go's CID library is more lenient and parses CIDs that have valid multibase
    /// prefixes but invalid hash components. Rust's library is stricter and rejects
    /// these. This function detects strings that "look like" CIDv1 so we can return
    /// a more appropriate error message for Go compatibility.
    fn looks_like_cidv1(s: &str) -> bool {
        // CIDv1 with base32lower typically starts with:
        // - "bafy" for dag-cbor + sha256
        // - "bafk" for raw + sha256
        // - "bafz" for dag-cbor + blake2b
        // CIDv0 starts with "Qm" (base58)
        // A minimum reasonable CID length is around 46 chars (CIDv0) or more
        if s.len() < 40 {
            return false;
        }
        s.starts_with("bafy")
            || s.starts_with("bafk")
            || s.starts_with("bafz")
            || s.starts_with("bafr")
            || s.starts_with("Qm")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commits_query_options_default() {
        let opts = CommitsQueryOptions::default();
        assert!(opts.doc_id.is_none());
        assert!(opts.cid.is_none());
        assert!(opts.depth.is_none());
        assert!(opts.field_name.is_none());
    }
}

#[cfg(test)]
mod additional_tests {
    use cid::Cid;
    use std::str::FromStr;

    #[test]
    fn test_invalid_cid_parsing() {
        let result = Cid::from_str("fhbnjfahfhfhanfhga");
        println!("Invalid CID result: {:?}", result);
        assert!(result.is_err(), "Invalid CID should fail to parse");
    }

    #[test]
    fn test_valid_cid_parsing() {
        let result = Cid::from_str("bafyreiajq6jmyblg2b6vupjdapzkaodbt7kkwqp4fijekdvydnyxvr4y7q");
        println!("Valid CID result: {:?}", result);
        assert!(result.is_ok(), "Valid CID should parse");
    }

    #[test]
    fn test_unknown_cid_parsing() {
        // This is the CID used in TestQueryCommitsWithUnknownCid
        // Go parses this successfully (lenient), Rust rejects it (strict)
        let result = Cid::from_str("bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist");
        println!("Unknown CID result: {:?}", result);
        if let Ok(cid) = &result {
            println!("CID codec: {:?}, hash: {:?}", cid.codec(), cid.hash());
        }
    }

    #[test]
    fn test_truly_invalid_cid_parsing() {
        // This is the CID used in TestQueryCommitsWithInvalidCid
        let result = Cid::from_str("fhbnjfahfhfhanfhga");
        println!("Truly invalid CID result: {:?}", result);
        assert!(result.is_err(), "Truly invalid CID should fail to parse");
    }

    #[test]
    fn test_looks_like_cidv1() {
        use crate::commits_fetcher::CommitsFetcher;
        use storage::backends::memory::MemoryStore;

        // Should be recognized as CIDv1-like
        assert!(CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist"
        ));
        assert!(CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "bafyreiajq6jmyblg2b6vupjdapzkaodbt7kkwqp4fijekdvydnyxvr4y7q"
        ));

        // Should NOT be recognized as CIDv1-like
        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "fhbnjfahfhfhanfhga"
        ));
        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1("short"));
        assert!(!CommitsFetcher::<MemoryStore>::looks_like_cidv1(
            "randomtext"
        ));
    }
}
