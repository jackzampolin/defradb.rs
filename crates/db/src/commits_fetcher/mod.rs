//! Commits fetcher for _commits system collection queries.
//!
//! This module provides fetching of commit history from the headstore and blockstore.
//! Commits are the CRDT blocks that make up document version history.

mod conversion;
mod delta_helpers;
#[cfg(test)]
mod tests;

use async_lock::Mutex as TokioMutex;
use cid::Cid;
use document::Document;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use storage::keys::HeadstorePriorityKey;

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
    /// Inclusive minimum commit height for indexed range scans
    pub height_start: Option<u64>,
    /// Exclusive maximum commit height for indexed range scans
    pub height_end: Option<u64>,
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

        if let Some(ref cid_str) = options.cid {
            return self.fetch_commit_by_cid(txn, cid_str, options).await;
        }

        if options.doc_id.is_some()
            && options.depth.is_none()
            && (options.height_start.is_some() || options.height_end.is_some())
        {
            return self.fetch_commits_by_height_range(txn, options).await;
        }

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
            if Self::looks_like_cidv1(cid_str) {
                Error::Serialization("cid either does not exist or belong to document".to_string())
            } else {
                Error::Serialization("invalid cid: selected encoding not supported".to_string())
            }
        })?;

        let block = self.load_block(txn, &cid).await?;
        let commit_doc = self.block_to_commit_doc(txn, &cid, &block).await?;

        if let Some(ref expected_doc_id) = options.doc_id {
            if let Some(actual_doc_id) = commit_doc.get("docID").and_then(|v| v.as_str()) {
                if actual_doc_id != expected_doc_id {
                    return Err(Error::Serialization(
                        "cid either does not exist or belong to document".to_string(),
                    ));
                }
            }
        }

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
        let head_cids = self.get_head_cids(txn, options).await?;

        let mut commits = Vec::new();
        let mut visited: HashSet<Cid> = HashSet::new();

        for cid in head_cids {
            if visited.contains(&cid) {
                continue;
            }

            let mut stack: Vec<(Cid, u64)> = Vec::new();
            stack.push((cid, 0));

            while let Some((current_cid, current_depth)) = stack.pop() {
                if visited.contains(&current_cid) {
                    continue;
                }
                visited.insert(current_cid);

                let block = match self.load_block(txn, &current_cid).await {
                    Ok(b) => b,
                    Err(_) => continue,
                };

                if let Some(ref field_filter) = options.field_name {
                    let field_name = self.get_field_name(&block.delta);
                    if field_name.as_deref() != Some(field_filter.as_str()) {
                        continue;
                    }
                }

                let commit_doc = self.block_to_commit_doc(txn, &current_cid, &block).await?;
                commits.push(commit_doc);

                let should_traverse = match options.depth {
                    None => true,
                    Some(max_depth) => current_depth + 1 < max_depth,
                };

                if should_traverse {
                    if let Some(ref heads) = block.heads {
                        for head_cid in heads {
                            stack.push((*head_cid, current_depth + 1));
                        }
                    }
                }
            }
        }

        self.sort_commits_go_order(&mut commits);

        Ok(commits)
    }

    /// Fetch commits through the secondary `(doc_id, priority) -> cid` index.
    async fn fetch_commits_by_height_range(
        &self,
        txn: &mut DbTxn<S>,
        options: &CommitsQueryOptions,
    ) -> Result<Vec<Document>> {
        let doc_id = options.doc_id.as_ref().ok_or_else(|| {
            Error::Serialization("doc_id is required for height range scans".to_string())
        })?;
        let headstore = txn.headstore()?;
        let systemstore = txn.systemstore()?;

        let Some(doc_ref) = crate::doc_id_map::get_doc_ref(&systemstore, doc_id).await? else {
            return Ok(Vec::new());
        };
        let doc_short_id = doc_ref.doc_short_id;

        let prefix = HeadstorePriorityKey::document_prefix(doc_short_id);
        let start =
            HeadstorePriorityKey::priority_prefix(doc_short_id, options.height_start.unwrap_or(0));

        let mut opts = IterOptions::new().with_prefix(prefix).with_start(start);
        if let Some(end) = options.height_end {
            opts = opts.with_end(HeadstorePriorityKey::priority_prefix(doc_short_id, end));
        }

        let cid_offset = HeadstorePriorityKey::cid_offset(doc_short_id);
        let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;
        let mut commits = Vec::new();
        let mut visited = HashSet::new();

        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            let Some(cid_bytes) = pair.key.get(cid_offset..) else {
                continue;
            };
            let Ok(cid) = Cid::try_from(cid_bytes) else {
                continue;
            };
            if !visited.insert(cid) {
                continue;
            }

            let block = match self.load_block(txn, &cid).await {
                Ok(block) => block,
                Err(_) => continue,
            };

            if let Some(ref field_filter) = options.field_name {
                let field_name = self.get_field_name(&block.delta);
                if field_name.as_deref() != Some(field_filter.as_str()) {
                    continue;
                }
            }

            let commit_doc = self.block_to_commit_doc(txn, &cid, &block).await?;
            commits.push(commit_doc);
        }
        iter.close().await.map_err(Error::Storage)?;

        self.sort_commits_go_order(&mut commits);

        Ok(commits)
    }

    /// Sort commits to match Go DefraDB's ordering.
    ///
    /// Go's ordering for commits:
    /// 1. Group by docID first (documents in order they were created/discovered)
    /// 2. Within each document, sort by field ID: regular fields first (by name),
    ///    composite field (_C) last
    fn sort_commits_go_order(&self, commits: &mut [Document]) {
        commits.sort_by(|a, b| {
            let doc_id_a = a.get("docID").and_then(|v| v.as_str()).unwrap_or("");
            let doc_id_b = b.get("docID").and_then(|v| v.as_str()).unwrap_or("");

            if doc_id_a != doc_id_b {
                return doc_id_a.cmp(doc_id_b);
            }

            let field_a = a.get("fieldName").and_then(|v| v.as_str()).unwrap_or("");
            let field_b = b.get("fieldName").and_then(|v| v.as_str()).unwrap_or("");

            match (field_a == "_C", field_b == "_C") {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => field_a.cmp(field_b),
            }
        });
    }

    /// Traverse depth from a starting block
    async fn traverse_depth(
        &self,
        txn: &mut DbTxn<S>,
        block: &defra_core::block::Block,
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

                if let Some(ref field_filter) = options.field_name {
                    let field_name = self.get_field_name(&head_block.delta);
                    if field_name.as_deref() != Some(field_filter.as_str()) {
                        continue;
                    }
                }

                let commit_doc = self.block_to_commit_doc(txn, head_cid, &head_block).await?;
                commits.push(commit_doc);

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
        let mut cids = Vec::new();

        if options.doc_id.is_none() {
            let col_opts = IterOptions::new().with_prefix(b"/c/".to_vec());
            let mut col_iter = headstore.iterator(col_opts).await.map_err(Error::Storage)?;

            while let Some(pair) = col_iter.next().await.map_err(Error::Storage)? {
                let key_str = String::from_utf8_lossy(&pair.key);
                let parts: Vec<&str> = key_str.split('/').collect();
                if parts.len() >= 4 {
                    if let Ok(cid) = Cid::from_str(parts[3]) {
                        cids.push(cid);
                    }
                }
            }
            col_iter.close().await.map_err(Error::Storage)?;
        }

        let doc_prefix = if let Some(ref doc_id) = options.doc_id {
            let systemstore = txn.systemstore()?;
            let Some(doc_ref) = crate::doc_id_map::get_doc_ref(&systemstore, doc_id).await? else {
                return Ok(cids);
            };
            storage::keys::headstore::HeadstoreDocKey::document_prefix(doc_ref.doc_short_id)
        } else {
            b"/d/".to_vec()
        };

        let opts = IterOptions::new().with_prefix(doc_prefix);
        let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;

        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            // Key: /d/{short_id uvarint}/{field}/{cid} — the short-ID segment
            // is binary, so only the trailing CID segment is parseable.
            let key_str = String::from_utf8_lossy(&pair.key);
            if let Some(cid_str) = key_str.rsplit('/').next() {
                if let Ok(cid) = Cid::from_str(cid_str) {
                    cids.push(cid);
                }
            }
        }
        iter.close().await.map_err(Error::Storage)?;

        Ok(cids)
    }
}
