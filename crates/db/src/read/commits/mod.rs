//! Commits fetcher for _commits system collection queries.
//!
//! This module provides fetching of commit history from the headstore and blockstore.
//! Commits are the CRDT blocks that make up document version history.

mod conversion;
mod delta;
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
        let owners = self.get_doc_ids(txn, &cid, &block).await?;
        let canonical_expected = match options.doc_id.as_deref() {
            Some(doc_id) => Some(self.canonical_doc_id(txn, doc_id).await?),
            None => None,
        };
        let selected_owners: Vec<Option<String>> = match (&canonical_expected, owners) {
            (Some(expected), Some(owners)) if owners.iter().any(|owner| owner == expected) => {
                vec![Some(expected.clone())]
            }
            (Some(_), _) => {
                return Err(Error::Serialization(
                    "cid either does not exist or belong to document".to_string(),
                ));
            }
            (None, Some(owners)) if !owners.is_empty() => owners.into_iter().map(Some).collect(),
            (None, Some(_)) => vec![None],
            (None, None) => {
                return Err(Error::Serialization(
                    "cid either does not exist or belong to document".to_string(),
                ));
            }
        };

        let mut commits = Vec::new();
        for owner in selected_owners {
            commits.push(
                self.block_to_commit_doc(txn, &cid, &block, owner.as_deref())
                    .await?,
            );
            if let Some(depth) = options.depth {
                if depth > 0 {
                    self.traverse_depth(
                        txn,
                        &block,
                        depth - 1,
                        &mut commits,
                        options,
                        owner.as_deref(),
                    )
                    .await?;
                }
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
        let mut visited: HashSet<(Cid, Option<String>)> = HashSet::new();

        for (cid, doc_id) in head_cids {
            if visited.contains(&(cid, doc_id.clone())) {
                continue;
            }

            let mut stack: Vec<(Cid, u64, Option<String>)> = Vec::new();
            stack.push((cid, 0, doc_id));

            while let Some((current_cid, current_depth, doc_id)) = stack.pop() {
                if !visited.insert((current_cid, doc_id.clone())) {
                    continue;
                }

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

                let commit_doc = self
                    .block_to_commit_doc(txn, &current_cid, &block, doc_id.as_deref())
                    .await?;
                commits.push(commit_doc);

                let should_traverse = match options.depth {
                    None => true,
                    Some(max_depth) => current_depth + 1 < max_depth,
                };

                if should_traverse {
                    if let Some(ref heads) = block.heads {
                        for head_cid in heads {
                            stack.push((*head_cid, current_depth + 1, doc_id.clone()));
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
        let canonical_doc_id = crate::doc_id_map::get_doc_id(&systemstore, doc_short_id)
            .await?
            .ok_or_else(|| {
                Error::InvalidDocument(format!(
                    "document short ID {doc_short_id} has no canonical DocID"
                ))
            })?;

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

            let commit_doc = self
                .block_to_commit_doc(txn, &cid, &block, Some(&canonical_doc_id))
                .await?;
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
        let mut doc_order = std::collections::HashMap::new();
        for commit in commits.iter() {
            let doc_id = commit
                .get("docID")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let next_index = doc_order.len();
            doc_order.entry(doc_id.to_string()).or_insert(next_index);
        }

        commits.sort_by(|a, b| {
            let doc_id_a = a.get("docID").and_then(|v| v.as_str()).unwrap_or("");
            let doc_id_b = b.get("docID").and_then(|v| v.as_str()).unwrap_or("");

            if doc_id_a != doc_id_b {
                return doc_order[doc_id_a].cmp(&doc_order[doc_id_b]);
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
        doc_id: Option<&str>,
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

                let commit_doc = self
                    .block_to_commit_doc(txn, head_cid, &head_block, doc_id)
                    .await?;
                commits.push(commit_doc);

                Box::pin(self.traverse_depth(
                    txn,
                    &head_block,
                    remaining_depth - 1,
                    commits,
                    options,
                    doc_id,
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
    ) -> Result<Vec<(Cid, Option<String>)>> {
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
                        cids.push((cid, None));
                    }
                }
            }
            col_iter.close().await.map_err(Error::Storage)?;
        }

        let (doc_prefix, fixed_doc_id) = if let Some(ref doc_id) = options.doc_id {
            let systemstore = txn.systemstore()?;
            let Some(doc_ref) = crate::doc_id_map::get_doc_ref(&systemstore, doc_id).await? else {
                return Ok(cids);
            };
            let canonical_doc_id =
                crate::doc_id_map::get_doc_id(&systemstore, doc_ref.doc_short_id)
                    .await?
                    .ok_or_else(|| {
                        Error::InvalidDocument(format!(
                            "document short ID {} has no canonical DocID",
                            doc_ref.doc_short_id
                        ))
                    })?;
            (
                storage::keys::headstore::HeadstoreDocKey::document_prefix(doc_ref.doc_short_id),
                Some(canonical_doc_id),
            )
        } else {
            (b"/d/".to_vec(), None)
        };

        let opts = IterOptions::new().with_prefix(doc_prefix);
        let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;

        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            // Key: /d/{short_id uvarint}/{field}/{cid} — the short-ID segment
            // is binary, so only the trailing CID segment is parseable.
            let key_str = String::from_utf8_lossy(&pair.key);
            if let Some(cid_str) = key_str.rsplit('/').next() {
                if let Ok(cid) = Cid::from_str(cid_str) {
                    let doc_id = if let Some(doc_id) = fixed_doc_id.as_ref() {
                        Some(doc_id.clone())
                    } else {
                        let Ok((_, doc_short_id)) =
                            storage::keys::doc_id_index::decode_doc_short_id_prefix(&pair.key[3..])
                        else {
                            continue;
                        };
                        let systemstore = txn.systemstore()?;
                        crate::doc_id_map::get_doc_id(&systemstore, doc_short_id).await?
                    };
                    if doc_id.is_some() {
                        cids.push((cid, doc_id));
                    }
                }
            }
        }
        iter.close().await.map_err(Error::Storage)?;

        Ok(cids)
    }
}
