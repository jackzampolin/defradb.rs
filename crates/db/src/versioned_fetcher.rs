//! Versioned document fetcher for CID-based time-travel queries.
//!
//! This module reconstructs documents at specific historical versions by:
//! 1. Walking the merkle DAG backwards from target CID to genesis
//! 2. Collecting all blocks in the path
//! 3. Replaying CRDT deltas forward to reconstruct document state

use async_lock::Mutex as TokioMutex;
use cid::Cid;
use defra_core::block::{Block, CrdtDelta};
use document::{Document, NormalValue};
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use storage::corekv::Store;

use crate::error::{Error, Result};
use crate::txn::DbTxn;

/// Fetcher for reconstructing documents at specific historical versions.
///
/// Given a CID, this fetcher walks backwards through the merkle DAG,
/// collects all blocks from the target CID to genesis, then replays
/// the CRDT deltas in forward order to reconstruct the document state.
pub struct VersionedFetcher<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
}

impl<S: Store> VersionedFetcher<S> {
    /// Create a new versioned fetcher with a shared transaction
    pub fn new(txn: Arc<TokioMutex<Option<DbTxn<S>>>>) -> Self {
        Self { txn }
    }

    /// Reconstruct a document at the specified CID.
    ///
    /// Returns the document state as it existed when the commit at `cid` was created.
    /// If `expected_doc_id` is provided, validates that the CID belongs to that document.
    pub async fn get_document_at_cid(
        &self,
        cid_str: &str,
        expected_doc_id: Option<&str>,
    ) -> Result<Document> {
        let docs = self.get_documents_at_cid(cid_str, expected_doc_id).await?;
        docs.into_iter().next().ok_or_else(|| {
            Error::Serialization("cid either does not exist or belong to document".to_string())
        })
    }

    /// Reconstruct documents at the specified CID.
    ///
    /// For document-level CIDs, returns a single document.
    /// For collection-level CIDs (branchable collections), walks the collection DAG
    /// and returns all documents visible at that collection state.
    pub async fn get_documents_at_cid(
        &self,
        cid_str: &str,
        expected_doc_id: Option<&str>,
    ) -> Result<Vec<Document>> {
        let mut guard = self.txn.lock().await;
        let txn = guard.as_mut().ok_or(Error::TxnNotActive)?;

        // Parse the CID
        let target_cid = Self::parse_cid(cid_str)?;

        // Load the target block first to validate and get doc info
        let target_block = self.load_block(txn, &target_cid).await?;

        // Check if this is a collection block (branchable collection CID)
        if matches!(&target_block.delta, CrdtDelta::Collection(_)) {
            return self
                .get_documents_at_collection_cid(txn, &target_cid, &target_block)
                .await;
        }

        // Regular document CID path
        let doc_id = self
            .resolve_doc_id(txn, &target_cid)
            .await?
            .ok_or_else(|| {
                Error::Serialization("cid either does not exist or belong to document".to_string())
            })?;

        if let Some(expected) = expected_doc_id {
            if doc_id != expected {
                return Err(Error::Serialization(
                    "cid either does not exist or belong to document".to_string(),
                ));
            }
        }

        // Collect all blocks from target CID back to genesis
        let blocks = self
            .collect_blocks_to_genesis(txn, &target_cid, &target_block)
            .await?;

        // Sort blocks by priority (ascending) for forward replay
        let mut sorted_blocks: Vec<(Cid, Block)> = blocks.into_iter().collect();
        sorted_blocks.sort_by_key(|(_, block)| block.delta.priority());

        // Replay deltas to reconstruct document
        let document = self.replay_deltas(&sorted_blocks, &doc_id)?;

        Ok(vec![document])
    }

    /// Reconstruct documents from a collection-level CID.
    ///
    /// Walks the collection DAG backwards to find all document composite CIDs,
    /// then reconstructs each unique document at its latest version up to
    /// the target collection state.
    async fn get_documents_at_collection_cid(
        &self,
        txn: &mut DbTxn<S>,
        start_cid: &Cid,
        start_block: &Block,
    ) -> Result<Vec<Document>> {
        // Walk the collection DAG backwards to find all document composite CIDs.
        // Each collection block links to one document composite block.
        // We track doc_id → (priority, composite_cid) keeping highest priority per doc.
        let mut doc_composites: HashMap<String, (u64, Cid)> = HashMap::new();
        let mut visited: HashSet<Cid> = HashSet::new();
        let mut queue: VecDeque<(Cid, Block)> = VecDeque::new();

        visited.insert(*start_cid);
        queue.push_back((*start_cid, start_block.clone()));

        while let Some((_col_cid, col_block)) = queue.pop_front() {
            // Extract document composite CID from collection block's links
            if let Some(ref links) = col_block.links {
                for link in links {
                    let doc_composite_cid = link.link;
                    // Load the document composite block to get its doc_id and priority
                    if let Ok(doc_block) = self.load_block(txn, &doc_composite_cid).await {
                        if let Some(doc_id) = self.resolve_doc_id(txn, &doc_composite_cid).await? {
                            let priority = doc_block.delta.priority();
                            match doc_composites.get(&doc_id) {
                                None => {
                                    doc_composites.insert(doc_id, (priority, doc_composite_cid));
                                }
                                Some((existing_priority, _)) => {
                                    if priority > *existing_priority {
                                        doc_composites
                                            .insert(doc_id, (priority, doc_composite_cid));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Traverse collection heads (previous collection blocks)
            if let Some(ref heads) = col_block.heads {
                for head_cid in heads {
                    if !visited.contains(head_cid) {
                        visited.insert(*head_cid);
                        if let Ok(head_block) = self.load_block(txn, head_cid).await {
                            queue.push_back((*head_cid, head_block));
                        }
                    }
                }
            }
        }

        // Reconstruct each document at its specific composite CID
        let mut documents = Vec::new();
        for (_priority, composite_cid) in doc_composites.values() {
            let composite_block = match self.load_block(txn, composite_cid).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            let doc_id = match self.resolve_doc_id(txn, composite_cid).await? {
                Some(id) => id,
                None => continue,
            };

            // Collect all blocks from this composite back to genesis
            let blocks = self
                .collect_blocks_to_genesis(txn, composite_cid, &composite_block)
                .await?;

            let mut sorted_blocks: Vec<(Cid, Block)> = blocks.into_iter().collect();
            sorted_blocks.sort_by_key(|(_, block)| block.delta.priority());

            let document = self.replay_deltas(&sorted_blocks, &doc_id)?;
            documents.push(document);
        }

        Ok(documents)
    }

    /// Parse a CID string, returning appropriate errors for invalid/unknown CIDs.
    fn parse_cid(cid_str: &str) -> Result<Cid> {
        Cid::from_str(cid_str).map_err(|_e| {
            // Go's CID library is more lenient. If it looks like a valid CIDv1
            // format, treat as "not found" rather than "invalid".
            if Self::looks_like_cidv1(cid_str) {
                Error::Serialization("cid either does not exist or belong to document".to_string())
            } else {
                Error::Serialization("invalid cid: selected encoding not supported".to_string())
            }
        })
    }

    /// Check if a string looks like a CIDv1.
    fn looks_like_cidv1(s: &str) -> bool {
        if s.len() < 40 {
            return false;
        }
        s.starts_with("bafy")
            || s.starts_with("bafk")
            || s.starts_with("bafz")
            || s.starts_with("bafr")
            || s.starts_with("Qm")
    }

    /// Resolve the DocID owning a block via the systemstore block-ownership
    /// index (`/d/b`). Deltas no longer carry docIDs (Go #4838).
    async fn resolve_doc_id(&self, txn: &mut DbTxn<S>, cid: &Cid) -> Result<Option<String>> {
        let systemstore = txn.systemstore()?;
        let owners =
            crate::doc_id_map::get_doc_ids_for_block(&systemstore, &cid.to_string()).await?;
        Ok(owners.into_iter().next())
    }

    /// Collect all blocks from target CID back to genesis using BFS.
    ///
    /// Returns a map of CID -> Block for all blocks in the history.
    async fn collect_blocks_to_genesis(
        &self,
        txn: &mut DbTxn<S>,
        start_cid: &Cid,
        start_block: &Block,
    ) -> Result<HashMap<Cid, Block>> {
        let mut blocks = HashMap::new();
        let mut visited: HashSet<Cid> = HashSet::new();
        let mut queue: VecDeque<Cid> = VecDeque::new();

        // Start with the target block
        blocks.insert(*start_cid, start_block.clone());
        visited.insert(*start_cid);

        // Queue up heads for traversal
        if let Some(ref heads) = start_block.heads {
            for head_cid in heads {
                queue.push_back(*head_cid);
            }
        }

        // Also traverse links (for composite blocks that link to field blocks)
        if let Some(ref links) = start_block.links {
            for link in links {
                if !visited.contains(&link.link) {
                    queue.push_back(link.link);
                }
            }
        }

        // BFS traversal
        while let Some(cid) = queue.pop_front() {
            if visited.contains(&cid) {
                continue;
            }
            visited.insert(cid);

            let block = match self.load_block(txn, &cid).await {
                Ok(b) => b,
                Err(_) => continue, // Skip missing blocks (genesis reached)
            };

            // Queue heads for further traversal
            if let Some(ref heads) = block.heads {
                for head_cid in heads {
                    if !visited.contains(head_cid) {
                        queue.push_back(*head_cid);
                    }
                }
            }

            // Queue links for composite blocks
            if let Some(ref links) = block.links {
                for link in links {
                    if !visited.contains(&link.link) {
                        queue.push_back(link.link);
                    }
                }
            }

            blocks.insert(cid, block);
        }

        Ok(blocks)
    }

    /// Load a block from blockstore by CID.
    async fn load_block(&self, txn: &mut DbTxn<S>, cid: &Cid) -> Result<Block> {
        let blockstore = txn.blockstore()?;

        let key = cid.to_bytes();
        let data = blockstore
            .get(&key)
            .await
            .map_err(Error::Storage)?
            .ok_or_else(|| {
                Error::Serialization(
                    "seek failed: (version fetcher) failed to get block in blockstore: ipld: could not find".to_string(),
                )
            })?;

        Block::from_dag_cbor(&data)
            .map_err(|e| Error::Serialization(format!("Failed to decode block: {}", e)))
    }

    /// Replay CRDT deltas to reconstruct document state.
    ///
    /// Blocks should be sorted by priority (ascending) before calling this method.
    fn replay_deltas(&self, blocks: &[(Cid, Block)], doc_id: &str) -> Result<Document> {
        let mut field_values: HashMap<String, (u64, NormalValue)> = HashMap::new();
        let mut is_deleted = false;
        let mut max_composite_priority: u64 = 0;

        for (_cid, block) in blocks {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    let field_name = &payload.field_name;
                    let priority = payload.priority;

                    // LWW merge: higher priority wins, on tie, lexicographic comparison
                    let should_apply = match field_values.get(field_name) {
                        None => true,
                        Some((current_priority, current_value)) => {
                            if priority > *current_priority {
                                true
                            } else if priority == *current_priority {
                                // Tie-break: lexicographic comparison of encoded data
                                let current_encoded = Self::encode_value(current_value);
                                payload.data > current_encoded
                            } else {
                                false
                            }
                        }
                    };

                    if should_apply {
                        if payload.data.is_empty() {
                            // Tombstone - remove field
                            field_values.remove(field_name);
                        } else {
                            // Decode and store value
                            match ciborium::from_reader::<NormalValue, _>(&payload.data[..]) {
                                Ok(value) => {
                                    field_values.insert(field_name.clone(), (priority, value));
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        field_name = %field_name,
                                        error = %e,
                                        "Failed to decode LWW value during replay"
                                    );
                                }
                            }
                        }
                    }
                }
                CrdtDelta::Counter(payload) => {
                    let field_name = &payload.field_name;

                    // Counter: accumulate increments
                    if !payload.data.is_empty() {
                        match ciborium::from_reader::<NormalValue, _>(&payload.data[..]) {
                            Ok(increment_value) => match &increment_value {
                                NormalValue::Int(increment) => {
                                    let current: i64 = field_values
                                        .get(field_name)
                                        .and_then(|(_, v)| v.as_int())
                                        .unwrap_or(0);
                                    let new_value = current.saturating_add(*increment);
                                    field_values.insert(
                                        field_name.clone(),
                                        (payload.priority, NormalValue::Int(new_value)),
                                    );
                                }
                                NormalValue::Float64(increment) => {
                                    let current: f64 = field_values
                                        .get(field_name)
                                        .and_then(|(_, v)| v.as_float64())
                                        .unwrap_or(0.0);
                                    let new_value = current + increment;
                                    field_values.insert(
                                        field_name.clone(),
                                        (payload.priority, NormalValue::Float64(new_value)),
                                    );
                                }
                                NormalValue::Float32(increment) => {
                                    let current: f32 = field_values
                                        .get(field_name)
                                        .and_then(|(_, v)| v.as_float32())
                                        .unwrap_or(0.0);
                                    let new_value = current + increment;
                                    field_values.insert(
                                        field_name.clone(),
                                        (payload.priority, NormalValue::Float32(new_value)),
                                    );
                                }
                                other => {
                                    tracing::warn!(
                                        field_name = %field_name,
                                        value_type = ?other,
                                        "Unexpected Counter value type during replay"
                                    );
                                }
                            },
                            Err(e) => {
                                tracing::warn!(
                                    field_name = %field_name,
                                    error = %e,
                                    "Failed to decode Counter value during replay"
                                );
                            }
                        }
                    }
                }
                CrdtDelta::Composite(payload) if payload.priority >= max_composite_priority => {
                    // Track document status from composite blocks.
                    // The highest-priority composite determines the final status.
                    max_composite_priority = payload.priority;
                    is_deleted = payload.status == 2;
                }
                _ => {
                    // Collection and schema definition deltas are not relevant for document reconstruction
                }
            }
        }

        // Build document from collected field values
        let mut document = Document::new();
        for (field_name, (_priority, value)) in field_values {
            document.set(&field_name, value);
        }

        // Set document ID
        if let Ok(doc_id_obj) = document::DocID::from_string(doc_id) {
            document.set_id(doc_id_obj);
        }

        // Set deleted status from composite block
        if is_deleted {
            document.set_deleted(true);
        }

        Ok(document)
    }

    /// Encode a NormalValue to bytes for comparison.
    fn encode_value(value: &NormalValue) -> Vec<u8> {
        let mut buf = Vec::new();
        if ciborium::into_writer(value, &mut buf).is_ok() {
            buf
        } else {
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_looks_like_cidv1() {
        assert!(
            VersionedFetcher::<storage::backends::memory::MemoryStore>::looks_like_cidv1(
                "bafyreiajq6jmyblg2b6vupjdapzkaodbt7kkwqp4fijekdvydnyxvr4y7q"
            )
        );
        assert!(
            VersionedFetcher::<storage::backends::memory::MemoryStore>::looks_like_cidv1(
                "bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist"
            )
        );
        assert!(
            !VersionedFetcher::<storage::backends::memory::MemoryStore>::looks_like_cidv1(
                "fhbnjfahfhfhanfhga"
            )
        );
        assert!(
            !VersionedFetcher::<storage::backends::memory::MemoryStore>::looks_like_cidv1("short")
        );
    }
}
