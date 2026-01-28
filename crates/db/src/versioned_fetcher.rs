//! Versioned document fetcher for CID-based time-travel queries.
//!
//! This module reconstructs documents at specific historical versions by:
//! 1. Walking the merkle DAG backwards from target CID to genesis
//! 2. Collecting all blocks in the path
//! 3. Replaying CRDT deltas forward to reconstruct document state

use cid::Cid;
use defra_core::block::{Block, CrdtDelta};
use document::{Document, NormalValue};
use std::collections::{HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use storage::corekv::Store;
use tokio::sync::Mutex as TokioMutex;

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
        let mut guard = self.txn.lock().await;
        let txn = guard.as_mut().ok_or(Error::TxnNotActive)?;

        // Parse the CID
        let target_cid = Self::parse_cid(cid_str)?;

        // Load the target block first to validate and get doc info
        let target_block = self.load_block(txn, &target_cid).await?;

        // Extract and validate document ID
        let doc_id = Self::extract_doc_id(&target_block.delta)?;

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

        Ok(document)
    }

    /// Parse a CID string, returning appropriate errors for invalid/unknown CIDs.
    fn parse_cid(cid_str: &str) -> Result<Cid> {
        Cid::from_str(cid_str).map_err(|e| {
            // Go's CID library is more lenient. If it looks like a valid CIDv1
            // format, treat as "not found" rather than "invalid".
            if Self::looks_like_cidv1(cid_str) {
                Error::Serialization("cid either does not exist or belong to document".to_string())
            } else {
                Error::Serialization(format!("invalid cid: {}", e))
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

    /// Extract document ID from a delta.
    fn extract_doc_id(delta: &CrdtDelta) -> Result<String> {
        delta
            .doc_id()
            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
            .ok_or_else(|| {
                Error::Serialization("cid either does not exist or belong to document".to_string())
            })
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
        let mut visited = HashSet::new();
        let mut queue: VecDeque<Cid> = VecDeque::new();

        // Start with the target block
        blocks.insert(*start_cid, start_block.clone());
        visited.insert(start_cid.to_string());

        // Queue up heads for traversal
        if let Some(ref heads) = start_block.heads {
            for head_cid in heads {
                queue.push_back(*head_cid);
            }
        }

        // Also traverse links (for composite blocks that link to field blocks)
        if let Some(ref links) = start_block.links {
            for link in links {
                if !visited.contains(&link.link.to_string()) {
                    queue.push_back(link.link);
                }
            }
        }

        // BFS traversal
        while let Some(cid) = queue.pop_front() {
            let cid_str = cid.to_string();
            if visited.contains(&cid_str) {
                continue;
            }
            visited.insert(cid_str);

            let block = match self.load_block(txn, &cid).await {
                Ok(b) => b,
                Err(_) => continue, // Skip missing blocks (genesis reached)
            };

            // Queue heads for further traversal
            if let Some(ref heads) = block.heads {
                for head_cid in heads {
                    if !visited.contains(&head_cid.to_string()) {
                        queue.push_back(*head_cid);
                    }
                }
            }

            // Queue links for composite blocks
            if let Some(ref links) = block.links {
                for link in links {
                    if !visited.contains(&link.link.to_string()) {
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
                Error::Serialization("cid either does not exist or belong to document".to_string())
            })?;

        Block::from_dag_cbor(&data)
            .map_err(|e| Error::Serialization(format!("Failed to decode block: {}", e)))
    }

    /// Replay CRDT deltas to reconstruct document state.
    ///
    /// Blocks should be sorted by priority (ascending) before calling this method.
    fn replay_deltas(&self, blocks: &[(Cid, Block)], doc_id: &str) -> Result<Document> {
        let mut field_values: HashMap<String, (u64, NormalValue)> = HashMap::new();

        for (_cid, block) in blocks {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    // Check if this delta belongs to our document
                    let delta_doc_id = String::from_utf8_lossy(&payload.doc_id);
                    if delta_doc_id != doc_id {
                        continue;
                    }

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
                    // Check if this delta belongs to our document
                    let delta_doc_id = String::from_utf8_lossy(&payload.doc_id);
                    if delta_doc_id != doc_id {
                        continue;
                    }

                    let field_name = &payload.field_name;

                    // Counter: accumulate increments
                    // Note: Counter replay needs nonce tracking for idempotency
                    // For now, we apply all deltas (may double-count on concurrent updates)
                    if !payload.data.is_empty() {
                        // Try decoding as NormalValue to handle both Int and Float counters
                        match ciborium::from_reader::<NormalValue, _>(&payload.data[..]) {
                            Ok(increment_value) => {
                                // Handle both Int and Float counter values
                                match &increment_value {
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
                                }
                            }
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
                CrdtDelta::Composite(_) => {
                    // Composite blocks link to field blocks, they don't contain data themselves
                    // The field blocks are already collected via the links traversal
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
