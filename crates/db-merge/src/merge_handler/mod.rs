//! Database merge handler for processing incoming P2P blocks.
//!
//! This module implements the `MergeHandler` trait from the P2P layer,
//! bridging incoming blocks to the CRDT system for document merging.

mod batch;
mod collection;
mod composite;
mod composite_fields;
mod composite_heads;
mod composite_persist;
mod counter;
mod definition;
pub(crate) mod error;
pub(crate) mod hook;
mod lww;
mod queue;
pub(crate) mod se_merge;
mod signature;

pub use error::MergeError;
pub(crate) use error::{CounterMergeResult, LwwMergeResult};
pub use queue::MergeQueue;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use crdt::traits::{Context, ReplicatedData, ValueReader};
use crdt::{Counter, CounterDelta, Lww, LwwDelta, NumericKind};
use datastore::NamespaceView;
use defra_core::block::{
    Block, CollectionDefinitionDeltaPayload, CrdtDelta, Encryption, FieldDefinitionDeltaPayload,
};
use defra_core::types::DocId;
use document::{DocID, Document, NormalValue};
use events::{MergeCompleteData, Message, Update};
use p2p::sync::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome, RecoveredBlockMetadata};
use schema::{
    self, CType, CollectionSource, CollectionVersion, FieldDescription, FieldKind, QuerySource,
    ScalarKind,
};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionVersionKey};
use zeroize::Zeroizing;

use db::collection::Collection;
use db::database::DB;
use db::index_manager::IndexManager;
use hook::CompositeMergeHook;

/// Maximum DAG recursion depth for merge operations.
///
/// Prevents stack overflow from a malicious or corrupt DAG with deeply nested heads.
/// Recursive async functions allocate a stack frame per level; 1024 is sufficient for
/// legitimate replication chains while remaining well within typical stack limits.
pub(crate) const MAX_MERGE_DEPTH: usize = 1024;

/// Encode a priority value as a varint (matches Go's binary.PutUvarint).
pub(crate) fn encode_priority_varint(priority: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10);
    let mut n = priority;
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
    buf
}

/// Database merge handler that processes incoming P2P blocks.
///
/// This handler decodes IPLD blocks, extracts CRDT deltas, and applies
/// them to the database using the appropriate CRDT type.
pub struct DbMergeHandler<S: Store, B: blockstore::Blockstore> {
    /// Reference to the database for creating transactions.
    pub(crate) db: Arc<DB<S>>,
    /// Reference to the blockstore for loading linked blocks.
    pub(crate) blockstore: Arc<B>,
    /// Optional merge hook for policy-specific behavior around composite merges.
    composite_merge_hook: std::sync::OnceLock<Arc<dyn CompositeMergeHook>>,
    /// Tracks composite CIDs that have already been merged, preventing
    /// duplicate processing from concurrent dual-broadcast paths (doc topic
    /// + collection topic). Matches Go's `loadComposites` dedup guard.
    pub(crate) merged_composites: std::sync::Mutex<HashSet<Cid>>,
    /// Tracks collection CIDs that have already been merged, preventing
    /// replayed collection blocks from re-adding obsolete collection heads.
    pub(crate) merged_collections: std::sync::Mutex<HashSet<Cid>>,
    /// Optional SE encryption key for generating search artifacts on replicated documents.
    /// When set, the merge handler generates SE artifacts after merging documents
    /// that belong to collections with encrypted indexes.
    se_enc_key: std::sync::OnceLock<Zeroizing<Vec<u8>>>,
    /// Optional KMS service. When set, `decrypt_block_data` routes DEK
    /// retrieval through the KMS (NAC/DAC-gated, cross-peer fetch) instead
    /// of reading the raw key directly from the Encryption block.
    kms: std::sync::OnceLock<Arc<dyn kms::KmsService>>,
    /// Per-document merge serialization queue.
    ///
    /// Ensures concurrent P2P merges for the same document are processed one
    /// at a time, preventing write-write races at the storage level.
    pub(crate) merge_queue: Arc<MergeQueue>,
}

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Create a new database merge handler.
    pub fn new(db: Arc<DB<S>>, blockstore: Arc<B>) -> Self {
        Self {
            db,
            blockstore,
            composite_merge_hook: std::sync::OnceLock::new(),
            merged_composites: std::sync::Mutex::new(HashSet::new()),
            merged_collections: std::sync::Mutex::new(HashSet::new()),
            se_enc_key: std::sync::OnceLock::new(),
            kms: std::sync::OnceLock::new(),
            merge_queue: Arc::new(MergeQueue::new()),
        }
    }

    /// Set the composite merge hook after construction.
    pub(crate) fn set_composite_merge_hook(&self, hook: Arc<dyn CompositeMergeHook>) {
        let _ = self.composite_merge_hook.set(hook);
    }

    pub(crate) fn composite_merge_hook(&self) -> Option<&Arc<dyn CompositeMergeHook>> {
        self.composite_merge_hook.get()
    }

    /// Set the SE encryption key for generating artifacts on replicated documents.
    pub fn set_se_enc_key(&self, key: Vec<u8>) {
        let _ = self.se_enc_key.set(Zeroizing::new(key));
    }

    /// Get the SE encryption key, if configured.
    pub(crate) fn se_enc_key(&self) -> Option<&[u8]> {
        self.se_enc_key.get().map(|k| k.as_slice())
    }

    /// Set the KMS service. Routes `decrypt_block_data` through the KMS once set.
    pub fn set_kms(&self, kms: Arc<dyn kms::KmsService>) {
        let _ = self.kms.set(kms);
    }

    /// Get the KMS service, if configured.
    pub(crate) fn kms(&self) -> Option<Arc<dyn kms::KmsService>> {
        self.kms.get().cloned()
    }

    /// Get reference to blockstore.
    pub fn blockstore(&self) -> &Arc<B> {
        &self.blockstore
    }

    pub(crate) async fn validate_explicit_replay_authorization(
        &self,
        authorization: Option<&p2p::ExplicitReplayAuthorization>,
        block: &MergeBlock,
    ) -> Result<(), MergeError> {
        let Some(authorization) = authorization else {
            return Ok(());
        };

        if authorization.collection_id != block.collection_id {
            return Err(MergeError::MergeFailed(format!(
                "explicit replay authorization collection '{}' does not match block collection '{}'",
                authorization.collection_id, block.collection_id
            )));
        }

        let decoded_block = Block::from_dag_cbor(&block.block_data)
            .map_err(|error| MergeError::BlockDecode(error.to_string()))?;
        let verified_creator = self
            .verify_block_signature(&block.cid, &decoded_block, &block.block_data)
            .await?;
        let effective_creator = verified_creator
            .as_deref()
            .unwrap_or(block.creator.as_str());

        if effective_creator != authorization.authorizer_did {
            return Err(MergeError::MergeFailed(format!(
                "explicit replay authorization authorizer '{}' does not match block creator '{}'",
                authorization.authorizer_did, effective_creator
            )));
        }

        Ok(())
    }

    async fn recover_metadata_from_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
    ) -> Result<Option<RecoveredBlockMetadata>, MergeError> {
        let block =
            Block::from_dag_cbor(block_data).map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        let Some((doc_id, collection_id)) = Self::doc_metadata_from_block(&block) else {
            return Ok(None);
        };

        let Some(creator) = self.verify_block_signature(cid, &block, block_data).await? else {
            return Ok(None);
        };

        Ok(Some(
            RecoveredBlockMetadata::new(doc_id, collection_id, creator.clone())
                .with_verified_creator(Some(creator)),
        ))
    }

    fn doc_metadata_from_block(block: &Block) -> Option<(String, String)> {
        match &block.delta {
            CrdtDelta::Lww(payload) => Some((
                String::from_utf8_lossy(&payload.doc_id).to_string(),
                payload.schema_version_id.clone(),
            )),
            CrdtDelta::Counter(payload) => Some((
                String::from_utf8_lossy(&payload.doc_id).to_string(),
                payload.schema_version_id.clone(),
            )),
            CrdtDelta::Composite(payload) => Some((
                String::from_utf8_lossy(&payload.doc_id).to_string(),
                payload.schema_version_id.clone(),
            )),
            _ => None,
        }
    }

    /// Decrypt block delta data using the encryption metadata block.
    ///
    /// If `encryption_cid` is Some, loads the Encryption block from encstore,
    /// falling back to the P2P blockstore when the metadata arrived via replay,
    /// extracts the AES key, and decrypts the data. Returns data unchanged if
    /// no encryption CID is present.
    fn kms_request_context(metadata: Option<&BlockMetadata<'_>>) -> kms::RequestContext {
        let Some(metadata) = metadata else {
            return kms::RequestContext::anonymous();
        };
        let Some(collection_id) = metadata.collection_id else {
            return kms::RequestContext::anonymous();
        };
        let Some(authorizer) = metadata.explicit_replay_authorizer_for(collection_id) else {
            return kms::RequestContext::anonymous();
        };
        match identity::Did::new(authorizer) {
            Ok(did) => kms::RequestContext::with_user(did),
            Err(_) => kms::RequestContext::anonymous(),
        }
    }

    pub(crate) async fn decrypt_block_data(
        &self,
        data: &[u8],
        encryption_cid: Option<&Cid>,
        metadata: Option<&BlockMetadata<'_>>,
    ) -> std::result::Result<Vec<u8>, MergeError> {
        let enc_cid = match encryption_cid {
            Some(cid) => cid,
            None => return Ok(data.to_vec()),
        };

        // KMS path: fetch the DEK through the KMS (NAC/DAC-gated). The KMS
        // resolves the key locally or via cross-peer fetch and returns the
        // plaintext key; we then AES-GCM decrypt the block data.
        if let Some(kms) = self.kms() {
            let ctx = Self::kms_request_context(metadata);
            let results = kms
                .get_keys(&ctx, std::slice::from_ref(enc_cid))
                .await
                .map_err(|e| MergeError::Storage(format!("kms get_keys: {e}")))?;
            let map = results
                .wait_all()
                .await
                .map_err(|e| MergeError::Storage(format!("kms wait_all: {e}")))?;
            let key = map
                .get(enc_cid)
                .ok_or_else(|| MergeError::Storage(format!("kms returned no key for {enc_cid}")))?;
            return crypto::encryption::aes::decrypt_aes(None, data, key, &[])
                .map_err(|e| MergeError::MergeFailed(format!("kms-keyed decryption failed: {e}")));
        }

        // Legacy path (unchanged): read the raw key directly from the
        // Encryption block in encstore/blockstore.
        let enc_txn = self.db.new_txn(true).await.map_err(MergeError::Database)?;
        let encstore = enc_txn.encstore().map_err(MergeError::Database)?;
        let enc_cid_bytes = enc_cid.to_bytes();
        let enc_data = if let Some(enc_data) = encstore
            .get(&enc_cid_bytes)
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?
        {
            enc_data
        } else if let Some(enc_data) = self
            .blockstore
            .get(enc_cid)
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?
        {
            enc_data.to_vec()
        } else {
            return Err(MergeError::Storage(format!(
                "Encryption block {} not found",
                enc_cid
            )));
        };

        let enc_block = Encryption::from_dag_cbor(&enc_data).map_err(|e| {
            MergeError::BlockDecode(format!("Failed to decode encryption block: {}", e))
        })?;

        // Decrypt using AES-256-GCM (nonce is prepended to ciphertext)
        match crypto::encryption::aes::decrypt_aes(None, data, &enc_block.key, &[]) {
            Ok(decrypted) => {
                tracing::debug!(
                    encryption_cid = %enc_cid,
                    plaintext_len = decrypted.len(),
                    "Decrypted block data"
                );
                Ok(decrypted)
            }
            Err(e) => {
                tracing::warn!(
                    encryption_cid = %enc_cid,
                    error = %e,
                    "Failed to decrypt block data"
                );
                Err(MergeError::MergeFailed(format!("Decryption failed: {}", e)))
            }
        }
    }

    /// Look up a previous CollectionVersion from block heads.
    ///
    /// For patched collection versions, the block's heads point to previous version CIDs.
    /// First checks systemstore, then falls back to decoding the head block directly
    /// from blockstore (for the UnknownCollection case where the initial version
    /// hasn't been processed yet).
    pub(super) async fn resolve_previous_collection_version(
        &self,
        block: &Block,
    ) -> Result<Option<schema::CollectionVersion>, MergeError> {
        use defra_core::block::CrdtDelta;
        use storage::corekv::Key;
        use storage::keys::systemstore::{CollectionKey, CollectionVersionKey};

        let heads = match &block.heads {
            Some(heads) if !heads.is_empty() => heads,
            _ => return Ok(None),
        };

        for head_cid in heads {
            // Fast path: check systemstore (KnownCollection case)
            let head_key = CollectionKey::new(head_cid.to_string());
            let txn = self.db.new_txn(true).await.map_err(MergeError::Database)?;
            let systemstore = txn.systemstore().map_err(MergeError::Database)?;

            if let Ok(Some(data)) = systemstore.get(&head_key.bytes()).await {
                if let Ok(prev) = serde_json::from_slice::<schema::CollectionVersion>(&data) {
                    tracing::debug!(
                        head_cid = %head_cid,
                        name = %prev.name,
                        collection_id = %prev.collection_id,
                        "Resolved previous collection version from systemstore"
                    );
                    return Ok(Some(prev));
                }
            }

            // Slow path: decode head block directly from blockstore.
            // Build a CollectionVersion from the raw block data without going
            // through the full merge handler (avoids async recursion).
            if let Ok(Some(head_block_data)) = self.blockstore.get(head_cid).await {
                let head_block = Block::from_dag_cbor(&head_block_data).map_err(|e| {
                    MergeError::BlockDecode(format!("Failed to decode head block: {}", e))
                })?;

                if let CrdtDelta::CollectionDefinition(head_payload) = &head_block.delta {
                    if let Some(name) = &head_payload.name {
                        tracing::debug!(
                            head_cid = %head_cid,
                            name = %name,
                            "Resolved previous collection version from blockstore"
                        );

                        // Decode field blocks from the head block's links
                        let mut prev_fields = Vec::new();
                        if let Some(links) = &head_block.links {
                            for link in links.iter() {
                                let field_cid = &link.link;
                                if let Ok(Some(field_bytes)) = self.blockstore.get(field_cid).await
                                {
                                    if let Ok(field_block) = Block::from_dag_cbor(&field_bytes) {
                                        if let CrdtDelta::FieldDefinition(fp) = &field_block.delta {
                                            if let Ok(fd) = self.field_definition_to_description(
                                                fp,
                                                &field_cid.to_string(),
                                            ) {
                                                prev_fields.push(fd);
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        let head_version_id = head_cid.to_string();
                        let mut prev = schema::CollectionVersion::new(
                            name,
                            &head_version_id,
                            &head_version_id,
                            prev_fields,
                        );
                        prev.is_active = false;
                        prev.is_materialized = true;

                        // Ensure _docID is first in fields
                        if let Some(pos) = prev.fields.iter().position(|f| f.name == "_docID") {
                            if pos > 0 {
                                let f = prev.fields.remove(pos);
                                prev.fields.insert(0, f);
                            }
                        }

                        // Store in systemstore so GetCollections can find it
                        let txn2 = self.db.new_txn(false).await.map_err(MergeError::Database)?;
                        {
                            let ss = txn2.systemstore().map_err(MergeError::Database)?;
                            let key = CollectionKey::new(&head_version_id);
                            let data = serde_json::to_vec(&prev).map_err(|e| {
                                MergeError::Storage(format!(
                                    "Failed to serialize prev collection: {}",
                                    e
                                ))
                            })?;
                            ss.set(&key.bytes(), &data).await.map_err(|e| {
                                MergeError::Storage(format!(
                                    "Failed to store prev collection: {}",
                                    e
                                ))
                            })?;
                            let vkey =
                                CollectionVersionKey::new(&head_version_id, &head_version_id);
                            ss.set(&vkey.bytes(), b"1").await.map_err(|e| {
                                MergeError::Storage(format!(
                                    "Failed to store prev version index: {}",
                                    e
                                ))
                            })?;
                        }
                        txn2.commit().await.map_err(MergeError::Database)?;
                        self.db
                            .add_collection_to_cache(prev.clone())
                            .map_err(MergeError::Database)?;

                        return Ok(Some(prev));
                    }
                }
            }
        }

        Ok(None)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static, B: blockstore::Blockstore + Send + Sync + 'static> MergeHandler
    for DbMergeHandler<S, B>
{
    type Error = MergeError;

    async fn validate_authorization(
        &self,
        authorization: Option<&p2p::ExplicitReplayAuthorization>,
        block: &MergeBlock,
    ) -> Result<(), Self::Error> {
        self.validate_explicit_replay_authorization(authorization, block)
            .await
    }

    async fn recover_block_metadata(
        &self,
        cid: &Cid,
        block_data: &[u8],
    ) -> Result<Option<RecoveredBlockMetadata>, Self::Error> {
        self.recover_metadata_from_block(cid, block_data).await
    }

    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        tracing::debug!(
            cid = %cid,
            block_size = block_data.len(),
            is_recovery = metadata.is_recovery,
            "Handling block for merge"
        );

        // Decode the block from DAG-CBOR
        let block =
            Block::from_dag_cbor(block_data).map_err(|e| MergeError::BlockDecode(e.to_string()))?;

        tracing::debug!(
            cid = %cid,
            delta_type = ?std::mem::discriminant(&block.delta),
            heads_count = block.heads.as_ref().map(|h| h.len()).unwrap_or(0),
            links_count = block.links.as_ref().map(|l| l.len()).unwrap_or(0),
            "Block decoded successfully"
        );

        // Verify block signature for P2P blocks (skip during recovery).
        // On success, populate verified_creator with the cryptographically
        // verified signer identity. Invalid signatures reject the block.
        let mut metadata = metadata;
        if !metadata.is_recovery {
            let verified = self.verify_block_signature(cid, &block, block_data).await?;
            metadata.verified_creator = verified;
        }

        // Decrypt delta data if the block has encryption.
        // If decryption fails (encryption key block unavailable), skip the
        // standalone field merge -- the composite merge will re-attempt
        // decryption when it processes the linked field blocks.
        let decrypted_block;
        let effective_block = if block.encryption.is_some() {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    match self
                        .decrypt_block_data(
                            &payload.data,
                            block.encryption.as_ref(),
                            Some(&metadata),
                        )
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Lww(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(e) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %e,
                                "Cannot decrypt standalone LWW block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                    }
                }
                CrdtDelta::Counter(payload) => {
                    match self
                        .decrypt_block_data(
                            &payload.data,
                            block.encryption.as_ref(),
                            Some(&metadata),
                        )
                        .await
                    {
                        Ok(decrypted_data) => {
                            let mut new_payload = payload.clone();
                            new_payload.data = decrypted_data;
                            decrypted_block = Block {
                                delta: CrdtDelta::Counter(new_payload),
                                heads: block.heads.clone(),
                                links: block.links.clone(),
                                encryption: block.encryption,
                                signature: block.signature,
                            };
                            &decrypted_block
                        }
                        Err(e) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %e,
                                "Cannot decrypt standalone Counter block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                    }
                }
                _ => &block,
            }
        } else {
            &block
        };

        // Process based on delta type
        match &effective_block.delta {
            CrdtDelta::Lww(payload) => self.process_lww_delta(cid, payload, &metadata).await,
            CrdtDelta::Counter(payload) => {
                self.process_counter_delta(cid, payload, &metadata).await
            }
            CrdtDelta::Composite(payload) => {
                self.process_composite_delta(cid, &block, payload, &metadata, false, 0)
                    .await
            }
            CrdtDelta::Collection(payload) => {
                self.process_collection_delta(cid, &block, payload, &metadata, 0)
                    .await
            }
            CrdtDelta::FieldDefinition(_) => {
                // Field definitions are processed as part of CollectionDefinition
                tracing::debug!(cid = %cid, "FieldDefinition delta - skipping (processed with collection)");
                Ok(MergeOutcome::terminal_skip(
                    "field definition processed with collection",
                ))
            }
            CrdtDelta::CollectionDefinition(payload) => {
                self.process_collection_definition_delta(cid, &block, payload, &metadata)
                    .await
            }
            CrdtDelta::CollectionSet(_) => {
                tracing::debug!(cid = %cid, "CollectionSet delta - skipping");
                Ok(MergeOutcome::terminal_skip("collection set delta"))
            }
            // Only the variant discriminant is reported — `CrdtDelta` carries
            // field-value bytes from user documents and must not be formatted
            // into error strings that may end up in logs.
            other => Err(MergeError::UnsupportedDelta(format!(
                "unhandled CrdtDelta variant in merge dispatch: {:?}",
                std::mem::discriminant(other)
            ))),
        }
    }

    async fn handle_block_batch(
        &self,
        blocks: &[MergeBlock],
    ) -> Vec<Result<MergeOutcome, Self::Error>> {
        if blocks.len() <= 1 {
            return self.merge_blocks_individually(blocks).await;
        }

        self.try_batch_merge_with_split(blocks).await
    }
}

#[cfg(test)]
mod tests {
    use super::hook::{CompositeMergeHook, CompositePostCommitAction};
    use super::*;
    use async_trait::async_trait;
    use blockstore::{Blockstore as _, DefraBlockstore};
    use crypto::PrivateKey as _;
    use defra_core::block::{
        Block, CollectionDefinitionDeltaPayload, CompositeDeltaPayload, CounterDeltaPayload,
        CrdtDelta, DAGLink, Encryption, LwwDeltaPayload, Signature, SignatureHeader, SignatureType,
    };
    use events::{Bus, ChannelBus, EventName};
    use schema::{CType, CollectionVersion, FieldDescription, FieldKind};
    use storage::backends::MemoryStore;
    use storage::corekv::Key;
    use storage::keys::systemstore::CollectionID;
    use tokio::time::{timeout, Duration};

    fn make_handler() -> (
        DbMergeHandler<MemoryStore, DefraBlockstore<MemoryStore>>,
        Arc<DefraBlockstore<MemoryStore>>,
    ) {
        let store = MemoryStore::new();
        let store_arc = Arc::new(store);
        let db = Arc::new(DB::from_arc(store_arc.clone()).unwrap());
        let blockstore = Arc::new(DefraBlockstore::new(store_arc, false));
        let handler = DbMergeHandler::new(db, blockstore.clone());
        (handler, blockstore)
    }

    async fn make_handler_with_schema_and_bus() -> (
        DbMergeHandler<MemoryStore, DefraBlockstore<MemoryStore>>,
        Arc<DefraBlockstore<MemoryStore>>,
        Arc<ChannelBus>,
    ) {
        let store = Arc::new(MemoryStore::new());
        let bus = Arc::new(ChannelBus::new());

        let mut db = DB::from_arc(store.clone()).unwrap();
        db.set_event_bus(bus.clone());
        let db = Arc::new(db);

        db.create_collection(CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        ))
        .await
        .unwrap();

        let blockstore = Arc::new(DefraBlockstore::new(store, false));
        let handler = DbMergeHandler::new(db, blockstore.clone());
        (handler, blockstore, bus)
    }

    async fn make_handler_with_counter_schema() -> (
        DbMergeHandler<MemoryStore, DefraBlockstore<MemoryStore>>,
        Arc<DefraBlockstore<MemoryStore>>,
    ) {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store.clone()).unwrap());

        db.create_collection(CollectionVersion::new(
            "Counters",
            "v1",
            "col-counters",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "score", FieldKind::int())
                    .with_crdt_type(CType::PnCounter),
            ],
        ))
        .await
        .unwrap();

        let blockstore = Arc::new(DefraBlockstore::new(store, true));
        let handler = DbMergeHandler::new(db, blockstore.clone());
        (handler, blockstore)
    }

    struct FailingPostCommitAction;

    #[async_trait]
    impl CompositePostCommitAction for FailingPostCommitAction {
        async fn run(self: Box<Self>) -> Result<(), MergeError> {
            Err(MergeError::MergeFailed(
                "test post-commit failure".to_string(),
            ))
        }
    }

    struct FailingCompositeHook;

    #[async_trait]
    impl CompositeMergeHook for FailingCompositeHook {
        fn post_commit_action(
            &self,
            _doc_id: &str,
            _collection: &CollectionVersion,
            _metadata: &BlockMetadata<'_>,
        ) -> Option<Box<dyn CompositePostCommitAction>> {
            Some(Box::new(FailingPostCommitAction))
        }
    }

    async fn build_merge_block(
        blockstore: &Arc<DefraBlockstore<MemoryStore>>,
        name: &str,
        age: i64,
    ) -> MergeBlock {
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String(name.to_string()));
        doc.set("age", NormalValue::Int(age));

        let result = db_blocks::build_blocks_from_document(&doc, "v1", blockstore)
            .await
            .unwrap();

        MergeBlock {
            cid: result.cid,
            block_data: bytes::Bytes::from(result.block),
            doc_id: result.doc_id,
            collection_id: "col-users".to_string(),
            creator: "did:key:z6MkrBatchMergeTest".to_string(),
            sender_peer: Some("peer1".to_string()),
            is_explicit_replicator: false,
            explicit_replay_authorization: None,
            verified_creator: None,
        }
    }

    fn make_lww_block(signature_cid: Option<Cid>) -> Block {
        let payload = LwwDeltaPayload {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: b"hello".to_vec(),
        };
        Block {
            delta: CrdtDelta::Lww(payload),
            heads: None,
            links: None,
            encryption: None,
            signature: signature_cid,
        }
    }

    #[tokio::test]
    async fn test_merge_handler_creation() {
        let store = MemoryStore::new();
        let store_arc = Arc::new(store);
        let db = Arc::new(DB::from_arc(store_arc.clone()).unwrap());
        let blockstore = Arc::new(DefraBlockstore::new(store_arc, false));
        let _handler = DbMergeHandler::new(db, blockstore);
    }

    #[tokio::test]
    async fn verify_unsigned_block_returns_none() {
        let (handler, _bs) = make_handler();
        let block = make_lww_block(None);
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();

        let result = handler
            .verify_block_signature(&cid, &block, &block_data)
            .await;
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "unsigned block should return None"
        );
    }

    /// Helper: sign a block with an Ed25519 key, store signature in blockstore.
    /// Returns (private_key, hex_pubkey, did).
    async fn sign_block_ed25519(
        block: &mut Block,
        blockstore: &DefraBlockstore<MemoryStore>,
    ) -> (crypto::Ed25519PrivateKey, String, String) {
        let private_key = crypto::generate_ed25519().unwrap();
        let public_key = private_key.public_key();
        let did = public_key.did().unwrap();
        // Identity in signature header is hex-encoded public key (matches Go)
        let pub_hex = hex::encode(public_key.raw());

        let signed_bytes = block.to_dag_cbor().unwrap();
        let sig_value = private_key.sign(&signed_bytes).unwrap();

        let sig_block = Signature::new(
            SignatureHeader::new(SignatureType::EdDSA, pub_hex.as_bytes().to_vec()),
            sig_value,
        );
        let sig_data = sig_block.to_dag_cbor().unwrap();
        let sig_cid = sig_block.generate_cid().unwrap();
        blockstore.put(&sig_cid, &sig_data).await.unwrap();
        block.signature = Some(sig_cid);

        (private_key, pub_hex, did)
    }

    #[tokio::test]
    async fn verify_valid_ed25519_signature_returns_did() {
        let (handler, blockstore) = make_handler();

        let mut block = make_lww_block(None);
        let (_priv_key, _pub_hex, did) = sign_block_ed25519(&mut block, &blockstore).await;

        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();

        let result = handler
            .verify_block_signature(&cid, &block, &block_data)
            .await;
        let verified_identity = result.expect("valid signature should succeed");
        assert_eq!(
            verified_identity.as_deref(),
            Some(did.as_str()),
            "should return the signer's DID"
        );
        assert!(
            verified_identity.unwrap().starts_with("did:key:"),
            "verified identity should be a DID"
        );
    }

    #[tokio::test]
    async fn recover_block_metadata_extracts_signed_lww_metadata() {
        let (handler, blockstore) = make_handler();

        let mut block = make_lww_block(None);
        let (_priv_key, _pub_hex, did) = sign_block_ed25519(&mut block, &blockstore).await;
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();

        let metadata = handler
            .recover_block_metadata(&cid, &block_data)
            .await
            .unwrap()
            .expect("signed document block should recover metadata");

        assert_eq!(metadata.doc_id, "doc1");
        assert_eq!(metadata.collection_id, "v1");
        assert_eq!(metadata.creator, did);
        assert_eq!(metadata.verified_creator.as_deref(), Some(did.as_str()));
    }

    #[tokio::test]
    async fn recover_block_metadata_refuses_unsigned_blocks() {
        let (handler, _blockstore) = make_handler();

        let block = make_lww_block(None);
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();

        let metadata = handler
            .recover_block_metadata(&cid, &block_data)
            .await
            .unwrap();

        assert!(
            metadata.is_none(),
            "recovery metadata must include a verifiable creator"
        );
    }

    #[tokio::test]
    async fn validate_explicit_replay_authorization_checks_collection_and_creator() {
        let (handler, blockstore) = make_handler();

        let mut block = make_lww_block(None);
        let (_priv_key, _pub_hex, did) = sign_block_ed25519(&mut block, &blockstore).await;
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();
        let mut merge_block = MergeBlock {
            cid,
            block_data: bytes::Bytes::from(block_data),
            doc_id: "doc1".to_string(),
            collection_id: "v1".to_string(),
            creator: did.clone(),
            sender_peer: Some("source-peer".to_string()),
            is_explicit_replicator: true,
            explicit_replay_authorization: None,
            verified_creator: None,
        };
        let valid_authorization = p2p::ExplicitReplayAuthorization {
            source_peer_id: "source-peer".to_string(),
            target_peer_id: "target-peer".to_string(),
            collection_id: "v1".to_string(),
            authorizer_did: did.clone(),
            expires_at: u64::MAX,
        };

        handler
            .validate_authorization(Some(&valid_authorization), &merge_block)
            .await
            .expect("matching explicit replay authorization should validate");

        let wrong_creator = p2p::ExplicitReplayAuthorization {
            authorizer_did: "did:key:z6MkWrongCreator".to_string(),
            ..valid_authorization.clone()
        };
        let error = handler
            .validate_authorization(Some(&wrong_creator), &merge_block)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("does not match block creator"));

        merge_block.collection_id = "other-collection".to_string();
        let error = handler
            .validate_authorization(Some(&valid_authorization), &merge_block)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match block collection"));
    }

    #[tokio::test]
    async fn verify_tampered_block_returns_error() {
        let (handler, blockstore) = make_handler();

        // Sign the original block
        let mut original_block = make_lww_block(None);
        sign_block_ed25519(&mut original_block, &blockstore).await;
        let sig_cid = original_block.signature.unwrap();

        // Create a DIFFERENT block (tampered) but attach the same signature
        let tampered_payload = LwwDeltaPayload {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: b"TAMPERED".to_vec(),
        };
        let tampered_block = Block {
            delta: CrdtDelta::Lww(tampered_payload),
            heads: None,
            links: None,
            encryption: None,
            signature: Some(sig_cid),
        };
        let cid = tampered_block.generate_cid().unwrap();
        let block_data = tampered_block.to_dag_cbor().unwrap();

        let result = handler
            .verify_block_signature(&cid, &tampered_block, &block_data)
            .await;
        assert!(result.is_err(), "tampered block should be rejected");
        assert!(
            matches!(
                result.unwrap_err(),
                MergeError::SignatureVerificationFailed { .. }
            ),
            "expected SignatureVerificationFailed"
        );
    }

    #[tokio::test]
    async fn verify_missing_signature_block_returns_error() {
        let (handler, _bs) = make_handler();

        // Create a block that references a signature CID that doesn't exist
        let fake_sig_cid = defra_core::block::generate_cid_from_bytes(b"nonexistent").unwrap();
        let block = make_lww_block(Some(fake_sig_cid));
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();

        let result = handler
            .verify_block_signature(&cid, &block, &block_data)
            .await;
        assert!(
            result.is_err(),
            "missing signature block should be rejected"
        );
        assert!(matches!(
            result.unwrap_err(),
            MergeError::SignatureVerificationFailed { .. }
        ));
    }

    #[tokio::test]
    async fn verify_corrupt_signature_block_returns_error() {
        let (handler, blockstore) = make_handler();

        // Store garbage data as a "signature block"
        let garbage_cid = defra_core::block::generate_cid_from_bytes(b"garbage").unwrap();
        blockstore
            .put(&garbage_cid, b"not-valid-dag-cbor")
            .await
            .unwrap();

        let block = make_lww_block(Some(garbage_cid));
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();

        let result = handler
            .verify_block_signature(&cid, &block, &block_data)
            .await;
        assert!(
            result.is_err(),
            "corrupt signature block should be rejected"
        );
        assert!(matches!(
            result.unwrap_err(),
            MergeError::SignatureVerificationFailed { .. }
        ));
    }

    /// Helper: sign a block with a BLS12-381 key (using blst directly), store signature in blockstore.
    /// Returns (hex_pubkey, did).
    async fn sign_block_bls(
        block: &mut Block,
        blockstore: &DefraBlockstore<MemoryStore>,
    ) -> (String, String) {
        // Generate a BLS secret key from random bytes
        let mut ikm = [0u8; 32];
        getrandom::getrandom(&mut ikm).unwrap();
        let sk = blst::min_pk::SecretKey::key_gen(&ikm, &[]).unwrap();
        let pk = sk.sk_to_pk();

        let pk_bytes = pk.compress();
        let pub_hex = hex::encode(pk_bytes);

        let bls_pub = crypto::BlsPublicKey::from_bytes(&pk_bytes).unwrap();
        let did = crypto::keys::PublicKey::did(&bls_pub).unwrap();

        let signed_bytes = block.to_dag_cbor().unwrap();
        let dst = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_NUL_";
        let sig = sk.sign(&signed_bytes, dst, &[]);
        let sig_bytes = sig.compress().to_vec();

        let sig_block = Signature::new(
            SignatureHeader::new(SignatureType::BLS, pub_hex.as_bytes().to_vec()),
            sig_bytes,
        );
        let sig_data = sig_block.to_dag_cbor().unwrap();
        let sig_cid = sig_block.generate_cid().unwrap();
        blockstore.put(&sig_cid, &sig_data).await.unwrap();
        block.signature = Some(sig_cid);

        (pub_hex, did)
    }

    #[tokio::test]
    async fn verify_valid_bls_signature_returns_did() {
        let (handler, blockstore) = make_handler();

        let mut block = make_lww_block(None);
        let (_pub_hex, did) = sign_block_bls(&mut block, &blockstore).await;

        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();

        let result = handler
            .verify_block_signature(&cid, &block, &block_data)
            .await;
        let verified_identity = result.expect("valid BLS signature should succeed");
        assert_eq!(
            verified_identity.as_deref(),
            Some(did.as_str()),
            "should return the signer's DID"
        );
        assert!(
            verified_identity.unwrap().starts_with("did:key:"),
            "verified identity should be a DID"
        );
    }

    #[tokio::test]
    async fn verify_forged_bls_signature_returns_error() {
        let (handler, blockstore) = make_handler();

        // Sign the original block with one BLS key
        let mut original_block = make_lww_block(None);
        sign_block_bls(&mut original_block, &blockstore).await;
        let sig_cid = original_block.signature.unwrap();

        // Create a different block but attach the original signature
        let tampered_payload = LwwDeltaPayload {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: b"FORGED".to_vec(),
        };
        let tampered_block = Block {
            delta: CrdtDelta::Lww(tampered_payload),
            heads: None,
            links: None,
            encryption: None,
            signature: Some(sig_cid),
        };
        let cid = tampered_block.generate_cid().unwrap();
        let block_data = tampered_block.to_dag_cbor().unwrap();

        let result = handler
            .verify_block_signature(&cid, &tampered_block, &block_data)
            .await;
        assert!(result.is_err(), "forged BLS signature should be rejected");
        assert!(matches!(
            result.unwrap_err(),
            MergeError::SignatureVerificationFailed { .. }
        ));
    }

    #[tokio::test]
    async fn verify_attacker_identity_not_victim() {
        let (handler, blockstore) = make_handler();

        // The attack scenario:
        // 1. Attacker signs a block with their own key
        // 2. Sets PushLog metadata.creator = victim's DID
        // 3. Without this fix, ACP would register doc under victim's DID
        let mut block = make_lww_block(None);
        let (_attacker_key, _pub_hex, attacker_did) =
            sign_block_ed25519(&mut block, &blockstore).await;

        let victim_did = "did:key:z6MkVICTIM_FAKE_DID";

        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();

        // Verification succeeds and returns ATTACKER's actual DID
        let result = handler
            .verify_block_signature(&cid, &block, &block_data)
            .await;
        let verified = result.expect("valid signature should succeed");
        assert_eq!(
            verified.as_deref(),
            Some(attacker_did.as_str()),
            "verified identity should be the actual signer, not the victim"
        );

        // effective_creator prefers verified over self-reported victim DID
        let mut metadata = BlockMetadata::normal("doc1", "col1", victim_did, None, false);
        metadata.verified_creator = verified;
        assert_eq!(
            metadata.effective_creator(),
            Some(attacker_did.as_str()),
            "effective_creator should return attacker's DID, not victim's"
        );
        assert!(
            metadata
                .effective_creator()
                .unwrap()
                .starts_with("did:key:"),
            "DID format preserved for ACP registration"
        );
    }

    #[tokio::test]
    async fn batch_merge_keeps_success_and_events_when_post_commit_action_fails() {
        let (handler, blockstore, bus) = make_handler_with_schema_and_bus().await;
        handler.set_composite_merge_hook(Arc::new(FailingCompositeHook));

        let mut subscription = bus.subscribe(&[EventName::Update]);
        let first = build_merge_block(&blockstore, "Alice", 30).await;
        let second = build_merge_block(&blockstore, "Bob", 31).await;
        let expected_doc_ids = [first.doc_id.clone(), second.doc_id.clone()];

        let results = handler.handle_block_batch(&[first, second]).await;

        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .all(|result| matches!(result, Ok(MergeOutcome::Merged))),
            "post-commit failures after commit must not turn merged blocks into failures"
        );

        let update1 = timeout(Duration::from_secs(1), subscription.recv())
            .await
            .expect("expected first update event")
            .expect("subscription closed unexpectedly");
        let update2 = timeout(Duration::from_secs(1), subscription.recv())
            .await
            .expect("expected second update event")
            .expect("subscription closed unexpectedly");

        let mut seen_doc_ids = vec![
            update1
                .as_update()
                .expect("expected update event")
                .doc_id
                .clone(),
            update2
                .as_update()
                .expect("expected update event")
                .doc_id
                .clone(),
        ];
        seen_doc_ids.sort();

        let mut expected = expected_doc_ids.to_vec();
        expected.sort();

        assert_eq!(seen_doc_ids, expected);
        assert!(
            subscription.try_recv().is_err(),
            "batch merge should publish exactly the queued update events"
        );
    }

    #[tokio::test]
    async fn synced_collection_definition_persists_short_id_mapping() {
        let (handler, _blockstore) = make_handler();

        let payload = CollectionDefinitionDeltaPayload::new(1).with_name("Users");
        let block = Block {
            delta: CrdtDelta::CollectionDefinition(payload.clone()),
            heads: None,
            links: None,
            encryption: None,
            signature: None,
        };
        let cid = block.generate_cid().unwrap();

        let outcome = handler
            .process_collection_definition_delta(
                &cid,
                &block,
                &payload,
                &BlockMetadata::schema_sync(),
            )
            .await
            .unwrap();
        assert_eq!(outcome, MergeOutcome::Merged);

        let txn = handler.db.new_txn(true).await.unwrap();
        let systemstore = txn.systemstore().unwrap();
        let mapping = systemstore
            .get(&CollectionID::new(cid.to_string()).bytes())
            .await
            .unwrap();
        let _ = txn.discard();

        assert!(
            mapping.is_some(),
            "expected synced schema to persist a root_id mapping"
        );
    }

    #[tokio::test]
    async fn counter_merge_marks_cid_merged() {
        let (handler, blockstore) = make_handler_with_counter_schema().await;
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();

        let mut delta_data = Vec::new();
        ciborium::into_writer(&5_i64, &mut delta_data).unwrap();

        let payload = CounterDeltaPayload {
            doc_id: doc_id.as_bytes().to_vec(),
            field_name: "score".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: delta_data,
            nonce: 4242,
        };
        let block = Block {
            delta: CrdtDelta::Counter(payload.clone()),
            heads: None,
            links: None,
            encryption: None,
            signature: None,
        };
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();
        blockstore.put(&cid, &block_data).await.unwrap();

        let metadata = BlockMetadata::normal(
            &doc_id,
            "col-counters",
            "did:key:z6MkrCounterMergeTest",
            None,
            false,
        );

        let outcome = handler
            .process_counter_delta(&cid, &payload, &metadata)
            .await
            .unwrap();
        assert_eq!(outcome, MergeOutcome::Merged);
        // The blockstore merged-set is the single source of CRDT idempotency
        // (see #847). The counter merge path no longer keeps per-delta nonce
        // markers, so there is nothing else to assert here.
        assert!(blockstore.is_merged(&cid).await.unwrap());
    }

    #[tokio::test]
    async fn handle_block_serializes_standalone_counter_by_doc_id() {
        let (handler, blockstore) = make_handler_with_counter_schema().await;
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        let doc_id_str = doc_id.to_string();

        let mut delta_data = Vec::new();
        ciborium::into_writer(&5_i64, &mut delta_data).unwrap();

        let payload = CounterDeltaPayload {
            doc_id: doc_id_str.as_bytes().to_vec(),
            field_name: "score".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: delta_data,
            nonce: 4243,
        };
        let block = Block {
            delta: CrdtDelta::Counter(payload),
            heads: None,
            links: None,
            encryption: None,
            signature: None,
        };
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();
        blockstore.put(&cid, &block_data).await.unwrap();

        let metadata = BlockMetadata::normal(
            &doc_id_str,
            "col-counters",
            "did:key:z6MkrCounterMergeQueueTest",
            None,
            false,
        );

        let guard = handler.merge_queue.acquire(&doc_id_str).await;
        let merge = handler.handle_block(&cid, &block_data, metadata);
        tokio::pin!(merge);

        assert!(
            timeout(Duration::from_millis(50), merge.as_mut())
                .await
                .is_err(),
            "standalone counter merge should wait on the per-document queue"
        );

        drop(guard);
        let outcome = timeout(Duration::from_secs(1), merge)
            .await
            .expect("counter merge should complete after releasing the queue")
            .unwrap();
        assert_eq!(outcome, MergeOutcome::Merged);
        assert!(blockstore.is_merged(&cid).await.unwrap());
    }

    #[tokio::test]
    async fn counter_standalone_skips_already_merged_block() {
        let (handler, blockstore) = make_handler_with_counter_schema().await;
        let collection = handler
            .db
            .find_collection_by_id("col-counters")
            .unwrap()
            .expect("counter collection should exist");
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().clone();
        let doc_id_str = doc_id.to_string();

        let mut delta_data = Vec::new();
        ciborium::into_writer(&5_i64, &mut delta_data).unwrap();

        let payload = CounterDeltaPayload {
            doc_id: doc_id_str.as_bytes().to_vec(),
            field_name: "score".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: delta_data,
            nonce: 999,
        };
        let block = Block {
            delta: CrdtDelta::Counter(payload.clone()),
            heads: None,
            links: None,
            encryption: None,
            signature: None,
        };
        let cid = block.generate_cid().unwrap();
        let block_data = block.to_dag_cbor().unwrap();
        blockstore.put(&cid, &block_data).await.unwrap();
        blockstore.mark_as_merged(&cid).await.unwrap();

        let metadata = BlockMetadata::normal(
            &doc_id_str,
            "col-counters",
            "did:key:z6MkrCounterReplayTest",
            None,
            false,
        );
        let outcome = handler
            .process_counter_delta(&cid, &payload, &metadata)
            .await
            .unwrap();
        assert!(outcome.is_terminal_skip());

        let txn = handler.db.new_txn(true).await.unwrap();
        let stored = {
            let datastore = txn.datastore().unwrap();
            collection
                .get_with_datastore(&datastore, &doc_id)
                .await
                .unwrap()
        };
        txn.force_discard().unwrap();
        assert!(
            stored.is_none(),
            "standalone re-delivery must not materialize an already-merged counter block"
        );
    }

    #[tokio::test]
    async fn composite_merge_skips_locally_merged_counter_parent() {
        let (handler, blockstore) = make_handler_with_counter_schema().await;
        let collection = handler
            .db
            .find_collection_by_id("col-counters")
            .unwrap()
            .expect("counter collection should exist");

        let mut doc = Document::new();
        doc.set_with_crdt("score", CType::PnCounter, NormalValue::Int(10))
            .unwrap();
        doc.generate_and_set_doc_id().unwrap();
        doc.set_schema_version_id("v1");
        let doc_id = doc.id().unwrap().clone();
        let doc_id_str = doc_id.to_string();

        let local_blocks = {
            let txn = handler.db.new_txn(false).await.unwrap();
            let blocks = {
                let datastore = txn.datastore().unwrap();
                let headstore = txn.headstore().unwrap();
                let raw_blockstore = txn.blockstore().unwrap();
                collection
                    .save_with_datastore(&datastore, &doc)
                    .await
                    .unwrap();
                db_blocks::write_document_blocks(
                    &raw_blockstore,
                    &headstore,
                    &doc,
                    "v1",
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap()
            };
            txn.force_commit().await.unwrap();
            blocks
        };
        assert!(
            blockstore.is_merged(&local_blocks.cid).await.unwrap(),
            "locally-created composite blocks are already merged"
        );

        let mut update_data = Vec::new();
        ciborium::into_writer(&10_i64, &mut update_data).unwrap();
        let update_field_payload = CounterDeltaPayload {
            doc_id: doc_id_str.as_bytes().to_vec(),
            field_name: "score".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            data: update_data,
            nonce: 99,
        };
        let update_field_block = Block::new(
            CrdtDelta::Counter(update_field_payload),
            local_blocks.field_cids.clone(),
            vec![],
        );
        let update_field_cid = update_field_block.generate_cid().unwrap();
        let update_field_data = update_field_block.to_dag_cbor().unwrap();
        blockstore
            .put(&update_field_cid, &update_field_data)
            .await
            .unwrap();

        let update_payload = CompositeDeltaPayload {
            doc_id: doc_id_str.as_bytes().to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            status: 1,
        };
        let update_composite_block = Block::new(
            CrdtDelta::Composite(update_payload.clone()),
            vec![local_blocks.cid],
            vec![DAGLink::new("score", update_field_cid)],
        );
        let update_composite_cid = update_composite_block.generate_cid().unwrap();
        let update_composite_data = update_composite_block.to_dag_cbor().unwrap();
        blockstore
            .put(&update_composite_cid, &update_composite_data)
            .await
            .unwrap();

        let metadata = BlockMetadata::normal(
            &doc_id_str,
            "col-counters",
            "did:key:z6MkrCompositeCounterParent",
            None,
            false,
        );
        let outcome = handler
            .process_composite_delta(
                &update_composite_cid,
                &update_composite_block,
                &update_payload,
                &metadata,
                false,
                0,
            )
            .await
            .unwrap();
        assert_eq!(outcome, MergeOutcome::Merged);

        let stored = {
            let txn = handler.db.new_txn(true).await.unwrap();
            let stored = {
                let datastore = txn.datastore().unwrap();
                collection
                    .get_with_datastore(&datastore, &doc_id)
                    .await
                    .unwrap()
                    .expect("document should still exist")
            };
            txn.force_discard().unwrap();
            stored
        };
        assert_eq!(stored.get("score"), Some(&NormalValue::Int(20)));
    }

    async fn make_handler_with_immutable_schema() -> (
        DbMergeHandler<MemoryStore, DefraBlockstore<MemoryStore>>,
        Arc<DefraBlockstore<MemoryStore>>,
    ) {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store.clone()).unwrap());

        db.create_collection(CollectionVersion::new(
            "AgentDocs",
            "v1",
            "col-agentdocs",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "agent_did", FieldKind::string()).as_immutable(),
                FieldDescription::new("3", "body", FieldKind::string()),
            ],
        ))
        .await
        .unwrap();

        let blockstore = Arc::new(DefraBlockstore::new(store, false));
        let handler = DbMergeHandler::new(db, blockstore.clone());
        (handler, blockstore)
    }

    /// Remote-merge enforcement of `@immutable` (filtered-replication B3 hazard).
    ///
    /// A higher-priority remote composite delta that flips an immutable field
    /// must be rejected by the merge handler, leaving the local value intact.
    /// This guards the `composite_persist.rs` path, which re-implements the
    /// check independently of the local-write validator — so it needs its own
    /// coverage. Honest two-node e2e cannot reach this: local validation blocks
    /// the originating update and content-addressed doc IDs prevent honest
    /// divergence, so the conflicting delta is crafted directly here.
    #[tokio::test]
    async fn remote_composite_merge_rejects_immutable_field_change() {
        let (handler, blockstore) = make_handler_with_immutable_schema().await;
        let collection = handler
            .db
            .find_collection_by_id("col-agentdocs")
            .unwrap()
            .expect("agentdocs collection should exist");

        let mut doc = Document::new();
        doc.set(
            "agent_did",
            NormalValue::String("did:key:alice".to_string()),
        );
        doc.set("body", NormalValue::String("v1".to_string()));
        doc.generate_and_set_doc_id().unwrap();
        doc.set_schema_version_id("v1");
        let doc_id = doc.id().unwrap().clone();
        let doc_id_str = doc_id.to_string();

        // Persist the initial document locally (agent_did = alice).
        let create_blocks = {
            let txn = handler.db.new_txn(false).await.unwrap();
            let blocks = {
                let datastore = txn.datastore().unwrap();
                let headstore = txn.headstore().unwrap();
                let raw_blockstore = txn.blockstore().unwrap();
                collection
                    .save_with_datastore(&datastore, &doc)
                    .await
                    .unwrap();
                db_blocks::write_document_blocks(
                    &raw_blockstore,
                    &headstore,
                    &doc,
                    "v1",
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap()
            };
            txn.force_commit().await.unwrap();
            blocks
        };

        // Craft a higher-priority remote update that flips the immutable field.
        let mut update_data = Vec::new();
        ciborium::into_writer(
            &NormalValue::String("did:key:bob".to_string()),
            &mut update_data,
        )
        .unwrap();
        let update_field_payload = LwwDeltaPayload {
            doc_id: doc_id_str.as_bytes().to_vec(),
            field_name: "agent_did".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            data: update_data,
        };
        let update_field_block = Block::new(
            CrdtDelta::Lww(update_field_payload),
            create_blocks.field_cids.clone(),
            vec![],
        );
        let update_field_cid = update_field_block.generate_cid().unwrap();
        blockstore
            .put(
                &update_field_cid,
                &update_field_block.to_dag_cbor().unwrap(),
            )
            .await
            .unwrap();

        let update_payload = CompositeDeltaPayload {
            doc_id: doc_id_str.as_bytes().to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            status: 1,
        };
        let update_composite_block = Block::new(
            CrdtDelta::Composite(update_payload.clone()),
            vec![create_blocks.cid],
            vec![DAGLink::new("agent_did", update_field_cid)],
        );
        let update_composite_cid = update_composite_block.generate_cid().unwrap();
        blockstore
            .put(
                &update_composite_cid,
                &update_composite_block.to_dag_cbor().unwrap(),
            )
            .await
            .unwrap();

        let metadata = BlockMetadata::normal(
            &doc_id_str,
            "col-agentdocs",
            "did:key:z6MkrRemoteImmutableMerge",
            None,
            false,
        );
        let result = handler
            .process_composite_delta(
                &update_composite_cid,
                &update_composite_block,
                &update_payload,
                &metadata,
                false,
                0,
            )
            .await;

        assert!(
            result.is_err(),
            "remote merge changing an immutable field must be rejected, got {result:?}"
        );
        assert!(
            result.unwrap_err().to_string().contains("immutable"),
            "rejection should cite the immutable field"
        );

        // The locally-stored immutable value must be unchanged.
        let stored = {
            let txn = handler.db.new_txn(true).await.unwrap();
            let stored = {
                let datastore = txn.datastore().unwrap();
                collection
                    .get_with_datastore(&datastore, &doc_id)
                    .await
                    .unwrap()
                    .expect("document should still exist")
            };
            txn.force_discard().unwrap();
            stored
        };
        assert_eq!(
            stored.get("agent_did"),
            Some(&NormalValue::String("did:key:alice".to_string())),
            "immutable field must survive a rejected remote merge"
        );
    }

    #[tokio::test]
    async fn composite_lww_reseeds_from_local_doc_when_crdt_store_is_stale() {
        let (handler, blockstore, _bus) = make_handler_with_schema_and_bus().await;
        let collection = handler
            .db
            .find_collection_by_id("col-users")
            .unwrap()
            .expect("users collection should exist");

        let mut doc = Document::new();
        doc.set("age", NormalValue::Int(21));
        doc.generate_and_set_doc_id().unwrap();
        doc.set_schema_version_id("v1");
        let doc_id = doc.id().unwrap().clone();
        let doc_id_str = doc_id.to_string();

        let create_blocks = {
            let txn = handler.db.new_txn(false).await.unwrap();
            let blocks = {
                let datastore = txn.datastore().unwrap();
                let headstore = txn.headstore().unwrap();
                let raw_blockstore = txn.blockstore().unwrap();
                collection
                    .save_with_datastore(&datastore, &doc)
                    .await
                    .unwrap();
                db_blocks::write_document_blocks(
                    &raw_blockstore,
                    &headstore,
                    &doc,
                    "v1",
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap()
            };
            txn.force_commit().await.unwrap();
            blocks
        };

        doc.set("age", NormalValue::Int(60));
        let mut modified_fields = HashSet::new();
        modified_fields.insert("age".to_string());
        {
            let txn = handler.db.new_txn(false).await.unwrap();
            {
                let datastore = txn.datastore().unwrap();
                let headstore = txn.headstore().unwrap();
                let raw_blockstore = txn.blockstore().unwrap();
                collection
                    .save_with_datastore(&datastore, &doc)
                    .await
                    .unwrap();
                db_blocks::write_document_blocks(
                    &raw_blockstore,
                    &headstore,
                    &doc,
                    "v1",
                    Some(&modified_fields),
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();
            }
            txn.force_commit().await.unwrap();
        }

        let lww = Lww::new("v1", doc_id_str.as_bytes(), "age").unwrap();
        let mut stale_value = Vec::new();
        ciborium::into_writer(&NormalValue::Int(30), &mut stale_value).unwrap();
        let stale_delta = LwwDelta::new(
            doc_id_str.as_bytes().to_vec(),
            "age".to_string(),
            2,
            "v1".to_string(),
            stale_value.clone(),
        )
        .unwrap();
        {
            let txn = handler.db.new_txn(false).await.unwrap();
            {
                let mut datastore = txn.datastore().unwrap();
                lww.merge(
                    &mut datastore,
                    &Context {
                        doc_id: DocId::new(&doc_id_str).unwrap(),
                        schema_version: "v1".to_string(),
                        is_create: false,
                    },
                    &stale_delta,
                )
                .await
                .unwrap();
            }
            txn.force_commit().await.unwrap();
        }

        let incoming_field_payload = LwwDeltaPayload {
            doc_id: doc_id_str.as_bytes().to_vec(),
            field_name: "age".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            data: stale_value,
        };
        let incoming_field_block = Block::new(
            CrdtDelta::Lww(incoming_field_payload),
            create_blocks.field_cids.clone(),
            vec![],
        );
        let incoming_field_cid = incoming_field_block.generate_cid().unwrap();
        let incoming_field_data = incoming_field_block.to_dag_cbor().unwrap();
        blockstore
            .put(&incoming_field_cid, &incoming_field_data)
            .await
            .unwrap();

        let incoming_composite_payload = CompositeDeltaPayload {
            doc_id: doc_id_str.as_bytes().to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            status: 1,
        };
        let incoming_composite_block = Block::new(
            CrdtDelta::Composite(incoming_composite_payload.clone()),
            vec![create_blocks.cid],
            vec![DAGLink::new("age", incoming_field_cid)],
        );
        let incoming_composite_cid = incoming_composite_block.generate_cid().unwrap();
        let incoming_composite_data = incoming_composite_block.to_dag_cbor().unwrap();
        blockstore
            .put(&incoming_composite_cid, &incoming_composite_data)
            .await
            .unwrap();

        let metadata = BlockMetadata::normal(
            &doc_id_str,
            "col-users",
            "did:key:z6MkrCompositeStaleLww",
            None,
            false,
        );
        let outcome = handler
            .process_composite_delta(
                &incoming_composite_cid,
                &incoming_composite_block,
                &incoming_composite_payload,
                &metadata,
                false,
                0,
            )
            .await
            .unwrap();
        assert_eq!(outcome, MergeOutcome::Merged);

        let stored = {
            let txn = handler.db.new_txn(true).await.unwrap();
            let stored = {
                let datastore = txn.datastore().unwrap();
                collection
                    .get_with_datastore(&datastore, &doc_id)
                    .await
                    .unwrap()
                    .expect("document should exist")
            };
            txn.force_discard().unwrap();
            stored
        };
        assert_eq!(stored.get("age"), Some(&NormalValue::Int(60)));
    }

    #[tokio::test]
    async fn composite_parent_replay_updates_headstore_for_merged_parent() {
        let (handler, blockstore, _bus) = make_handler_with_schema_and_bus().await;
        let collection = handler
            .db
            .find_collection_by_id("col-users")
            .unwrap()
            .expect("users collection should exist");

        let mut doc = Document::new();
        doc.set("name", NormalValue::String("John".to_string()));
        doc.generate_and_set_doc_id().unwrap();
        doc.set_schema_version_id("v1");
        let doc_id = doc.id().unwrap().clone();
        let doc_id_str = doc_id.to_string();

        let local_blocks = {
            let txn = handler.db.new_txn(false).await.unwrap();
            let blocks = {
                let datastore = txn.datastore().unwrap();
                let headstore = txn.headstore().unwrap();
                let raw_blockstore = txn.blockstore().unwrap();
                collection
                    .save_with_datastore(&datastore, &doc)
                    .await
                    .unwrap();
                db_blocks::write_document_blocks(
                    &raw_blockstore,
                    &headstore,
                    &doc,
                    "v1",
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .unwrap()
            };
            txn.force_commit().await.unwrap();
            blocks
        };

        let build_update =
            |name: &str, priority: u64, field_heads: Vec<Cid>, composite_heads: Vec<Cid>| {
                let mut data = Vec::new();
                ciborium::into_writer(&NormalValue::String(name.to_string()), &mut data).unwrap();
                let field_payload = LwwDeltaPayload {
                    doc_id: doc_id_str.as_bytes().to_vec(),
                    field_name: "name".to_string(),
                    schema_version_id: "v1".to_string(),
                    priority,
                    data,
                };
                let field_block = Block::new(CrdtDelta::Lww(field_payload), field_heads, vec![]);
                let field_cid = field_block.generate_cid().unwrap();
                let field_data = field_block.to_dag_cbor().unwrap();

                let composite_payload = CompositeDeltaPayload {
                    doc_id: doc_id_str.as_bytes().to_vec(),
                    schema_version_id: "v1".to_string(),
                    priority,
                    status: 1,
                };
                let composite_block = Block::new(
                    CrdtDelta::Composite(composite_payload.clone()),
                    composite_heads,
                    vec![DAGLink::new("name", field_cid)],
                );
                let composite_cid = composite_block.generate_cid().unwrap();
                let composite_data = composite_block.to_dag_cbor().unwrap();

                (
                    field_cid,
                    field_data,
                    composite_cid,
                    composite_block,
                    composite_payload,
                    composite_data,
                )
            };

        let (
            parent_field_cid,
            parent_field_data,
            parent_cid,
            _parent_block,
            _parent_payload,
            parent_data,
        ) = build_update(
            "Shahzad",
            2,
            local_blocks.field_cids.clone(),
            vec![local_blocks.cid],
        );
        blockstore
            .put(&parent_field_cid, &parent_field_data)
            .await
            .unwrap();
        blockstore.put(&parent_cid, &parent_data).await.unwrap();

        let (child_field_cid, child_field_data, child_cid, child_block, child_payload, child_data) =
            build_update("Chris", 3, vec![parent_field_cid], vec![parent_cid]);
        blockstore
            .put(&child_field_cid, &child_field_data)
            .await
            .unwrap();
        blockstore.put(&child_cid, &child_data).await.unwrap();

        let metadata = BlockMetadata::normal(
            &doc_id_str,
            "col-users",
            "did:key:z6MkrCompositeParentReplay",
            None,
            false,
        );
        let outcome = handler
            .process_composite_delta(
                &child_cid,
                &child_block,
                &child_payload,
                &metadata,
                false,
                0,
            )
            .await
            .unwrap();
        assert_eq!(outcome, MergeOutcome::Merged);

        let txn = handler.db.new_txn(true).await.unwrap();
        let head_keys = {
            let headstore = txn.headstore().unwrap();
            let mut iter = headstore
                .iterator(storage::corekv::IterOptions::new().with_prefix(
                    storage::keys::headstore::HeadstoreDocKey::field_prefix(&doc_id_str, "name"),
                ))
                .await
                .unwrap();
            let mut keys = Vec::new();
            while let Some(pair) = iter.next().await.unwrap() {
                keys.push(pair.key);
            }
            iter.close().await.unwrap();
            keys
        };
        txn.force_discard().unwrap();

        assert_eq!(
            head_keys,
            vec![storage::keys::headstore::HeadstoreDocKey::new(
                &doc_id_str,
                "name",
                child_field_cid
            )
            .bytes()]
        );
    }

    #[tokio::test]
    async fn composite_skip_field_does_not_mark_unreadable_linked_counter_merged() {
        let (handler, blockstore) = make_handler_with_counter_schema().await;
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();

        let encryption = Encryption::new_for_field(
            doc_id.as_bytes().to_vec(),
            "score".to_string(),
            b"wrong-key".to_vec(),
        );
        let encryption_cid = encryption.generate_cid().unwrap();
        let encryption_data = encryption.to_dag_cbor().unwrap();
        blockstore
            .put(&encryption_cid, &encryption_data)
            .await
            .unwrap();

        let field_payload = CounterDeltaPayload {
            doc_id: doc_id.as_bytes().to_vec(),
            field_name: "score".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: b"not-a-valid-encrypted-counter".to_vec(),
            nonce: 777,
        };
        let field_block = Block {
            delta: CrdtDelta::Counter(field_payload),
            heads: None,
            links: None,
            encryption: Some(encryption_cid),
            signature: None,
        };
        let field_cid = field_block.generate_cid().unwrap();
        let field_block_data = field_block.to_dag_cbor().unwrap();
        blockstore.put(&field_cid, &field_block_data).await.unwrap();

        let composite_payload = CompositeDeltaPayload {
            doc_id: doc_id.as_bytes().to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            status: 0,
        };
        let composite_block = Block {
            delta: CrdtDelta::Composite(composite_payload.clone()),
            heads: None,
            links: Some(vec![DAGLink::new("score", field_cid)]),
            encryption: None,
            signature: None,
        };
        let composite_cid = composite_block.generate_cid().unwrap();

        let metadata = BlockMetadata::normal(
            &doc_id,
            "col-counters",
            "did:key:z6MkrCompositeEncryptedSkip",
            None,
            false,
        );

        let outcome = handler
            .process_composite_delta(
                &composite_cid,
                &composite_block,
                &composite_payload,
                &metadata,
                false,
                0,
            )
            .await
            .unwrap();

        assert_eq!(outcome, MergeOutcome::Merged);
        assert!(
            !blockstore.is_merged(&field_cid).await.unwrap(),
            "linked field block should stay unmerged when decryption fails and the field is skipped"
        );
    }

    /// Stub KMS that returns a fixed key for every requested CID.
    struct StubKms {
        key: [u8; 32],
    }

    #[async_trait]
    impl kms::KmsService for StubKms {
        async fn get_keys(
            &self,
            _: &kms::RequestContext,
            cids: &[kms::EncryptionCid],
        ) -> kms::Result<kms::KeyResults> {
            let (results, tx) = kms::KeyResults::new(cids.len().max(1));
            for cid in cids {
                let _ = tx.send(Ok((*cid, self.key))).await;
            }
            drop(tx);
            Ok(results)
        }

        async fn generate_key(
            &self,
            _: &kms::RequestContext,
            _: kms::KeyScope,
        ) -> kms::Result<(kms::EncryptionCid, [u8; 32])> {
            Err(kms::Error::Unsupported("stub"))
        }

        async fn serve_request(
            &self,
            _: kms::PeerIdentity,
            _: kms::FetchEncryptionKeyRequest,
        ) -> kms::Result<kms::FetchEncryptionKeyReply> {
            Err(kms::Error::Unsupported("stub"))
        }
    }

    #[tokio::test]
    async fn decrypt_block_data_routes_through_kms_when_set() {
        let (handler, _blockstore) = make_handler();

        // Encrypt a payload with a known key; nonce is prepended to ciphertext.
        let key = [7u8; 32];
        let plaintext = b"kms-routed plaintext".to_vec();
        let (ciphertext, _nonce) =
            crypto::encryption::aes::encrypt_aes(&plaintext, &key, &[], true).unwrap();

        // Arbitrary CID — the KMS resolves it without touching the encstore.
        let enc_cid =
            Cid::try_from("bafyreidykglsfhoixmivffc5uwhcgshx4j465xwqntbmu43nb2dzqwfvae").unwrap();

        handler.set_kms(Arc::new(StubKms { key }));

        let decrypted = handler
            .decrypt_block_data(&ciphertext, Some(&enc_cid), None)
            .await
            .expect("kms-keyed decryption should succeed");
        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn decrypt_block_data_no_kms_no_cid_passthrough() {
        let (handler, _blockstore) = make_handler();
        let data = b"plaintext".to_vec();
        let out = handler.decrypt_block_data(&data, None, None).await.unwrap();
        assert_eq!(out, data);
    }
}
