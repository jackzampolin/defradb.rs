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
mod doc_identity;
pub(crate) mod error;
pub(crate) mod hook;
mod lww;
pub(crate) mod se_merge;
mod signature;

pub use error::MergeError;
pub(crate) use error::{CounterMergeResult, LwwMergeResult};
pub(crate) use signature::verify_signature_data;

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
use defra_core::merge::{
    BlockMetadata, ExplicitReplayAuthorization, MergeBlock, MergeHandler, MergeOutcome,
    RecoveredBlockMetadata,
};
use defra_core::types::DocId;
use document::{DocID, Document, NormalValue};
use events::{MergeCompleteData, Message, Update};
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

#[cfg(not(target_arch = "wasm32"))]
fn spawn_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(future);
}

#[cfg(target_arch = "wasm32")]
fn spawn_task<F>(future: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    wasm_bindgen_futures::spawn_local(future);
}

/// Maximum parent-chain depth for merge operations.
///
/// Bounds the work and heap used while traversing a malicious or corrupt DAG.
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
    /// Per-document write serialization queue, shared with the DB so that local
    /// writes and P2P merges that touch the same document are mutually
    /// serialized. Ensures concurrent merges (and concurrent local writes) for
    /// the same document are processed one at a time, preventing read-modify-write
    /// races on the CRDT accumulation store (#1021).
    pub(crate) merge_queue: Arc<db::DocWriteQueue>,
    /// Encryption CIDs with a background DEK prefetch currently in flight, so
    /// repeated deliveries of the same deferred field block
    /// (pushlog + gossip + retries) don't fan out duplicate cross-peer
    /// fetches.
    prefetched_dek_cids: Arc<std::sync::Mutex<HashSet<Cid>>>,
}

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    /// Create a new database merge handler.
    pub fn new(db: Arc<DB<S>>, blockstore: Arc<B>) -> Self {
        let merge_queue = db.doc_write_queue();
        Self {
            db,
            blockstore,
            composite_merge_hook: std::sync::OnceLock::new(),
            merged_composites: std::sync::Mutex::new(HashSet::new()),
            merged_collections: std::sync::Mutex::new(HashSet::new()),
            se_enc_key: std::sync::OnceLock::new(),
            kms: std::sync::OnceLock::new(),
            merge_queue,
            prefetched_dek_cids: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Fire-and-forget cross-peer DEK request for an encrypted field block
    /// whose owner is not yet known locally (its merge is deferred to the
    /// composite). Go parity: Go requests the DEK unconditionally at DAG-sync
    /// time (internal/db/p2p/sync_dag.go `kms.GetKeys`), BEFORE any merge
    /// admission — which is also what makes an unauthorized node's request
    /// observable as a serve-side denial on the key holder
    /// (proofs/tests/behavioral/kms.rs). The prefetch runs detached so the
    /// merge path never blocks on it; on success the reply handler caches the
    /// key in the local store.
    pub(crate) fn spawn_dek_prefetch(&self, enc_cid: Cid, metadata: &BlockMetadata<'_>) {
        let Some(kms) = self.kms() else {
            return;
        };
        {
            let mut seen = self.prefetched_dek_cids.lock().unwrap();
            if !seen.insert(enc_cid) {
                return;
            }
        }
        let ctx = Self::kms_request_context(Some(metadata));
        let prefetched_dek_cids = Arc::clone(&self.prefetched_dek_cids);
        spawn_task(async move {
            let result = match kms.get_keys(&ctx, std::slice::from_ref(&enc_cid)).await {
                Ok(results) => results.wait_all().await.map(|_| ()),
                Err(error) => Err(error),
            };
            prefetched_dek_cids.lock().unwrap().remove(&enc_cid);
            if let Err(error) = result {
                tracing::debug!(enc_cid = %enc_cid, error = %error, "DEK prefetch failed");
            }
        });
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
        authorization: Option<&ExplicitReplayAuthorization>,
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

        // Deltas carry no document identity: recover it from the ownership
        // index (composites can also derive it from their DAG).
        let doc_id = match &block.delta {
            CrdtDelta::Composite(_) => match self.resolve_composite_doc_id(cid, &block).await {
                Ok(doc_id) => doc_id,
                // No recoverable identity → treat as unrecoverable metadata;
                // real infrastructure errors propagate.
                Err(MergeError::MergeFailed(_)) => return Ok(None),
                Err(e) => return Err(e),
            },
            CrdtDelta::Lww(_) | CrdtDelta::Counter(_) => {
                match self.resolve_field_block_doc_id(cid).await? {
                    Some(doc_id) => doc_id,
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
        let Some(collection_id) = block.delta.schema_version_id().map(ToString::to_string) else {
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
            let results = kms.get_keys(&ctx, std::slice::from_ref(enc_cid)).await?;
            let mut receiver = results.into_receiver();
            let mut denied = None;
            let mut unavailable = None;
            while let Some(result) = receiver.recv().await {
                match result {
                    Ok((cid, key)) if cid == *enc_cid => {
                        return crypto::encryption::aes::decrypt_aes(None, data, &key, &[])
                            .map_err(|e| {
                                kms::Error::Crypto(format!("KMS-keyed decryption failed: {e}"))
                                    .into()
                            });
                    }
                    Ok(_) => {}
                    Err(error @ kms::Error::AccessDenied { .. }) => denied = Some(error),
                    Err(error) => unavailable = Some(error),
                }
            }
            return Err(unavailable
                .or(denied)
                .unwrap_or(kms::Error::KeyUnavailable)
                .into());
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
impl<S: Store + 'static, B: blockstore::Blockstore + 'static> MergeHandler
    for DbMergeHandler<S, B>
{
    type Error = MergeError;

    async fn validate_authorization(
        &self,
        authorization: Option<&ExplicitReplayAuthorization>,
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
        // Go parity (internal/db/merge.go): merges race concurrent merges and
        // local writes on shared systemstore keys — the /seq/doc short-ID
        // sequence and co-owned block-ownership entries — so an optimistic
        // TxnConflict is expected business. Go retries `executeMerge` up to
        // MaxTxnRetries; the p2p layer treats a Failed merge as terminal (the
        // pusher was already acked), so dropping a conflicted merge silently
        // loses the document (observed as encrypted filtered-replication poll
        // timeouts on the Linux CI runner).
        const MAX_TXN_RETRIES: usize = 5;

        let mut result = self
            .merge_block_attempt(cid, block_data, metadata.clone())
            .await;
        for attempt in 1..MAX_TXN_RETRIES {
            match &result {
                Err(e) if e.is_txn_conflict() => {
                    tracing::debug!(cid = %cid, attempt, "Merge txn conflict, retrying");
                    result = self
                        .merge_block_attempt(cid, block_data, metadata.clone())
                        .await;
                }
                _ => break,
            }
        }
        if let Err(e) = &result {
            if e.is_txn_conflict() {
                tracing::warn!(
                    cid = %cid,
                    max_retries = MAX_TXN_RETRIES,
                    "Merge txn conflict retries exhausted — document merge failed"
                );
            }
        }
        match result {
            // Signature verification is a property of the block bytes. It
            // cannot become valid on a later retry, so route it through the
            // existing terminal-rejection outcome and pending-DAG quarantine
            // instead of returning `Err` to the receiver retry clock (#1159).
            Err(error @ MergeError::SignatureVerificationFailed { .. }) => {
                Ok(MergeOutcome::Rejected {
                    reason: error.to_string(),
                })
            }
            other => other,
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

impl<S: Store + 'static, B: blockstore::Blockstore + 'static> DbMergeHandler<S, B> {
    /// One merge attempt for a single block. Conflict retry lives in the
    /// `MergeHandler::handle_block` wrapper above (Go's `executeMerge` split).
    pub(crate) async fn merge_block_attempt(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, MergeError> {
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

        // A standalone encrypted field block can only be merged once the
        // block-CID -> DocID ownership index names a single owner (recorded
        // by the composite merge that links it); otherwise it is merged via
        // its composite instead (see `process_lww_delta`/`process_counter_delta`).
        // Check that BEFORE attempting decryption: decrypting requires a KMS
        // fetch that may cross the network, and paying for that round trip
        // only to discard the result when ownership turns out to be unknown
        // wastes a fetch the composite merge will redundantly repeat moments
        // later (and, under load, can blow the caller's retry/poll budget).
        if block.encryption.is_some()
            && matches!(block.delta, CrdtDelta::Lww(_) | CrdtDelta::Counter(_))
            && self.resolve_field_block_doc_id(cid).await?.is_none()
        {
            // Still REQUEST the DEK (detached), Go-parity with sync-time
            // GetKeys: it warms the local key store for the composite merge
            // and keeps the serve-side authorization decision — including the
            // observable denial for unauthorized nodes — prompt.
            if let Some(enc_cid) = block.encryption {
                self.spawn_dek_prefetch(enc_cid, &metadata);
            }
            return Ok(MergeOutcome::terminal_skip(
                "field block has no unambiguous owner; merged via its composite",
            ));
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
                        Err(error @ MergeError::Kms(kms::Error::AccessDenied { .. })) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone LWW block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                        Err(error @ MergeError::Kms(_)) => return Err(error),
                        Err(error) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
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
                        Err(error @ MergeError::Kms(kms::Error::AccessDenied { .. })) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
                                "Cannot decrypt standalone Counter block, skipping (canRead=false)"
                            );
                            return Ok(MergeOutcome::terminal_skip(
                                "encryption key unavailable for standalone field block",
                            ));
                        }
                        Err(error @ MergeError::Kms(_)) => return Err(error),
                        Err(error) => {
                            tracing::debug!(
                                cid = %cid,
                                error = %error,
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
}

#[cfg(test)]
mod tests;
