//! Database merge handler for processing incoming P2P blocks.
//!
//! This module implements the `MergeHandler` trait from the P2P layer,
//! bridging incoming blocks to the CRDT system for document merging.

mod batch;
mod collection;
mod composite;
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
use p2p::sync::{BlockMetadata, MergeBlock, MergeHandler, MergeOutcome};
use schema::{
    self, CType, CollectionSource, CollectionVersion, FieldDescription, FieldKind, QuerySource,
    ScalarKind,
};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionVersionKey};
use zeroize::Zeroizing;

use crate::collection::collection_short_id;
use crate::database::DB;
use crate::index_manager::IndexManager;
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
    /// Optional SE encryption key for generating search artifacts on replicated documents.
    /// When set, the merge handler generates SE artifacts after merging documents
    /// that belong to collections with encrypted indexes.
    se_enc_key: std::sync::OnceLock<Zeroizing<Vec<u8>>>,
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
            se_enc_key: std::sync::OnceLock::new(),
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

    /// Get reference to blockstore.
    pub fn blockstore(&self) -> &Arc<B> {
        &self.blockstore
    }

    /// Decrypt block delta data using the encryption metadata block.
    ///
    /// If `encryption_cid` is Some, loads the Encryption block from blockstore,
    /// extracts the AES key, and decrypts the data. Returns data unchanged if
    /// no encryption CID is present.
    pub(crate) async fn decrypt_block_data(
        &self,
        data: &[u8],
        encryption_cid: Option<&Cid>,
    ) -> std::result::Result<Vec<u8>, MergeError> {
        let enc_cid = match encryption_cid {
            Some(cid) => cid,
            None => return Ok(data.to_vec()),
        };

        // Load the Encryption block from blockstore
        let enc_data = self
            .blockstore
            .get(enc_cid)
            .await
            .map_err(|e| MergeError::Storage(e.to_string()))?
            .ok_or_else(|| {
                MergeError::Storage(format!("Encryption block {} not found", enc_cid))
            })?;

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

        // Decrypt delta data if the block has encryption
        let decrypted_block;
        let effective_block = if block.encryption.is_some() {
            match &block.delta {
                CrdtDelta::Lww(payload) => {
                    match self
                        .decrypt_block_data(&payload.data, block.encryption.as_ref())
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
                        Err(_) => &block, // Decryption failed (no key) — use encrypted data
                    }
                }
                CrdtDelta::Counter(payload) => {
                    match self
                        .decrypt_block_data(&payload.data, block.encryption.as_ref())
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
                        Err(_) => &block,
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
            _ => unreachable!(),
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
        Block, CrdtDelta, LwwDeltaPayload, Signature, SignatureHeader, SignatureType,
    };
    use events::{Bus, ChannelBus, EventName};
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use storage::backends::MemoryStore;
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

        let result = crate::block_builder::build_blocks_from_document(&doc, "v1", blockstore)
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
}
