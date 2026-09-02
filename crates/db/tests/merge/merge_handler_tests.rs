use async_trait::async_trait;
use blockstore::Blockstore as _;
use blockstore::DefraBlockstore;
use cid::Cid;
use crdt::traits::Context;
use crdt::traits::ReplicatedData;
use crdt::traits::ValueReader;
use crdt::Counter;
use crdt::Lww;
use crdt::LwwDelta;
use crdt::NumericKind;
use crypto::PrivateKey as _;
use db::collection::Collection;
use db::database::DB;
use db::merge::merge_handler::hook::CompositeMergeHook;
use db::merge::merge_handler::hook::CompositePostCommitAction;
use db::merge::merge_handler::*;
use db::DbTransactionRegistry;
use db::IndexManager;
use defra_core::block::Block;
use defra_core::block::CollectionDefinitionDeltaPayload;
use defra_core::block::CollectionDeltaPayload;
use defra_core::block::CompositeDeltaPayload;
use defra_core::block::CounterDeltaPayload;
use defra_core::block::CrdtDelta;
use defra_core::block::DAGLink;
use defra_core::block::Encryption;
use defra_core::block::LwwDeltaPayload;
use defra_core::block::Signature;
use defra_core::block::SignatureHeader;
use defra_core::block::SignatureType;
use defra_core::merge::BlockMetadata;
use defra_core::merge::MergeBlock;
use defra_core::merge::MergeErrorDisposition;
use defra_core::merge::MergeHandler;
use defra_core::merge::MergeOutcome;
use defra_core::types::DocId;
use document::DocID;
use document::Document;
use document::NormalValue;
use events::Bus;
use events::ChannelBus;
use events::EventName;
use query::txn::TransactionRegistry;
use schema::CType;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use storage::corekv::Key;
use storage::index::IndexIterator;
use storage::keys::systemstore::CollectionID;
use storage::RegolithStore;
use tokio::time::timeout;
use tokio::time::Duration;

struct CountingBlockstore<B> {
    inner: Arc<B>,
    gets: AtomicUsize,
}

impl<B> CountingBlockstore<B> {
    fn new(inner: Arc<B>) -> Self {
        Self {
            inner,
            gets: AtomicUsize::new(0),
        }
    }

    fn get_count(&self) -> usize {
        self.gets.load(Ordering::Relaxed)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<B: blockstore::Blockstore> blockstore::Blockstore for CountingBlockstore<B> {
    async fn get(&self, cid: &Cid) -> blockstore::Result<Option<bytes::Bytes>> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(cid).await
    }

    async fn put(&self, cid: &Cid, data: &[u8]) -> blockstore::Result<()> {
        self.inner.put(cid, data).await
    }

    async fn put_many(&self, blocks: &[(&Cid, &[u8])]) -> blockstore::Result<()> {
        self.inner.put_many(blocks).await
    }

    async fn has(&self, cid: &Cid) -> blockstore::Result<bool> {
        self.inner.has(cid).await
    }

    async fn delete(&self, cid: &Cid) -> blockstore::Result<()> {
        self.inner.delete(cid).await
    }

    async fn get_size(&self, cid: &Cid) -> blockstore::Result<Option<usize>> {
        self.inner.get_size(cid).await
    }

    async fn all_cids(&self) -> blockstore::Result<Vec<Cid>> {
        self.inner.all_cids().await
    }

    fn hash_on_read(&self, enabled: bool) {
        self.inner.hash_on_read(enabled);
    }

    async fn is_merged(&self, cid: &Cid) -> blockstore::Result<bool> {
        self.inner.is_merged(cid).await
    }

    async fn mark_as_merged(&self, cid: &Cid) -> blockstore::Result<()> {
        self.inner.mark_as_merged(cid).await
    }

    async fn mark_batch_as_merged(&self, cids: &[Cid]) -> blockstore::Result<()> {
        self.inner.mark_batch_as_merged(cids).await
    }

    async fn get_unmerged(&self) -> blockstore::Result<Vec<Cid>> {
        self.inner.get_unmerged().await
    }
}

async fn register_test_block_owner(
    handler: &DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    collection_short_id: u32,
    doc_id: &str,
    cid: &Cid,
) {
    let txn = handler.db().new_txn(false).await.unwrap();
    {
        let systemstore = txn.systemstore().unwrap();
        handler
            .db()
            .resolve_or_allocate_doc_short_id(&systemstore, collection_short_id, doc_id)
            .await
            .unwrap();
        db::docid::map::set_block_doc_id_mapping(&systemstore, &cid.to_string(), doc_id)
            .await
            .unwrap();
    }
    txn.force_commit().await.unwrap();
}

/// Create a document locally through the genesis-CID identity flow:
/// blocks first (derives the DocID), then mappings, then the blob.
async fn create_doc_locally(
    handler: &DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    collection: &Collection,
    doc: &mut Document,
    schema_version_id: &str,
) -> (DocID, u64, db::block::builder::BlockResult) {
    let txn = handler.db().new_txn(false).await.unwrap();
    let output = {
        let datastore = txn.datastore().unwrap();
        let headstore = txn.headstore().unwrap();
        let raw_blockstore = txn.blockstore().unwrap();
        let systemstore = txn.systemstore().unwrap();
        let short_id = handler.db().next_doc_short_id().await.unwrap();
        let identity =
            db::block::builder::DocStorageIdentity::new(collection.resolved_root_id(), short_id);
        let result = db::block::builder::write_document_blocks(
            &raw_blockstore,
            &headstore,
            doc,
            schema_version_id,
            identity,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        db::docid::map::set_doc_id_mapping(
            &systemstore,
            collection.resolved_root_id(),
            short_id,
            &result.doc_id,
        )
        .await
        .unwrap();
        db::docid::map::set_block_doc_id_mapping(
            &systemstore,
            &result.cid.to_string(),
            &result.doc_id,
        )
        .await
        .unwrap();
        for field_cid in &result.field_cids {
            db::docid::map::set_block_doc_id_mapping(
                &systemstore,
                &field_cid.to_string(),
                &result.doc_id,
            )
            .await
            .unwrap();
        }
        let doc_id = DocID::from_string(&result.doc_id).unwrap();
        doc.set_id(doc_id.clone());
        collection
            .save_with_datastore(&datastore, doc, short_id)
            .await
            .unwrap();
        (doc_id, short_id, result)
    };
    txn.force_commit().await.unwrap();
    output
}

fn make_handler() -> (
    DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    Arc<DefraBlockstore<RegolithStore>>,
) {
    let store = RegolithStore::in_memory().unwrap();
    let store_arc = Arc::new(store);
    let db = Arc::new(DB::from_arc(store_arc.clone()).unwrap());
    let blockstore = Arc::new(DefraBlockstore::new(store_arc, false));
    let handler = DbMergeHandler::new(db, blockstore.clone());
    (handler, blockstore)
}

async fn make_handler_with_schema_and_bus() -> (
    DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    Arc<DefraBlockstore<RegolithStore>>,
    Arc<ChannelBus>,
) {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
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

async fn make_counting_handler_with_schema() -> (
    DbMergeHandler<RegolithStore, CountingBlockstore<DefraBlockstore<RegolithStore>>>,
    Arc<DefraBlockstore<RegolithStore>>,
    Arc<CountingBlockstore<DefraBlockstore<RegolithStore>>>,
) {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store.clone()).unwrap());
    db.create_collection(CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    ))
    .await
    .unwrap();

    let inner = Arc::new(DefraBlockstore::new(store, false));
    let blockstore = Arc::new(CountingBlockstore::new(inner.clone()));
    let handler = DbMergeHandler::new(db, blockstore.clone());
    (handler, inner, blockstore)
}

async fn make_handler_with_counter_schema() -> (
    DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    Arc<DefraBlockstore<RegolithStore>>,
) {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store.clone()).unwrap());

    db.create_collection(CollectionVersion::new(
        "Counters",
        "v1",
        "col-counters",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "score", FieldKind::int()).with_crdt_type(CType::PnCounter),
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
    blockstore: &Arc<DefraBlockstore<RegolithStore>>,
    name: &str,
    age: i64,
) -> MergeBlock {
    let mut doc = Document::new();
    doc.set("name", NormalValue::String(name.to_string()));
    doc.set("age", NormalValue::Int(age));

    let result = db::block::builder::build_blocks_from_document(&doc, "v1", blockstore)
        .await
        .unwrap();

    MergeBlock {
        cid: result.cid,
        block_data: result.block,
        doc_id: result.doc_id,
        collection_id: "col-users".to_string(),
        creator: "did:key:z6MkrBatchMergeTest".to_string(),
        sender_peer: Some("peer1".to_string()),
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        verified_creator: None,
    }
}

fn composite_merge_block(cid: Cid, block: &Block, doc_id: &str) -> MergeBlock {
    MergeBlock {
        cid,
        block_data: bytes::Bytes::from(block.to_dag_cbor().unwrap()),
        doc_id: doc_id.to_string(),
        collection_id: "col-users".to_string(),
        creator: "did:key:z6MkrBatchIdentityTest".to_string(),
        sender_peer: Some("peer1".to_string()),
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        verified_creator: None,
    }
}

fn make_lww_block(signature_cid: Option<Cid>) -> Block {
    let payload = LwwDeltaPayload {
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
    let store = RegolithStore::in_memory().unwrap();
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
    blockstore: &DefraBlockstore<RegolithStore>,
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
    register_test_block_owner(&handler, 1, "doc1", &cid).await;

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
        capability: None,
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
    assert_eq!(
        handler.error_disposition(&error),
        MergeErrorDisposition::Terminal
    );

    merge_block.collection_id = "other-collection".to_string();
    let error = handler
        .validate_authorization(Some(&valid_authorization), &merge_block)
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("does not match block collection"));
    assert_eq!(
        handler.error_disposition(&error),
        MergeErrorDisposition::Terminal
    );
}

#[tokio::test]
async fn malformed_block_is_a_terminal_merge_rejection() {
    let (handler, _blockstore) = make_handler();
    let cid = make_lww_block(None).generate_cid().unwrap();

    let outcome = handler
        .handle_block(
            &cid,
            b"not dag-cbor",
            BlockMetadata::normal("doc1", "v1", "creator", Some("peer1"), true),
        )
        .await
        .expect("malformed content should be classified, not retried");

    assert!(matches!(outcome, MergeOutcome::Rejected { .. }));
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

    let outcome = handler
        .handle_block(
            &cid,
            &block_data,
            BlockMetadata::normal("doc1", "v1", "creator", None, false),
        )
        .await
        .expect("a deterministic signature failure is a merge outcome, not a retry error");
    assert!(
        matches!(outcome, MergeOutcome::Rejected { .. }),
        "tampered blocks must flow to pending-DAG quarantine"
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
    blockstore: &DefraBlockstore<RegolithStore>,
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
    let (_attacker_key, _pub_hex, attacker_did) = sign_block_ed25519(&mut block, &blockstore).await;

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
    let expected_blocks = [
        (first.doc_id.clone(), first.block_data.clone()),
        (second.doc_id.clone(), second.block_data.clone()),
    ];

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

    let updates = [
        update1.as_update().expect("expected update event"),
        update2.as_update().expect("expected update event"),
    ];
    for update in updates {
        let (_, expected_block) = expected_blocks
            .iter()
            .find(|(doc_id, _)| doc_id == &update.doc_id)
            .expect("unexpected update document");
        assert_eq!(update.block.as_ref(), expected_block.as_ref());
    }

    let mut seen_doc_ids = updates.map(|update| update.doc_id.clone()).to_vec();
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
        .process_collection_definition_delta(&cid, &block, &payload, &BlockMetadata::schema_sync())
        .await
        .unwrap();
    assert_eq!(outcome, MergeOutcome::Merged);

    let txn = handler.db().new_txn(true).await.unwrap();
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
    let doc_id = DocID::new_v0(
        "bafyreihgg6a5auqhikq4nvw6fj3kbreovdbazlisbs5kerkahoqwwiz75i"
            .parse()
            .unwrap(),
    )
    .to_string();

    let mut delta_data = Vec::new();
    ciborium::into_writer(&5_i64, &mut delta_data).unwrap();

    let payload = CounterDeltaPayload {
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
    register_test_block_owner(&handler, 1, &doc_id, &cid).await;

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

/// Read the PNCounter accumulation store value (authoritative store, not the
/// materialized blob) for a doc/field via a fresh read txn on `db`.
async fn read_counter_accumulation_store(
    db: &Arc<DB<RegolithStore>>,
    schema_version_id: &str,
    doc_id: &str,
    field: &str,
) -> i64 {
    let txn = db.new_txn(true).await.expect("read txn");
    let datastore = txn.datastore().expect("datastore");
    let counter = Counter::new(
        schema_version_id.to_string(),
        doc_id.as_bytes(),
        field.to_string(),
        true,
        NumericKind::Int64,
    )
    .expect("counter");
    let bytes = ValueReader::value(&counter, &datastore)
        .await
        .expect("counter value");
    assert_eq!(bytes.len(), 8, "int64 counter store value is 8 bytes");
    i64::from_be_bytes(bytes.as_ref().try_into().unwrap())
}

/// Build + store a standalone counter delta block for `doc_id`/`score` with the
/// given integer increment, returning its CID and the metadata to merge it.
/// The block is persisted in the blockstore so `process_counter_delta` can find
/// and mark it merged.
async fn put_counter_delta_block(
    blockstore: &Arc<DefraBlockstore<RegolithStore>>,
    _doc_id: &str,
    increment: i64,
    priority: u64,
    nonce: i64,
) -> (Cid, CounterDeltaPayload) {
    let mut delta_data = Vec::new();
    ciborium::into_writer(&increment, &mut delta_data).unwrap();
    let payload = CounterDeltaPayload {
        field_name: "score".to_string(),
        schema_version_id: "v1".to_string(),
        priority,
        data: delta_data,
        nonce,
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
    (cid, payload)
}

/// #1044 residual coverage gap: an interactive/explicit-transaction counter
/// increment racing a same-document P2P merge.
///
/// PROPERTY (proven DETERMINISTICALLY by controlling ordering, not a real
/// race): an interactive counter txn must NOT silently clobber a concurrent
/// same-doc merge. The per-doc guard excludes partial-RMW interleaving during
/// the finalize's held window, but a merge that COMMITS *before* the
/// interactive finalize makes the interactive txn's `begin()` snapshot stale →
/// on commit the storage SSI/OCC conflict tracker (`ConflictTracker` in
/// `storage::backends::shared`, used by `RegolithStore`) aborts the interactive
/// commit with a `TxnConflict` ("transaction conflict. Please retry"), and the
/// merge's value survives. No data loss, no double-apply — the client retries.
/// This matches Go's storage-txn isolation.
///
/// CONSTRUCTION (real merge handler, no fallback):
///   1. Seed doc with `score` (PNCounter) at N=10 via the registry auto-finalize
///      path; confirm the accumulation store == 10.
///   2. `begin()` an explicit txn and `update` +3 through the interactive
///      mutator — this RECORDS the pending op and writes the provisional blob but
///      does NOT finalize. The txn's `read_version` snapshot is fixed here.
///   3. BEFORE committing, apply a same-doc merge of +5 through the REAL merge
///      handler (`process_counter_delta`, which takes its own `merge_queue`
///      guard and commits its own txn). Confirm the store advanced to 15.
///   4. Commit the interactive txn. ASSERT: commit returns a conflict ERROR and
///      the store STILL == 15 (the merge survived; no clobber to 13).
///   5. RETRY the +3 in a fresh explicit txn (snapshot now sees 15) → store == 18.
///
/// REGRESSION CAUGHT: if the finalize ignored OCC and force-wrote its stale RMW,
/// the interactive commit would succeed and the store would be 13 (the merge's
/// +5 lost) — both assertions (error + store==15) would fail.
#[tokio::test]
async fn interactive_counter_increment_conflicts_with_concurrent_same_doc_merge() {
    let (handler, blockstore) = make_handler_with_counter_schema().await;
    let db = Arc::clone(handler.db());
    // Same DB / same RegolithStore => same ConflictTracker shared across the
    // interactive registry txn and the merge handler's own txn.
    let registry = DbTransactionRegistry::new(Arc::clone(&db));

    // (1) Seed the doc with score = 10 via the registry (auto-finalize on commit).
    let handle = registry.begin(false).await.expect("begin seed");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let created = mutator
        .create(
            "Counters",
            Document::from_json_str(r#"{"score": 10}"#).unwrap(),
        )
        .await
        .expect("create seed");
    let doc_id = created.doc_id.to_string();
    drop(mutator);
    drop(ctx);
    registry.commit(&handle).await.expect("commit seed");
    assert_eq!(
        read_counter_accumulation_store(&db, "v1", &doc_id, "score").await,
        10,
        "seed must leave the accumulation store at N=10"
    );

    // (2) Begin an EXPLICIT txn and increment +3 through the interactive
    // mutator. This records the pending op + writes the provisional blob but
    // does NOT finalize; the txn's read_version snapshot is fixed at begin().
    let handle = registry.begin(false).await.expect("begin interactive");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let mut update_doc = Document::from_json_str(r#"{"score": 13}"#).unwrap();
    update_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
    update_doc.set_counter_delta("score".to_string(), NormalValue::Int(3));
    let mut modified = std::collections::HashSet::new();
    modified.insert("score".to_string());
    mutator
        .update("Counters", update_doc, modified)
        .await
        .expect("interactive update +3");
    drop(mutator);
    drop(ctx);
    // NOTE: NOT committed yet.

    // (3) BEFORE committing, apply a concurrent same-doc merge of +5 through the
    // REAL merge handler. process_counter_delta takes its own merge_queue guard
    // and commits its OWN txn, advancing the accumulation store to N+D2 = 15.
    let (merge_cid, merge_payload) =
        put_counter_delta_block(&blockstore, &doc_id, 5, 2, 7777).await;
    register_test_block_owner(&handler, 1, &doc_id, &merge_cid).await;
    let metadata = BlockMetadata::normal(
        &doc_id,
        "col-counters",
        "did:key:z6MkrInteractiveMergeRace",
        None,
        false,
    );
    let merge_outcome = handler
        .process_counter_delta(&merge_cid, &merge_payload, &metadata)
        .await
        .expect("merge handler must apply the +5 delta");
    assert_eq!(merge_outcome, MergeOutcome::Merged);
    assert_eq!(
        read_counter_accumulation_store(&db, "v1", &doc_id, "score").await,
        15,
        "merge of +5 must advance the accumulation store to N+D2 = 15"
    );

    // (4) Now COMMIT the interactive txn. Its begin() snapshot is stale (the
    // merge committed after it), so the OCC tracker aborts the commit with a
    // TxnConflict.
    let commit_result = registry.commit(&handle).await;
    let err = commit_result.expect_err(
        "interactive commit MUST conflict with the committed merge — if it \
             succeeds it silently clobbered the merge (CRITICAL production bug)",
    );
    let err_msg = err.to_string().to_lowercase();
    assert!(
        err_msg.contains("transaction conflict"),
        "interactive commit must fail with the storage OCC TxnConflict \
             (corekv Error::TxnConflict = \"transaction conflict. Please retry\"), \
             got: {err}"
    );
    // (4b) The merge's value survived; the interactive txn did NOT clobber it
    // down to N+D1 = 13.
    assert_eq!(
        read_counter_accumulation_store(&db, "v1", &doc_id, "score").await,
        15,
        "after the conflicted interactive commit the merge's value (15) must \
             survive — NOT be clobbered to 13"
    );

    // (5) Correct recovery: RETRY the +3 in a fresh explicit txn (snapshot now
    // sees 15) → store == N+D2+D1 = 18. Conflict → retry → correct convergence.
    let handle = registry.begin(false).await.expect("begin retry");
    let ctx = registry.get(&handle).into_result().unwrap().unwrap();
    let mutator = ctx.doc_mutator().expect("mutator");
    let mut retry_doc = Document::from_json_str(r#"{"score": 18}"#).unwrap();
    retry_doc.set_id(document::DocID::from_string(&doc_id).unwrap());
    retry_doc.set_counter_delta("score".to_string(), NormalValue::Int(3));
    let mut modified = std::collections::HashSet::new();
    modified.insert("score".to_string());
    mutator
        .update("Counters", retry_doc, modified)
        .await
        .expect("retry update +3");
    drop(mutator);
    drop(ctx);
    registry
        .commit(&handle)
        .await
        .expect("retry commit must succeed");
    assert_eq!(
        read_counter_accumulation_store(&db, "v1", &doc_id, "score").await,
        18,
        "retry on a fresh snapshot must converge to N+D2+D1 = 18"
    );
}

#[tokio::test]
async fn handle_block_serializes_standalone_counter_by_doc_id() {
    let (handler, blockstore) = make_handler_with_counter_schema().await;
    let doc_id_str = DocID::new_v0(
        "bafyreidwus7muqrpwwf22gvpqpow6xg37woh4ikztgl27deo37ehs5ehaa"
            .parse()
            .unwrap(),
    )
    .to_string();

    let mut delta_data = Vec::new();
    ciborium::into_writer(&5_i64, &mut delta_data).unwrap();

    let payload = CounterDeltaPayload {
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
    register_test_block_owner(&handler, 1, &doc_id_str, &cid).await;

    let metadata = BlockMetadata::normal(
        &doc_id_str,
        "col-counters",
        "did:key:z6MkrCounterMergeQueueTest",
        None,
        false,
    );

    let guard = handler.merge_queue().acquire(&doc_id_str).await;
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
        .db()
        .find_collection_by_id("col-counters")
        .unwrap()
        .expect("counter collection should exist");
    let doc_id = DocID::new_v0(
        "bafyreie7rtdexuf47f633477mfieshkeh5rwnjeommkgqrzl22n6g4bfmm"
            .parse()
            .unwrap(),
    );
    let doc_id_str = doc_id.to_string();

    let mut delta_data = Vec::new();
    ciborium::into_writer(&5_i64, &mut delta_data).unwrap();

    let payload = CounterDeltaPayload {
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
    register_test_block_owner(&handler, 1, &doc_id_str, &cid).await;

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

    let txn = handler.db().new_txn(true).await.unwrap();
    let stored = {
        let datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();
        collection
            .get_by_doc_id(&datastore, &systemstore, &doc_id)
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
async fn shared_counter_block_is_applied_once_per_document() {
    let (handler, blockstore) = make_handler_with_counter_schema().await;
    let doc_ids = [
        DocID::new_v0(
            "bafyreihgg6a5auqhikq4nvw6fj3kbreovdbazlisbs5kerkahoqwwiz75i"
                .parse()
                .unwrap(),
        )
        .to_string(),
        DocID::new_v0(
            "bafyreie7rtdexuf47f633477mfieshkeh5rwnjeommkgqrzl22n6g4bfmm"
                .parse()
                .unwrap(),
        )
        .to_string(),
    ];
    let mut data = Vec::new();
    ciborium::into_writer(&5_i64, &mut data).unwrap();
    let payload = CounterDeltaPayload {
        field_name: "score".to_string(),
        schema_version_id: "v1".to_string(),
        priority: 1,
        data,
        nonce: 7,
    };
    let block = Block::new(CrdtDelta::Counter(payload.clone()), vec![], vec![]);
    let cid = block.generate_cid().unwrap();
    blockstore
        .put(&cid, &block.to_dag_cbor().unwrap())
        .await
        .unwrap();

    for doc_id in &doc_ids {
        register_test_block_owner(&handler, 1, doc_id, &cid).await;
    }

    for (index, doc_id) in doc_ids.iter().enumerate() {
        let txn = handler.db().new_txn(false).await.unwrap();
        let doc_short_id = {
            let systemstore = txn.systemstore().unwrap();
            db::docid::map::get_doc_ref(&systemstore, doc_id)
                .await
                .unwrap()
                .unwrap()
                .doc_short_id
        };
        {
            let mut datastore = txn.datastore().unwrap();
            let headstore = txn.headstore().unwrap();
            let result = handler
                .process_counter_delta_in_txn(
                    &mut datastore,
                    &headstore,
                    &cid,
                    &payload,
                    Some("col-counters"),
                    doc_id,
                    doc_short_id,
                )
                .await
                .unwrap();
            assert!(result.applied());
        }
        txn.force_commit().await.unwrap();
        if index == 0 {
            blockstore.mark_as_merged(&cid).await.unwrap();
        }
    }

    for doc_id in &doc_ids {
        assert_eq!(
            read_counter_accumulation_store(handler.db(), "v1", doc_id, "score").await,
            5
        );
    }
}

#[tokio::test]
async fn composite_merge_skips_locally_merged_counter_parent() {
    let (handler, blockstore) = make_handler_with_counter_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-counters")
        .unwrap()
        .expect("counter collection should exist");

    let mut doc = Document::new();
    doc.set_with_crdt("score", CType::PnCounter, NormalValue::Int(10))
        .unwrap();
    doc.set_schema_version_id("v1");
    let (doc_id, _doc_short_id, local_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();
    assert!(
        blockstore.is_merged(&local_blocks.cid).await.unwrap(),
        "locally-created composite blocks are already merged"
    );

    let mut update_data = Vec::new();
    ciborium::into_writer(&10_i64, &mut update_data).unwrap();
    let update_field_payload = CounterDeltaPayload {
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
        let txn = handler.db().new_txn(true).await.unwrap();
        let stored = {
            let datastore = txn.datastore().unwrap();
            let systemstore = txn.systemstore().unwrap();
            collection
                .get_by_doc_id(&datastore, &systemstore, &doc_id)
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
    DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    Arc<DefraBlockstore<RegolithStore>>,
) {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
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
        .db()
        .find_collection_by_id("col-agentdocs")
        .unwrap()
        .expect("agentdocs collection should exist");

    let mut doc = Document::new();
    doc.set(
        "agent_did",
        NormalValue::String("did:key:alice".to_string()),
    );
    doc.set("body", NormalValue::String("v1".to_string()));
    doc.set_schema_version_id("v1");

    // Persist the initial document locally (agent_did = alice).
    let (doc_id, _doc_short_id, create_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();

    // Craft a higher-priority remote update that flips the immutable field.
    let mut update_data = Vec::new();
    ciborium::into_writer(
        &NormalValue::String("did:key:bob".to_string()),
        &mut update_data,
    )
    .unwrap();
    let update_field_payload = LwwDeltaPayload {
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
        .expect("an immutable-field rejection is a terminal skip, not a hard error");

    // The rejection must be TERMINAL (skipped), not a transient Err — a hard
    // error would leave the CID unmarked and re-fetched on every sync pass.
    assert!(
        outcome.is_terminal_skip(),
        "remote merge changing an immutable field must be terminally skipped, got {outcome:?}"
    );

    // The locally-stored immutable value must be unchanged.
    let stored = {
        let txn = handler.db().new_txn(true).await.unwrap();
        let stored = {
            let datastore = txn.datastore().unwrap();
            let systemstore = txn.systemstore().unwrap();
            collection
                .get_by_doc_id(&datastore, &systemstore, &doc_id)
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

/// Clearing an @immutable field (an empty/tombstone LWW delta) is also a
/// change and must be rejected — not silently applied to the field store.
#[tokio::test]
async fn remote_composite_merge_rejects_immutable_field_clear() {
    let (handler, blockstore) = make_handler_with_immutable_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-agentdocs")
        .unwrap()
        .expect("agentdocs collection should exist");

    let mut doc = Document::new();
    doc.set(
        "agent_did",
        NormalValue::String("did:key:alice".to_string()),
    );
    doc.set("body", NormalValue::String("v1".to_string()));
    doc.set_schema_version_id("v1");

    let (doc_id, _doc_short_id, create_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();

    // Tombstone: an LWW delta with empty data clears the field.
    let clear_field = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "agent_did".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            data: Vec::new(),
        }),
        create_blocks.field_cids.clone(),
        vec![],
    );
    let clear_field_cid = clear_field.generate_cid().unwrap();
    blockstore
        .put(&clear_field_cid, &clear_field.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let clear_payload = CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority: 2,
        status: 1,
    };
    let clear_composite = Block::new(
        CrdtDelta::Composite(clear_payload.clone()),
        vec![create_blocks.cid],
        vec![DAGLink::new("agent_did", clear_field_cid)],
    );
    let clear_cid = clear_composite.generate_cid().unwrap();
    blockstore
        .put(&clear_cid, &clear_composite.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let outcome = handler
        .process_composite_delta(
            &clear_cid,
            &clear_composite,
            &clear_payload,
            &BlockMetadata::normal(
                &doc_id_str,
                "col-agentdocs",
                "did:key:z6MkrClr",
                None,
                false,
            ),
            false,
            0,
        )
        .await
        .expect("immutable clear rejection is a terminal skip, not a hard error");
    assert!(
        outcome.is_terminal_skip(),
        "clearing an immutable field must be terminally skipped, got {outcome:?}"
    );
}

/// A remotely-deleted document still retains its bytes (handle_deletion only
/// sets the marker), so a later merge that re-materializes it must NOT be able
/// to change an immutable field via delete+recreate. Without the
/// deleted-inclusive baseline read, get_with_datastore returns None and the
/// check is skipped.
#[tokio::test]
async fn remote_merge_rejects_immutable_change_after_delete() {
    let (handler, blockstore) = make_handler_with_immutable_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-agentdocs")
        .unwrap()
        .expect("agentdocs collection should exist");

    let mut doc = Document::new();
    doc.set(
        "agent_did",
        NormalValue::String("did:key:alice".to_string()),
    );
    doc.set("body", NormalValue::String("v1".to_string()));
    doc.set_schema_version_id("v1");

    let (doc_id, _doc_short_id, create_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();

    // Remote deletion (status = 2): sets the deleted marker, retains bytes.
    let delete_payload = CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority: 2,
        status: 2,
    };
    let delete_block = Block::new(
        CrdtDelta::Composite(delete_payload.clone()),
        vec![create_blocks.cid],
        vec![],
    );
    let delete_cid = delete_block.generate_cid().unwrap();
    blockstore
        .put(&delete_cid, &delete_block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let delete_meta = BlockMetadata::normal(
        &doc_id_str,
        "col-agentdocs",
        "did:key:z6MkrDeleter",
        None,
        false,
    );
    let delete_outcome = handler
        .process_composite_delta(
            &delete_cid,
            &delete_block,
            &delete_payload,
            &delete_meta,
            false,
            0,
        )
        .await
        .expect("delete merge");
    assert_eq!(delete_outcome, MergeOutcome::Merged);

    // Remote re-materialize with a CHANGED immutable field (priority 3).
    let mut update_data = Vec::new();
    ciborium::into_writer(
        &NormalValue::String("did:key:bob".to_string()),
        &mut update_data,
    )
    .unwrap();
    let recreate_field_payload = LwwDeltaPayload {
        field_name: "agent_did".to_string(),
        schema_version_id: "v1".to_string(),
        priority: 3,
        data: update_data,
    };
    let recreate_field_block = Block::new(
        CrdtDelta::Lww(recreate_field_payload),
        create_blocks.field_cids.clone(),
        vec![],
    );
    let recreate_field_cid = recreate_field_block.generate_cid().unwrap();
    blockstore
        .put(
            &recreate_field_cid,
            &recreate_field_block.to_dag_cbor().unwrap(),
        )
        .await
        .unwrap();

    let recreate_payload = CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority: 3,
        status: 1,
    };
    let recreate_block = Block::new(
        CrdtDelta::Composite(recreate_payload.clone()),
        vec![delete_cid],
        vec![DAGLink::new("agent_did", recreate_field_cid)],
    );
    let recreate_cid = recreate_block.generate_cid().unwrap();
    blockstore
        .put(&recreate_cid, &recreate_block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let recreate_meta = BlockMetadata::normal(
        &doc_id_str,
        "col-agentdocs",
        "did:key:z6MkrRecreator",
        None,
        false,
    );
    let outcome = handler
        .process_composite_delta(
            &recreate_cid,
            &recreate_block,
            &recreate_payload,
            &recreate_meta,
            false,
            0,
        )
        .await
        .expect("an immutable rejection is a terminal skip, not a hard error");
    assert!(
            outcome.is_terminal_skip(),
            "re-materializing a deleted doc with a changed immutable field must be rejected, got {outcome:?}"
        );
}

/// A composite that does NOT touch an immutable field must not be rejected,
/// even against a deleted prior version. Only linked immutable fields are
/// validated, so a partial update is not falsely flagged.
#[tokio::test]
async fn remote_merge_allows_partial_update_to_deleted_doc() {
    let (handler, blockstore) = make_handler_with_immutable_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-agentdocs")
        .unwrap()
        .expect("agentdocs collection should exist");

    let mut doc = Document::new();
    doc.set(
        "agent_did",
        NormalValue::String("did:key:alice".to_string()),
    );
    doc.set("body", NormalValue::String("v1".to_string()));
    doc.set_schema_version_id("v1");

    let (doc_id, _doc_short_id, create_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();

    // Delete (status 2).
    let delete_payload = CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority: 2,
        status: 2,
    };
    let delete_block = Block::new(
        CrdtDelta::Composite(delete_payload.clone()),
        vec![create_blocks.cid],
        vec![],
    );
    let delete_cid = delete_block.generate_cid().unwrap();
    blockstore
        .put(&delete_cid, &delete_block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    handler
        .process_composite_delta(
            &delete_cid,
            &delete_block,
            &delete_payload,
            &BlockMetadata::normal(
                &doc_id_str,
                "col-agentdocs",
                "did:key:z6MkrDel",
                None,
                false,
            ),
            false,
            0,
        )
        .await
        .expect("delete merge");

    // Re-materialize touching only the (mutable) body field — must be allowed.
    let mut body_data = Vec::new();
    ciborium::into_writer(&NormalValue::String("v2".to_string()), &mut body_data).unwrap();
    let body_payload = LwwDeltaPayload {
        field_name: "body".to_string(),
        schema_version_id: "v1".to_string(),
        priority: 3,
        data: body_data,
    };
    let body_block = Block::new(
        CrdtDelta::Lww(body_payload),
        create_blocks.field_cids.clone(),
        vec![],
    );
    let body_cid = body_block.generate_cid().unwrap();
    blockstore
        .put(&body_cid, &body_block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let recreate_payload = CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority: 3,
        status: 1,
    };
    let recreate_block = Block::new(
        CrdtDelta::Composite(recreate_payload.clone()),
        vec![delete_cid],
        vec![DAGLink::new("body", body_cid)],
    );
    let recreate_cid = recreate_block.generate_cid().unwrap();
    blockstore
        .put(&recreate_cid, &recreate_block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let outcome = handler
        .process_composite_delta(
            &recreate_cid,
            &recreate_block,
            &recreate_payload,
            &BlockMetadata::normal(&doc_id_str, "col-agentdocs", "did:key:z6MkrRe", None, false),
            false,
            0,
        )
        .await
        .expect("partial update merge");
    assert!(
        !outcome.is_terminal_skip(),
        "a partial update that does not touch an immutable field must be allowed, got {outcome:?}"
    );
}

/// Batch path: a composite that changes an @immutable field is terminally
/// skipped and leaves NO partial write (not even of its sibling mutable
/// field), while a valid block in the same batch still commits.
#[tokio::test]
async fn batch_merge_rejects_immutable_change_without_partial_write() {
    let (handler, blockstore) = make_handler_with_immutable_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-agentdocs")
        .unwrap()
        .expect("agentdocs collection should exist");

    let mut doc = Document::new();
    doc.set(
        "agent_did",
        NormalValue::String("did:key:alice".to_string()),
    );
    doc.set("body", NormalValue::String("v1".to_string()));
    doc.set_schema_version_id("v1");

    let (doc_id, _doc_short_id, create_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();

    // Violating composite: changes the immutable agent_did AND a benign body.
    let encode = |s: &str| {
        let mut b = Vec::new();
        ciborium::into_writer(&NormalValue::String(s.to_string()), &mut b).unwrap();
        b
    };
    let did_block = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "agent_did".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            data: encode("did:key:bob"),
        }),
        create_blocks.field_cids.clone(),
        vec![],
    );
    let did_cid = did_block.generate_cid().unwrap();
    blockstore
        .put(&did_cid, &did_block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let body_block = Block::new(
        CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "body".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 2,
            data: encode("hijacked"),
        }),
        create_blocks.field_cids.clone(),
        vec![],
    );
    let body_cid = body_block.generate_cid().unwrap();
    blockstore
        .put(&body_cid, &body_block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let bad_payload = CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority: 2,
        status: 1,
    };
    let bad_block = Block::new(
        CrdtDelta::Composite(bad_payload),
        vec![create_blocks.cid],
        vec![
            DAGLink::new("agent_did", did_cid),
            DAGLink::new("body", body_cid),
        ],
    );
    let bad_cid = bad_block.generate_cid().unwrap();
    blockstore
        .put(&bad_cid, &bad_block.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let bad_merge = MergeBlock {
        cid: bad_cid,
        block_data: bytes::Bytes::from(bad_block.to_dag_cbor().unwrap()),
        doc_id: doc_id_str.clone(),
        collection_id: "col-agentdocs".to_string(),
        creator: "did:key:z6MkrBad".to_string(),
        sender_peer: Some("peer1".to_string()),
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        verified_creator: None,
    };

    // Valid sibling: a fresh document in the same batch.
    let mut sibling = Document::new();
    sibling.set(
        "agent_did",
        NormalValue::String("did:key:carol".to_string()),
    );
    sibling.set("body", NormalValue::String("sibling".to_string()));
    let sibling_result =
        db::block::builder::build_blocks_from_document(&sibling, "v1", &blockstore)
            .await
            .unwrap();
    let sibling_id = sibling_result.doc_id.clone();
    let sibling_merge = MergeBlock {
        cid: sibling_result.cid,
        block_data: sibling_result.block,
        doc_id: sibling_result.doc_id,
        collection_id: "col-agentdocs".to_string(),
        creator: "did:key:z6MkrSib".to_string(),
        sender_peer: Some("peer1".to_string()),
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        verified_creator: None,
    };

    let results = handler
        .handle_block_batch(&[bad_merge, sibling_merge])
        .await;
    assert!(
        matches!(results[0], Ok(ref o) if o.is_terminal_skip()),
        "immutable-violating batch block must be terminally skipped, got {:?}",
        results[0]
    );
    assert!(
        matches!(results[1], Ok(MergeOutcome::Merged)),
        "valid sibling block must commit, got {:?}",
        results[1]
    );

    // No partial write: neither the immutable field NOR the benign body of the
    // rejected composite persisted; the sibling did.
    let txn = handler.db().new_txn(true).await.unwrap();
    let (doc1, sibling_present) = {
        let datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();
        let doc1 = collection
            .get_by_doc_id(&datastore, &systemstore, &doc_id)
            .await
            .unwrap()
            .expect("doc1 exists");
        let sibling_present = collection
            .get_by_doc_id(
                &datastore,
                &systemstore,
                &DocID::from_string(&sibling_id).unwrap(),
            )
            .await
            .unwrap()
            .is_some();
        (doc1, sibling_present)
    };
    txn.force_discard().unwrap();

    assert_eq!(
        doc1.get("agent_did"),
        Some(&NormalValue::String("did:key:alice".to_string())),
        "immutable field must be unchanged"
    );
    assert_eq!(
        doc1.get("body"),
        Some(&NormalValue::String("v1".to_string())),
        "rejected composite must not have written its sibling field (no partial write)"
    );
    assert!(sibling_present, "valid sibling document must be committed");
}

#[tokio::test]
async fn composite_lww_reseeds_from_local_doc_when_crdt_store_is_stale() {
    let (handler, blockstore, _bus) = make_handler_with_schema_and_bus().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-users")
        .unwrap()
        .expect("users collection should exist");

    let mut doc = Document::new();
    doc.set("age", NormalValue::Int(21));
    doc.set_schema_version_id("v1");

    let (doc_id, _doc_short_id, create_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();

    doc.set("age", NormalValue::Int(60));
    let mut modified_fields = HashSet::new();
    modified_fields.insert("age".to_string());
    {
        let txn = handler.db().new_txn(false).await.unwrap();
        {
            let datastore = txn.datastore().unwrap();
            let headstore = txn.headstore().unwrap();
            let raw_blockstore = txn.blockstore().unwrap();
            collection
                .save_with_datastore(&datastore, &doc, _doc_short_id)
                .await
                .unwrap();
            db::block::builder::write_document_blocks(
                &raw_blockstore,
                &headstore,
                &doc,
                "v1",
                db::block::builder::DocStorageIdentity::new(
                    collection.resolved_root_id(),
                    _doc_short_id,
                ),
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
        let txn = handler.db().new_txn(false).await.unwrap();
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
        let txn = handler.db().new_txn(true).await.unwrap();
        let stored = {
            let datastore = txn.datastore().unwrap();
            let systemstore = txn.systemstore().unwrap();
            collection
                .get_by_doc_id(&datastore, &systemstore, &doc_id)
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
        .db()
        .find_collection_by_id("col-users")
        .unwrap()
        .expect("users collection should exist");

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("John".to_string()));
    doc.set_schema_version_id("v1");

    let (doc_id, _doc_short_id, local_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();

    let build_update =
        |name: &str, priority: u64, field_heads: Vec<Cid>, composite_heads: Vec<Cid>| {
            let mut data = Vec::new();
            ciborium::into_writer(&NormalValue::String(name.to_string()), &mut data).unwrap();
            let field_payload = LwwDeltaPayload {
                field_name: "name".to_string(),
                schema_version_id: "v1".to_string(),
                priority,
                data,
            };
            let field_block = Block::new(CrdtDelta::Lww(field_payload), field_heads, vec![]);
            let field_cid = field_block.generate_cid().unwrap();
            let field_data = field_block.to_dag_cbor().unwrap();

            let composite_payload = CompositeDeltaPayload {
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

    let txn = handler.db().new_txn(true).await.unwrap();
    let (head_keys, doc_short_id) = {
        let headstore = txn.headstore().unwrap();
        let systemstore = txn.systemstore().unwrap();
        let doc_short_id = db::docid::map::get_doc_ref(&systemstore, &doc_id_str)
            .await
            .unwrap()
            .expect("merged doc has a short-ID mapping")
            .doc_short_id;
        let mut iter = headstore
            .iterator(storage::corekv::IterOptions::new().with_prefix(
                storage::keys::headstore::HeadstoreDocKey::field_prefix(doc_short_id, "name"),
            ))
            .await
            .unwrap();
        let mut keys = Vec::new();
        while let Some(pair) = iter.next().await.unwrap() {
            keys.push(pair.key);
        }
        iter.close().await.unwrap();
        (keys, doc_short_id)
    };
    txn.force_discard().unwrap();

    assert_eq!(
        head_keys,
        vec![
            storage::keys::headstore::HeadstoreDocKey::new(doc_short_id, "name", child_field_cid)
                .bytes()
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deep_composite_parent_chain_merges_on_worker_stack() {
    let (handler, blockstore, _bus) = make_handler_with_schema_and_bus().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-users")
        .unwrap()
        .expect("users collection should exist");

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("initial".to_string()));
    doc.set_schema_version_id("v1");

    let (doc_id, _doc_short_id, local_blocks) =
        create_doc_locally(&handler, &collection, &mut doc, "v1").await;
    let doc_id_str = doc_id.to_string();
    let mut field_heads = local_blocks.field_cids;
    let mut composite_heads = vec![local_blocks.cid];
    let mut latest = None;

    for priority in 2..=257 {
        let name = format!("name-{priority}");
        let mut data = Vec::new();
        ciborium::into_writer(&NormalValue::String(name), &mut data).unwrap();
        let field_block = Block::new(
            CrdtDelta::Lww(LwwDeltaPayload {
                field_name: "name".to_string(),
                schema_version_id: "v1".to_string(),
                priority,
                data,
            }),
            field_heads,
            vec![],
        );
        let field_cid = field_block.generate_cid().unwrap();
        blockstore
            .put(&field_cid, &field_block.to_dag_cbor().unwrap())
            .await
            .unwrap();

        let payload = CompositeDeltaPayload {
            schema_version_id: "v1".to_string(),
            priority,
            status: 1,
        };
        let composite_block = Block::new(
            CrdtDelta::Composite(payload.clone()),
            composite_heads,
            vec![DAGLink::new("name", field_cid)],
        );
        let composite_cid = composite_block.generate_cid().unwrap();
        blockstore
            .put(&composite_cid, &composite_block.to_dag_cbor().unwrap())
            .await
            .unwrap();

        field_heads = vec![field_cid];
        composite_heads = vec![composite_cid];
        latest = Some((composite_cid, composite_block, payload));
    }

    let (latest_cid, latest_block, latest_payload) = latest.expect("chain is not empty");
    let (handler, outcome) = tokio::spawn(async move {
        let metadata = BlockMetadata::normal(
            &doc_id_str,
            "col-users",
            "did:key:z6MkrDeepCompositeReplay",
            None,
            false,
        );
        let outcome = handler
            .process_composite_delta(
                &latest_cid,
                &latest_block,
                &latest_payload,
                &metadata,
                false,
                0,
            )
            .await
            .unwrap();
        (handler, outcome)
    })
    .await
    .expect("composite merge task should not overflow its worker stack");
    assert_eq!(outcome, MergeOutcome::Merged);

    let stored = {
        let txn = handler.db().new_txn(true).await.unwrap();
        let stored = {
            let datastore = txn.datastore().unwrap();
            let systemstore = txn.systemstore().unwrap();
            collection
                .get_by_doc_id(&datastore, &systemstore, &doc_id)
                .await
                .unwrap()
                .expect("document should exist")
        };
        txn.force_discard().unwrap();
        stored
    };
    assert_eq!(
        stored.get("name"),
        Some(&NormalValue::String("name-257".to_string()))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deep_collection_parent_chain_merges_on_worker_stack() {
    let (handler, blockstore, _bus) = make_handler_with_schema_and_bus().await;
    let mut heads = Vec::new();
    let mut latest = None;

    for priority in 1..=256 {
        let payload = CollectionDeltaPayload {
            schema_version_id: "v1".to_string(),
            priority,
        };
        let block = Block::new(CrdtDelta::Collection(payload.clone()), heads, vec![]);
        let cid = block.generate_cid().unwrap();
        blockstore
            .put(&cid, &block.to_dag_cbor().unwrap())
            .await
            .unwrap();
        heads = vec![cid];
        latest = Some((cid, block, payload));
    }

    let (latest_cid, latest_block, latest_payload) = latest.expect("chain is not empty");
    let (handler, outcome) = tokio::spawn(async move {
        let metadata = BlockMetadata::normal(
            "collection-chain",
            "col-users",
            "did:key:z6MkrDeepCollectionReplay",
            None,
            false,
        );
        let outcome = handler
            .process_collection_delta(&latest_cid, &latest_block, &latest_payload, &metadata, 0)
            .await
            .unwrap();
        (handler, outcome)
    })
    .await
    .expect("collection merge task should not overflow its worker stack");

    assert!(outcome.is_terminal_skip());
    assert_eq!(
        handler
            .merged_collections()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len(),
        256
    );
}

#[tokio::test]
async fn composite_access_denial_does_not_mark_unreadable_linked_counter_merged() {
    let (handler, blockstore) = make_handler_with_counter_schema().await;
    handler.set_kms(Arc::new(StubKms::access_denied()));

    let encryption = Encryption::new(b"wrong-key".to_vec());
    let encryption_cid = encryption.generate_cid().unwrap();
    let encryption_data = encryption.to_dag_cbor().unwrap();
    blockstore
        .put(&encryption_cid, &encryption_data)
        .await
        .unwrap();

    let field_payload = CounterDeltaPayload {
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
    let doc_id = db::block::builder::derive_doc_id(&composite_cid);

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
    let txn = handler.db().new_txn(true).await.unwrap();
    let systemstore = txn.systemstore().unwrap();
    let doc_short_id = db::docid::map::get_doc_ref(&systemstore, &doc_id)
        .await
        .unwrap()
        .unwrap()
        .doc_short_id;
    for owned_cid in [field_cid, encryption_cid] {
        assert_eq!(
            db::docid::map::get_doc_ids_for_block(&systemstore, &owned_cid.to_string(),)
                .await
                .unwrap(),
            vec![doc_id.clone()]
        );
    }
    let headstore = txn.headstore().unwrap();
    assert!(!headstore
        .has(&storage::keys::HeadstorePriorityKey::new(doc_short_id, 1, field_cid).bytes())
        .await
        .unwrap());
}

enum StubKmsResponse {
    Key([u8; 32]),
    AccessDenied,
    UnavailableThenKey([u8; 32]),
}

struct StubKms {
    response: StubKmsResponse,
    calls: std::sync::atomic::AtomicUsize,
}

impl StubKms {
    fn fixed_key(key: [u8; 32]) -> Self {
        Self {
            response: StubKmsResponse::Key(key),
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn access_denied() -> Self {
        Self {
            response: StubKmsResponse::AccessDenied,
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn unavailable_then_key(key: [u8; 32]) -> Self {
        Self {
            response: StubKmsResponse::UnavailableThenKey(key),
            calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl kms::KmsService for StubKms {
    async fn get_keys(
        &self,
        _: &kms::RequestContext,
        cids: &[kms::EncryptionCid],
    ) -> kms::Result<kms::KeyResults> {
        let (results, tx) = kms::KeyResults::new(cids.len().max(1));
        let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        match &self.response {
            StubKmsResponse::Key(key) => {
                for cid in cids {
                    let _ = tx.send(Ok((*cid, *key))).await;
                }
            }
            StubKmsResponse::AccessDenied => {
                let _ = tx
                    .send(Err(kms::Error::AccessDenied {
                        reason: "test policy denied".into(),
                    }))
                    .await;
            }
            StubKmsResponse::UnavailableThenKey(_) if call == 0 => {
                let _ = tx.send(Err(kms::Error::KeyUnavailable)).await;
            }
            StubKmsResponse::UnavailableThenKey(key) => {
                for cid in cids {
                    let _ = tx.send(Ok((*cid, *key))).await;
                }
            }
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
async fn composite_kms_unavailable_rolls_back_and_retries() {
    let (handler, blockstore, _bus) = make_handler_with_schema_and_bus().await;
    let key = [7u8; 32];
    handler.set_kms(Arc::new(StubKms::unavailable_then_key(key)));

    let mut plaintext = Vec::new();
    ciborium::into_writer(&NormalValue::String("Alice".to_string()), &mut plaintext).unwrap();
    let (ciphertext, _) =
        crypto::encryption::aes::encrypt_aes(&plaintext, &key, &[], true).unwrap();

    let encryption = Encryption::new(key.to_vec());
    let encryption_cid = encryption.generate_cid().unwrap();
    blockstore
        .put(&encryption_cid, &encryption.to_dag_cbor().unwrap())
        .await
        .unwrap();

    let field_block = Block {
        delta: CrdtDelta::Lww(LwwDeltaPayload {
            field_name: "name".to_string(),
            schema_version_id: "v1".to_string(),
            priority: 1,
            data: ciphertext,
        }),
        heads: None,
        links: None,
        encryption: Some(encryption_cid),
        signature: None,
    };
    let field_cid = field_block.generate_cid().unwrap();
    blockstore
        .put(&field_cid, &field_block.to_dag_cbor().unwrap())
        .await
        .unwrap();

    let composite_payload = CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority: 1,
        status: 0,
    };
    let composite_block = Block {
        delta: CrdtDelta::Composite(composite_payload.clone()),
        heads: None,
        links: Some(vec![DAGLink::new("name", field_cid)]),
        encryption: None,
        signature: None,
    };
    let composite_cid = composite_block.generate_cid().unwrap();
    let doc_id = db::block::builder::derive_doc_id(&composite_cid);
    let metadata = BlockMetadata::normal(
        &doc_id,
        "col-users",
        "did:key:z6MkrKmsTimeoutRetry",
        None,
        false,
    );

    let first = handler
        .process_composite_delta(
            &composite_cid,
            &composite_block,
            &composite_payload,
            &metadata,
            false,
            0,
        )
        .await;
    assert!(matches!(
        first,
        Err(MergeError::Kms(kms::Error::KeyUnavailable))
    ));

    {
        let txn = handler.db().new_txn(true).await.unwrap();
        let systemstore = txn.systemstore().unwrap();
        assert!(db::docid::map::get_doc_ref(&systemstore, &doc_id)
            .await
            .unwrap()
            .is_none());
    }

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

    let collection = handler
        .db()
        .find_collection_by_id("col-users")
        .unwrap()
        .unwrap();
    let txn = handler.db().new_txn(true).await.unwrap();
    let systemstore = txn.systemstore().unwrap();
    let doc_ref = db::docid::map::get_doc_ref(&systemstore, &doc_id)
        .await
        .unwrap()
        .expect("successful retry registers document identity");
    let datastore = txn.datastore().unwrap();
    let stored = collection
        .get_with_datastore_include_deleted(
            &datastore,
            doc_ref.doc_short_id,
            &DocID::from_string(&doc_id).unwrap(),
            false,
        )
        .await
        .unwrap()
        .expect("successful retry materializes document")
        .0;
    assert_eq!(
        stored.get("name"),
        Some(&NormalValue::String("Alice".to_string()))
    );
}

#[tokio::test]
async fn dek_prefetch_can_restart_after_completion() {
    let (handler, _blockstore) = make_handler();
    let kms = Arc::new(StubKms::unavailable_then_key([7u8; 32]));
    handler.set_kms(kms.clone());
    let enc_cid = Encryption::new(vec![7u8; 32]).generate_cid().unwrap();
    let metadata = BlockMetadata::normal(
        "bafyreic6n5r4s3gjg6wdfbts6ijx6hnqc6qkver5msv2qzuqchh2u6w6sm",
        "col-users",
        "did:key:z6MkrKmsPrefetchRetry",
        None,
        false,
    );

    for expected_calls in 1..=2 {
        handler.spawn_dek_prefetch(enc_cid, &metadata);
        timeout(Duration::from_secs(1), async {
            loop {
                let calls = kms.calls.load(std::sync::atomic::Ordering::SeqCst);
                let finished = !handler
                    .prefetched_dek_cids()
                    .lock()
                    .unwrap()
                    .contains(&enc_cid);
                if calls == expected_calls && finished {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("DEK prefetch should complete");
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

    handler.set_kms(Arc::new(StubKms::fixed_key(key)));

    let decrypted = handler
        .decrypt_block_data(&ciphertext, Some(&enc_cid), None)
        .await
        .expect("kms-keyed decryption should succeed");
    assert_eq!(decrypted, plaintext);
}

#[tokio::test]
async fn decrypt_block_data_treats_wrong_kms_key_as_retryable() {
    let (handler, _blockstore) = make_handler();
    let correct_key = [7u8; 32];
    let wrong_key = [8u8; 32];
    let plaintext = b"secret field value";
    let (ciphertext, _) =
        crypto::encryption::aes::encrypt_aes(plaintext, &correct_key, &[], true).unwrap();
    let enc_cid = Encryption::new(correct_key.to_vec())
        .generate_cid()
        .unwrap();
    handler.set_kms(Arc::new(StubKms::fixed_key(wrong_key)));

    let result = handler
        .decrypt_block_data(&ciphertext, Some(&enc_cid), None)
        .await;

    assert!(matches!(
        result,
        Err(MergeError::Kms(kms::Error::Crypto(_)))
    ));
}

#[tokio::test]
async fn decrypt_block_data_no_kms_no_cid_passthrough() {
    let (handler, _blockstore) = make_handler();
    let data = b"plaintext".to_vec();
    let out = handler.decrypt_block_data(&data, None, None).await.unwrap();
    assert_eq!(out, data);
}

async fn make_handler_with_unique_index_schema() -> (
    DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    Arc<DefraBlockstore<RegolithStore>>,
) {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store.clone()).unwrap());

    db.create_collection(
        CollectionVersion::new(
            "Sessions",
            "v1",
            "col-sessions",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "session_id", FieldKind::string()),
            ],
        )
        .with_index(schema::IndexDescription {
            name: "idx_session_id_unique".to_string(),
            id: 1,
            fields: vec![schema::IndexedFieldDescription {
                name: "session_id".to_string(),
                descending: false,
            }],
            unique: true,
            kind: None,
            auto_generated: false,
        }),
    )
    .await
    .unwrap();

    let blockstore = Arc::new(DefraBlockstore::new(store, false));
    let handler = DbMergeHandler::new(db, blockstore.clone());
    (handler, blockstore)
}

/// #1128 composed with #1126: a replicated composite merge that trips a
/// LIVE twin unique-index conflict (two distinct, both-alive documents
/// racing the same unique value) no longer classifies as
/// `MergeOutcome::Rejected`. #1126's merge-path index maintenance
/// (`on_document_create_merge`/`on_document_update_merge`) resolves this
/// deterministically — smallest docID wins the index entry — instead of
/// erroring, because a CRDT merge cannot preserve cross-replica
/// uniqueness and failing here wedged the document's forward history in
/// permanent retry on both replicas (#1111). Both documents persist;
/// the classification seam in `composite_persist.rs` remains as
/// defense-in-depth for the degenerate arms that `on_document_*_merge`
/// still errors on (see
/// `remote_composite_merge_with_corrupted_unique_index_entry_is_rejected`
/// below).
#[tokio::test]
async fn remote_composite_merge_with_unique_index_twin_conflict_merges_via_canonical_pick() {
    let (handler, blockstore) = make_handler_with_unique_index_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-sessions")
        .unwrap()
        .expect("sessions collection should exist");

    // Doc A: first writer of session_id="dup-session" — merges cleanly.
    let mut doc_a = Document::new();
    doc_a.set("name", NormalValue::String("Alice".to_string()));
    doc_a.set("session_id", NormalValue::String("dup-session".to_string()));
    let result_a = db::block::builder::build_blocks_from_document(&doc_a, "v1", &blockstore)
        .await
        .unwrap();
    let doc_a_id = DocID::from_string(&result_a.doc_id).unwrap();
    let metadata_a = BlockMetadata::normal(
        &result_a.doc_id,
        "col-sessions",
        "did:key:z6MkrSessionA",
        None,
        false,
    );
    let outcome_a = handler
        .handle_block(&result_a.cid, &result_a.block, metadata_a)
        .await
        .expect("first writer of the unique value should merge");
    assert_eq!(outcome_a, MergeOutcome::Merged);

    // Doc B: distinct document content (different name => different doc_id
    // and CID), but the SAME session_id — a genuine cross-document live
    // unique conflict, exactly what a replicated push can carry. #1126
    // converges this instead of rejecting it.
    let mut doc_b = Document::new();
    doc_b.set("name", NormalValue::String("Bob".to_string()));
    doc_b.set("session_id", NormalValue::String("dup-session".to_string()));
    let result_b = db::block::builder::build_blocks_from_document(&doc_b, "v1", &blockstore)
        .await
        .unwrap();
    let doc_b_id = DocID::from_string(&result_b.doc_id).unwrap();
    let metadata_b = BlockMetadata::normal(
        &result_b.doc_id,
        "col-sessions",
        "did:key:z6MkrSessionB",
        None,
        false,
    );
    let outcome_b = handler
        .handle_block(&result_b.cid, &result_b.block, metadata_b)
        .await;

    assert!(
        matches!(outcome_b, Ok(MergeOutcome::Merged)),
        "a live twin unique conflict on a replicated merge must converge via \
             #1126's canonical pick, not classify as Rejected: {:?}",
        outcome_b
    );

    handler
        .db()
        .materialize_collection("Sessions")
        .await
        .expect("reindexing must preserve the merge-time canonical unique winner");

    let txn = handler.db().new_txn(true).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();
    let index_manager =
        IndexManager::from_collection(collection.resolved_root_id(), collection.schema()).unwrap();
    let mut entries = index_manager
        .get_index("idx_session_id_unique")
        .unwrap()
        .get(
            &datastore,
            &[NormalValue::String("dup-session".to_string())],
        )
        .await
        .unwrap();
    let entries = entries.collect_all().await.unwrap();
    assert_eq!(entries.len(), 1);
    let indexed_doc_id = db::docid::map::get_doc_id(&systemstore, entries[0].doc_short_id)
        .await
        .unwrap();
    assert_eq!(
        indexed_doc_id,
        Some(std::cmp::min(doc_a_id.to_string(), doc_b_id.to_string()))
    );

    // Both documents persist — the CRDT merge never drops data, even
    // though only the deterministic winner keeps the index entry.
    assert!(
        read_session_doc(&handler, &collection, &doc_a_id)
            .await
            .is_some(),
        "doc A must remain readable after the twin conflict converges"
    );
    assert!(
        read_session_doc(&handler, &collection, &doc_b_id)
            .await
            .is_some(),
        "doc B must remain readable after the twin conflict converges"
    );
}

/// #1128 composed with #1126: #1126's merge-path resolution heals the
/// common live-twin case (above), but its degenerate arms — reached when
/// `UniqueIndex::conflicting_doc_id` cannot identify a holder for a key
/// it just observed as present — still propagate
/// `storage::corekv::Error::UniqueConstraintViolation` unchanged. That is
/// internal index-state inconsistency (damaged data), not a live
/// conflict a deterministic pick can resolve, so it must still classify
/// as `MergeOutcome::Rejected` rather than retrying forever. This test
/// exercises the code path end-to-end (the merge disposition is real
/// production code), but the precondition (key-present/value-empty unique
/// entry) is synthetic corruption not producible by current write paths;
/// the test proves the seam exists, not that the state occurs in production.
#[tokio::test]
async fn remote_composite_merge_with_corrupted_unique_index_entry_is_rejected() {
    let (handler, blockstore) = make_handler_with_unique_index_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-sessions")
        .unwrap()
        .expect("sessions collection should exist");

    corrupt_unique_index_entry(&handler, &collection, "dup-session").await;

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("session_id", NormalValue::String("dup-session".to_string()));
    let result = db::block::builder::build_blocks_from_document(&doc, "v1", &blockstore)
        .await
        .unwrap();
    let metadata = BlockMetadata::normal(
        &result.doc_id,
        "col-sessions",
        "did:key:z6MkrSessionA",
        None,
        false,
    );
    let outcome = handler
        .handle_block(&result.cid, &result.block, metadata)
        .await;

    match outcome {
        Ok(MergeOutcome::Rejected { reason }) => {
            assert!(
                !reason.is_empty(),
                "rejection reason should carry the typed violation detail"
            );
        }
        other => panic!(
            "a corrupted unique-index entry (degenerate arm, #1126 cannot heal) \
                 must classify as Ok(MergeOutcome::Rejected), not {:?}",
            other
        ),
    }
}

// The pure discrimination between a deterministic unique violation and
// any other `db::index::Error` (including other storage failures, which
// must keep surfacing as `Err` so the caller retries) is independent of
// collection/store state, so it is additionally covered directly at the
// classification seam: see `composite_persist::classify_tests`
// (`non_unique_storage_error_is_not_classified`,
// `non_storage_index_error_is_not_classified`).

async fn build_session_merge_block(
    blockstore: &Arc<DefraBlockstore<RegolithStore>>,
    name: &str,
    session_id: &str,
) -> (MergeBlock, DocID) {
    let mut doc = Document::new();
    doc.set("name", NormalValue::String(name.to_string()));
    doc.set("session_id", NormalValue::String(session_id.to_string()));
    let result = db::block::builder::build_blocks_from_document(&doc, "v1", blockstore)
        .await
        .unwrap();
    let doc_id = DocID::from_string(&result.doc_id).unwrap();
    let merge_block = MergeBlock {
        cid: result.cid,
        block_data: result.block,
        doc_id: result.doc_id,
        collection_id: "col-sessions".to_string(),
        creator: format!("did:key:z6MkrSession{name}"),
        sender_peer: Some("peer1".to_string()),
        is_explicit_replicator: false,
        explicit_replay_authorization: None,
        verified_creator: None,
    };
    (merge_block, doc_id)
}

async fn read_session_doc(
    handler: &DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    collection: &Collection,
    doc_id: &DocID,
) -> Option<Document> {
    let txn = handler.db().new_txn(true).await.unwrap();
    let doc = {
        let datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();
        collection
            .get_by_doc_id(&datastore, &systemstore, doc_id)
            .await
            .unwrap()
    };
    txn.force_discard().unwrap();
    doc
}

/// Corrupt the `idx_session_id_unique` unique index (id=1, see
/// `make_handler_with_unique_index_schema`) with an entry whose key is
/// present but whose value is empty. `UniqueIndex::save()` treats
/// key-presence alone as a violation regardless of value content, while
/// `conflicting_doc_id()` only reports a holder for a non-empty value —
/// so a merge that targets `session_id` afterward trips the degenerate
/// arm in `save_healing_stale_unique`/`save_resolving_unique_conflict`
/// (#1126) that #1128's classification seam still converts to
/// `MergeOutcome::Rejected`, rather than the live-twin case #1126 now
/// resolves deterministically.
async fn corrupt_unique_index_entry(
    handler: &DbMergeHandler<RegolithStore, DefraBlockstore<RegolithStore>>,
    collection: &Collection,
    session_id: &str,
) {
    let short_id = collection.resolved_root_id();
    let corrupted_key = storage::keys::IndexDataStoreKey::new(
        short_id,
        1,
        vec![storage::keys::IndexedField::new(
            NormalValue::String(session_id.to_string()),
            false,
        )],
    )
    .try_bytes()
    .unwrap();
    let write_txn = handler.db().new_txn(false).await.unwrap();
    {
        let datastore = write_txn.datastore().unwrap();
        datastore.set(&corrupted_key, &[]).await.unwrap();
    }
    write_txn.commit().await.unwrap();
}

/// Batch path of the #1128 classification: a unique-index violation is
/// detected AFTER `persist_merged_document` has staged the doc's field
/// data in the SHARED batch txn, so a `Rejected` outcome must poison the
/// whole batch attempt (discard + per-block fallback). The rejected doc's
/// raw field data must NOT be committed (it would be queryable but
/// un-indexed), while the valid sibling in the same batch still merges.
///
/// #1128 composed with #1126: a live twin (two documents racing the same
/// value) no longer rejects — it merges via #1126's canonical pick (see
/// `remote_composite_merge_with_unique_index_twin_conflict_merges_via_canonical_pick`).
/// The poison/fallback mechanics this test guards are independent of
/// *why* a block rejects, so the violating block here is manufactured
/// via the still-live degenerate arm (a corrupted, holder-less unique
/// index entry) rather than a twin conflict.
#[tokio::test]
async fn batch_merge_rejects_unique_violation_without_partial_write() {
    let (handler, blockstore) = make_handler_with_unique_index_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-sessions")
        .unwrap()
        .expect("sessions collection should exist");

    corrupt_unique_index_entry(&handler, &collection, "ghost-session").await;

    // Batch: [valid sibling, violating block targeting the corrupted entry].
    let (block_valid, valid_id) =
        build_session_merge_block(&blockstore, "Carol", "other-session").await;
    let (block_violating, violating_id) =
        build_session_merge_block(&blockstore, "Bob", "ghost-session").await;

    let results = handler
        .handle_block_batch(&[block_valid, block_violating])
        .await;
    assert!(
        matches!(results[0], Ok(MergeOutcome::Merged)),
        "valid sibling block must merge, got {:?}",
        results[0]
    );
    assert!(
        matches!(results[1], Ok(MergeOutcome::Rejected { .. })),
        "unique-violating batch block must classify as Rejected, got {:?}",
        results[1]
    );

    // No partial write: the rejected doc's field data must not be readable.
    assert!(
        read_session_doc(&handler, &collection, &violating_id)
            .await
            .is_none(),
        "rejected doc's field data must NOT survive the batch txn"
    );
    assert!(
        read_session_doc(&handler, &collection, &valid_id)
            .await
            .is_some(),
        "valid sibling must be committed"
    );
}

/// Ordering contrast for the poison-then-fallback flow: the violating
/// block comes FIRST, so the batch attempt is poisoned before the valid
/// sibling is processed. The fallback must still merge the sibling and
/// keep result order aligned with the input blocks.
///
/// #1128 composed with #1126: as above, the violating block is
/// manufactured via the still-live degenerate arm (corrupted,
/// holder-less unique index entry) since a live twin now merges instead
/// of rejecting.
#[tokio::test]
async fn batch_merge_unique_violation_first_still_merges_valid_sibling() {
    let (handler, blockstore) = make_handler_with_unique_index_schema().await;
    let collection = handler
        .db()
        .find_collection_by_id("col-sessions")
        .unwrap()
        .expect("sessions collection should exist");

    corrupt_unique_index_entry(&handler, &collection, "ghost-session").await;

    // Batch: [violating block targeting the corrupted entry, valid sibling].
    let (block_violating, violating_id) =
        build_session_merge_block(&blockstore, "Bob", "ghost-session").await;
    let (block_valid, valid_id) =
        build_session_merge_block(&blockstore, "Carol", "other-session").await;

    let results = handler
        .handle_block_batch(&[block_violating, block_valid])
        .await;
    assert!(
        matches!(results[0], Ok(MergeOutcome::Rejected { .. })),
        "unique-violating batch block must classify as Rejected, got {:?}",
        results[0]
    );
    assert!(
        matches!(results[1], Ok(MergeOutcome::Merged)),
        "valid sibling after a poisoning rejection must still merge, got {:?}",
        results[1]
    );

    assert!(
        read_session_doc(&handler, &collection, &violating_id)
            .await
            .is_none(),
        "rejected doc's field data must NOT survive the batch txn"
    );
    assert!(
        read_session_doc(&handler, &collection, &valid_id)
            .await
            .is_some(),
        "valid sibling must be committed"
    );
}

/// Ownership recorded by an earlier block in a shared batch transaction must
/// be visible when the next composite resolves its identity. Neither block is
/// placed in the blockstore: the child can resolve only through the parent's
/// staged owner mapping, not by opening a separate read transaction and
/// walking the parent block.
#[tokio::test]
async fn batch_composite_identity_observes_staged_parent_ownership() {
    let (handler, _blockstore, _bus) = make_handler_with_schema_and_bus().await;

    let payload = |priority| CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority,
        status: 1,
    };
    let genesis = Block::new(CrdtDelta::Composite(payload(1)), vec![], vec![]);
    let genesis_cid = genesis.generate_cid().unwrap();
    let doc_id = db::block::builder::derive_doc_id(&genesis_cid);
    let child = Block::new(CrdtDelta::Composite(payload(2)), vec![genesis_cid], vec![]);
    let child_cid = child.generate_cid().unwrap();

    let blocks = [
        composite_merge_block(genesis_cid, &genesis, &doc_id),
        composite_merge_block(child_cid, &child, &doc_id),
    ];
    let results = handler
        .try_batch_merge(&blocks)
        .await
        .expect("child identity must observe the genesis owner staged by the same batch");

    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|result| matches!(result, Ok(MergeOutcome::Merged))),
        "both staged parent and child must merge: {results:?}"
    );
}

/// A batch ancestry replay resolves the root identity once and carries it
/// through every explicit merge frame. Counting blockstore reads makes the
/// complexity regression deterministic: resolving every ancestor afresh costs
/// O(N^2), while one identity walk plus one merge walk is O(N).
#[tokio::test]
async fn batch_composite_identity_walk_is_linear_in_ancestry_depth() {
    let (handler, inner_blockstore, counting_blockstore) =
        make_counting_handler_with_schema().await;

    const DEPTH: usize = 64;
    let mut parent_cid = None;
    let mut genesis_cid = None;
    let mut tip = None;

    for priority in 1..=DEPTH as u64 {
        let block = Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "v1".to_string(),
                priority,
                status: 1,
            }),
            parent_cid.into_iter().collect(),
            vec![],
        );
        let cid = block.generate_cid().unwrap();
        inner_blockstore
            .put(&cid, &block.to_dag_cbor().unwrap())
            .await
            .unwrap();
        genesis_cid.get_or_insert(cid);
        parent_cid = Some(cid);
        tip = Some((cid, block));
    }

    let (tip_cid, tip_block) = tip.expect("chain has a tip");
    let doc_id = db::block::builder::derive_doc_id(&genesis_cid.expect("chain has a genesis"));
    let results = handler
        .try_batch_merge(&[composite_merge_block(tip_cid, &tip_block, &doc_id)])
        .await
        .unwrap();
    assert!(matches!(results.as_slice(), [Ok(MergeOutcome::Merged)]));

    let get_count = counting_blockstore.get_count();
    assert!(
        get_count <= DEPTH * 3,
        "depth-{DEPTH} replay performed {get_count} block reads; expected linear work"
    );
}

#[test]
fn merge_depth_policy_accepts_last_supported_depth_and_rejects_limit() {
    let (handler, _) = make_handler();
    let block = Block::new(
        CrdtDelta::Composite(CompositeDeltaPayload {
            schema_version_id: "v1".to_string(),
            priority: 1,
            status: 1,
        }),
        vec![],
        vec![],
    );
    let cid = block.generate_cid().unwrap();

    assert!(handler
        .ensure_merge_depth(&cid, DEFAULT_MAX_MERGE_DEPTH - 1)
        .is_ok());
    assert!(matches!(
        handler.ensure_merge_depth(&cid, DEFAULT_MAX_MERGE_DEPTH),
        Err(MergeError::DepthExceeded {
            cid: error_cid,
            depth: DEFAULT_MAX_MERGE_DEPTH,
        }) if error_cid == cid
    ));

    let custom_handler = DbMergeHandler::new_with_max_merge_depth(
        handler.db().clone(),
        handler.blockstore().clone(),
        17,
    );
    assert!(custom_handler.ensure_merge_depth(&cid, 16).is_ok());
    assert!(matches!(
        custom_handler.ensure_merge_depth(&cid, 17),
        Err(MergeError::DepthExceeded { depth: 17, .. })
    ));
}

#[tokio::test]
async fn composite_identity_uses_merge_depth_policy() {
    let (handler, blockstore) = make_handler();
    let handler = DbMergeHandler::new_with_max_merge_depth(
        handler.db().clone(),
        handler.blockstore().clone(),
        17,
    );
    let payload = |priority| CompositeDeltaPayload {
        schema_version_id: "v1".to_string(),
        priority,
        status: 1,
    };
    let genesis = Block::new(CrdtDelta::Composite(payload(1)), vec![], vec![]);
    let genesis_cid = genesis.generate_cid().unwrap();
    blockstore
        .put(&genesis_cid, &genesis.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let child = Block::new(CrdtDelta::Composite(payload(2)), vec![genesis_cid], vec![]);
    let child_cid = child.generate_cid().unwrap();

    assert!(matches!(
        handler
            .resolve_composite_doc_id(&child_cid, &child, 16)
            .await,
        Err(MergeError::DepthExceeded {
            cid: error_cid,
            depth: 17,
        }) if error_cid == genesis_cid
    ));
}

/// Regression: resolving a composite's DocID walks its full ancestry, and
/// that ancestry is as deep as the document's update history. The recursive
/// predecessor of `resolve_composite_doc_id_inner` burned one async frame
/// per ancestor and overflowed small thread stacks (iOS FFI workers
/// crash-looped on launch merging long-lived documents). This drives the
/// resolver over a 4096-deep ownerless chain on a deliberately small
/// (512 KiB) stack: the iterative walk succeeds where recursion dies.
#[test]
fn resolve_composite_doc_id_walks_deep_ancestry_on_a_small_stack() {
    std::thread::Builder::new()
        .name("small-stack-resolver".to_string())
        .stack_size(512 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let (handler, blockstore) = make_handler();

                const DEPTH: usize = 4096;
                let genesis = Block::new(
                    CrdtDelta::Composite(CompositeDeltaPayload {
                        schema_version_id: "v1".to_string(),
                        priority: 1,
                        status: 1,
                    }),
                    vec![],
                    vec![],
                );
                let genesis_cid = genesis.generate_cid().unwrap();
                blockstore
                    .put(&genesis_cid, &genesis.to_dag_cbor().unwrap())
                    .await
                    .unwrap();

                let mut parent_cid = genesis_cid;
                let mut tip_block = genesis;
                for priority in 2..(DEPTH as u64 + 2) {
                    let block = Block::new(
                        CrdtDelta::Composite(CompositeDeltaPayload {
                            schema_version_id: "v1".to_string(),
                            priority,
                            status: 1,
                        }),
                        vec![parent_cid],
                        vec![],
                    );
                    let cid = block.generate_cid().unwrap();
                    blockstore
                        .put(&cid, &block.to_dag_cbor().unwrap())
                        .await
                        .unwrap();
                    parent_cid = cid;
                    tip_block = block;
                }

                // No owner-index entries anywhere: the resolver must walk
                // the entire chain to the genesis and derive from its CID.
                let resolved = handler
                    .resolve_composite_doc_id(&parent_cid, &tip_block, 0)
                    .await
                    .expect("deep ancestry must resolve without overflowing the stack");
                assert_eq!(resolved, db::block::builder::derive_doc_id(&genesis_cid));
            });
        })
        .unwrap()
        .join()
        .expect("small-stack resolver thread must not crash");
}

/// Regression (PR #1316 review): later heads must not be probed until the
/// first head's subtree is exhausted. The tip has heads `[first, later]`
/// where `first` is a reachable genesis and `later` carries a unique owner
/// entry for a DIFFERENT document — eager sibling probing would return the
/// later head's owner; first-head-first DFS (recursive parity) must resolve
/// through `first`'s genesis.
#[tokio::test]
async fn resolve_composite_doc_id_explores_first_head_before_probing_later_siblings() {
    let (handler, blockstore) = make_handler();

    let make_composite = |priority: u64, heads: Vec<Cid>| {
        Block::new(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "v1".to_string(),
                priority,
                status: 1,
            }),
            heads,
            vec![],
        )
    };

    let genesis_a = make_composite(1, vec![]);
    let genesis_a_cid = genesis_a.generate_cid().unwrap();
    blockstore
        .put(&genesis_a_cid, &genesis_a.to_dag_cbor().unwrap())
        .await
        .unwrap();

    // A distinct composite so its CID differs from the other genesis.
    let genesis_b = make_composite(2, vec![]);
    let genesis_b_cid = genesis_b.generate_cid().unwrap();
    blockstore
        .put(&genesis_b_cid, &genesis_b.to_dag_cbor().unwrap())
        .await
        .unwrap();

    // Block::new sorts heads lexicographically by CID string, so which
    // genesis is the FIRST head is decided by the stored order, not
    // construction order — read it back and orient the fixture on it.
    let tip = make_composite(3, vec![genesis_a_cid, genesis_b_cid]);
    let tip_cid = tip.generate_cid().unwrap();
    blockstore
        .put(&tip_cid, &tip.to_dag_cbor().unwrap())
        .await
        .unwrap();
    let stored_heads = tip.heads.clone().expect("tip has two heads");
    let first_head_cid = stored_heads[0];
    let later_head_cid = stored_heads[1];

    // Register a unique owner for the LATER head only: the wrong answer if
    // sibling probing happens before the first subtree is explored.
    register_test_block_owner(&handler, 7, "doc-of-later-head", &later_head_cid).await;

    let resolved = handler
        .resolve_composite_doc_id(&tip_cid, &tip, 0)
        .await
        .unwrap();
    assert_eq!(
        resolved,
        db::block::builder::derive_doc_id(&first_head_cid),
        "the first head's subtree must resolve before any later sibling is probed"
    );
}
