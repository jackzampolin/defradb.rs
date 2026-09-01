use cid::Cid;
use crypto::PrivateKey as _;
use db::merge::browser_sync::BrowserSyncEngine;
use db::merge::browser_sync::BrowserSyncError;
use defra_core::block::generate_cid_from_bytes;
use defra_core::browser_sync::BrowserSyncBlock;
use defra_core::browser_sync::BrowserSyncDocument;
use defra_core::browser_sync::MAX_SYNC_BLOCKS_PER_DOCUMENT;
use defra_core::browser_sync::MAX_SYNC_ROOTS_PER_DOCUMENT;
use defra_core::Block;
use defra_core::CrdtDelta;
use defra_core::Signature;
use defra_core::SignatureHeader;
use defra_core::SignatureType;
use document::DocID;
use document::Document;
use document::NormalValue;
use query::mutator::DocMutator;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::sync::Arc;
use storage::RegolithStore;

fn users_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "users-version",
        "users-collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

fn admins_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Admins",
        "admins-version",
        "admins-collection",
        vec![
            FieldDescription::new("10", "_docID", FieldKind::doc_id()),
            FieldDescription::new("11", "name", FieldKind::string()),
        ],
    )
}

fn decode_wire_block(document: &BrowserSyncDocument, cid: &str) -> Block {
    let block = document
        .blocks
        .iter()
        .find(|block| block.cid == cid)
        .expect("wire block is present");
    Block::from_dag_cbor(&hex::decode(&block.data).unwrap()).unwrap()
}

fn replace_wire_block(document: &mut BrowserSyncDocument, old_cid: &str, block: &Block) -> Cid {
    let data = block.to_dag_cbor().unwrap();
    let cid = generate_cid_from_bytes(&data).unwrap();
    let wire_block = document
        .blocks
        .iter_mut()
        .find(|block| block.cid == old_cid)
        .expect("wire block is present");
    wire_block.cid = cid.to_string();
    wire_block.data = hex::encode(data);
    cid
}

#[test]
fn validation_distinguishes_empty_and_over_limit_counts() {
    let database = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let sync = BrowserSyncEngine::new(database);
    let block = BrowserSyncBlock {
        cid: "cid".into(),
        data: "data".into(),
    };

    let empty_roots = BrowserSyncDocument {
        doc_id: "doc".into(),
        collection_id: "collection".into(),
        roots: Vec::new(),
        blocks: vec![block.clone()],
    };
    assert!(matches!(
        sync.validate_document(&empty_roots),
        Err(BrowserSyncError::Invalid(_))
    ));

    let too_many_roots = BrowserSyncDocument {
        roots: vec!["root".into(); MAX_SYNC_ROOTS_PER_DOCUMENT + 1],
        ..empty_roots.clone()
    };
    assert!(matches!(
        sync.validate_document(&too_many_roots),
        Err(BrowserSyncError::TooLarge(_))
    ));

    let too_many_blocks = BrowserSyncDocument {
        roots: vec!["root".into()],
        blocks: vec![block; MAX_SYNC_BLOCKS_PER_DOCUMENT + 1],
        ..empty_roots
    };
    assert!(matches!(
        sync.validate_document(&too_many_blocks),
        Err(BrowserSyncError::TooLarge(_))
    ));
}

#[tokio::test]
async fn document_round_trip_uses_crdt_blocks() {
    let source = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let target = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    source.create_collection(users_schema()).await.unwrap();
    target.create_collection(users_schema()).await.unwrap();

    let mut document = Document::new();
    document.set("name", "Alice");
    let created = db::AutoCommitMutator::new(source.clone())
        .create("Users", document)
        .await
        .unwrap();
    let doc_id = created.doc_id.to_string();

    let source_sync = BrowserSyncEngine::new(source);
    let document_ref = source_sync.document_ref(&doc_id).await.unwrap().unwrap();
    let wire_document = source_sync
        .load_document(&document_ref)
        .await
        .unwrap()
        .unwrap();

    let target_sync = BrowserSyncEngine::new(target.clone());
    target_sync
        .apply_document(&wire_document, "browser")
        .await
        .unwrap();

    let txn = target.new_txn(true).await.unwrap();
    let collection = target.get_collection("Users").unwrap().unwrap();
    let stored = collection
        .get_by_doc_id(
            &txn.datastore().unwrap(),
            &txn.systemstore().unwrap(),
            &DocID::from_string(&doc_id).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.get("name"),
        Some(&NormalValue::String("Alice".into()))
    );
}

#[tokio::test]
async fn rejects_forged_document_id_before_merge() {
    let source = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    source.create_collection(users_schema()).await.unwrap();
    let mut document = Document::new();
    document.set("name", "Alice");
    let created = db::AutoCommitMutator::new(source.clone())
        .create("Users", document)
        .await
        .unwrap();
    let doc_id = created.doc_id.to_string();
    let sync = BrowserSyncEngine::new(source);
    let document_ref = sync.document_ref(&doc_id).await.unwrap().unwrap();
    let mut wire_document = sync.load_document(&document_ref).await.unwrap().unwrap();
    wire_document.doc_id = "bae-forged".into();

    let error = sync
        .apply_document(&wire_document, "browser")
        .await
        .unwrap_err();
    assert!(matches!(error, BrowserSyncError::Invalid(_)));
}

#[tokio::test]
async fn alias_lookup_uses_canonical_document_id() {
    let database = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    database.create_collection(users_schema()).await.unwrap();
    let mut document = Document::new();
    document.set("name", "Alice");
    let created = db::AutoCommitMutator::new(database.clone())
        .create("Users", document)
        .await
        .unwrap();
    let canonical_doc_id = created.doc_id.to_string();
    let sync = BrowserSyncEngine::new(database);
    let canonical_ref = sync.document_ref(&canonical_doc_id).await.unwrap().unwrap();

    let txn = sync.database().new_txn(false).await.unwrap();
    db::docid::map::set_doc_id_alias(
        &txn.systemstore().unwrap(),
        sync.database()
            .get_collection("Users")
            .unwrap()
            .unwrap()
            .resolved_root_id(),
        canonical_ref.doc_short_id(),
        "legacy-user-id",
    )
    .await
    .unwrap();
    txn.commit().await.unwrap();

    let alias_ref = sync.document_ref("legacy-user-id").await.unwrap().unwrap();
    assert_eq!(alias_ref.doc_id, canonical_doc_id);
    assert!(sync.load_document(&alias_ref).await.unwrap().is_some());
}

#[tokio::test]
async fn rejects_field_block_from_another_collection() {
    let database = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    database.create_collection(users_schema()).await.unwrap();
    database.create_collection(admins_schema()).await.unwrap();
    let mut document = Document::new();
    document.set("name", "Alice");
    let created = db::AutoCommitMutator::new(database.clone())
        .create("Users", document)
        .await
        .unwrap();
    let sync = BrowserSyncEngine::new(database);
    let document_ref = sync
        .document_ref(&created.doc_id.to_string())
        .await
        .unwrap()
        .unwrap();
    let mut wire_document = sync.load_document(&document_ref).await.unwrap().unwrap();

    let old_root = wire_document.roots[0].clone();
    let mut root = decode_wire_block(&wire_document, &old_root);
    let old_field = root.links.as_ref().unwrap()[0].link.to_string();
    let mut field = decode_wire_block(&wire_document, &old_field);
    match &mut field.delta {
        CrdtDelta::Lww(payload) => payload.schema_version_id = "admins-version".into(),
        CrdtDelta::Counter(payload) => payload.schema_version_id = "admins-version".into(),
        _ => panic!("document field block has an unexpected delta"),
    }
    let field_cid = replace_wire_block(&mut wire_document, &old_field, &field);
    root.links.as_mut().unwrap()[0].link = field_cid;
    let root_cid = replace_wire_block(&mut wire_document, &old_root, &root);
    wire_document.roots[0] = root_cid.to_string();
    wire_document.doc_id = db::block::builder::derive_doc_id(&root_cid);

    let Err(error) = sync.validate_document(&wire_document) else {
        panic!("cross-collection field block was accepted");
    };
    assert!(error.to_string().contains("belongs to collection"));
}

#[tokio::test]
async fn rejects_field_link_name_mismatch() {
    let database = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    database.create_collection(users_schema()).await.unwrap();
    let mut document = Document::new();
    document.set("name", "Alice");
    let created = db::AutoCommitMutator::new(database.clone())
        .create("Users", document)
        .await
        .unwrap();
    let sync = BrowserSyncEngine::new(database);
    let document_ref = sync
        .document_ref(&created.doc_id.to_string())
        .await
        .unwrap()
        .unwrap();
    let mut wire_document = sync.load_document(&document_ref).await.unwrap().unwrap();

    let old_root = wire_document.roots[0].clone();
    let mut root = decode_wire_block(&wire_document, &old_root);
    root.links.as_mut().unwrap()[0].name = "email".into();
    let root_cid = replace_wire_block(&mut wire_document, &old_root, &root);
    wire_document.roots[0] = root_cid.to_string();
    wire_document.doc_id = db::block::builder::derive_doc_id(&root_cid);

    let Err(error) = sync.validate_document(&wire_document) else {
        panic!("mismatched field link was accepted");
    };
    assert!(error.to_string().contains("links field 'name' as 'email'"));
}

#[tokio::test]
async fn validation_accepts_reachable_signature_blocks() {
    let database = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    database.create_collection(users_schema()).await.unwrap();
    let mut document = Document::new();
    document.set("name", "Alice");
    let created = db::AutoCommitMutator::new(database.clone())
        .create("Users", document)
        .await
        .unwrap();
    let sync = BrowserSyncEngine::new(database);
    let document_ref = sync
        .document_ref(&created.doc_id.to_string())
        .await
        .unwrap()
        .unwrap();
    let mut wire_document = sync.load_document(&document_ref).await.unwrap().unwrap();
    let signer_did = sign_genesis(&mut wire_document);

    let validated = sync.validate_document(&wire_document).unwrap();
    assert_eq!(
        validated.verified_genesis_creator(),
        Some(signer_did.as_str()),
        "validation must surface the verified genesis signer"
    );
}

#[tokio::test]
async fn validation_rejects_forged_genesis_signature() {
    let database = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    database.create_collection(users_schema()).await.unwrap();
    let mut document = Document::new();
    document.set("name", "Alice");
    let created = db::AutoCommitMutator::new(database.clone())
        .create("Users", document)
        .await
        .unwrap();
    let sync = BrowserSyncEngine::new(database);
    let document_ref = sync
        .document_ref(&created.doc_id.to_string())
        .await
        .unwrap()
        .unwrap();
    let mut wire_document = sync.load_document(&document_ref).await.unwrap().unwrap();

    let old_root = wire_document.roots[0].clone();
    let root = wire_document
        .blocks
        .iter_mut()
        .find(|block| block.cid == old_root)
        .unwrap();
    let mut decoded_root = Block::from_dag_cbor(&hex::decode(&root.data).unwrap()).unwrap();
    let signature = Signature::new(SignatureHeader::new(SignatureType::EdDSA, vec![1]), vec![2]);
    let signature_data = signature.to_dag_cbor().unwrap();
    let signature_cid = generate_cid_from_bytes(&signature_data).unwrap();
    decoded_root.signature = Some(signature_cid);
    let root_data = decoded_root.to_dag_cbor().unwrap();
    let root_cid = generate_cid_from_bytes(&root_data).unwrap();
    root.cid = root_cid.to_string();
    root.data = hex::encode(root_data);
    wire_document.roots[0] = root_cid.to_string();
    wire_document.doc_id = db::block::builder::derive_doc_id(&root_cid);
    wire_document.blocks.push(BrowserSyncBlock {
        cid: signature_cid.to_string(),
        data: hex::encode(signature_data),
    });

    let error = sync
        .validate_document(&wire_document)
        .err()
        .expect("an unverifiable genesis signature must reject the push");
    assert!(matches!(error, BrowserSyncError::Invalid(_)));
}

/// Re-sign a wire document's genesis with a fresh key, as a device authoring
/// its own commits does with a key the node never holds. Returns the DID the
/// node should verify from that signature.
fn sign_genesis(wire_document: &mut BrowserSyncDocument) -> String {
    let old_root = wire_document.roots[0].clone();
    let root = wire_document
        .blocks
        .iter_mut()
        .find(|block| block.cid == old_root)
        .unwrap();
    let mut decoded_root = Block::from_dag_cbor(&hex::decode(&root.data).unwrap()).unwrap();
    let private_key = crypto::generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let signer_did = public_key.did().unwrap();
    let signed_bytes = decoded_root.to_dag_cbor().unwrap();
    let signature = Signature::new(
        SignatureHeader::new(
            SignatureType::EdDSA,
            hex::encode(public_key.raw()).into_bytes(),
        ),
        private_key.sign(&signed_bytes).unwrap(),
    );
    let signature_data = signature.to_dag_cbor().unwrap();
    let signature_cid = generate_cid_from_bytes(&signature_data).unwrap();
    decoded_root.signature = Some(signature_cid);
    let root_data = decoded_root.to_dag_cbor().unwrap();
    let root_cid = generate_cid_from_bytes(&root_data).unwrap();
    root.cid = root_cid.to_string();
    root.data = hex::encode(root_data);
    wire_document.roots[0] = root_cid.to_string();
    wire_document.doc_id = db::block::builder::derive_doc_id(&root_cid);
    wire_document.blocks.push(BrowserSyncBlock {
        cid: signature_cid.to_string(),
        data: hex::encode(signature_data),
    });
    signer_did
}

/// `TxnBroadcaster` test double: captures every event it is handed.
struct CapturingBroadcaster {
    events: Arc<std::sync::Mutex<Vec<db::event::emission::TxnBroadcastEvent>>>,
}

#[async_trait::async_trait]
impl db::event::emission::TxnBroadcaster for CapturingBroadcaster {
    async fn broadcast_update(&self, event: db::event::emission::TxnBroadcastEvent) {
        self.events.lock().unwrap().push(event);
    }
}

/// A wire document from `source`, its genesis signed by a key neither node
/// holds. `None` leaves the node's own signature in place.
async fn pushable_document(
    source: Arc<db::DB<RegolithStore>>,
    signed: bool,
) -> (BrowserSyncDocument, Option<String>) {
    let mut document = Document::new();
    document.set("name", "Alice");
    let created = db::AutoCommitMutator::new(source.clone())
        .create("Users", document)
        .await
        .unwrap();
    let sync = BrowserSyncEngine::new(source);
    let document_ref = sync
        .document_ref(&created.doc_id.to_string())
        .await
        .unwrap()
        .unwrap();
    let mut wire_document = sync.load_document(&document_ref).await.unwrap().unwrap();
    let signer_did = signed.then(|| sign_genesis(&mut wire_document));
    (wire_document, signer_did)
}

async fn apply_and_capture(
    signed: bool,
) -> (
    BrowserSyncDocument,
    Option<String>,
    Vec<db::event::emission::TxnBroadcastEvent>,
) {
    let source = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let target = Arc::new(db::DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    source.create_collection(users_schema()).await.unwrap();
    target.create_collection(users_schema()).await.unwrap();

    let (wire_document, signer_did) = pushable_document(source, signed).await;

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let broadcaster: Arc<dyn db::event::emission::TxnBroadcaster> =
        Arc::new(CapturingBroadcaster {
            events: events.clone(),
        });
    BrowserSyncEngine::with_broadcaster(target, broadcaster)
        .apply_document(&wire_document, "the-pusher")
        .await
        .unwrap();

    let captured = events.lock().unwrap().drain(..).collect();
    (wire_document, signer_did, captured)
}

#[tokio::test]
async fn a_merged_fragment_is_announced_to_peers() {
    // A device-signed document leaves the node it was pushed to only if that
    // node announces what it merged: nothing else on this path talks to a
    // replicator or a gossip topic.
    let (wire_document, signer_did, events) = apply_and_capture(true).await;

    assert_eq!(events.len(), 1, "a merged fragment must be announced once");
    let event = &events[0];
    assert_eq!(event.doc_id, wire_document.doc_id);
    assert_eq!(event.collection_id, wire_document.collection_id);
    assert_eq!(
        event.doc_cid.to_string(),
        wire_document.roots[0],
        "peers must be announced the root that was merged"
    );
    assert_eq!(
        event.creator_did.as_deref(),
        signer_did.as_deref(),
        "the announced creator must be the DID proven by the signature, never the caller who delivered the push"
    );

    // A replicator with a filter on the collection is skipped unless the push
    // carries a document to match against, so a fragment -- which is blocks,
    // not a body -- has to have the merged state read back for it.
    assert_eq!(
        event
            .document_json
            .as_ref()
            .and_then(|json| json.get("name")),
        Some(&serde_json::json!("Alice")),
        "a filtered replicator has nothing to match on without the merged document"
    );
}

#[tokio::test]
async fn an_unsigned_fragment_is_announced_without_a_creator_claim() {
    // Announcing is not the same as vouching. An unsigned fragment still has
    // to reach peers, but it carries no DID for them to register as owner.
    let (_, _, events) = apply_and_capture(false).await;

    assert_eq!(
        events.len(),
        1,
        "an unsigned fragment must still be announced"
    );
    assert_eq!(
        events[0].creator_did, None,
        "with no signature there is no owner to claim on the receiving node"
    );
}
