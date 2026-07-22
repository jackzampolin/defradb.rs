use std::sync::Arc;

use cid::Cid;
use defra_core::block::generate_cid_from_bytes;
use defra_core::browser_sync::{BrowserSyncBlock, BrowserSyncDocument};
use defra_core::{Block, CrdtDelta, Signature, SignatureHeader, SignatureType};
use document::{DocID, Document, NormalValue};
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

use super::{BrowserSyncEngine, BrowserSyncError};

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

#[tokio::test]
async fn document_round_trip_uses_crdt_blocks() {
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let target = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
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
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
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
    let database = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
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
    db::doc_id_map::set_doc_id_alias(
        &txn.systemstore().unwrap(),
        sync.database()
            .get_collection("Users")
            .unwrap()
            .unwrap()
            .resolved_root_id(),
        canonical_ref.doc_short_id,
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
    let database = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
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
    wire_document.doc_id = db_blocks::derive_doc_id(&root_cid);

    let Err(error) = sync.validate_document(&wire_document) else {
        panic!("cross-collection field block was accepted");
    };
    assert!(error.to_string().contains("belongs to collection"));
}

#[tokio::test]
async fn rejects_field_link_name_mismatch() {
    let database = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
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
    wire_document.doc_id = db_blocks::derive_doc_id(&root_cid);

    let Err(error) = sync.validate_document(&wire_document) else {
        panic!("mismatched field link was accepted");
    };
    assert!(error.to_string().contains("links field 'name' as 'email'"));
}

#[tokio::test]
async fn validation_accepts_reachable_signature_blocks() {
    let database = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
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
    wire_document.doc_id = db_blocks::derive_doc_id(&root_cid);
    wire_document.blocks.push(BrowserSyncBlock {
        cid: signature_cid.to_string(),
        data: hex::encode(signature_data),
    });

    sync.validate_document(&wire_document).unwrap();
}
