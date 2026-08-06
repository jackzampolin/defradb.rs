//! Encryption-derivation behaviour for `write_document_blocks`.
//!
//! Split out of `write.rs` to keep that file within the repo's size guidance.

use super::*;
use datastore::{NamespaceView, SharedTxn};
use defra_core::encryption::EncryptionConfig;
use std::collections::HashSet;
use storage::backends::MemoryStore;
use storage::corekv::Store;
use storage::namespace::Namespace;

async fn stores() -> (NamespaceView, NamespaceView) {
    let store = MemoryStore::new();
    let txn = store.new_txn(false).await.unwrap();
    let shared = SharedTxn::new(txn);
    (
        NamespaceView::new(shared.clone(), Namespace::Blockstore),
        NamespaceView::new(shared, Namespace::Headstore),
    )
}

async fn field_block(blockstore: &NamespaceView, cid: &Cid) -> Block {
    let bytes = blockstore
        .get(&cid.to_bytes())
        .await
        .unwrap()
        .expect("block stored");
    Block::from_dag_cbor(&bytes).unwrap()
}

/// An update that carries no encryption config must still be encrypted when
/// the field's previous block was encrypted, by inheriting that block's
/// encryption link. Mirrors Go's `determineBlockEncryption`
/// (internal/core/block/store.go) and its
/// `TestDocEncryption_UponUpdateOnLWWCRDT_ShouldEncryptCommitDelta`.
#[tokio::test]
async fn update_without_config_inherits_encryption_from_previous_block() {
    let (blockstore, headstore) = stores().await;
    let identity = DocStorageIdentity::new(1, 1);

    let mut doc = Document::new();
    doc.set("secret", NormalValue::String("classified".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: false,
        encrypt_fields: vec!["secret".to_string()],
    };

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        None,
    )
    .await
    .expect("create should succeed");

    let created_enc_cid = field_block(&blockstore, &created.field_cids[0])
        .await
        .encryption
        .expect("created field block must be encrypted");

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set(
        "secret",
        NormalValue::String("still classified".to_string()),
    );
    let modified: HashSet<String> = ["secret".to_string()].into_iter().collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        None,
    )
    .await
    .expect("update should succeed");

    assert_eq!(
        field_block(&blockstore, &updated.field_cids[0])
            .await
            .encryption,
        Some(created_enc_cid),
        "update must inherit the previous block's encryption link, not write plaintext"
    );
}

/// The composite block carries its own encryption link for whole-document
/// encryption, and must inherit it on update for the same reason.
#[tokio::test]
async fn update_without_config_inherits_composite_encryption() {
    let (blockstore, headstore) = stores().await;
    let identity = DocStorageIdentity::new(2, 2);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: true,
        encrypt_fields: vec![],
    };

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        None,
    )
    .await
    .expect("create should succeed");

    let created_enc_cid = field_block(&blockstore, &created.cid)
        .await
        .encryption
        .expect("created composite block must be encrypted");

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set("name", NormalValue::String("Alicia".to_string()));
    let modified: HashSet<String> = ["name".to_string()].into_iter().collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        None,
    )
    .await
    .expect("update should succeed");

    assert_eq!(
        field_block(&blockstore, &updated.cid).await.encryption,
        Some(created_enc_cid),
        "composite block must inherit the previous composite block's encryption link"
    );
}

/// Inheriting reuses the existing `Encryption` block rather than writing a
/// new one, matching Go's `determineBlockEncryption`, which returns the
/// previous link untouched.
#[tokio::test]
async fn inherited_encryption_writes_no_new_encryption_block() {
    let (blockstore, headstore) = stores().await;
    let identity = DocStorageIdentity::new(4, 4);

    let mut doc = Document::new();
    doc.set("secret", NormalValue::String("classified".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: false,
        encrypt_fields: vec!["secret".to_string()],
    };

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        None,
    )
    .await
    .expect("create should succeed");
    assert_eq!(
        created.encryption_cids.len(),
        1,
        "create must mint one encryption block"
    );

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set(
        "secret",
        NormalValue::String("still classified".to_string()),
    );
    let modified: HashSet<String> = ["secret".to_string()].into_iter().collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        None,
    )
    .await
    .expect("update should succeed");

    assert!(
        updated.encryption_cids.is_empty(),
        "inheriting must not mint a new encryption block"
    );
}

/// An explicit config on the update takes precedence over what the heads
/// say, and rotates the key.
#[tokio::test]
async fn explicit_config_on_update_overrides_inheritance() {
    let (blockstore, headstore) = stores().await;
    let identity = DocStorageIdentity::new(5, 5);

    let mut doc = Document::new();
    doc.set("secret", NormalValue::String("classified".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: false,
        encrypt_fields: vec!["secret".to_string()],
    };

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        None,
    )
    .await
    .expect("create should succeed");
    let created_enc_cid = field_block(&blockstore, &created.field_cids[0])
        .await
        .encryption
        .expect("created field block must be encrypted");

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set(
        "secret",
        NormalValue::String("still classified".to_string()),
    );
    let modified: HashSet<String> = ["secret".to_string()].into_iter().collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        Some(&enc),
        None,
        None,
    )
    .await
    .expect("update should succeed");

    let updated_enc_cid = field_block(&blockstore, &updated.field_cids[0])
        .await
        .encryption
        .expect("explicitly configured update must be encrypted");
    assert_ne!(
        updated_enc_cid, created_enc_cid,
        "an explicit config must mint a fresh key rather than inherit"
    );
    assert_eq!(
        updated.encryption_cids.len(),
        1,
        "the explicit path must record the encryption block it wrote"
    );
}

/// Inheritance is per field: a field that was never encrypted does not
/// become encrypted because a sibling field was.
#[tokio::test]
async fn inheritance_is_per_field() {
    let (blockstore, headstore) = stores().await;
    let identity = DocStorageIdentity::new(6, 6);

    let mut doc = Document::new();
    doc.set("secret", NormalValue::String("classified".to_string()));
    doc.set("public", NormalValue::String("open".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: false,
        encrypt_fields: vec!["secret".to_string()],
    };

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        None,
    )
    .await
    .expect("create should succeed");

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set(
        "secret",
        NormalValue::String("still classified".to_string()),
    );
    doc.set("public", NormalValue::String("still open".to_string()));
    let modified: HashSet<String> = ["secret".to_string(), "public".to_string()]
        .into_iter()
        .collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        None,
    )
    .await
    .expect("update should succeed");

    let mut encrypted = 0;
    for cid in &updated.field_cids {
        if field_block(&blockstore, cid).await.encryption.is_some() {
            encrypted += 1;
        }
    }
    assert_eq!(
        (updated.field_cids.len(), encrypted),
        (2, 1),
        "exactly the previously-encrypted field should inherit encryption"
    );
}

/// KNOWN GAP, deliberately matching Go: a field first written by an update
/// has no heads of its own, so there is nothing to inherit from and the
/// delta is written in plaintext — even on a document created with
/// `encrypt_doc: true`.
///
/// Go behaves identically. `addDelta` builds its head set from the field's
/// own prefix (`internal/core/block/store.go:82-86`,
/// `NewHeadSet(txn.Headstore(), crdtData.HeadstorePrefix())`), so
/// `determineBlockEncryption` sees no heads and attaches no encryption.
///
/// This is a characterization test, not an endorsement: it pins the
/// behaviour so a future change cannot alter it silently. To be disclosed
/// upstream to the Go team.
#[tokio::test]
async fn field_added_by_update_is_written_plaintext_matching_go() {
    let (blockstore, headstore) = stores().await;
    let identity = DocStorageIdentity::new(7, 7);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: true,
        encrypt_fields: vec![],
    };

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        None,
    )
    .await
    .expect("create should succeed");

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set("bio", NormalValue::String("ssn 123-45-6789".to_string()));
    let modified: HashSet<String> = ["bio".to_string()].into_iter().collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        None,
    )
    .await
    .expect("update should succeed");

    assert_eq!(updated.field_cids.len(), 1, "only `bio` should get a block");
    assert_eq!(
        field_block(&blockstore, &updated.field_cids[0])
            .await
            .encryption,
        None,
        "matches Go: a field with no prior head has nothing to inherit. \
         Changing this is a deliberate divergence, not a bug fix."
    );
}

/// Derivation must not invent encryption: a document created in the clear
/// stays in the clear.
#[tokio::test]
async fn update_of_unencrypted_document_stays_plaintext() {
    let (blockstore, headstore) = stores().await;
    let identity = DocStorageIdentity::new(3, 3);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Bob".to_string()));

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("create should succeed");

    doc.set_id(document::DocID::from_string(&created.doc_id).unwrap());
    doc.set("name", NormalValue::String("Bobby".to_string()));
    let modified: HashSet<String> = ["name".to_string()].into_iter().collect();

    let updated = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        Some(&modified),
        None,
        None,
        None,
    )
    .await
    .expect("update should succeed");

    assert_eq!(
        field_block(&blockstore, &updated.field_cids[0])
            .await
            .encryption,
        None,
        "an unencrypted document must not become encrypted by derivation"
    );
    assert_eq!(
        field_block(&blockstore, &updated.cid).await.encryption,
        None
    );
}

/// Deleting an encrypted document must leave a tombstone carrying the same
/// `Encryption` link as the composite it supersedes. Go's delete path runs
/// through the same `addDelta` as every other write
/// (`internal/db/document_delete.go:181-195`), so `determineBlockEncryption`
/// inherits from the composite heads. The composite payload itself is never
/// ciphertext (`store.go:213-215`); only the link is attached.
#[tokio::test]
async fn delete_block_inherits_composite_encryption() {
    let (blockstore, headstore) = stores().await;
    let identity = DocStorageIdentity::new(8, 8);

    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));

    let enc = EncryptionConfig {
        encrypt_doc: true,
        encrypt_fields: vec![],
    };

    let created = write_document_blocks(
        &blockstore,
        &headstore,
        &doc,
        "schema-v1",
        identity,
        None,
        Some(&enc),
        None,
        None,
    )
    .await
    .expect("create should succeed");

    let created_enc_cid = field_block(&blockstore, &created.cid)
        .await
        .encryption
        .expect("created composite must carry an encryption link");

    let deleted = write_delete_block(
        &blockstore,
        &headstore,
        &created.doc_id,
        8,
        "schema-v1",
        None,
    )
    .await
    .expect("delete should succeed");

    assert_eq!(
        field_block(&blockstore, &deleted.cid).await.encryption,
        Some(created_enc_cid),
        "delete tombstone must inherit the composite's encryption link"
    );
}
