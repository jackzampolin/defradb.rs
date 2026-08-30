//! An `@branchable` SDL type must record its collection heads where
//! `BranchableSync` will look for them.
//!
//! The local write path keys collection heads by
//! `CollectionVersion::resolved_root_id()`, which falls back to a hash of the
//! collection id when `root_id` is 0; the sync responder resolves the
//! persisted sequence id. If a write ever runs with an unpopulated `root_id`,
//! heads land under the hash prefix, the responder scans the sequence prefix,
//! and every `sync_branchable_collection` response is silently empty.

use defra_node::EmbeddedNode;
use storage::corekv::{IterOptions, Store};

#[tokio::test]
async fn branchable_directive_survives_sdl_parsing() {
    let node = EmbeddedNode::builder().build().await.expect("build node");
    node.add_schema("type Note @branchable { title: String }")
        .await
        .expect("add schema");

    let collection = node
        .get_collection("Note")
        .expect("get collection")
        .expect("collection exists");
    assert!(
        collection.is_branchable,
        "@branchable was dropped during SDL parsing: {collection:?}"
    );
}

/// Full-stack version of `crates/db/tests/merge/collection_heads.rs`: SDL in,
/// GraphQL mutation in, then the raw store is inspected for where the
/// collection head actually landed.
#[tokio::test]
async fn graphql_write_records_collection_head_under_persisted_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = EmbeddedNode::builder()
        .data_path(dir.path())
        .build()
        .await
        .expect("build node");
    node.add_schema("type Note @branchable { title: String }")
        .await
        .expect("add schema");
    let response = node
        .execute(r#"mutation { create_Note(input: {title: "first"}) { _docID } }"#)
        .await;
    assert!(response.errors.is_empty(), "create failed: {response:?}");

    let collection = node
        .get_collection("Note")
        .expect("get collection")
        .expect("collection exists");
    let persisted_prefix = format!("/c/{}/", collection.resolved_root_id());
    let legacy_prefix = format!(
        "/c/{}/",
        schema::legacy_collection_short_id(&collection.collection_id)
    );
    node.shutdown().await;
    drop(node);

    // The node releases the redb file lock shortly after shutdown; retry the
    // reopen briefly rather than racing it (same pacing as blob_size_cost.rs).
    let mut store = None;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        match storage::RedbStore::open_with_options(
            dir.path().to_str().expect("utf-8 path"),
            storage::RedbStoreOptions::new(),
        ) {
            Ok(s) => {
                store = Some(s);
                break;
            }
            Err(_) => continue,
        }
    }
    let store = store.expect("reopen redb store after shutdown");
    let txn = store.new_txn(true).await.expect("read txn");
    let mut iter = txn.iterator(IterOptions::new()).await.expect("iterator");
    let mut col_head_keys = Vec::new();
    while let Some(pair) = iter.next().await.expect("iterate") {
        let key = String::from_utf8_lossy(&pair.key).into_owned();
        if key.contains("/c/") {
            col_head_keys.push(key);
        }
    }

    assert!(
        col_head_keys.iter().any(|k| k.contains(&persisted_prefix)),
        "no collection head under the persisted prefix {persisted_prefix:?} \
         (legacy hash prefix would be {legacy_prefix:?}); '/c/' keys found: {col_head_keys:?}"
    );
}

/// Same as above, but writing through the signed-query runtime the way an
/// embedder with a node identity does (block signing on, batch signing
/// sessions active).
#[tokio::test]
async fn signed_graphql_write_records_collection_head_under_persisted_id() {
    use crypto::Key;
    use identity::Identity as _;

    defra_core::signing::clear_identity_store();
    let private_key = crypto::generate_ed25519().expect("generate ed25519 key");
    let raw = identity::RawIdentity::from_ed25519(private_key.clone()).expect("raw identity");
    let did = raw.did().expect("derive DID").to_string();
    let public_key_bytes = raw.public_key_bytes();
    defra_core::signing::store_identity(
        &did,
        defra_core::signing::SigningConfig {
            key_type: defra_core::signing::SigningKeyType::Ed25519,
            private_key_bytes: private_key.raw().to_vec(),
            public_key_hex: hex::encode(&public_key_bytes),
            public_key_bytes,
            remote_signer: None,
            signing_authorization: None,
        },
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let node = EmbeddedNode::builder()
        .data_path(dir.path())
        .with_node_identity_did(&did)
        .build()
        .await
        .expect("build signed node");
    node.add_schema("type Note @branchable { title: String }")
        .await
        .expect("add schema");
    let response = node
        .execute(r#"mutation { create_Note(input: {title: "first"}) { _docID } }"#)
        .await;
    assert!(response.errors.is_empty(), "create failed: {response:?}");

    let collection = node
        .get_collection("Note")
        .expect("get collection")
        .expect("collection exists");
    let persisted_prefix = format!("/c/{}/", collection.resolved_root_id());
    let legacy_prefix = format!(
        "/c/{}/",
        schema::legacy_collection_short_id(&collection.collection_id)
    );
    node.shutdown().await;
    drop(node);

    let mut store = None;
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        match storage::RedbStore::open_with_options(
            dir.path().to_str().expect("utf-8 path"),
            storage::RedbStoreOptions::new(),
        ) {
            Ok(s) => {
                store = Some(s);
                break;
            }
            Err(_) => continue,
        }
    }
    let store = store.expect("reopen redb store after shutdown");
    let txn = store.new_txn(true).await.expect("read txn");
    let mut iter = txn.iterator(IterOptions::new()).await.expect("iterator");
    let mut col_head_keys = Vec::new();
    while let Some(pair) = iter.next().await.expect("iterate") {
        let key = String::from_utf8_lossy(&pair.key).into_owned();
        if key.contains("/c/") {
            col_head_keys.push(key);
        }
    }

    assert!(
        col_head_keys.iter().any(|k| k.contains(&persisted_prefix)),
        "signed write left no collection head under the persisted prefix \
         {persisted_prefix:?} (legacy hash prefix would be {legacy_prefix:?}); \
         '/c/' keys found: {col_head_keys:?}"
    );
}
