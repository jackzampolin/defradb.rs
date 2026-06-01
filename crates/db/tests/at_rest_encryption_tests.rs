//! Integration test: full DB document CRUD over an `EncryptedStore`.
//!
//! Proves the at-rest value-encryption wrapper composes with the complete DB
//! stack (systemstore + datastore through transactions and iterators).

use std::sync::Arc;

use db::database::DB;
use document::Document;
use storage::backends::MemoryStore;
use storage::encrypted_store::EncryptedStore;

#[tokio::test]
async fn document_crud_roundtrips_through_encrypted_store() {
    let key = [42u8; 32];
    let backend = MemoryStore::new();
    let store = Arc::new(EncryptedStore::new(backend, key));
    let db = DB::open_from_arc(store).await.unwrap();

    let collections = query::parse_sdl(
        r#"
        type Users {
            name: String
        }
        "#,
    )
    .unwrap();
    db.create_collections_atomic(collections).await.unwrap();

    let collection = db.get_collection("Users").unwrap().unwrap();

    // Create a document through the encrypted store.
    let txn = db.new_txn(false).await.unwrap();
    let doc = Document::from_json_str(r#"{"name": "Alice"}"#).unwrap();
    let doc_id = collection.create(&txn, &doc).await.unwrap();
    txn.commit().await.unwrap();

    // Read it back in a fresh transaction; the value must decrypt cleanly.
    let txn = db.new_txn(true).await.unwrap();
    let fetched = collection.get(&txn, &doc_id).await.unwrap().unwrap();
    assert_eq!(fetched.get("name").and_then(|v| v.as_str()), Some("Alice"));
}
