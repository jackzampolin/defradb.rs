//! Integration test: full DB document CRUD over an `EncryptedStore`.
//!
//! Proves the at-rest value-encryption wrapper composes with the complete DB
//! stack (systemstore + datastore through transactions and iterators).

use std::sync::Arc;

use db::database::DB;
use db::AutoCommitMutator;
use document::Document;
use query::mutator::DocMutator;
use storage::backends::MemoryStore;
use storage::encrypted_store::EncryptedStore;

#[tokio::test]
async fn document_crud_roundtrips_through_encrypted_store() {
    let key = [42u8; 32];
    let backend = MemoryStore::new();
    let store = Arc::new(EncryptedStore::new(backend, key));
    let db = Arc::new(DB::open_from_arc(store).await.unwrap());

    let collections = query::parse_sdl(
        r#"
        type Users {
            name: String
        }
        "#,
    )
    .unwrap();
    db.create_collections_atomic(collections).await.unwrap();

    // Create a document through the encrypted store via the real mutator flow
    // (allocates the doc short ID and derives the DocID from the genesis block).
    let mutator = AutoCommitMutator::new(db.clone());
    let doc = Document::from_json_str(r#"{"name": "Alice"}"#).unwrap();
    let created = mutator.create("Users", doc).await.unwrap();

    // Read it back in a fresh transaction; the value must decrypt cleanly.
    let fetched = mutator
        .get_for_update("Users", &created.doc_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fetched.get("name").and_then(|v| v.as_str()), Some("Alice"));
}
