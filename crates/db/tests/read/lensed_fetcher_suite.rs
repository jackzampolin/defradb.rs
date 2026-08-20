use db::read::lensed::fetcher::*;
use document::Document;
use lens::TargetedHistoryLink;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn test_doc_to_lens_doc_conversion() {
    let mut doc = Document::new();
    doc.set("name", Value::String("Alice".to_string()));
    doc.set("age", Value::Number(30.into()));

    let lens_doc = LensedDocFetcher::<storage::MemoryStore>::doc_to_lens_doc(&doc).unwrap();

    assert_eq!(
        lens_doc.get("name").unwrap(),
        &Value::String("Alice".to_string())
    );
    assert_eq!(lens_doc.get("age").unwrap(), &Value::Number(30.into()));
}

#[tokio::test]
async fn unknown_document_version_passes_through() {
    let db = Arc::new(db::DB::new(storage::MemoryStore::new()).unwrap());
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let fetcher =
        LensedDocFetcher::new(db, txn, Arc::new(lens::MemoryTransformStore::new()), false);

    let collection = db::Collection::new(schema::CollectionVersion::new(
        "Users",
        "v2",
        "users-collection",
        vec![schema::FieldDescription::new(
            "1",
            "name",
            schema::FieldKind::string(),
        )],
    ));
    let history = HashMap::from([
        (
            "v1".to_string(),
            TargetedHistoryLink::new("v1", "users-collection").with_next("v2"),
        ),
        (
            "v2".to_string(),
            TargetedHistoryLink::new("v2", "users-collection")
                .with_transform(Some("transform-v1-v2".to_string()))
                .with_previous("v1"),
        ),
    ]);
    fetcher
        .insert_history("users-collection:v2".to_string(), history)
        .await;

    let mut doc = Document::new();
    doc.set("name", "Alice");
    doc.set_schema_version_id("foreign-v3");

    let returned = fetcher
        .process_document(doc, &collection, &datastore, true)
        .await
        .unwrap();
    assert_eq!(
        returned.get("name").and_then(|value| value.as_str()),
        Some("Alice")
    );
    assert_eq!(returned.schema_version_id(), Some("foreign-v3"));

    drop(datastore);
    fetcher.take_txn().await.unwrap().discard().unwrap();
}
