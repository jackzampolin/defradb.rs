use std::collections::HashMap;
use std::sync::Arc;

use lens::TargetedHistoryLink;

use super::LensedAutoCommitFetcher;

#[tokio::test]
async fn unknown_document_version_passes_through() {
    let db = Arc::new(crate::DB::new(storage::MemoryStore::new()).unwrap());
    let fetcher = LensedAutoCommitFetcher::new(db.clone());
    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();

    let collection = crate::Collection::new(schema::CollectionVersion::new(
        "Users",
        "v2",
        "users-collection",
        vec![schema::FieldDescription::new(
            "1",
            "name",
            schema::FieldKind::string(),
        )],
    ));
    let history = Some(HashMap::from([
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
    ]));

    let mut doc = document::Document::new();
    doc.set("name", "Alice");
    doc.set_schema_version_id("foreign-v3");

    let returned = fetcher
        .process_document(doc, &collection, &datastore, &systemstore, true, &history)
        .await
        .unwrap();
    assert_eq!(
        returned.get("name").and_then(|value| value.as_str()),
        Some("Alice")
    );
    assert_eq!(returned.schema_version_id(), Some("foreign-v3"));

    drop(datastore);
    drop(systemstore);
    txn.discard().unwrap();
}
