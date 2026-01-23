//! Integration tests for subscription event emission.
//!
//! Tests that document mutations (create/update/delete) emit events
//! that can be received by subscribers.

use std::sync::Arc;
use std::time::Duration;

use db::auto_commit_mutator::AutoCommitMutator;
use db::database::DB;
use document::{Document, NormalValue};
use events::{Bus, ChannelBus, EventName};
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;
use tokio::time::timeout;

fn test_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    )
}

#[tokio::test]
async fn test_create_emits_update_event() {
    // Set up DB with event bus
    let store = MemoryStore::new();
    let mut db = DB::new(store);
    let event_bus = Arc::new(ChannelBus::new());
    db.set_event_bus(event_bus.clone());
    let db = Arc::new(db);
    db.create_collection(test_schema()).await.unwrap();

    // Subscribe to events before mutation
    let mut subscription = event_bus.subscribe(&[EventName::Update]);

    let mutator = AutoCommitMutator::new(db.clone());

    // Create a document
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Alice".to_string()));
    doc.set("age", NormalValue::Int(30));

    let result = mutator.create("Users", doc).await.unwrap();
    let created_doc_id = result.doc_id.to_string();

    // Verify we receive the update event
    let event = timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("timeout waiting for event")
        .expect("event channel closed");

    assert!(event.as_update().is_some(), "expected Update event");
    let update = event.as_update().unwrap();
    assert_eq!(update.doc_id, created_doc_id);
    assert_eq!(update.collection_id, "col-users");
    assert!(!update.is_retry);
    assert!(!update.is_relay);
}

#[tokio::test]
async fn test_update_emits_update_event() {
    let store = MemoryStore::new();
    let mut db = DB::new(store);
    let event_bus = Arc::new(ChannelBus::new());
    db.set_event_bus(event_bus.clone());
    let db = Arc::new(db);
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db.clone());

    // Create initial document
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Bob".to_string()));
    doc.set("age", NormalValue::Int(25));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    // Subscribe after create (so we only get update event)
    let mut subscription = event_bus.subscribe(&[EventName::Update]);

    // Update the document
    let mut updated_doc = Document::with_id(doc_id.clone());
    updated_doc.set("name", NormalValue::String("Robert".to_string()));
    updated_doc.set("age", NormalValue::Int(26));
    mutator.update("Users", updated_doc).await.unwrap();

    // Verify we receive the update event
    let event = timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("timeout waiting for event")
        .expect("event channel closed");

    assert!(event.as_update().is_some());
    let update = event.as_update().unwrap();
    assert_eq!(update.doc_id, doc_id.to_string());
    assert_eq!(update.collection_id, "col-users");
}

#[tokio::test]
async fn test_delete_emits_update_event() {
    let store = MemoryStore::new();
    let mut db = DB::new(store);
    let event_bus = Arc::new(ChannelBus::new());
    db.set_event_bus(event_bus.clone());
    let db = Arc::new(db);
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db.clone());

    // Create initial document
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Charlie".to_string()));
    doc.set("age", NormalValue::Int(40));
    let result = mutator.create("Users", doc).await.unwrap();
    let doc_id = result.doc_id.clone();

    // Subscribe after create
    let mut subscription = event_bus.subscribe(&[EventName::Update]);

    // Delete the document
    mutator.delete("Users", &doc_id).await.unwrap();

    // Verify we receive the update event
    let event = timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("timeout waiting for event")
        .expect("event channel closed");

    assert!(event.as_update().is_some());
    let update = event.as_update().unwrap();
    assert_eq!(update.doc_id, doc_id.to_string());
    assert_eq!(update.collection_id, "col-users");
}

#[tokio::test]
async fn test_multiple_mutations_emit_multiple_events() {
    let store = MemoryStore::new();
    let mut db = DB::new(store);
    let event_bus = Arc::new(ChannelBus::new());
    db.set_event_bus(event_bus.clone());
    let db = Arc::new(db);
    db.create_collection(test_schema()).await.unwrap();

    // Subscribe before any mutations
    let mut subscription = event_bus.subscribe(&[EventName::Update]);

    let mutator = AutoCommitMutator::new(db.clone());

    // Create 3 documents
    let names = ["Alice", "Bob", "Charlie"];
    let mut doc_ids = Vec::new();

    for (i, name) in names.iter().enumerate() {
        let mut doc = Document::new();
        doc.set("name", NormalValue::String(name.to_string()));
        doc.set("age", NormalValue::Int((20 + i * 5) as i64));
        let result = mutator.create("Users", doc).await.unwrap();
        doc_ids.push(result.doc_id.to_string());
    }

    // Verify we receive 3 events
    for expected_doc_id in &doc_ids {
        let event = timeout(Duration::from_secs(1), subscription.recv())
            .await
            .expect("timeout waiting for event")
            .expect("event channel closed");

        assert!(event.as_update().is_some());
        let update = event.as_update().unwrap();
        assert_eq!(&update.doc_id, expected_doc_id);
    }
}

#[tokio::test]
async fn test_no_event_bus_no_crash() {
    // Verify mutations work even without an event bus configured
    let store = MemoryStore::new();
    let db = Arc::new(DB::new(store));
    db.create_collection(test_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db.clone());

    // Create should succeed without event bus
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("NoEvents".to_string()));
    doc.set("age", NormalValue::Int(99));

    let result = mutator.create("Users", doc).await;
    assert!(result.is_ok(), "create should succeed without event bus");

    // Update should succeed
    let doc_id = result.unwrap().doc_id;
    let mut updated = Document::with_id(doc_id.clone());
    updated.set("name", NormalValue::String("StillNoEvents".to_string()));
    updated.set("age", NormalValue::Int(100));
    assert!(
        mutator.update("Users", updated).await.is_ok(),
        "update should succeed without event bus"
    );

    // Delete should succeed
    assert!(
        mutator.delete("Users", &doc_id).await.is_ok(),
        "delete should succeed without event bus"
    );
}

#[tokio::test]
async fn test_wildcard_subscription_receives_all_events() {
    let store = MemoryStore::new();
    let mut db = DB::new(store);
    let event_bus = Arc::new(ChannelBus::new());
    db.set_event_bus(event_bus.clone());
    let db = Arc::new(db);
    db.create_collection(test_schema()).await.unwrap();

    // Subscribe with wildcard
    let mut subscription = event_bus.subscribe(&[EventName::WildCard]);

    let mutator = AutoCommitMutator::new(db.clone());

    // Create a document
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("Wildcard".to_string()));
    doc.set("age", NormalValue::Int(42));
    mutator.create("Users", doc).await.unwrap();

    // Wildcard should receive the Update event
    let event = timeout(Duration::from_secs(1), subscription.recv())
        .await
        .expect("timeout waiting for event")
        .expect("event channel closed");

    assert!(
        event.as_update().is_some(),
        "wildcard should receive Update events"
    );
}

#[tokio::test]
async fn test_closed_bus_does_not_block_mutations() {
    let store = MemoryStore::new();
    let mut db = DB::new(store);
    let event_bus = Arc::new(ChannelBus::new());
    db.set_event_bus(event_bus.clone());
    let db = Arc::new(db);
    db.create_collection(test_schema()).await.unwrap();

    // Close the event bus
    event_bus.close();

    let mutator = AutoCommitMutator::new(db.clone());

    // Mutations should still succeed even with closed bus
    let mut doc = Document::new();
    doc.set("name", NormalValue::String("ClosedBus".to_string()));
    doc.set("age", NormalValue::Int(1));

    let result = mutator.create("Users", doc).await;
    assert!(
        result.is_ok(),
        "create should succeed with closed event bus"
    );
}
