use db::collection::snapshot::*;
use db::collection::Collection;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use std::collections::HashMap;

fn test_collection() -> Collection {
    Collection::new(CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
    ))
}

#[test]
fn test_snapshot_get() {
    let mut map = HashMap::new();
    map.insert("Users".to_string(), test_collection());
    let snapshot = CollectionSnapshot::new(map);

    assert!(snapshot.get("Users").is_some());
    assert!(snapshot.get("Posts").is_none());
}

#[test]
fn test_snapshot_contains() {
    let mut map = HashMap::new();
    map.insert("Users".to_string(), test_collection());
    let snapshot = CollectionSnapshot::new(map);

    assert!(snapshot.contains("Users"));
    assert!(!snapshot.contains("Posts"));
}

#[test]
fn test_snapshot_len() {
    let mut map = HashMap::new();
    map.insert("Users".to_string(), test_collection());
    let snapshot = CollectionSnapshot::new(map);

    assert_eq!(snapshot.len(), 1);
    assert!(!snapshot.is_empty());
}

#[test]
fn test_empty_snapshot() {
    let snapshot = CollectionSnapshot::new(HashMap::new());
    assert!(snapshot.is_empty());
    assert_eq!(snapshot.len(), 0);
}

#[test]
fn test_snapshot_clone_is_cheap() {
    let mut map = HashMap::new();
    map.insert("Users".to_string(), test_collection());
    let snapshot1 = CollectionSnapshot::new(map);
    let snapshot2 = snapshot1.clone();

    // Both should point to the same Arc
    assert!(snapshot1.ptr_eq(&snapshot2));
}

#[test]
fn test_snapshot_names() {
    let mut map = HashMap::new();
    map.insert("Users".to_string(), test_collection());
    map.insert(
        "Posts".to_string(),
        Collection::new(CollectionVersion::new("Posts", "v1", "col-posts", vec![])),
    );
    let snapshot = CollectionSnapshot::new(map);

    let mut names = snapshot.names();
    names.sort();
    assert_eq!(names, vec!["Posts", "Users"]);
}
