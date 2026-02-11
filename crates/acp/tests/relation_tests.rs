//! Tests for RelationTuple types.

use acp::{RelationTuple, OWNER_RELATION};
use identity::Did;

fn test_did() -> Did {
    Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
}

#[test]
fn test_relation_tuple_try_new() {
    let did = test_did();

    // Valid components should work
    assert!(RelationTuple::try_new(did.clone(), "reader", "users", "doc123").is_ok());

    // Relation with slash should fail
    assert!(RelationTuple::try_new(did.clone(), "reader/admin", "users", "doc123").is_err());

    // Collection ID with slash should fail
    assert!(RelationTuple::try_new(did.clone(), "reader", "users/internal", "doc123").is_err());

    // Doc ID with slash should fail
    assert!(RelationTuple::try_new(did.clone(), "reader", "users", "doc/123").is_err());

    // Backslash should also fail
    assert!(RelationTuple::try_new(did.clone(), "reader\\admin", "users", "doc123").is_err());

    // Empty relation should fail
    assert!(RelationTuple::try_new(did.clone(), "", "users", "doc123").is_err());

    // Empty collection_id should fail
    assert!(RelationTuple::try_new(did.clone(), "reader", "", "doc123").is_err());

    // Empty doc_id should fail
    assert!(RelationTuple::try_new(did.clone(), "reader", "users", "").is_err());

    // Null bytes should fail
    assert!(RelationTuple::try_new(did.clone(), "reader\0admin", "users", "doc123").is_err());
    assert!(RelationTuple::try_new(did.clone(), "reader", "users\0internal", "doc123").is_err());
    assert!(RelationTuple::try_new(did.clone(), "reader", "users", "doc\x00123").is_err());
}

#[test]
fn test_relation_tuple_owner() {
    let did = test_did();
    let tuple = RelationTuple::try_new(did.clone(), OWNER_RELATION, "users", "doc123").unwrap();

    assert_eq!(tuple.relation(), OWNER_RELATION);
    assert!(tuple.is_owner());
}

#[test]
fn test_relation_tuple_accessors() {
    let did = test_did();
    let tuple = RelationTuple::try_new(did.clone(), "reader", "users", "doc123").unwrap();

    assert_eq!(tuple.subject(), &did);
    assert_eq!(tuple.relation(), "reader");
    assert_eq!(tuple.collection_id(), "users");
    assert_eq!(tuple.doc_id(), "doc123");
}

#[test]
fn test_relation_tuple_storage_key() {
    let did = test_did();
    let tuple = RelationTuple::try_new(did.clone(), "reader", "users", "doc123").unwrap();

    let key = tuple.storage_key();
    assert!(key.starts_with("/acp/"));
    assert!(key.contains("users"));
    assert!(key.contains("doc123"));
    assert!(key.contains("reader"));
    assert!(key.contains(did.as_str()));
}

#[test]
fn test_doc_prefix() {
    let prefix = RelationTuple::doc_prefix_validated("users", "doc123").unwrap();
    assert_eq!(prefix, "/acp/users/doc123/");
}

#[test]
fn test_relation_prefix() {
    let prefix = RelationTuple::relation_prefix_validated("users", "doc123", "owner").unwrap();
    assert_eq!(prefix, "/acp/users/doc123/owner/");
}

#[test]
fn test_relation_tuple_display() {
    let did = test_did();
    let tuple = RelationTuple::try_new(did, "reader", "users", "doc123").unwrap();
    let display = format!("{}", tuple);
    assert!(display.contains("reader"));
    assert!(display.contains("users"));
    assert!(display.contains("doc123"));
}

#[test]
fn test_relation_tuple_serde() {
    let did = test_did();
    let tuple = RelationTuple::try_new(did, "owner", "users", "doc123").unwrap();
    let json = serde_json::to_string(&tuple).unwrap();
    let parsed: RelationTuple = serde_json::from_str(&json).unwrap();
    assert_eq!(tuple, parsed);
}

#[test]
fn test_deserialize_rejects_path_traversal() {
    // Craft JSON with path separator in relation field
    let malicious_json = r#"{
        "subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
        "relation": "reader/../admin",
        "collection_id": "users",
        "doc_id": "doc123"
    }"#;

    let result: std::result::Result<RelationTuple, _> = serde_json::from_str(malicious_json);
    assert!(
        result.is_err(),
        "should reject path traversal in relation field"
    );

    // Craft JSON with path separator in collection_id
    let malicious_json = r#"{
        "subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
        "relation": "reader",
        "collection_id": "users/../secrets",
        "doc_id": "doc123"
    }"#;

    let result: std::result::Result<RelationTuple, _> = serde_json::from_str(malicious_json);
    assert!(
        result.is_err(),
        "should reject path traversal in collection_id field"
    );

    // Craft JSON with path separator in doc_id
    let malicious_json = r#"{
        "subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
        "relation": "reader",
        "collection_id": "users",
        "doc_id": "doc/123"
    }"#;

    let result: std::result::Result<RelationTuple, _> = serde_json::from_str(malicious_json);
    assert!(
        result.is_err(),
        "should reject path traversal in doc_id field"
    );
}
