use super::*;

#[test]
fn test_replicator_key() {
    let key = ReplicatorKey::new("replicator_user_collection_peer1");
    assert_eq!(key.to_string(), "/rep/id/replicator_user_collection_peer1");
    assert_eq!(key.bytes(), key.to_string().as_bytes());
    assert_eq!(key.replicator_id(), "replicator_user_collection_peer1");

    let prefix = ReplicatorKey::replicator_prefix();
    assert_eq!(prefix, b"/rep/id/");
}

#[test]
fn test_replicator_key_try_new() {
    // Valid ID
    let key = ReplicatorKey::try_new("valid_id");
    assert!(key.is_some());
    assert_eq!(key.unwrap().replicator_id(), "valid_id");

    // Empty ID should fail
    let key = ReplicatorKey::try_new("");
    assert!(key.is_none());
}

#[test]
#[should_panic(expected = "replicator_id cannot be empty")]
fn test_replicator_key_new_empty_panics() {
    let _ = ReplicatorKey::new("");
}

#[test]
fn test_replicator_key_from_bytes() {
    // Valid key bytes
    let key_bytes = b"/rep/id/peer123";
    let key = ReplicatorKey::from_bytes(key_bytes);
    assert!(key.is_some());
    assert_eq!(key.unwrap().replicator_id(), "peer123");

    // Missing prefix
    let key = ReplicatorKey::from_bytes(b"peer123");
    assert!(key.is_none());

    // Empty ID after prefix
    let key = ReplicatorKey::from_bytes(b"/rep/id/");
    assert!(key.is_none());

    // Invalid UTF-8
    let key = ReplicatorKey::from_bytes(&[0xFF, 0xFE]);
    assert!(key.is_none());

    // Wrong prefix
    let key = ReplicatorKey::from_bytes(b"/other/prefix/peer123");
    assert!(key.is_none());
}

#[test]
fn test_replicator_key_roundtrip() {
    let original = ReplicatorKey::new("my_peer_id");
    let bytes = original.bytes();
    let restored = ReplicatorKey::from_bytes(&bytes);
    assert!(restored.is_some());
    assert_eq!(restored.unwrap(), original);
}

#[test]
fn test_replicator_retry_id_key() {
    let peer_id = "QmXxxx123456789";
    let key = ReplicatorRetryIDKey::new(peer_id);
    assert_eq!(key.to_string(), format!("/rep/retry/id/{}", peer_id));
    assert_eq!(key.bytes(), key.to_string().as_bytes());

    let prefix = ReplicatorRetryIDKey::retry_prefix();
    assert_eq!(prefix, b"/rep/retry/id/");
}

#[test]
fn test_replicator_retry_doc_id_key() {
    let peer_id = "QmXxxx123456789";
    let doc_id = "bae123456789abcdef0123456789abcdef012345";
    let key = ReplicatorRetryDocIDKey::new(peer_id, doc_id);
    assert_eq!(
        key.to_string(),
        format!("/rep/retry/doc/{}/{}", peer_id, doc_id)
    );
    assert_eq!(key.bytes(), key.to_string().as_bytes());

    let prefix = ReplicatorRetryDocIDKey::retry_doc_prefix();
    assert_eq!(prefix, b"/rep/retry/doc/");

    let prefix = ReplicatorRetryDocIDKey::peer_prefix(peer_id);
    assert_eq!(prefix, format!("/rep/retry/doc/{}/", peer_id).as_bytes());
}

#[test]
fn test_peerstore_se_retry() {
    let peer_id = "QmXxxx123456789";
    let collection_id = "users";
    let doc_id = "bae123456789abcdef0123456789abcdef012345";

    let key = PeerstoreSERetry::new(peer_id, collection_id, doc_id);
    assert_eq!(
        key.to_string(),
        format!("/se-retry/{}/{}/{}", peer_id, collection_id, doc_id)
    );
    assert_eq!(key.bytes(), key.to_string().as_bytes());

    let prefix = PeerstoreSERetry::se_retry_prefix();
    assert_eq!(prefix, b"/se-retry/");

    let prefix = PeerstoreSERetry::peer_prefix(peer_id);
    assert_eq!(prefix, format!("/se-retry/{}/", peer_id).as_bytes());

    let prefix = PeerstoreSERetry::peer_collection_prefix(peer_id, collection_id);
    assert_eq!(
        prefix,
        format!("/se-retry/{}/{}/", peer_id, collection_id).as_bytes()
    );
}
