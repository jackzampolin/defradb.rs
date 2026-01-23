
//! Tests for replicator types.

use p2p::replicator::{ReplicatorError, ReplicatorInfo};
use p2p::{Multiaddr, PeerId};

#[test]
fn test_replicator_info_new() {
    let peer_id = PeerId::random();
    let collections = vec!["users".to_string(), "posts".to_string()];

    let info = ReplicatorInfo::new(peer_id, collections.clone());

    assert_eq!(info.peer_id(), Some(peer_id));
    assert_eq!(info.collections, collections);
    assert!(info.addresses().is_empty());
}

#[test]
fn test_replicator_info_with_addresses() {
    let peer_id = PeerId::random();
    let collections = vec!["users".to_string()];
    let addr: Multiaddr = "/ip4/127.0.0.1/tcp/4001".parse().unwrap();

    let info = ReplicatorInfo::with_addresses(peer_id, collections.clone(), vec![addr.clone()]);

    assert_eq!(info.peer_id(), Some(peer_id));
    assert_eq!(info.collections, collections);
    assert_eq!(info.addresses(), vec![addr]);
}

#[test]
fn test_replicator_info_serialization() {
    let peer_id = PeerId::random();
    let collections = vec!["users".to_string()];
    let info = ReplicatorInfo::new(peer_id, collections);

    let bytes = info.to_bytes().unwrap();
    let restored = ReplicatorInfo::from_bytes(&bytes).unwrap();

    assert_eq!(info, restored);
}

#[test]
fn test_replicator_info_invalid_peer_id() {
    let info = ReplicatorInfo::from_raw("invalid".to_string(), vec![], vec![]);

    assert!(info.peer_id().is_none());
    assert!(info.try_peer_id().is_err());
}

#[test]
fn test_replicator_info_invalid_address_filtered() {
    let peer_id = PeerId::random();
    let info = ReplicatorInfo::from_raw(
        peer_id.to_string(),
        vec![],
        vec![
            "/ip4/127.0.0.1/tcp/4001".to_string(),
            "invalid-address".to_string(),
        ],
    );

    let addrs = info.addresses();
    assert_eq!(addrs.len(), 1);
}

#[test]
fn test_replicator_info_try_new_validates_collections() {
    let peer_id = PeerId::random();

    // Empty collections should fail
    let result = ReplicatorInfo::try_new(peer_id, vec![]);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ReplicatorError::EmptyCollections
    ));

    // Non-empty collections should succeed
    let result = ReplicatorInfo::try_new(peer_id, vec!["users".to_string()]);
    assert!(result.is_ok());
}

#[test]
fn test_replicator_info_cbor_roundtrip_with_addresses() {
    let peer_id = PeerId::random();
    let addr: Multiaddr = "/ip4/192.168.1.1/tcp/4001".parse().unwrap();
    let collections = vec![
        "users".to_string(),
        "posts".to_string(),
        "comments".to_string(),
    ];

    let info = ReplicatorInfo::with_addresses(peer_id, collections.clone(), vec![addr.clone()]);

    // Serialize to CBOR bytes
    let bytes = info.to_bytes().unwrap();

    // Deserialize back
    let restored = ReplicatorInfo::from_bytes(&bytes).unwrap();

    // Verify all fields match
    assert_eq!(restored.peer_id(), Some(peer_id));
    assert_eq!(restored.collections, collections);
    assert_eq!(restored.addresses(), vec![addr]);
}

#[test]
fn test_replicator_info_from_invalid_cbor() {
    // Random bytes that are not valid CBOR
    let result = ReplicatorInfo::from_bytes(&[0x00, 0x01, 0x02]);
    assert!(result.is_err());
}

#[test]
fn test_replicator_info_from_truncated_cbor() {
    let peer_id = PeerId::random();
    let info = ReplicatorInfo::new(peer_id, vec!["users".to_string()]);
    let mut bytes = info.to_bytes().unwrap();

    // Truncate to half the length
    bytes.truncate(bytes.len() / 2);

    let result = ReplicatorInfo::from_bytes(&bytes);
    assert!(result.is_err());
}

#[test]
fn test_replicator_info_from_empty_bytes() {
    let result = ReplicatorInfo::from_bytes(&[]);
    assert!(result.is_err());
}

#[test]
fn test_replicator_info_empty_collections() {
    let peer_id = PeerId::random();
    let info = ReplicatorInfo::new(peer_id, vec![]);

    // Should serialize and deserialize correctly
    let bytes = info.to_bytes().unwrap();
    let restored = ReplicatorInfo::from_bytes(&bytes).unwrap();

    assert_eq!(restored.peer_id(), Some(peer_id));
    assert!(restored.collections.is_empty());
}

#[test]
fn test_replicator_info_many_collections() {
    let peer_id = PeerId::random();
    let collections: Vec<String> = (0..100).map(|i| format!("collection_{}", i)).collect();

    let info = ReplicatorInfo::new(peer_id, collections.clone());
    let bytes = info.to_bytes().unwrap();
    let restored = ReplicatorInfo::from_bytes(&bytes).unwrap();

    assert_eq!(restored.collections.len(), 100);
    assert_eq!(restored.collections, collections);
}
