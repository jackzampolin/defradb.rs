//! P2P command unit tests

use super::collection::P2pCollectionCreateArgs;
use super::replicator::P2pReplicatorCreateArgs;

#[test]
fn test_p2p_replicator_create_args() {
    let args = P2pReplicatorCreateArgs {
        collection: vec!["Users".to_string(), "Posts".to_string()],
        addresses: vec!["/ip4/127.0.0.1/tcp/9000".to_string()],
    };
    assert_eq!(args.collection.len(), 2);
    assert_eq!(args.addresses.len(), 1);
}

#[test]
fn test_p2p_replicator_create_args_no_address() {
    let args = P2pReplicatorCreateArgs {
        collection: vec!["Users".to_string()],
        addresses: vec![],
    };
    assert!(args.addresses.is_empty());
}

#[test]
fn test_p2p_collection_create_args() {
    let args = P2pCollectionCreateArgs {
        collections: "Users".to_string(),
    };
    assert_eq!(args.collections, "Users");
}
