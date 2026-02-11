//! P2P command unit tests

use super::collection::P2pCollectionAddArgs;
use super::replicator::P2pReplicatorSetArgs;

#[test]
fn test_p2p_replicator_set_args() {
    let args = P2pReplicatorSetArgs {
        collection: vec!["Users".to_string(), "Posts".to_string()],
        address: Some("/ip4/127.0.0.1/tcp/9000".to_string()),
    };
    assert_eq!(args.collection.len(), 2);
    assert!(args.address.is_some());
}

#[test]
fn test_p2p_replicator_set_args_no_address() {
    let args = P2pReplicatorSetArgs {
        collection: vec!["Users".to_string()],
        address: None,
    };
    assert!(args.address.is_none());
}

#[test]
fn test_p2p_collection_add_args() {
    let args = P2pCollectionAddArgs {
        collection: vec!["Users".to_string()],
    };
    assert_eq!(args.collection.len(), 1);
}
