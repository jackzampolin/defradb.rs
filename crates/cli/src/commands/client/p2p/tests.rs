//! P2P command unit tests

use super::collection::P2pCollectionAddArgs;
use super::replicator::P2pReplicatorAddArgs;

#[test]
fn test_p2p_replicator_add_args() {
    let args = P2pReplicatorAddArgs {
        collection: vec!["Users".to_string(), "Posts".to_string()],
        addresses: vec!["/ip4/127.0.0.1/tcp/9000".to_string()],
    };
    assert_eq!(args.collection.len(), 2);
    assert_eq!(args.addresses.len(), 1);
}

#[test]
fn test_p2p_replicator_add_args_no_address() {
    let args = P2pReplicatorAddArgs {
        collection: vec!["Users".to_string()],
        addresses: vec![],
    };
    assert!(args.addresses.is_empty());
}

#[test]
fn test_p2p_collection_add_args() {
    let args = P2pCollectionAddArgs {
        collections: "Users".to_string(),
    };
    assert_eq!(args.collections, "Users");
}
