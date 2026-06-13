//! P2P command unit tests

use super::collection::P2pCollectionAddArgs;
use super::replicator::P2pReplicatorAddArgs;

#[test]
fn test_p2p_replicator_add_args() {
    let args = P2pReplicatorAddArgs {
        collection: vec!["Users".to_string(), "Posts".to_string()],
        addresses: vec!["/ip4/127.0.0.1/tcp/9000".to_string()],
        filter_field: None,
        filter_value: None,
    };
    assert_eq!(args.collection.len(), 2);
    assert_eq!(args.addresses.len(), 1);
}

#[test]
fn test_p2p_replicator_add_args_no_address() {
    let args = P2pReplicatorAddArgs {
        collection: vec!["Users".to_string()],
        addresses: vec![],
        filter_field: None,
        filter_value: None,
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

#[test]
fn test_p2p_manage_collection_add_parses() {
    use clap::Parser;

    use super::manage::{P2pManageCollectionCommand, P2pManageCommand};
    use super::P2pCommand;

    #[derive(Parser)]
    struct Wrap {
        #[command(subcommand)]
        cmd: P2pCommand,
    }

    let wrap = Wrap::parse_from([
        "p2p",
        "manage",
        "collection",
        "add",
        "--target",
        "/ip4/127.0.0.1/tcp/9000/p2p/abc",
        "--identity",
        "deadbeef",
        "Users",
        "Posts",
    ]);

    let P2pCommand::Manage(manage) = wrap.cmd else {
        panic!("expected manage subcommand");
    };
    let P2pManageCommand::Collection(collection) = manage.command else {
        panic!("expected collection subcommand");
    };
    let P2pManageCollectionCommand::Add(add) = collection.command else {
        panic!("expected add subcommand");
    };
    assert_eq!(add.target.target, "/ip4/127.0.0.1/tcp/9000/p2p/abc");
    assert_eq!(add.target.identity.as_deref(), Some("deadbeef"));
    assert_eq!(add.collection_ids, vec!["Users", "Posts"]);
}

#[test]
fn test_p2p_manage_replicator_add_parses() {
    use clap::Parser;

    use super::manage::{P2pManageCommand, P2pManageReplicatorCommand};
    use super::P2pCommand;

    #[derive(Parser)]
    struct Wrap {
        #[command(subcommand)]
        cmd: P2pCommand,
    }

    let wrap = Wrap::parse_from([
        "p2p",
        "manage",
        "replicator",
        "add",
        "--target",
        "/ip4/127.0.0.1/tcp/9000/p2p/abc",
        "--identity",
        "deadbeef",
        "--address",
        "/ip4/127.0.0.1/tcp/9001/p2p/def",
        "Users",
    ]);

    let P2pCommand::Manage(manage) = wrap.cmd else {
        panic!("expected manage subcommand");
    };
    let P2pManageCommand::Replicator(replicator) = manage.command else {
        panic!("expected replicator subcommand");
    };
    let P2pManageReplicatorCommand::Add(add) = replicator.command else {
        panic!("expected add subcommand");
    };
    assert_eq!(add.address, "/ip4/127.0.0.1/tcp/9001/p2p/def");
    assert_eq!(add.collection_ids, vec!["Users"]);
}
