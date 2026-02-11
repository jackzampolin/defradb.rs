//! Tests for the start command

use cli::commands::StartArgs;
use cli::config::{Config, DatastoreType};
use cli::error::Error;

fn default_start_args() -> StartArgs {
    StartArgs {
        peers: None,
        max_txn_retries: None,
        store: None,
        valuelogfilesize: None,
        p2paddr: None,
        no_p2p: None,
        allowed_origins: None,
        pubkeypath: None,
        privkeypath: None,
        development: None,
        no_encryption: None,
        no_telemetry: None,
        no_signing: None,
        default_key_type: None,
        no_searchable_encryption: None,
        identity: None,
        replicator_retry_intervals: None,
        durability: None,
    }
}

#[test]
fn test_apply_to_config_invalid_store_returns_error() {
    let mut config = Config::default();
    let mut args = default_start_args();
    args.store = Some("postgres".to_string());

    let result = args.apply_to_config(&mut config);
    assert!(matches!(result, Err(Error::InvalidDatastore(s)) if s == "postgres"));
}

#[test]
fn test_apply_to_config_valid_store_succeeds() {
    let mut config = Config::default();
    let mut args = default_start_args();
    args.store = Some("memory".to_string());

    let result = args.apply_to_config(&mut config);
    assert!(result.is_ok());
    assert_eq!(config.datastore.store, DatastoreType::Memory);
}

#[test]
fn test_apply_to_config_badger_store_succeeds() {
    let mut config = Config::default();
    config.datastore.store = DatastoreType::Memory; // Start with non-default
    let mut args = default_start_args();
    args.store = Some("badger".to_string());

    let result = args.apply_to_config(&mut config);
    assert!(result.is_ok());
    assert_eq!(config.datastore.store, DatastoreType::Badger);
}

#[test]
fn test_apply_to_config_redb_alias_succeeds() {
    let mut config = Config::default();
    let mut args = default_start_args();
    args.store = Some("redb".to_string());

    let result = args.apply_to_config(&mut config);
    assert!(result.is_ok());
    // redb is an alias for badger in Rust implementation
    assert_eq!(config.datastore.store, DatastoreType::Badger);
}

#[test]
fn test_apply_to_config_all_flags() {
    let mut config = Config::default();
    let args = StartArgs {
        peers: Some(vec!["peer1".to_string(), "peer2".to_string()]),
        max_txn_retries: Some(10),
        store: Some("memory".to_string()),
        valuelogfilesize: Some(2 << 30),
        p2paddr: Some(vec!["/ip4/0.0.0.0/tcp/4001".to_string()]),
        no_p2p: Some(true),
        allowed_origins: Some(vec!["http://localhost:3000".to_string()]),
        pubkeypath: Some("/path/to/pub.key".to_string()),
        privkeypath: Some("/path/to/priv.key".to_string()),
        development: Some(true),
        no_encryption: Some(true),
        no_telemetry: Some(true),
        no_signing: Some(true),
        default_key_type: Some("ed25519".to_string()),
        no_searchable_encryption: Some(true),
        identity: None, // identity is handled in Node::new, not apply_to_config
        replicator_retry_intervals: Some(vec![10, 20, 30]),
        durability: None,
    };

    let result = args.apply_to_config(&mut config);
    assert!(result.is_ok());

    assert_eq!(config.net.peers, vec!["peer1", "peer2"]);
    assert_eq!(config.datastore.max_txn_retries, 10);
    assert_eq!(config.datastore.store, DatastoreType::Memory);
    assert_eq!(config.datastore.valuelogfilesize, 2 << 30);
    assert_eq!(config.net.p2p_addresses, vec!["/ip4/0.0.0.0/tcp/4001"]);
    assert!(config.net.p2p_disabled);
    assert_eq!(config.api.allowed_origins, vec!["http://localhost:3000"]);
    assert_eq!(config.api.pubkey_path, "/path/to/pub.key");
    assert_eq!(config.api.privkey_path, "/path/to/priv.key");
    assert!(config.development);
    assert!(config.datastore.no_encryption);
    assert!(config.telemetry_disabled);
    assert!(config.datastore.no_signing);
    assert_eq!(config.datastore.default_key_type, "ed25519");
    assert!(config.datastore.no_searchable_encryption);
    assert_eq!(config.replicator_retry_intervals, vec![10, 20, 30]);
}
