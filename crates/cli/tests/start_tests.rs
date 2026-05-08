//! Tests for the start command

use cli::commands::StartArgs;
use cli::config::{Config, DatastoreType};
use cli::error::Error;

fn default_start_args() -> StartArgs {
    StartArgs {
        profile: false,
        peers: None,
        max_txn_retries: None,
        store: None,
        valuelogfilesize: None,
        p2paddr: None,
        no_p2p: None,
        allowed_origins: None,
        pubkeypath: None,
        privkeypath: None,
        no_encryption: None,
        no_telemetry: None,
        no_signing: None,
        default_key_type: None,
        no_searchable_encryption: None,
        identity: None,
        replicator_retry_intervals: None,
        durability: None,
        signer_type: None,
        signer_orbis_endpoint: None,
        signer_orbis_ring_id: None,
        signer_orbis_derivation: None,
        max_body_size: None,
        max_schema_size: None,
        max_backup_size: None,
        request_timeout: None,
        max_concurrent_requests: None,
        max_msg_size: None,
        max_car_size: None,
        stream_timeout: None,
        max_p2p_tasks: None,
        max_connections_in: None,
        max_connections_out: None,
        max_connections_per_peer: None,
        p2p_rate_limit_burst: None,
        p2p_rate_limit_rate: None,
        max_merge_depth: None,
        query_timeout: None,
        p2p_transport: None,
        pg_address: None,
        acp_cache_ttl: None,
        acp_circuit_breaker_threshold: None,
        acp_circuit_breaker_reset_timeout: None,
        acp_request_timeout: None,
        acp_receipt_timeout: None,
        embedding_url: None,
        embedding_model: None,
        embedding_api_key_env: None,
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
fn test_apply_to_config_redb_store_succeeds() {
    let mut config = Config::default();
    config.datastore.store = DatastoreType::Memory; // Start with non-default
    let mut args = default_start_args();
    args.store = Some("redb".to_string());

    let result = args.apply_to_config(&mut config);
    assert!(result.is_ok());
    assert_eq!(config.datastore.store, DatastoreType::Redb);
}

#[test]
fn test_apply_to_config_all_flags() {
    let mut config = Config::default();
    let args = StartArgs {
        profile: false,
        peers: Some(vec!["peer1".to_string(), "peer2".to_string()]),
        max_txn_retries: Some(10),
        store: Some("memory".to_string()),
        valuelogfilesize: Some(2 << 30),
        p2paddr: Some(vec!["/ip4/0.0.0.0/tcp/4001".to_string()]),
        no_p2p: Some(true),
        allowed_origins: Some(vec!["http://localhost:3000".to_string()]),
        pubkeypath: Some("/path/to/pub.key".to_string()),
        privkeypath: Some("/path/to/priv.key".to_string()),
        no_encryption: Some(true),
        no_telemetry: Some(true),
        no_signing: Some(true),
        default_key_type: Some("ed25519".to_string()),
        no_searchable_encryption: Some(true),
        identity: None, // identity is handled in Node::new, not apply_to_config
        replicator_retry_intervals: Some(vec![10, 20, 30]),
        durability: None,
        signer_type: None,
        signer_orbis_endpoint: None,
        signer_orbis_ring_id: None,
        signer_orbis_derivation: None,
        max_body_size: Some(1024),
        max_schema_size: Some(2048),
        max_backup_size: Some(4096),
        request_timeout: Some(120),
        max_concurrent_requests: Some(500),
        max_msg_size: Some(32 * 1024 * 1024),
        max_car_size: Some(128 * 1024 * 1024),
        stream_timeout: Some(60),
        max_p2p_tasks: Some(128),
        max_connections_in: Some(200),
        max_connections_out: Some(800),
        max_connections_per_peer: Some(8),
        p2p_rate_limit_burst: Some(32),
        p2p_rate_limit_rate: Some(4.5),
        max_merge_depth: Some(2048),
        query_timeout: Some(45),
        p2p_transport: None,
        pg_address: None,
        acp_cache_ttl: None,
        acp_circuit_breaker_threshold: None,
        acp_circuit_breaker_reset_timeout: None,
        acp_request_timeout: None,
        acp_receipt_timeout: None,
        embedding_url: Some("http://localhost:11434/v1".to_string()),
        embedding_model: Some("nomic-embed-text".to_string()),
        embedding_api_key_env: Some("CUSTOM_EMBEDDING_KEY".to_string()),
    };

    let result = args.apply_to_config(&mut config);
    assert!(result.is_ok());

    assert_eq!(config.net.max_msg_size, 32 * 1024 * 1024);
    assert_eq!(config.net.max_car_size, 128 * 1024 * 1024);
    assert_eq!(config.net.stream_timeout, 60);
    assert_eq!(config.net.max_p2p_tasks, 128);
    assert_eq!(config.net.max_connections_in, 200);
    assert_eq!(config.net.max_connections_out, 800);
    assert_eq!(config.net.max_connections_per_peer, 8);
    assert_eq!(config.net.p2p_rate_limit_burst, 32);
    assert_eq!(config.net.p2p_rate_limit_rate, 4.5);
    assert_eq!(config.datastore.max_merge_depth, 2048);
    assert_eq!(config.api.max_body_size, 1024);
    assert_eq!(config.api.max_schema_size, 2048);
    assert_eq!(config.api.max_backup_size, 4096);
    assert_eq!(config.api.request_timeout, 120);
    assert_eq!(config.api.max_concurrent_requests, 500);
    assert_eq!(config.net.peers, vec!["peer1", "peer2"]);
    assert_eq!(config.datastore.max_txn_retries, 10);
    assert_eq!(config.datastore.store, DatastoreType::Memory);
    assert_eq!(config.datastore.valuelogfilesize, 2 << 30);
    assert_eq!(config.net.p2p_addresses, vec!["/ip4/0.0.0.0/tcp/4001"]);
    assert!(config.net.p2p_disabled);
    assert_eq!(config.api.allowed_origins, vec!["http://localhost:3000"]);
    assert_eq!(config.api.pubkey_path, "/path/to/pub.key");
    assert_eq!(config.api.privkey_path, "/path/to/priv.key");
    assert!(config.datastore.no_encryption);
    assert!(config.telemetry_disabled);
    assert!(config.datastore.no_signing);
    assert_eq!(config.datastore.default_key_type, "ed25519");
    assert!(config.datastore.no_searchable_encryption);
    assert_eq!(config.replicator_retry_intervals, vec![10, 20, 30]);
    assert_eq!(config.api.query_timeout, 45);
    assert_eq!(config.embedding.url, "http://localhost:11434/v1");
    assert_eq!(config.embedding.model, "nomic-embed-text");
    assert_eq!(config.embedding.api_key_env, "CUSTOM_EMBEDDING_KEY");
}
