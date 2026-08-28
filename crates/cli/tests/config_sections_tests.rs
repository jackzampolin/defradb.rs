//! Tests for configuration section structs

use cli::config::{
    ApiConfig, DatastoreConfig, DatastoreType, KeyringBackend, KeyringConfig, LogConfig, LogFormat,
    LogLevel, LogOutput, NetConfig,
};
use cli::error::Error;
use storage::backends::DurabilityMode;

#[test]
fn test_log_config_defaults() {
    let config = LogConfig::default();
    assert_eq!(config.level, LogLevel::Info);
    assert_eq!(config.output, LogOutput::Stderr);
    assert_eq!(config.format, LogFormat::Text);
    assert!(!config.stacktrace);
    assert!(!config.source);
    assert!(!config.color_disabled);
    assert!(config.overrides.is_empty());
}

#[test]
fn test_api_config_defaults() {
    let config = ApiConfig::default();
    assert_eq!(config.address, "127.0.0.1:9181");
    assert!(config.allowed_origins.is_empty());
    assert!(config.pubkey_path.is_empty());
    assert!(config.privkey_path.is_empty());
    assert_eq!(config.query_max_depth, query::DEFAULT_MAX_QUERY_DEPTH);
    assert_eq!(config.query_max_width, query::DEFAULT_MAX_QUERY_WIDTH);
    assert_eq!(
        config.query_max_filter_depth,
        query::DEFAULT_MAX_FILTER_DEPTH
    );
}

#[test]
fn test_datastore_config_defaults() {
    let config = DatastoreConfig::default();
    assert_eq!(config.store, DatastoreType::Regolith);
    assert_eq!(config.path, "data");
    assert_eq!(config.max_txn_retries, 5);
    assert_eq!(config.valuelogfilesize, None);
    assert!(!config.no_encryption);
    assert!(!config.no_signing);
    assert_eq!(config.default_key_type, "secp256k1");
    assert_eq!(config.durability, DurabilityMode::Immediate);
}

#[test]
fn test_net_config_defaults() {
    let config = NetConfig::default();
    assert!(!config.p2p_disabled);
    assert_eq!(config.p2p_addresses, vec!["/ip4/127.0.0.1/tcp/9171"]);
    assert!(config.peers.is_empty());
    assert!(config.pubsub_enabled);
    assert!(config.relay_enabled);
}

#[test]
fn test_keyring_config_defaults() {
    let config = KeyringConfig::default();
    assert_eq!(config.backend, KeyringBackend::File);
    assert_eq!(config.path, "keys");
    assert_eq!(config.namespace, "defradb");
    assert!(!config.disabled);
}

#[test]
fn test_api_config_validate_valid_address() {
    let config = ApiConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_api_config_validate_invalid_address() {
    let config = ApiConfig {
        address: "not-an-address".to_string(),
        ..Default::default()
    };
    let result = config.validate();
    assert!(matches!(result, Err(Error::InvalidApiAddress(addr, _)) if addr == "not-an-address"));
}

#[test]
fn test_api_config_validate_incomplete_tls_pubkey_only() {
    let config = ApiConfig {
        pubkey_path: "/path/to/pub.key".to_string(),
        ..Default::default()
    };
    let result = config.validate();
    assert!(matches!(result, Err(Error::IncompleteTlsConfig)));
}

#[test]
fn test_api_config_validate_incomplete_tls_privkey_only() {
    let config = ApiConfig {
        privkey_path: "/path/to/priv.key".to_string(),
        ..Default::default()
    };
    let result = config.validate();
    assert!(matches!(result, Err(Error::IncompleteTlsConfig)));
}

#[test]
fn test_api_config_validate_complete_tls() {
    let config = ApiConfig {
        pubkey_path: "/path/to/pub.key".to_string(),
        privkey_path: "/path/to/priv.key".to_string(),
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_api_config_tls_enabled_false_by_default() {
    let config = ApiConfig::default();
    assert!(!config.tls_enabled());
}

#[test]
fn test_api_config_tls_enabled_true_when_configured() {
    let config = ApiConfig {
        pubkey_path: "/path/to/pub.key".to_string(),
        privkey_path: "/path/to/priv.key".to_string(),
        ..Default::default()
    };
    assert!(config.tls_enabled());
}

#[test]
fn test_api_config_tls_enabled_false_partial_config() {
    let config = ApiConfig {
        pubkey_path: "/path/to/pub.key".to_string(),
        ..Default::default()
    };
    assert!(!config.tls_enabled());
}

#[test]
fn test_net_config_validate_valid_multiaddr() {
    let config = NetConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_net_config_validate_invalid_multiaddr() {
    let config = NetConfig {
        p2p_addresses: vec!["not-a-multiaddr".to_string()],
        ..Default::default()
    };
    let result = config.validate();
    assert!(matches!(result, Err(Error::InvalidMultiaddr(_))));
}

#[test]
fn test_net_config_validate_skipped_when_p2p_disabled() {
    let config = NetConfig {
        p2p_disabled: true,
        p2p_addresses: vec!["not-a-multiaddr".to_string()],
        ..Default::default()
    };
    assert!(config.validate().is_ok());
}

/// Every node started on a pre-`Option<u64>` binary had the old 1 GiB default
/// serialized into its own `config.yaml` by `create_if_missing`. Reading it
/// back would apply a 16x compaction-target change to regolith and rocksdb for an
/// operator who never passed the flag, so the key must not deserialize at all.
#[test]
fn legacy_valuelogfilesize_in_a_config_file_is_ignored() {
    let yaml = r#"
store: regolith
path: data
max_txn_retries: 5
valuelogfilesize: 1073741824
no_encryption: false
no_searchable_encryption: false
no_signing: false
default_key_type: secp256k1
"#;
    let config: DatastoreConfig = serde_yaml::from_str(yaml).expect("legacy config still loads");
    assert_eq!(
        config.valuelogfilesize, None,
        "a value written by an older binary must not be mistaken for an operator setting it"
    );
}

/// And the key must no longer be written, so the ambiguity cannot recur.
#[test]
fn valuelogfilesize_is_not_serialized_into_generated_config() {
    let yaml = serde_yaml::to_string(&DatastoreConfig::default()).expect("serializes");
    assert!(
        !yaml.contains("valuelogfilesize"),
        "generated config.yaml must not carry the key:\n{yaml}"
    );
}
