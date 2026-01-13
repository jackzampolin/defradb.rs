// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Configuration section structs

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use super::types::{DatastoreType, KeyringBackend, LogFormat, LogLevel, LogOutput};
use crate::error::{Error, Result};

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    pub level: LogLevel,
    pub output: LogOutput,
    pub format: LogFormat,
    pub stacktrace: bool,
    pub source: bool,
    pub color_disabled: bool,
    #[serde(default)]
    pub overrides: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            output: LogOutput::Stderr,
            format: LogFormat::Text,
            stacktrace: false,
            source: false,
            color_disabled: false,
            overrides: String::new(),
        }
    }
}

/// API configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub address: String,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub pubkey_path: String,
    #[serde(default)]
    pub privkey_path: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:9181".to_string(),
            allowed_origins: Vec::new(),
            pubkey_path: String::new(),
            privkey_path: String::new(),
        }
    }
}

impl ApiConfig {
    /// Validate the API configuration
    pub fn validate(&self) -> Result<()> {
        self.address
            .parse::<SocketAddr>()
            .map_err(|e| Error::InvalidApiAddress(self.address.clone(), e.to_string()))?;

        let has_pub = !self.pubkey_path.is_empty();
        let has_priv = !self.privkey_path.is_empty();
        if has_pub != has_priv {
            return Err(Error::IncompleteTlsConfig);
        }

        Ok(())
    }
}

/// Datastore configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatastoreConfig {
    pub store: DatastoreType,
    pub path: String,
    pub max_txn_retries: u32,
    pub valuelogfilesize: u64,
    pub no_encryption: bool,
    pub no_searchable_encryption: bool,
    pub no_signing: bool,
    pub default_key_type: String,
}

impl Default for DatastoreConfig {
    fn default() -> Self {
        Self {
            store: DatastoreType::Badger,
            path: "data".to_string(),
            max_txn_retries: 5,
            valuelogfilesize: 1 << 30, // 1GB
            no_encryption: false,
            no_searchable_encryption: false,
            no_signing: false,
            default_key_type: "secp256k1".to_string(),
        }
    }
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetConfig {
    pub p2p_disabled: bool,
    pub p2p_addresses: Vec<String>,
    #[serde(default)]
    pub peers: Vec<String>,
    pub pubsub_enabled: bool,
    pub relay_enabled: bool,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            p2p_disabled: false,
            p2p_addresses: vec!["/ip4/127.0.0.1/tcp/9171".to_string()],
            peers: Vec::new(),
            pubsub_enabled: true,
            relay_enabled: false,
        }
    }
}

impl NetConfig {
    /// Validate the network configuration
    pub fn validate(&self) -> Result<()> {
        if self.p2p_disabled {
            return Ok(());
        }

        for addr_str in &self.p2p_addresses {
            addr_str
                .parse::<p2p::Multiaddr>()
                .map_err(|e| Error::InvalidMultiaddr(format!("{}: {}", addr_str, e)))?;
        }

        Ok(())
    }
}

/// Keyring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyringConfig {
    pub backend: KeyringBackend,
    pub path: String,
    pub namespace: String,
    pub disabled: bool,
}

impl Default for KeyringConfig {
    fn default() -> Self {
        Self {
            backend: KeyringBackend::File,
            path: "keys".to_string(),
            namespace: "defradb".to_string(),
            disabled: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[test]
    fn test_datastore_config_defaults() {
        let config = DatastoreConfig::default();
        assert_eq!(config.store, DatastoreType::Badger);
        assert_eq!(config.path, "data");
        assert_eq!(config.max_txn_retries, 5);
        assert_eq!(config.valuelogfilesize, 1 << 30);
        assert!(!config.no_encryption);
        assert!(!config.no_signing);
        assert_eq!(config.default_key_type, "secp256k1");
    }

    #[test]
    fn test_net_config_defaults() {
        let config = NetConfig::default();
        assert!(!config.p2p_disabled);
        assert_eq!(config.p2p_addresses, vec!["/ip4/127.0.0.1/tcp/9171"]);
        assert!(config.peers.is_empty());
        assert!(config.pubsub_enabled);
        assert!(!config.relay_enabled);
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
        let mut config = ApiConfig::default();
        config.address = "not-an-address".to_string();
        let result = config.validate();
        assert!(matches!(result, Err(Error::InvalidApiAddress(addr, _)) if addr == "not-an-address"));
    }

    #[test]
    fn test_api_config_validate_incomplete_tls_pubkey_only() {
        let mut config = ApiConfig::default();
        config.pubkey_path = "/path/to/pub.key".to_string();
        let result = config.validate();
        assert!(matches!(result, Err(Error::IncompleteTlsConfig)));
    }

    #[test]
    fn test_api_config_validate_incomplete_tls_privkey_only() {
        let mut config = ApiConfig::default();
        config.privkey_path = "/path/to/priv.key".to_string();
        let result = config.validate();
        assert!(matches!(result, Err(Error::IncompleteTlsConfig)));
    }

    #[test]
    fn test_api_config_validate_complete_tls() {
        let mut config = ApiConfig::default();
        config.pubkey_path = "/path/to/pub.key".to_string();
        config.privkey_path = "/path/to/priv.key".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_net_config_validate_valid_multiaddr() {
        let config = NetConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_net_config_validate_invalid_multiaddr() {
        let mut config = NetConfig::default();
        config.p2p_addresses = vec!["not-a-multiaddr".to_string()];
        let result = config.validate();
        assert!(matches!(result, Err(Error::InvalidMultiaddr(_))));
    }

    #[test]
    fn test_net_config_validate_skipped_when_p2p_disabled() {
        let mut config = NetConfig::default();
        config.p2p_disabled = true;
        config.p2p_addresses = vec!["not-a-multiaddr".to_string()];
        assert!(config.validate().is_ok());
    }
}
