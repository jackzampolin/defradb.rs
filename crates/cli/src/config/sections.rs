//! Configuration section structs

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use super::types::{
    AcpDocumentType, DatastoreType, KeyringBackend, LogFormat, LogLevel, LogOutput,
};
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

    /// Check if TLS is enabled (both pubkey_path and privkey_path are configured)
    pub fn tls_enabled(&self) -> bool {
        !self.pubkey_path.is_empty() && !self.privkey_path.is_empty()
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

/// Access Control Policy (ACP) configuration.
///
/// ACP provides two levels of access control:
/// - Node Access Control (NAC): Controls access to node-level operations
/// - Document Access Control (DAC): Controls access to individual documents
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcpConfig {
    /// Enable Node Access Control (NAC).
    ///
    /// When enabled, node operations require authentication and authorization.
    /// Default: false (all operations allowed without authentication)
    pub node_enable: bool,

    /// Document ACP type.
    ///
    /// - `none`: No document-level access control (default)
    /// - `local`: Local Zanzibar-based access control
    /// - `source-hub`: Remote SourceHub access control
    pub document_type: AcpDocumentType,
}

impl Default for AcpConfig {
    fn default() -> Self {
        Self {
            node_enable: false,
            document_type: AcpDocumentType::None,
        }
    }
}
