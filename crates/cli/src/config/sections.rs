//! Configuration section structs

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use storage::backends::DurabilityMode;

use super::types::{
    AcpDocumentType, DatastoreType, KeyringBackend, LogFormat, LogLevel, LogOutput, TransportType,
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
    /// Max request body size in bytes (0 = unlimited). Default: 0 (no limit).
    #[serde(default)]
    pub max_body_size: u64,
    /// Max schema request body size in bytes (0 = unlimited). Default: 0 (no limit).
    #[serde(default)]
    pub max_schema_size: u64,
    /// Max backup import body size in bytes (0 = unlimited). Default: 0 (no limit).
    #[serde(default)]
    pub max_backup_size: u64,
    /// Request timeout in seconds (0 = no timeout). Default: 300 (5 minutes).
    #[serde(default = "default_request_timeout")]
    pub request_timeout: u64,
    /// Max concurrent requests (0 = unlimited). Default: 1000.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_requests: usize,
    /// Query execution timeout in seconds (0 = no timeout). Default: 30.
    #[serde(default = "default_query_timeout")]
    pub query_timeout: u64,
    /// Postgres wire protocol address (empty = disabled). Default: "" (disabled).
    #[serde(default)]
    pub pg_address: String,
}

fn default_request_timeout() -> u64 {
    300
}

fn default_max_concurrent() -> usize {
    1000
}

fn default_query_timeout() -> u64 {
    30
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:9181".to_string(),
            allowed_origins: Vec::new(),
            pubkey_path: String::new(),
            privkey_path: String::new(),
            max_body_size: 0,
            max_schema_size: 0,
            max_backup_size: 0,
            request_timeout: default_request_timeout(),
            max_concurrent_requests: default_max_concurrent(),
            query_timeout: default_query_timeout(),
            pg_address: String::new(),
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
    /// Durability mode for the storage backend.
    ///
    /// - `eventual` (default): defer fsync to OS
    /// - `immediate`: fsync on every commit — safe against OS crashes
    #[serde(default)]
    pub durability: DurabilityMode,
    /// Max DAG recursion depth for merge operations. Default: 1024.
    #[serde(default = "default_max_merge_depth")]
    pub max_merge_depth: usize,
}

fn default_max_merge_depth() -> usize {
    1024
}

impl Default for DatastoreConfig {
    fn default() -> Self {
        Self {
            store: DatastoreType::Redb,
            path: "data".to_string(),
            max_txn_retries: 5,
            valuelogfilesize: 1 << 30, // 1GB
            no_encryption: false,
            no_searchable_encryption: false,
            no_signing: false,
            default_key_type: "secp256k1".to_string(),
            durability: DurabilityMode::default(),
            max_merge_depth: default_max_merge_depth(),
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
    /// Max P2P protocol message size in bytes. Default: 16 MiB.
    #[serde(default = "default_max_msg_size")]
    pub max_msg_size: u64,
    /// Max P2P CAR file size in bytes. Default: 64 MiB.
    #[serde(default = "default_max_car_size")]
    pub max_car_size: u64,
    /// P2P stream read timeout in seconds. Default: 30.
    #[serde(default = "default_stream_timeout")]
    pub stream_timeout: u64,
    /// Max concurrent P2P stream handler tasks. Default: 64.
    #[serde(default = "default_max_p2p_tasks")]
    pub max_p2p_tasks: usize,
    /// Max established inbound P2P connections. Default: 100.
    #[serde(default = "default_max_connections_in")]
    pub max_connections_in: u32,
    /// Max established outbound P2P connections. Default: 400.
    #[serde(default = "default_max_connections_out")]
    pub max_connections_out: u32,
    /// Max established connections per peer. Default: 4.
    #[serde(default = "default_max_connections_per_peer")]
    pub max_connections_per_peer: u32,
    #[serde(default)]
    pub transport: TransportType,
    /// Custom relay URL for iroh transport (overrides default N0 relay).
    #[serde(default)]
    pub iroh_relay_url: Option<String>,
    /// Enable DNS-based peer discovery for iroh transport (default: true).
    #[serde(default = "default_true")]
    pub iroh_discovery: bool,
    /// Fixed UDP bind port for iroh transport. None = ephemeral (OS-assigned).
    #[serde(default)]
    pub iroh_bind_port: Option<u16>,
}

fn default_max_msg_size() -> u64 {
    16 * 1024 * 1024
}
fn default_max_car_size() -> u64 {
    64 * 1024 * 1024
}
fn default_stream_timeout() -> u64 {
    30
}
fn default_max_p2p_tasks() -> usize {
    64
}
fn default_max_connections_in() -> u32 {
    100
}
fn default_max_connections_out() -> u32 {
    400
}
fn default_max_connections_per_peer() -> u32 {
    4
}
fn default_true() -> bool {
    true
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            p2p_disabled: false,
            p2p_addresses: vec!["/ip4/127.0.0.1/tcp/9171".to_string()],
            peers: Vec::new(),
            pubsub_enabled: true,
            relay_enabled: false,
            max_msg_size: default_max_msg_size(),
            max_car_size: default_max_car_size(),
            stream_timeout: default_stream_timeout(),
            max_p2p_tasks: default_max_p2p_tasks(),
            max_connections_in: default_max_connections_in(),
            max_connections_out: default_max_connections_out(),
            max_connections_per_peer: default_max_connections_per_peer(),
            transport: TransportType::default(),
            iroh_relay_url: None,
            iroh_discovery: true,
            iroh_bind_port: None,
        }
    }
}

impl NetConfig {
    /// Validate the network configuration
    pub fn validate(&self) -> Result<()> {
        if self.p2p_disabled {
            return Ok(());
        }

        // Multiaddr validation only applies to libp2p transport
        if self.transport == TransportType::Libp2p {
            for addr_str in &self.p2p_addresses {
                addr_str
                    .parse::<p2p::Multiaddr>()
                    .map_err(|e| Error::InvalidMultiaddr(format!("{}: {}", addr_str, e)))?;
            }
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

    /// SourceHub gRPC/LCD endpoint (e.g., "http://localhost:1317")
    #[serde(default)]
    pub sourcehub_address: String,

    /// SourceHub CometBFT RPC endpoint (e.g., "http://localhost:26657")
    #[serde(default)]
    pub sourcehub_comet_address: String,

    /// SourceHub chain ID (e.g., "sourcehub-test")
    #[serde(default)]
    pub sourcehub_chain_id: String,

    /// hub.rs JSON-RPC endpoint (e.g., "http://localhost:8545")
    #[serde(default)]
    pub hub_rs_address: String,
}

impl Default for AcpConfig {
    fn default() -> Self {
        Self {
            node_enable: false,
            document_type: AcpDocumentType::None,
            sourcehub_address: String::new(),
            sourcehub_comet_address: String::new(),
            sourcehub_chain_id: String::new(),
            hub_rs_address: String::new(),
        }
    }
}
