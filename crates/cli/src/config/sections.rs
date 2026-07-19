//! Configuration section structs

use std::net::SocketAddr;

use db::{DEFAULT_TRANSACTION_CLEANUP_INTERVAL, DEFAULT_TRANSACTION_IDLE_TIMEOUT};
use serde::{Deserialize, Serialize};

use query::{DEFAULT_MAX_FILTER_DEPTH, DEFAULT_MAX_QUERY_DEPTH, DEFAULT_MAX_QUERY_WIDTH};
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
    /// Max idle age for explicit HTTP transactions in seconds (0 = disabled). Default: 600.
    #[serde(default = "default_transaction_idle_timeout")]
    pub transaction_idle_timeout: u64,
    /// Interval between explicit HTTP transaction cleanup sweeps in seconds. Default: 60.
    #[serde(default = "default_transaction_cleanup_interval")]
    pub transaction_cleanup_interval: u64,
    /// Max GraphQL selection nesting depth (0 = unlimited). Default: 20.
    #[serde(default = "default_query_max_depth")]
    pub query_max_depth: usize,
    /// Max fields at any GraphQL selection level (0 = unlimited). Default: 100.
    #[serde(default = "default_query_max_width")]
    pub query_max_width: usize,
    /// Max recursive filter nesting depth (0 = unlimited). Default: 50.
    #[serde(default = "default_query_max_filter_depth")]
    pub query_max_filter_depth: usize,
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

fn default_transaction_idle_timeout() -> u64 {
    DEFAULT_TRANSACTION_IDLE_TIMEOUT.as_secs()
}

fn default_transaction_cleanup_interval() -> u64 {
    DEFAULT_TRANSACTION_CLEANUP_INTERVAL.as_secs()
}

fn default_query_max_depth() -> usize {
    DEFAULT_MAX_QUERY_DEPTH
}

fn default_query_max_width() -> usize {
    DEFAULT_MAX_QUERY_WIDTH
}

fn default_query_max_filter_depth() -> usize {
    DEFAULT_MAX_FILTER_DEPTH
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
            transaction_idle_timeout: default_transaction_idle_timeout(),
            transaction_cleanup_interval: default_transaction_cleanup_interval(),
            query_max_depth: default_query_max_depth(),
            query_max_width: default_query_max_width(),
            query_max_filter_depth: default_query_max_filter_depth(),
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

        if self.transaction_idle_timeout > 0 && self.transaction_cleanup_interval == 0 {
            return Err(Error::InvalidConfig(
                "api.transaction_cleanup_interval must be > 0 when transaction_idle_timeout is enabled"
                    .to_string(),
            ));
        }

        Ok(())
    }

    /// Check if TLS is enabled (both pubkey_path and privkey_path are configured)
    pub fn tls_enabled(&self) -> bool {
        !self.pubkey_path.is_empty() && !self.privkey_path.is_empty()
    }
}

/// Embedding configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub model: String,
    #[serde(default = "default_embedding_api_key_env")]
    pub api_key_env: String,
}

fn default_embedding_api_key_env() -> String {
    "OPENAI_API_KEY".to_string()
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            model: String::new(),
            api_key_env: default_embedding_api_key_env(),
        }
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
    /// - `immediate` (default): fsync on every commit, safe against OS crashes
    /// - `eventual`: defer fsync to OS for higher write throughput
    #[serde(default)]
    pub durability: DurabilityMode,
    /// Max DAG recursion depth for merge operations. Default: 1024.
    #[serde(default = "default_max_merge_depth")]
    pub max_merge_depth: usize,
    /// Enable transparent at-rest value encryption for the storage backend.
    ///
    /// When enabled, all stored values are encrypted with AES-256-GCM keyed by
    /// the keyring `encryption-key` (generated on first use if absent). Keys
    /// remain plaintext to preserve prefix/range iteration. Default: false.
    /// Once enabled for a store, it must stay enabled: reading without the
    /// matching key fails loudly rather than returning garbage.
    #[serde(default)]
    pub at_rest_encryption: bool,
}

fn default_max_merge_depth() -> usize {
    1024
}

impl Default for DatastoreConfig {
    fn default() -> Self {
        Self {
            store: DatastoreType::Lark,
            path: "data".to_string(),
            max_txn_retries: 5,
            valuelogfilesize: 1 << 30, // 1GB
            no_encryption: false,
            no_searchable_encryption: false,
            no_signing: false,
            default_key_type: "secp256k1".to_string(),
            durability: DurabilityMode::default(),
            max_merge_depth: default_max_merge_depth(),
            at_rest_encryption: false,
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
    /// Enable libp2p relay client support. Default: true.
    #[serde(default = "default_true")]
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
    /// P2P established connection low watermark. Default: 100.
    #[serde(default = "default_connection_manager_low_water")]
    pub connection_manager_low_water: u32,
    /// P2P established connection high watermark. Default: 400.
    #[serde(default = "default_connection_manager_high_water")]
    pub connection_manager_high_water: u32,
    /// P2P connection manager grace period in milliseconds. Default: 20000.
    #[serde(default = "default_connection_manager_grace_period_ms")]
    pub connection_manager_grace_period_ms: u64,
    /// Max established connections per peer. Default: 4.
    #[serde(default = "default_max_connections_per_peer")]
    pub max_connections_per_peer: u32,
    #[serde(default)]
    pub transport: TransportType,
    /// Custom relay URL for iroh transport (overrides default N0 relay).
    #[serde(default)]
    pub iroh_relay_url: Option<String>,
    /// Relay mode for iroh transport: "default", "disabled", or "custom".
    #[serde(default)]
    pub iroh_relay_mode: Option<String>,
    /// Custom relay URLs for iroh transport.
    #[serde(default)]
    pub iroh_relay_urls: Vec<String>,
    /// Enable DNS-based peer discovery for iroh transport (default: true).
    #[serde(default = "default_true")]
    pub iroh_discovery: bool,
    /// Custom DNS origin domain for iroh discovery.
    #[serde(default)]
    pub iroh_discovery_origin_domain: Option<String>,
    /// Custom pkarr relay URL for iroh discovery publishing.
    #[serde(default)]
    pub iroh_pkarr_relay_url: Option<String>,
    /// Fixed UDP bind port for iroh transport. None = ephemeral (OS-assigned).
    #[serde(default)]
    pub iroh_bind_port: Option<u16>,
    /// Bind iroh to a specific IP address. Prevents advertising unreachable
    /// LAN addresses to peers on different networks. None = 0.0.0.0 (all interfaces).
    #[serde(default)]
    pub iroh_bind_addr: Option<std::net::IpAddr>,
    /// Per-peer rate limit burst capacity (max tokens in bucket). Default: 500.
    #[serde(default = "default_rate_limit_burst")]
    pub p2p_rate_limit_burst: u32,
    /// Per-peer rate limit refill rate (tokens per second). Default: 50.
    #[serde(default = "default_rate_limit_rate")]
    pub p2p_rate_limit_rate: f64,
    /// Max document IDs accepted in one DocSync request. Default: 1000.
    #[serde(default = "default_max_doc_sync_request_doc_ids")]
    pub p2p_max_doc_sync_request_doc_ids: usize,
    /// Max pending-DAG registrations held while Bitswap completes missing
    /// links; overflow is nacked back to the pusher. Each source peer may use
    /// at most one quarter of this capacity. Default: 1000.
    #[serde(default = "default_max_pending_dags")]
    pub p2p_max_pending_dags: usize,
    /// Max queued outbound push jobs; overflow defers to the persisted retry
    /// ladder. Default: 1024.
    #[serde(default = "default_push_queue_capacity")]
    pub p2p_push_queue_capacity: usize,
    /// Max resident bytes across queued outbound push jobs. Default: 32 MiB.
    #[serde(default = "default_push_queue_byte_capacity")]
    pub p2p_push_queue_byte_capacity: usize,
    /// Max outbound push jobs concurrently in flight to one peer. Default: 4.
    #[serde(default = "default_max_active_pushes_per_peer")]
    pub p2p_max_active_pushes_per_peer: usize,
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
fn default_connection_manager_low_water() -> u32 {
    100
}
fn default_connection_manager_high_water() -> u32 {
    400
}
fn default_connection_manager_grace_period_ms() -> u64 {
    20_000
}
fn default_max_connections_per_peer() -> u32 {
    4
}
fn default_rate_limit_burst() -> u32 {
    500
}
fn default_rate_limit_rate() -> f64 {
    50.0
}
fn default_max_doc_sync_request_doc_ids() -> usize {
    p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS
}
fn default_max_pending_dags() -> usize {
    p2p::sync::DEFAULT_MAX_PENDING_DAGS
}
fn default_push_queue_capacity() -> usize {
    p2p::sync::DEFAULT_PUSH_QUEUE_CAPACITY
}
fn default_push_queue_byte_capacity() -> usize {
    p2p::sync::DEFAULT_PUSH_QUEUE_BYTE_CAPACITY
}
fn default_max_active_pushes_per_peer() -> usize {
    p2p::sync::DEFAULT_MAX_ACTIVE_PUSHES_PER_PEER
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
            relay_enabled: true,
            max_msg_size: default_max_msg_size(),
            max_car_size: default_max_car_size(),
            stream_timeout: default_stream_timeout(),
            max_p2p_tasks: default_max_p2p_tasks(),
            connection_manager_low_water: default_connection_manager_low_water(),
            connection_manager_high_water: default_connection_manager_high_water(),
            connection_manager_grace_period_ms: default_connection_manager_grace_period_ms(),
            max_connections_per_peer: default_max_connections_per_peer(),
            transport: TransportType::default(),
            iroh_relay_url: None,
            iroh_relay_mode: None,
            iroh_relay_urls: Vec::new(),
            iroh_discovery: true,
            iroh_discovery_origin_domain: None,
            iroh_pkarr_relay_url: None,
            iroh_bind_port: None,
            iroh_bind_addr: None,
            p2p_rate_limit_burst: default_rate_limit_burst(),
            p2p_rate_limit_rate: default_rate_limit_rate(),
            p2p_max_doc_sync_request_doc_ids: default_max_doc_sync_request_doc_ids(),
            p2p_max_pending_dags: default_max_pending_dags(),
            p2p_push_queue_capacity: default_push_queue_capacity(),
            p2p_push_queue_byte_capacity: default_push_queue_byte_capacity(),
            p2p_max_active_pushes_per_peer: default_max_active_pushes_per_peer(),
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

    /// Circuit breaker failure threshold before tripping. Default: 3.
    #[serde(default = "default_acp_cb_threshold")]
    pub circuit_breaker_threshold: u32,

    /// Circuit breaker reset timeout in seconds. Default: 30.
    #[serde(default = "default_acp_cb_reset_timeout")]
    pub circuit_breaker_reset_timeout: u64,

    /// Request timeout in seconds for SourceHub/hub.rs network calls. Default: 5.
    #[serde(default = "default_acp_request_timeout")]
    pub request_timeout: u64,

    /// Policy cache TTL in seconds. Default: 300.
    #[serde(default = "default_acp_cache_ttl")]
    pub cache_ttl: u64,

    /// Receipt polling timeout in seconds for hub.rs transactions. Default: 30.
    #[serde(default = "default_acp_receipt_timeout")]
    pub receipt_timeout: u64,
}

fn default_acp_cb_threshold() -> u32 {
    3
}
fn default_acp_cb_reset_timeout() -> u64 {
    30
}
fn default_acp_request_timeout() -> u64 {
    5
}
fn default_acp_cache_ttl() -> u64 {
    300
}
fn default_acp_receipt_timeout() -> u64 {
    30
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
            circuit_breaker_threshold: default_acp_cb_threshold(),
            circuit_breaker_reset_timeout: default_acp_cb_reset_timeout(),
            request_timeout: default_acp_request_timeout(),
            cache_ttl: default_acp_cache_ttl(),
            receipt_timeout: default_acp_receipt_timeout(),
        }
    }
}

impl AcpConfig {
    /// Validate ACP tuning parameters.
    pub fn validate(&self) -> Result<()> {
        if self.circuit_breaker_threshold == 0 {
            return Err(Error::InvalidConfig(
                "acp_circuit_breaker_threshold must be > 0: \
                 a zero threshold means the circuit breaker trips immediately"
                    .into(),
            ));
        }
        if self.circuit_breaker_reset_timeout == 0 {
            return Err(Error::InvalidConfig(
                "acp_circuit_breaker_reset_timeout must be > 0: \
                 a zero reset timeout means the circuit breaker never recovers"
                    .into(),
            ));
        }
        if self.request_timeout == 0 {
            return Err(Error::InvalidConfig(
                "acp_request_timeout must be > 0: \
                 a zero timeout disables the deadline and requests may hang indefinitely"
                    .into(),
            ));
        }
        if self.cache_ttl == 0 {
            return Err(Error::InvalidConfig(
                "acp_cache_ttl must be > 0: \
                 a zero TTL means every policy lookup bypasses the cache entirely"
                    .into(),
            ));
        }
        if self.receipt_timeout == 0 {
            return Err(Error::InvalidConfig(
                "acp_receipt_timeout must be > 0: \
                 a zero timeout means hub.rs transaction receipts are never awaited"
                    .into(),
            ));
        }
        Ok(())
    }
}
