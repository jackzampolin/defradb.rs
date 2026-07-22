//! Configuration types for the embedded DefraDB node.

#[cfg(feature = "http")]
use std::time::Duration;

#[cfg(feature = "http")]
use db::{DEFAULT_TRANSACTION_CLEANUP_INTERVAL, DEFAULT_TRANSACTION_IDLE_TIMEOUT};

/// Document ACP configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum DocumentAcpConfig {
    #[default]
    Local,
    SourceHub(SourceHubConfig),
}

/// SourceHub document ACP configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceHubConfig {
    pub grpc_address: String,
    pub comet_rpc_address: String,
    pub chain_id: String,
    pub signer_key: Vec<u8>,
}

/// Configuration for the optional HTTP GraphQL server.
#[cfg(feature = "http")]
pub struct HttpConfig {
    pub address: std::net::SocketAddr,
    pub(crate) request_timeout: Duration,
    pub(crate) transaction_idle_timeout: Duration,
    pub(crate) transaction_cleanup_interval: Duration,
    pub(crate) extra_routes: Option<axum::Router>,
}

#[cfg(feature = "http")]
impl HttpConfig {
    pub fn new(port: u16) -> Self {
        Self::with_addr(std::net::SocketAddr::from(([127, 0, 0, 1], port)))
    }

    pub fn with_addr(addr: impl Into<std::net::SocketAddr>) -> Self {
        Self {
            address: addr.into(),
            request_timeout: Duration::from_secs(
                defra_http::ServerConfig::default().request_timeout,
            ),
            transaction_idle_timeout: DEFAULT_TRANSACTION_IDLE_TIMEOUT,
            transaction_cleanup_interval: DEFAULT_TRANSACTION_CLEANUP_INTERVAL,
            extra_routes: None,
        }
    }

    /// Set the HTTP request timeout.
    ///
    /// `Duration::ZERO` disables request timeouts.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the max idle age for explicit HTTP transactions.
    ///
    /// `Duration::ZERO` disables idle transaction cleanup.
    pub fn with_transaction_idle_timeout(mut self, timeout: Duration) -> Self {
        self.transaction_idle_timeout = timeout;
        self
    }

    /// Set the interval between explicit HTTP transaction cleanup sweeps.
    ///
    /// Must be non-zero when transaction idle cleanup is enabled.
    pub fn with_transaction_cleanup_interval(mut self, interval: Duration) -> Self {
        self.transaction_cleanup_interval = interval;
        self
    }

    pub fn with_extra_routes(mut self, extra: axum::Router) -> Self {
        self.extra_routes = Some(extra);
        self
    }
}

/// Configuration for the optional P2P networking layer (IROH/QUIC).
#[cfg(feature = "p2p")]
pub struct P2PConfig {
    /// UDP port for QUIC listener.
    pub port: u16,
    /// Bind to a specific IP address. When set, IROH only listens on this
    /// interface -- use the Tailscale IP to keep P2P within the mesh and
    /// prevent IROH from advertising unreachable LAN addresses across sites.
    /// None = 0.0.0.0 (all interfaces).
    pub bind_addr: Option<std::net::IpAddr>,
    /// Relay behavior for NAT traversal.
    pub relay_mode: p2p::iroh::IrohRelayModeConfig,
    /// Address publishing / lookup behavior.
    pub discovery: p2p::iroh::IrohDiscoveryConfig,
    /// Maximum concurrent QUIC paths per connection. None keeps iroh's default.
    pub max_concurrent_multipath_paths: Option<u32>,
    /// Path to persist secret key. None = ephemeral (new identity each restart).
    pub secret_key_path: Option<std::path::PathBuf>,
    /// Reload collection subscriptions persisted in the local store on startup.
    /// When false, only explicit subscribe calls in the current process take effect.
    pub load_persisted_collections: bool,
    /// Maximum concurrent DAG fetch tasks. Lower values reduce resource pressure
    /// on constrained clients (mobile, embedded). Default: 4.
    pub max_concurrent_dag_fetches: usize,
    /// Maximum concurrent push tasks for sending blocks to replicators.
    /// Default: 8.
    pub max_concurrent_push_tasks: usize,
    /// Maximum document IDs accepted in a single DocSync request. Default: 1000.
    pub max_doc_sync_request_doc_ids: usize,
    /// Per-peer rate limit burst capacity (max tokens in bucket). Default: 500.
    pub rate_limit_burst: u32,
    /// Per-peer rate limit refill rate (tokens per second). Default: 50.
    pub rate_limit_rate: f64,
    /// Maximum pending-DAG registrations held while Bitswap completes missing
    /// links; overflow is nacked back to the pusher. Each source peer may use
    /// at most one quarter of this capacity. Default: 1000.
    pub max_pending_dags: usize,
}
