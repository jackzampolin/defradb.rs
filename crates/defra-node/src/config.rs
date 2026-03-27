//! Configuration types for the embedded DefraDB node.

/// Configuration for the optional HTTP GraphQL server.
#[cfg(feature = "http")]
pub struct HttpConfig {
    pub address: std::net::SocketAddr,
    pub(crate) extra_routes: Option<axum::Router>,
}

#[cfg(feature = "http")]
impl HttpConfig {
    pub fn new(port: u16) -> Self {
        Self {
            address: std::net::SocketAddr::from(([127, 0, 0, 1], port)),
            extra_routes: None,
        }
    }

    pub fn with_addr(addr: impl Into<std::net::SocketAddr>) -> Self {
        Self {
            address: addr.into(),
            extra_routes: None,
        }
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
    /// Per-peer rate limit burst capacity (max tokens in bucket). Default: 500.
    pub rate_limit_burst: u32,
    /// Per-peer rate limit refill rate (tokens per second). Default: 50.
    pub rate_limit_rate: f64,
}
