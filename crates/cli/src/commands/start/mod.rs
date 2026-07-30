//! Start command implementation

mod node;
mod p2p;
mod run;
mod server;
mod server_acp;
mod server_http;
mod server_p2p;
mod server_query;

use clap::Args;

use crate::config::Config;
use crate::error::{Error, Result};
use identity::Identity;
use storage::backends::DurabilityMode;

pub use node::Node;

const DEV_MODE_BANNER: &str = r#"
******************************************
**     DEVELOPMENT MODE IS ENABLED      **
** ------------------------------------ **
**   if this is a production database   **
** disable development mode and restart **
**   or you may risk losing all data    **
******************************************
"#;

/// Arguments for the start command
#[derive(Args, Debug)]
pub struct StartArgs {
    /// Emit a Chrome trace file for profiling
    #[arg(long)]
    pub profile: bool,

    /// List of peers to connect to
    #[arg(long, value_delimiter = ',')]
    pub peers: Option<Vec<String>>,

    /// Specify the maximum number of retries per transaction
    #[arg(long)]
    pub max_txn_retries: Option<u32>,

    /// Specify the datastore to use (supported: lark, redb, memory, fjall, rocksdb)
    #[arg(long)]
    pub store: Option<String>,

    /// Specify the datastore value log file size (in bytes)
    #[arg(long)]
    pub valuelogfilesize: Option<u64>,

    /// Listen addresses for the p2p network (formatted as a libp2p MultiAddr)
    #[arg(long, value_delimiter = ',')]
    pub p2paddr: Option<Vec<String>>,

    /// Disable the peer-to-peer network synchronization system
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = crate::cli::bool_value_parser())]
    pub no_p2p: Option<bool>,

    /// List of origins to allow for CORS requests
    #[arg(long, value_delimiter = ',')]
    pub allowed_origins: Option<Vec<String>>,

    /// Path to the public key for TLS
    #[arg(long)]
    pub pubkeypath: Option<String>,

    /// Path to the private key for TLS
    #[arg(long)]
    pub privkeypath: Option<String>,

    /// Skip generating an encryption key. Encryption at rest will be disabled.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = crate::cli::bool_value_parser())]
    pub no_encryption: Option<bool>,

    /// Disable signing of commits
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = crate::cli::bool_value_parser())]
    pub no_signing: Option<bool>,

    /// Default key type to generate new node identity
    #[arg(long)]
    pub default_key_type: Option<String>,

    /// Skip generating a searchable encryption key
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true", value_parser = crate::cli::bool_value_parser())]
    pub no_searchable_encryption: Option<bool>,

    /// Enable transparent at-rest value encryption for the storage backend,
    /// keyed by the keyring `encryption-key`. Default: false.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub at_rest_encryption: Option<bool>,

    /// Hex formatted private key used to authenticate with ACP.
    ///
    /// The key type is auto-detected from the key length:
    /// - 64 bytes (128 hex chars) -> Ed25519
    /// - 32 bytes (64 hex chars) -> secp256k1
    #[arg(short = 'i', long)]
    pub identity: Option<String>,

    /// Retry intervals for the replicator (comma-separated seconds)
    #[arg(long, value_delimiter = ',')]
    pub replicator_retry_intervals: Option<Vec<u32>>,

    /// Storage durability mode: "immediate" (default, fsync every commit) or
    /// "eventual" (defer fsync to OS for higher write throughput)
    #[arg(long)]
    pub durability: Option<String>,

    /// Signer type: "local" (default) or "orbis" (Orbis ring threshold signing)
    #[cfg(feature = "orbis")]
    #[arg(long)]
    pub signer_type: Option<String>,

    /// Orbis gRPC endpoint (required when --signer-type=orbis)
    #[cfg(feature = "orbis")]
    #[arg(long)]
    pub signer_orbis_endpoint: Option<String>,

    /// Orbis ring ID from DKG (required when --signer-type=orbis)
    #[cfg(feature = "orbis")]
    #[arg(long)]
    pub signer_orbis_ring_id: Option<String>,

    /// Orbis derivation label for the ring's derived key (e.g. "x-archive")
    #[cfg(feature = "orbis")]
    #[arg(long)]
    pub signer_orbis_derivation: Option<String>,

    /// Max request body size in bytes (0 = unlimited, default)
    #[arg(long)]
    pub max_body_size: Option<u64>,

    /// Max schema request body size in bytes (0 = unlimited, default)
    #[arg(long)]
    pub max_schema_size: Option<u64>,

    /// Max backup import body size in bytes (0 = unlimited, default)
    #[arg(long)]
    pub max_backup_size: Option<u64>,

    /// Request timeout in seconds (0 = no timeout, default: 300)
    #[arg(long)]
    pub request_timeout: Option<u64>,

    /// Max concurrent HTTP requests (0 = unlimited, default: 1000)
    #[arg(long)]
    pub max_concurrent_requests: Option<usize>,

    /// Max P2P protocol message size in bytes (default: 16777216 = 16MiB)
    #[arg(long)]
    pub max_msg_size: Option<u64>,

    /// Max P2P CAR file size in bytes (default: 67108864 = 64MiB)
    #[arg(long)]
    pub max_car_size: Option<u64>,

    /// P2P stream read timeout in seconds (default: 30)
    #[arg(long)]
    pub stream_timeout: Option<u64>,

    /// Max concurrent P2P stream handler tasks (default: 64)
    #[arg(long)]
    pub max_p2p_tasks: Option<usize>,

    /// P2P established connection low watermark (default: 100)
    #[arg(long)]
    pub connection_manager_low_water: Option<u32>,

    /// P2P established connection high watermark (default: 400)
    #[arg(long)]
    pub connection_manager_high_water: Option<u32>,

    /// P2P connection manager grace period in milliseconds (default: 20000)
    #[arg(long)]
    pub connection_manager_grace_period_ms: Option<u64>,

    /// Max established P2P connections per peer (default: 4)
    #[arg(long)]
    pub max_connections_per_peer: Option<u32>,

    /// Max DAG recursion depth for merge operations (default: 1024)
    #[arg(long)]
    pub max_merge_depth: Option<usize>,

    /// Query execution timeout in seconds (0 = no timeout, default: 30)
    #[arg(long)]
    pub query_timeout: Option<u64>,

    /// Max idle age for explicit HTTP transactions in seconds (0 = disabled, default: 600)
    #[arg(long)]
    pub transaction_idle_timeout: Option<u64>,

    /// Interval between explicit HTTP transaction cleanup sweeps in seconds (default: 60)
    #[arg(long)]
    pub transaction_cleanup_interval: Option<u64>,

    /// Max GraphQL selection nesting depth (0 = unlimited, default: 20)
    #[arg(long)]
    pub query_max_depth: Option<usize>,

    /// Max fields at any GraphQL selection level (0 = unlimited, default: 100)
    #[arg(long)]
    pub query_max_width: Option<usize>,

    /// Max recursive filter nesting depth (0 = unlimited, default: 50)
    #[arg(long)]
    pub query_max_filter_depth: Option<usize>,

    /// Per-peer rate limit burst capacity (max tokens). Default: 500.
    #[arg(long, env = "DEFRA_P2P_RATE_LIMIT_BURST")]
    pub p2p_rate_limit_burst: Option<u32>,

    /// Per-peer rate limit refill rate (tokens per second). Default: 50.
    #[arg(long, env = "DEFRA_P2P_RATE_LIMIT_RATE")]
    pub p2p_rate_limit_rate: Option<f64>,

    /// Max document IDs accepted in one DocSync request. Default: 1000.
    #[arg(long)]
    pub p2p_max_doc_sync_request_doc_ids: Option<usize>,

    /// Max pending-DAG registrations awaiting Bitswap completion; overflow is
    /// nacked back to the pusher. Each source peer may use at most one quarter.
    /// Default: 1000.
    #[arg(long, env = "DEFRA_P2P_MAX_PENDING_DAGS")]
    pub p2p_max_pending_dags: Option<usize>,

    /// Max queued outbound push jobs; overflow defers to the persisted retry
    /// ladder. Default: 1024.
    #[arg(long, env = "DEFRA_P2P_PUSH_QUEUE_CAPACITY")]
    pub p2p_push_queue_capacity: Option<usize>,

    /// Max resident bytes across queued outbound push jobs. Default: 32 MiB.
    #[arg(long, env = "DEFRA_P2P_PUSH_QUEUE_BYTES")]
    pub p2p_push_queue_byte_capacity: Option<usize>,

    /// Max outbound push jobs concurrently in flight to one peer. Default: 4.
    #[arg(long, env = "DEFRA_P2P_MAX_ACTIVE_PUSHES_PER_PEER")]
    pub p2p_max_active_pushes_per_peer: Option<usize>,

    /// P2P transport backend: "libp2p" (default) or "iroh"
    #[arg(long)]
    pub p2p_transport: Option<String>,

    /// Address for Postgres wire protocol compatibility (e.g., "127.0.0.1:5433")
    #[cfg(feature = "postgres")]
    #[arg(long)]
    pub pg_address: Option<String>,

    /// ACP circuit breaker failure threshold (default: 3)
    #[arg(long, env = "DEFRA_ACP_CIRCUIT_BREAKER_THRESHOLD")]
    pub acp_circuit_breaker_threshold: Option<u32>,

    /// ACP circuit breaker reset timeout in seconds (default: 30)
    #[arg(long, env = "DEFRA_ACP_CIRCUIT_BREAKER_RESET_TIMEOUT")]
    pub acp_circuit_breaker_reset_timeout: Option<u64>,

    /// ACP request timeout in seconds for SourceHub/hub.rs calls (default: 5)
    #[arg(long, env = "DEFRA_ACP_REQUEST_TIMEOUT")]
    pub acp_request_timeout: Option<u64>,

    /// ACP policy cache TTL in seconds (default: 300)
    #[arg(long, env = "DEFRA_ACP_CACHE_TTL")]
    pub acp_cache_ttl: Option<u64>,

    /// ACP receipt polling timeout in seconds for hub.rs transactions (default: 30)
    #[arg(long, env = "DEFRA_ACP_RECEIPT_TIMEOUT")]
    pub acp_receipt_timeout: Option<u64>,

    /// OpenAI-compatible embedding base URL (without /embeddings suffix)
    #[arg(long, env = "DEFRA_EMBEDDING_URL")]
    pub embedding_url: Option<String>,

    /// Embedding model name used when schema model is empty
    #[arg(long, env = "DEFRA_EMBEDDING_MODEL")]
    pub embedding_model: Option<String>,

    /// Environment variable name containing the embedding API key
    #[arg(long, env = "DEFRA_EMBEDDING_API_KEY_ENV")]
    pub embedding_api_key_env: Option<String>,
}

impl StartArgs {
    /// Execute the start command
    pub async fn execute(self, mut config: Config) -> Result<()> {
        // Apply start-specific flags to config
        self.apply_to_config(&mut config)?;

        // Create config if it doesn't exist
        config.create_if_missing()?;

        // Show development mode banner
        if config.development {
            eprintln!("{}", DEV_MODE_BANNER);
        }

        // Parse user identity from --identity flag if provided
        let user_identity = self.parse_user_identity()?;

        // Set up Orbis remote signer if configured
        #[cfg(feature = "orbis")]
        if self.signer_type.as_deref() == Some("orbis") {
            self.setup_orbis_signer(&user_identity).await?;
        }

        // Start the node
        let node = Node::new(config, user_identity).await?;
        node.run().await
    }

    /// Set up Orbis ring threshold signing.
    ///
    /// Connects to the Orbis ring, derives the BLS public key, and stores
    /// a SigningConfig with a remote signer under the signer's DID.
    #[cfg(feature = "orbis")]
    async fn setup_orbis_signer(
        &self,
        user_identity: &Option<std::sync::Arc<identity::RawIdentity>>,
    ) -> Result<()> {
        let service_identity = user_identity.as_ref().ok_or_else(|| {
            Error::InvalidConfig(
                "--identity is required when --signer-type=orbis \
                 (service key signs JWTs for Orbis auth)"
                    .into(),
            )
        })?;

        let endpoint = self.signer_orbis_endpoint.as_ref().ok_or_else(|| {
            Error::InvalidConfig("--signer-orbis-endpoint required for orbis signer".into())
        })?;

        let ring_id = self.signer_orbis_ring_id.as_ref().ok_or_else(|| {
            Error::InvalidConfig("--signer-orbis-ring-id required for orbis signer".into())
        })?;

        let derivation = self.signer_orbis_derivation.clone().unwrap_or_default();

        let client = orbis::OrbisClient::new(
            endpoint.clone(),
            ring_id.clone(),
            derivation,
            service_identity.clone(),
        )
        .await
        .map_err(|e| Error::InvalidConfig(format!("Orbis signer setup failed: {}", e)))?;

        let signer_did = client.signer_did().to_string();
        let public_key_bytes = client.public_key_bytes().to_vec();
        let public_key_hex = client.public_key_hex().to_string();

        defra_core::signing::store_identity(
            &signer_did,
            defra_core::signing::SigningConfig {
                key_type: defra_core::signing::SigningKeyType::Bls,
                private_key_bytes: vec![],
                public_key_bytes,
                public_key_hex,
                remote_signer: Some(std::sync::Arc::new(client)),
                signing_authorization: None,
            },
        );

        tracing::info!(
            signer_did = %signer_did,
            "Orbis remote signer configured"
        );

        Ok(())
    }

    /// Parse the user identity from the --identity flag.
    ///
    /// The identity flag should contain a hex-encoded private key.
    /// Key type is auto-detected from byte length:
    /// - 64 bytes -> Ed25519
    /// - 32 bytes -> secp256k1
    fn parse_user_identity(&self) -> Result<Option<std::sync::Arc<identity::RawIdentity>>> {
        let hex_key = match &self.identity {
            Some(key) => key,
            None => return Ok(None),
        };

        // Remove 0x prefix if present
        let hex_str = hex_key.strip_prefix("0x").unwrap_or(hex_key);

        // Decode hex to bytes
        let key_bytes = hex::decode(hex_str).map_err(|e| {
            Error::InvalidIdentity(format!("invalid hex in --identity flag: {}", e))
        })?;

        // Auto-detect key type from byte length
        let key_type = match key_bytes.len() {
            64 => identity::IdentityKeyType::Ed25519,
            32 => identity::IdentityKeyType::Secp256k1,
            n => {
                return Err(Error::InvalidIdentity(format!(
                    "invalid key length {} bytes: expected 64 (ed25519) or 32 (secp256k1)",
                    n
                )));
            }
        };

        // Create identity from bytes
        let raw_identity = identity::RawIdentity::from_identity_key_type(key_type, &key_bytes)?;

        let did = raw_identity.did()?;
        tracing::info!("User identity DID: {}", did);

        Ok(Some(std::sync::Arc::new(raw_identity)))
    }

    /// Apply start command flags to config
    ///
    /// Returns an error if any flag value fails to parse.
    pub fn apply_to_config(&self, config: &mut Config) -> Result<()> {
        if let Some(ref peers) = self.peers {
            config.net.peers = peers.clone();
        }
        if let Some(retries) = self.max_txn_retries {
            config.datastore.max_txn_retries = retries;
        }
        if let Some(ref store) = self.store {
            config.datastore.store = store.parse()?;
        }
        if let Some(size) = self.valuelogfilesize {
            config.datastore.valuelogfilesize = size;
        }
        if let Some(ref addrs) = self.p2paddr {
            config.net.p2p_addresses = addrs.clone();
        }
        if let Some(no_p2p) = self.no_p2p {
            config.net.p2p_disabled = no_p2p;
        }
        if let Some(ref origins) = self.allowed_origins {
            config.api.allowed_origins = origins.clone();
        }
        if let Some(ref path) = self.pubkeypath {
            config.api.pubkey_path = path.clone();
        }
        if let Some(ref path) = self.privkeypath {
            config.api.privkey_path = path.clone();
        }
        if let Some(no_enc) = self.no_encryption {
            config.datastore.no_encryption = no_enc;
        }
        if let Some(no_sign) = self.no_signing {
            config.datastore.no_signing = no_sign;
        }
        if let Some(ref key_type) = self.default_key_type {
            config.datastore.default_key_type = key_type.clone();
        }
        if let Some(no_se) = self.no_searchable_encryption {
            config.datastore.no_searchable_encryption = no_se;
        }
        if let Some(at_rest) = self.at_rest_encryption {
            config.datastore.at_rest_encryption = at_rest;
        }
        if let Some(ref intervals) = self.replicator_retry_intervals {
            config.replicator_retry_intervals = intervals.clone();
        }
        if let Some(burst) = self.p2p_rate_limit_burst {
            config.net.p2p_rate_limit_burst = burst;
        }
        if let Some(rate) = self.p2p_rate_limit_rate {
            config.net.p2p_rate_limit_rate = rate;
        }
        if let Some(max) = self.p2p_max_doc_sync_request_doc_ids {
            config.net.p2p_max_doc_sync_request_doc_ids = max;
        }
        if let Some(max) = self.p2p_max_pending_dags {
            config.net.p2p_max_pending_dags = max;
        }
        if let Some(capacity) = self.p2p_push_queue_capacity {
            config.net.p2p_push_queue_capacity = capacity;
        }
        if let Some(capacity) = self.p2p_push_queue_byte_capacity {
            config.net.p2p_push_queue_byte_capacity = capacity;
        }
        if let Some(cap) = self.p2p_max_active_pushes_per_peer {
            config.net.p2p_max_active_pushes_per_peer = cap;
        }
        if let Some(ref transport) = self.p2p_transport {
            config.net.transport = transport.parse()?;
        }
        if let Some(ref durability) = self.durability {
            config.datastore.durability = match durability.as_str() {
                "immediate" => DurabilityMode::Immediate,
                "eventual" => DurabilityMode::Eventual,
                other => {
                    return Err(Error::InvalidConfig(format!(
                        "invalid durability mode '{}': expected 'immediate' or 'eventual'",
                        other
                    )));
                }
            };
        }
        if let Some(size) = self.max_body_size {
            config.api.max_body_size = size;
        }
        if let Some(size) = self.max_schema_size {
            config.api.max_schema_size = size;
        }
        if let Some(size) = self.max_backup_size {
            config.api.max_backup_size = size;
        }
        if let Some(timeout) = self.request_timeout {
            config.api.request_timeout = timeout;
        }
        if let Some(max) = self.max_concurrent_requests {
            config.api.max_concurrent_requests = max;
        }
        if let Some(size) = self.max_msg_size {
            config.net.max_msg_size = size;
        }
        if let Some(size) = self.max_car_size {
            config.net.max_car_size = size;
        }
        if let Some(timeout) = self.stream_timeout {
            config.net.stream_timeout = timeout;
        }
        if let Some(max) = self.max_p2p_tasks {
            config.net.max_p2p_tasks = max;
        }
        if let Some(low_water) = self.connection_manager_low_water {
            config.net.connection_manager_low_water = low_water;
        }
        if let Some(high_water) = self.connection_manager_high_water {
            config.net.connection_manager_high_water = high_water;
        }
        if let Some(grace_period_ms) = self.connection_manager_grace_period_ms {
            config.net.connection_manager_grace_period_ms = grace_period_ms;
        }
        if let Some(max) = self.max_connections_per_peer {
            config.net.max_connections_per_peer = max;
        }
        if let Some(depth) = self.max_merge_depth {
            config.datastore.max_merge_depth = depth;
        }
        if let Some(timeout) = self.query_timeout {
            config.api.query_timeout = timeout;
        }
        if let Some(timeout) = self.transaction_idle_timeout {
            config.api.transaction_idle_timeout = timeout;
        }
        if let Some(interval) = self.transaction_cleanup_interval {
            config.api.transaction_cleanup_interval = interval;
        }
        if let Some(depth) = self.query_max_depth {
            config.api.query_max_depth = depth;
        }
        if let Some(width) = self.query_max_width {
            config.api.query_max_width = width;
        }
        if let Some(depth) = self.query_max_filter_depth {
            config.api.query_max_filter_depth = depth;
        }
        #[cfg(feature = "postgres")]
        if let Some(ref addr) = self.pg_address {
            config.api.pg_address = addr.clone();
        }
        if let Some(threshold) = self.acp_circuit_breaker_threshold {
            config.acp.circuit_breaker_threshold = threshold;
        }
        if let Some(timeout) = self.acp_circuit_breaker_reset_timeout {
            config.acp.circuit_breaker_reset_timeout = timeout;
        }
        if let Some(timeout) = self.acp_request_timeout {
            config.acp.request_timeout = timeout;
        }
        if let Some(ttl) = self.acp_cache_ttl {
            config.acp.cache_ttl = ttl;
        }
        if let Some(timeout) = self.acp_receipt_timeout {
            config.acp.receipt_timeout = timeout;
        }
        if let Some(ref url) = self.embedding_url {
            config.embedding.url = url.clone();
        }
        if let Some(ref model) = self.embedding_model {
            config.embedding.model = model.clone();
        }
        if let Some(ref api_key_env) = self.embedding_api_key_env {
            config.embedding.api_key_env = api_key_env.clone();
        }
        config.api.validate()?;
        Ok(())
    }
}
