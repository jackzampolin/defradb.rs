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

    /// Specify the datastore to use (supported: regolith, memory)
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

    /// Hex private key this node authenticates to the Orbis ring with,
    /// separately from `--identity`.
    ///
    /// The ring authenticates a Sign request by a JWT whose algorithm follows
    /// the key type, and it accepts EdDSA (ed25519) only. `--identity` is also
    /// the key that signs SourceHub/Vera transactions, which must be
    /// secp256k1. One key therefore cannot serve both roles: without this
    /// flag a node with a secp256k1 identity presents an ES256K token and the
    /// ring rejects it as an unknown algorithm.
    ///
    /// Defaults to `--identity` when unset, which keeps the previous behaviour
    /// for a node whose identity is already ed25519.
    #[cfg(feature = "orbis")]
    #[arg(long)]
    pub signer_orbis_identity: Option<String>,

    /// Max request body size in bytes (0 = unlimited, default)
    #[arg(long)]
    pub max_body_size: Option<u64>,

    /// Max schema request body size in bytes (0 = unlimited, default)
    #[arg(long)]
    pub max_schema_size: Option<u64>,

    /// Max backup import body size in bytes (0 = unlimited, default: 100 MiB)
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

    /// Maximum DAG traversal depth for merge operations
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

    /// Re-announce blocks merged from peers on their gossip topics, so
    /// subscribers with no transport route to the author still receive them
    /// through whichever peer merged them (multi-hop propagation). Bare
    /// `--p2p-rebroadcast-on-merge` enables; `=false` (or the environment
    /// variable set to `false`) disables, overriding the config file.
    /// Default: off.
    #[arg(
        long,
        env = "DEFRA_P2P_REBROADCAST_ON_MERGE",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "true"
    )]
    pub p2p_rebroadcast_on_merge: Option<bool>,

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
        let service_identity = match self.parse_orbis_service_identity()? {
            Some(identity) => identity,
            None => user_identity.as_ref().cloned().ok_or_else(|| {
                Error::InvalidConfig(
                    "--identity or --signer-orbis-identity is required when \
                     --signer-type=orbis (service key signs JWTs for Orbis auth)"
                        .into(),
                )
            })?,
        };

        Self::require_ed25519_service_key(
            service_identity.key_type(),
            self.signer_orbis_identity.is_some(),
        )?;

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

    /// Parse `--signer-orbis-identity`, the key used only to authenticate to
    /// the Orbis ring. It is separate from `--identity` because the ring
    /// accepts an EdDSA token while the chain needs a secp256k1 signer.
    #[cfg(feature = "orbis")]
    pub fn parse_orbis_service_identity(
        &self,
    ) -> Result<Option<std::sync::Arc<identity::RawIdentity>>> {
        let Some(hex_key) = self.signer_orbis_identity.as_deref() else {
            return Ok(None);
        };
        let identity = Self::identity_from_hex(hex_key)?;
        tracing::info!("Orbis service identity DID: {}", identity.did()?);
        Ok(Some(std::sync::Arc::new(identity)))
    }

    /// Parse the user identity from the --identity flag.
    ///
    /// The identity flag should contain a hex-encoded private key.
    /// Key type is auto-detected from byte length:
    /// - 64 bytes -> Ed25519
    /// - 32 bytes -> secp256k1
    pub fn parse_user_identity(&self) -> Result<Option<std::sync::Arc<identity::RawIdentity>>> {
        let hex_key = match &self.identity {
            Some(key) => key,
            None => return Ok(None),
        };

        let raw_identity = Self::identity_from_hex(hex_key)?;

        let did = raw_identity.did()?;
        tracing::info!("User identity DID: {}", did);

        Ok(Some(std::sync::Arc::new(raw_identity)))
    }

    /// The Orbis ring authenticates a Sign request with a bearer token and
    /// accepts EdDSA only. `new_token_with_custom_claims` picks the algorithm
    /// from the key type, so a secp256k1 service key presents ES256K and the
    /// ring refuses it. Reject it here rather than letting it surface as
    /// `unknown variant ES256K` from the ring at the first write, long after
    /// the node booted clean.
    ///
    /// `from_flag` says whether the key came from `--signer-orbis-identity`,
    /// because the two ways to get here need different advice: the fallback to
    /// `--identity` needs the flag, while a key already passed to the flag is
    /// almost always a 32-byte ed25519 seed, which [`Self::identity_from_hex`]
    /// reads as secp256k1 by length.
    #[cfg(feature = "orbis")]
    fn require_ed25519_service_key(key_type: crypto::KeyType, from_flag: bool) -> Result<()> {
        if key_type == crypto::KeyType::Ed25519 {
            return Ok(());
        }
        let advice = if from_flag {
            "--signer-orbis-identity must be the 64-byte ed25519 form, seed \
             followed by public key. A 32-byte value is read as secp256k1 by \
             length, which is what happened here."
        } else {
            "Pass one with --signer-orbis-identity. --identity is not usable \
             as the service key when it is secp256k1, because it also signs \
             chain transactions and must stay that type."
        };
        Err(Error::InvalidConfig(format!(
            "--signer-type=orbis needs an ed25519 service key, got {:?}. {}",
            key_type, advice
        )))
    }

    /// Build an identity from a hex private key, choosing the key type by
    /// length: 64 bytes is ed25519 (seed + public key), 32 is secp256k1.
    fn identity_from_hex(hex_key: &str) -> Result<identity::RawIdentity> {
        let hex_str = hex_key.strip_prefix("0x").unwrap_or(hex_key);
        let key_bytes = hex::decode(hex_str)
            .map_err(|e| Error::InvalidIdentity(format!("invalid hex in identity key: {}", e)))?;

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

        Ok(identity::RawIdentity::from_identity_key_type(
            key_type, &key_bytes,
        )?)
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
            config.datastore.valuelogfilesize = Some(size);
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
        if let Some(rebroadcast) = self.p2p_rebroadcast_on_merge {
            config.net.p2p_rebroadcast_on_merge = rebroadcast;
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

#[cfg(test)]
mod tests {
    use super::*;

    const SECP256K1_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// 64 bytes: seed followed by public key, the layout DefraDB stores an
    /// ed25519 private key in. RFC 8032 test vector 1, so the public half
    /// really is the seed's public key; the loader checks that.
    const ED25519_KEY: &str = concat!(
        "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
        "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
    );

    #[test]
    fn identity_key_type_follows_the_key_length() {
        let secp = StartArgs::identity_from_hex(SECP256K1_KEY).expect("secp256k1 identity");
        assert_eq!(secp.key_type(), crypto::KeyType::Secp256k1);

        let ed = StartArgs::identity_from_hex(ED25519_KEY).expect("ed25519 identity");
        assert_eq!(ed.key_type(), crypto::KeyType::Ed25519);
    }

    #[test]
    fn identity_from_hex_accepts_a_0x_prefix() {
        let prefixed = format!("0x{}", SECP256K1_KEY);
        assert_eq!(
            StartArgs::identity_from_hex(&prefixed)
                .expect("prefixed")
                .did()
                .expect("did"),
            StartArgs::identity_from_hex(SECP256K1_KEY)
                .expect("bare")
                .did()
                .expect("did"),
            "the 0x prefix must not change the identity"
        );
    }

    #[test]
    fn identity_from_hex_rejects_other_lengths() {
        let err = StartArgs::identity_from_hex("00112233").expect_err("8 bytes is neither");
        assert!(
            format!("{err}").contains("invalid key length"),
            "unexpected error: {err}"
        );
    }

    #[cfg(feature = "orbis")]
    #[test]
    fn an_ed25519_service_key_is_accepted() {
        for from_flag in [true, false] {
            StartArgs::require_ed25519_service_key(crypto::KeyType::Ed25519, from_flag)
                .expect("ed25519 is what the ring accepts");
        }
    }

    #[cfg(feature = "orbis")]
    #[test]
    fn a_non_ed25519_service_key_is_refused_before_the_ring_sees_it() {
        for key_type in [crypto::KeyType::Secp256k1, crypto::KeyType::Secp256r1] {
            for from_flag in [true, false] {
                let err = StartArgs::require_ed25519_service_key(key_type, from_flag)
                    .expect_err("the ring accepts EdDSA only");
                let message = format!("{err}");
                assert!(
                    message.contains("--signer-orbis-identity"),
                    "the error must name the flag that fixes it: {message}"
                );
                assert!(
                    message.contains(&format!("{key_type:?}")),
                    "the error must name the key type it got: {message}"
                );
            }
        }
    }

    /// A raw 32-byte ed25519 seed is read as secp256k1 by length, so the error
    /// for a key that came from the flag has to say which form to pass.
    #[cfg(feature = "orbis")]
    #[test]
    fn a_seed_passed_to_the_flag_is_told_it_needs_the_64_byte_form() {
        let seed = StartArgs::identity_from_hex(SECP256K1_KEY).expect("32 bytes parses");
        assert_eq!(
            seed.key_type(),
            crypto::KeyType::Secp256k1,
            "a 32-byte value is secp256k1 by length, which is the trap"
        );
        let err = StartArgs::require_ed25519_service_key(seed.key_type(), true)
            .expect_err("a seed is not the ed25519 form the ring needs");
        let message = format!("{err}");
        assert!(
            message.contains("64-byte"),
            "the error must name the form to pass: {message}"
        );
    }
}
