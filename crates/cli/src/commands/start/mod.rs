//! Start command implementation

mod node;
mod p2p;
mod run;
mod server;

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
    /// List of peers to connect to
    #[arg(long, value_delimiter = ',')]
    pub peers: Option<Vec<String>>,

    /// Specify the maximum number of retries per transaction
    #[arg(long)]
    pub max_txn_retries: Option<u32>,

    /// Specify the datastore to use (supported: redb, memory, fjall, rocksdb)
    #[arg(long)]
    pub store: Option<String>,

    /// Specify the datastore value log file size (in bytes)
    #[arg(long)]
    pub valuelogfilesize: Option<u64>,

    /// Listen addresses for the p2p network (formatted as a libp2p MultiAddr)
    #[arg(long, value_delimiter = ',')]
    pub p2paddr: Option<Vec<String>>,

    /// Disable the peer-to-peer network synchronization system
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
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

    /// Enables development mode features
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub development: Option<bool>,

    /// Skip generating an encryption key. Encryption at rest will be disabled.
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub no_encryption: Option<bool>,

    /// Disable telemetry reporting
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub no_telemetry: Option<bool>,

    /// Disable signing of commits
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub no_signing: Option<bool>,

    /// Default key type to generate new node identity
    #[arg(long)]
    pub default_key_type: Option<String>,

    /// Skip generating a searchable encryption key
    #[arg(long, num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    pub no_searchable_encryption: Option<bool>,

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

    /// Storage durability mode: "eventual" (default, matches Go) or
    /// "immediate" (fsync every commit, safer against OS crashes)
    #[arg(long)]
    pub durability: Option<String>,

    /// Signer type: "local" (default) or "orbis" (Orbis ring threshold signing)
    #[arg(long)]
    pub signer_type: Option<String>,

    /// Orbis gRPC endpoint (required when --signer-type=orbis)
    #[arg(long)]
    pub signer_orbis_endpoint: Option<String>,

    /// Orbis ring ID from DKG (required when --signer-type=orbis)
    #[arg(long)]
    pub signer_orbis_ring_id: Option<String>,

    /// Orbis derivation label for the ring's derived key (e.g. "x-archive")
    #[arg(long)]
    pub signer_orbis_derivation: Option<String>,
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
                key_type: "bls".to_string(),
                private_key_bytes: vec![],
                public_key_bytes,
                public_key_hex,
                remote_signer: Some(std::sync::Arc::new(client)),
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
        if let Some(dev) = self.development {
            config.development = dev;
        }
        if let Some(no_enc) = self.no_encryption {
            config.datastore.no_encryption = no_enc;
        }
        if let Some(no_tel) = self.no_telemetry {
            config.telemetry_disabled = no_tel;
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
        if let Some(ref intervals) = self.replicator_retry_intervals {
            config.replicator_retry_intervals = intervals.clone();
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
        Ok(())
    }
}
