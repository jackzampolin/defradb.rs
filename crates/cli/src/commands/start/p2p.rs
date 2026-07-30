//! P2P initialization methods for Node

use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{error, info};

use super::node::Node;
use crate::config::{AcpDocumentType, Config};
use crate::error::{Error, Result};

impl Node {
    /// Initialize or load the peer key from keyring.
    ///
    /// If a peer key exists in the keyring, it is loaded and converted to a libp2p Keypair.
    /// If no peer key exists, a new Ed25519 key is generated and stored in the keyring.
    pub(super) fn init_peer_key(config: &Config) -> Result<(p2p::Keypair, String)> {
        use keyring::PEER_KEY;

        let kr = crate::commands::open_keyring(config)?;

        // Try to load existing peer key
        match kr.get(PEER_KEY) {
            Ok(key_bytes) => {
                info!("Loaded existing peer key from keyring");
                let did = Self::derive_and_log_identity_did(&key_bytes)?;
                let keypair = Self::keypair_from_ed25519_bytes(&key_bytes)?;
                Ok((keypair, did))
            }
            Err(keyring::Error::NotFound(_)) => {
                info!("Generating new peer key");
                use crypto::Key;
                let private_key = crypto::generate_ed25519()
                    .map_err(|e| Error::Keyring(format!("failed to generate peer key: {}", e)))?;
                let key_bytes = private_key.raw();

                // Store in keyring
                kr.set(PEER_KEY, key_bytes)
                    .map_err(|e| Error::Keyring(e.to_string()))?;

                let did = Self::derive_and_log_identity_did(key_bytes)?;
                let keypair = Self::keypair_from_ed25519_bytes(key_bytes)?;
                Ok((keypair, did))
            }
            Err(e) => Err(Error::Keyring(e.to_string())),
        }
    }

    /// Derive and log the node's DID from peer key bytes.
    fn derive_and_log_identity_did(key_bytes: &[u8]) -> Result<String> {
        use identity::{Identity, IdentityKeyType, RawIdentity};

        let identity = RawIdentity::from_identity_key_type(IdentityKeyType::Ed25519, key_bytes)?;
        let did = identity
            .did()
            .map_err(|e| Error::Keyring(format!("failed to derive DID: {}", e)))?;
        info!("Node identity DID: {}", did);
        Ok(did.to_string())
    }

    /// Convert Ed25519 key bytes to libp2p Keypair.
    ///
    /// Ed25519 keys are stored as 64 bytes: 32-byte seed + 32-byte public key.
    /// libp2p expects the 32-byte seed to derive the keypair.
    fn keypair_from_ed25519_bytes(key_bytes: &[u8]) -> Result<p2p::Keypair> {
        use libp2p::identity::ed25519;

        if key_bytes.len() != 64 {
            return Err(Error::Keyring(format!(
                "invalid peer key length: expected 64 bytes, got {}",
                key_bytes.len()
            )));
        }

        // Ed25519 key format: 32-byte seed + 32-byte public key
        // libp2p needs the seed (first 32 bytes) to derive the keypair
        let seed: [u8; 32] = key_bytes[..32]
            .try_into()
            .map_err(|_| Error::Keyring("invalid key format".to_string()))?;

        let secret_key = ed25519::SecretKey::try_from_bytes(seed)
            .map_err(|e| Error::Keyring(format!("invalid Ed25519 key: {}", e)))?;

        Ok(p2p::Keypair::from(ed25519::Keypair::from(secret_key)))
    }

    /// Start P2P networking with the given bitswap store and optional keypair.
    ///
    /// Returns the handle, events receiver, and host task handle so that the caller
    /// can connect events to the sync coordinator and track the host task for shutdown.
    pub(super) async fn start_p2p(
        config: &Config,
        bitswap_store: p2p::BitswapStoreAdapter<blockstore::DefraBlockstore<storage::DynStore>>,
        keypair: Option<p2p::Keypair>,
        enable_pubsub: bool,
        classifier: std::sync::Arc<dyn p2p::bitswap::BlockClassifier>,
        serve_acp: std::sync::Arc<p2p::bitswap::LateBoundServeAcp>,
    ) -> Result<(
        p2p::P2PHostHandle,
        tokio::sync::mpsc::Receiver<p2p::HostEvent>,
        std::sync::Arc<p2p::ReplicatorRegistry>,
        JoinHandle<()>,
    )> {
        let access_mode = if config.acp.document_type != AcpDocumentType::None {
            p2p::bitswap::AccessMode::Controlled
        } else {
            p2p::bitswap::AccessMode::Open
        };
        let p2p_config = p2p::P2PHostConfig {
            enable_pubsub,
            enable_relay: config.net.relay_enabled,
            max_msg_size: config.net.max_msg_size,
            max_car_size: config.net.max_car_size,
            stream_timeout: config.net.stream_timeout,
            max_p2p_tasks: config.net.max_p2p_tasks,
            connection_manager_low_water: config.net.connection_manager_low_water,
            connection_manager_high_water: config.net.connection_manager_high_water,
            connection_manager_grace_period: Duration::from_millis(
                config.net.connection_manager_grace_period_ms,
            ),
            max_connections_per_peer: config.net.max_connections_per_peer,
            access_mode,
        };
        let keypair = keypair.unwrap_or_else(p2p::Keypair::generate_ed25519);
        let (host, handle, events, replicators) =
            p2p::P2PHost::with_keypair_and_config_and_identity_and_serve_gate(
                keypair,
                bitswap_store,
                p2p_config,
                None,
                classifier,
                serve_acp,
            )
            .await
            .map_err(Error::P2P)?;

        // Spawn the host event loop FIRST - it must be running to process commands
        // Track the task handle for graceful shutdown
        let host_task = tokio::spawn(host.run());

        // Start listening on configured addresses
        for addr_str in &config.net.p2p_addresses {
            let addr: p2p::Multiaddr = addr_str
                .parse()
                .map_err(|e| Error::InvalidMultiaddr(format!("{}: {}", addr_str, e)))?;

            handle.listen(addr.clone()).await.map_err(Error::P2P)?;
            info!("P2P listening on {}", addr);
        }

        // Log bootstrap peers
        if !config.net.peers.is_empty() {
            info!("Bootstrap peers configured: {:?}", config.net.peers);
        }

        if config.net.pubsub_enabled {
            info!("GossipSub pubsub enabled");
        } else {
            info!("GossipSub pubsub disabled");
        }

        if config.net.relay_enabled {
            info!("Relay client transport enabled");
        } else {
            info!("Relay client disabled");
        }

        // Get and display peer ID
        match handle.local_peer_id().await {
            Ok(peer_id) => info!("Local peer ID: {}", peer_id),
            Err(e) => error!("Failed to get local peer ID: {}", e),
        }

        Ok((handle, events, replicators, host_task))
    }
}
