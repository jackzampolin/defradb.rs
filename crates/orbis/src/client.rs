//! Orbis gRPC client for threshold BLS signing.
//!
//! Delegates document signing to an Orbis ring's UtilityService.
//! The client maintains a dedicated single-threaded tokio runtime
//! to allow synchronous `sign_sync()` calls from the block builder
//! (which runs inside `spawn_blocking` → `Handle::block_on`).

use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::info;

use defra_core::signing::RemoteSigner;

use crate::proto::utility_service_client::UtilityServiceClient;
use crate::proto::{DerivePublicKeyRequest, SignRequest};

/// Orbis gRPC client for threshold BLS signing.
///
/// Holds a dedicated single-threaded tokio runtime so that `sign_sync()`
/// can be called from synchronous contexts without nesting `block_on`.
pub struct OrbisClient {
    endpoint: String,
    ring_id: String,
    derivation: Vec<u8>,
    /// BLS public key bytes from DerivePublicKey response
    public_key_bytes: Vec<u8>,
    /// Hex-encoded public key
    public_key_hex: String,
    /// DID derived from the BLS public key
    signer_did: String,
    /// Service identity for signing JWTs (authenticates Sign RPC)
    service_identity: Arc<identity::RawIdentity>,
    /// Dedicated runtime for sync bridge
    runtime: tokio::runtime::Runtime,
}

impl OrbisClient {
    /// Connect to an Orbis ring and derive the signer's BLS public key.
    ///
    /// Calls `DerivePublicKey` (unauthenticated) at startup to learn what
    /// DID the signer represents.
    pub async fn new(
        endpoint: String,
        ring_id: String,
        derivation: String,
        service_identity: Arc<identity::RawIdentity>,
    ) -> Result<Self, String> {
        let derivation_bytes = derivation.into_bytes();

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| format!("invalid Orbis endpoint: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to Orbis at {}: {}", endpoint, e))?;

        let mut client = UtilityServiceClient::new(channel);

        let resp = client
            .derive_public_key(DerivePublicKeyRequest {
                ring_id: ring_id.clone(),
                derivation: derivation_bytes.clone(),
            })
            .await
            .map_err(|e| format!("DerivePublicKey failed: {}", e))?;

        let public_key_bytes = resp.into_inner().public_key;
        if public_key_bytes.is_empty() {
            return Err("DerivePublicKey returned empty public key".into());
        }

        let public_key_hex = hex::encode(&public_key_bytes);

        let signer_did = crypto::did::create_did_key(crypto::KeyType::Bls12381, &public_key_bytes)
            .map_err(|e| format!("failed to derive BLS DID: {}", e))?;

        info!(
            ring_id = %ring_id,
            signer_did = %signer_did,
            "Orbis signer initialized"
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("failed to create Orbis runtime: {}", e))?;

        Ok(Self {
            endpoint,
            ring_id,
            derivation: derivation_bytes,
            public_key_bytes,
            public_key_hex,
            signer_did,
            service_identity,
            runtime,
        })
    }

    /// The DID that this signer represents (derived from the ring's BLS public key).
    pub fn signer_did(&self) -> &str {
        &self.signer_did
    }

    /// Raw BLS public key bytes.
    pub fn public_key_bytes(&self) -> &[u8] {
        &self.public_key_bytes
    }

    /// Hex-encoded BLS public key.
    pub fn public_key_hex(&self) -> &str {
        &self.public_key_hex
    }

    /// Create a JWT bearer token signed by the service identity.
    fn create_bearer_token(&self) -> Result<String, String> {
        let token_bytes = identity::new_token(
            self.service_identity.as_ref(),
            Duration::from_secs(300), // 5 minutes
            None,
            None,
        )
        .map_err(|e| format!("failed to create JWT for Orbis auth: {}", e))?;

        String::from_utf8(token_bytes).map_err(|e| format!("JWT is not valid UTF-8: {}", e))
    }

    /// Sign data via the Orbis ring (async).
    async fn sign_async(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        let channel = Channel::from_shared(self.endpoint.clone())
            .map_err(|e| format!("invalid Orbis endpoint: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to Orbis: {}", e))?;

        let bearer_token = self.create_bearer_token()?;

        let mut client =
            UtilityServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                let val: MetadataValue<_> = format!("Bearer {}", bearer_token)
                    .parse()
                    .map_err(|_| tonic::Status::internal("invalid bearer token"))?;
                req.metadata_mut().insert("authorization", val);
                Ok(req)
            });

        let resp = client
            .sign(SignRequest {
                ring_id: self.ring_id.clone(),
                message: data.to_vec(),
                derivation: self.derivation.clone(),
                algorithm: 0, // UNSPECIFIED — use ring's native algorithm
                options: Default::default(),
            })
            .await
            .map_err(|e| format!("Orbis Sign RPC failed: {}", e))?;

        let signature = resp.into_inner().signature;
        if signature.is_empty() {
            return Err("Orbis Sign returned empty signature".into());
        }

        Ok(signature)
    }
}

impl RemoteSigner for OrbisClient {
    fn sign_sync(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        self.runtime.block_on(self.sign_async(data))
    }
}
