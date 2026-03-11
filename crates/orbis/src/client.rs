//! Orbis gRPC client for threshold BLS signing.
//!
//! Delegates document signing to an Orbis ring's UtilityService.
//! The client maintains a dedicated single-threaded tokio runtime
//! to allow synchronous `sign_sync()` calls from the block builder
//! (which runs inside `spawn_blocking` → `Handle::block_on`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use defra_core::signing::SigningAuthorization;
use serde::Serialize;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::info;

use defra_core::signing::RemoteSigner;

use crate::proto::utility_service_client::UtilityServiceClient;
use crate::proto::{DerivePublicKeyRequest, SignRequest};

#[derive(Debug, Clone, Serialize)]
struct UtilitySignClaims {
    #[serde(skip_serializing_if = "String::is_empty")]
    namespace: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    derivation_id: String,
    message: Vec<u8>,
    ring_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    derivation: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "String::is_empty")]
    policy_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    resource: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    object_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    permission: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    decision_id: String,
}

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

    fn build_sign_request(
        &self,
        data: &[u8],
        authorization: Option<&SigningAuthorization>,
    ) -> SignRequest {
        let (policy_id, resource, object_id, permission, decision_id) = match authorization {
            Some(SigningAuthorization::Policy {
                policy_id,
                resource,
                object_id,
                permission,
            }) => (
                policy_id.clone(),
                resource.clone(),
                object_id.clone(),
                permission.clone(),
                String::new(),
            ),
            Some(SigningAuthorization::Decision { decision_id }) => (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                decision_id.clone(),
            ),
            None => (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ),
        };

        SignRequest {
            ring_id: self.ring_id.clone(),
            message: data.to_vec(),
            derivation: self.derivation.clone(),
            algorithm: 0, // UNSPECIFIED — use ring's native algorithm
            options: Default::default(),
            policy_id,
            resource,
            object_id,
            permission,
            decision_id,
        }
    }

    /// Create a JWT bearer token signed by the service identity.
    fn create_bearer_token(
        &self,
        message: Vec<u8>,
        authorization: Option<&SigningAuthorization>,
    ) -> Result<String, String> {
        let (policy_id, resource, object_id, permission, decision_id) = match authorization {
            Some(SigningAuthorization::Policy {
                policy_id,
                resource,
                object_id,
                permission,
            }) => (
                policy_id.clone(),
                resource.clone(),
                object_id.clone(),
                permission.clone(),
                String::new(),
            ),
            Some(SigningAuthorization::Decision { decision_id }) => (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                decision_id.clone(),
            ),
            None => (
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                String::new(),
            ),
        };

        let token_bytes = identity::new_token_with_custom_claims(
            self.service_identity.as_ref(),
            Duration::from_secs(300), // 5 minutes
            None,
            None,
            UtilitySignClaims {
                namespace: String::new(),
                derivation_id: String::new(),
                message,
                ring_id: self.ring_id.clone(),
                derivation: if self.derivation.is_empty() {
                    None
                } else {
                    Some(self.derivation.clone())
                },
                policy_id,
                resource,
                object_id,
                permission,
                decision_id,
            },
        )
        .map_err(|e| format!("failed to create JWT for Orbis auth: {}", e))?;

        String::from_utf8(token_bytes).map_err(|e| format!("JWT is not valid UTF-8: {}", e))
    }

    /// Sign data via the Orbis ring (async).
    async fn sign_async(
        &self,
        data: &[u8],
        authorization: Option<SigningAuthorization>,
    ) -> Result<Vec<u8>, String> {
        let connect_start = Instant::now();
        let channel = Channel::from_shared(self.endpoint.clone())
            .map_err(|e| format!("invalid Orbis endpoint: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("failed to connect to Orbis: {}", e))?;
        info!(
            ring_id = %self.ring_id,
            elapsed = ?connect_start.elapsed(),
            "Orbis gRPC channel connected"
        );

        let bearer_token = self.create_bearer_token(data.to_vec(), authorization.as_ref())?;

        #[allow(clippy::result_large_err)]
        let mut client =
            UtilityServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
                let val: MetadataValue<_> = format!("Bearer {}", bearer_token)
                    .parse()
                    .map_err(|_| tonic::Status::internal("invalid bearer token"))?;
                req.metadata_mut().insert("authorization", val);
                Ok(req)
            });

        let sign_rpc_start = Instant::now();
        let resp = client
            .sign(self.build_sign_request(data, authorization.as_ref()))
            .await
            .map_err(|e| format!("Orbis Sign RPC failed: {}", e))?;
        info!(
            ring_id = %self.ring_id,
            elapsed = ?sign_rpc_start.elapsed(),
            "Orbis Sign RPC completed"
        );

        let signature = resp.into_inner().signature;
        if signature.is_empty() {
            return Err("Orbis Sign returned empty signature".into());
        }

        Ok(signature)
    }
}

impl RemoteSigner for OrbisClient {
    fn sign_sync(
        &self,
        data: &[u8],
        authorization: Option<&SigningAuthorization>,
    ) -> Result<Vec<u8>, String> {
        self.runtime
            .block_on(self.sign_async(data, authorization.cloned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use identity::Identity;

    fn make_test_client() -> OrbisClient {
        let private_key = crypto::generate_secp256k1().expect("should generate secp256k1 key");
        let service_identity =
            Arc::new(identity::RawIdentity::from_secp256k1(private_key).expect("identity"));

        OrbisClient {
            endpoint: "http://127.0.0.1:50051".to_string(),
            ring_id: "ring-123".to_string(),
            derivation: b"platform".to_vec(),
            public_key_bytes: vec![1, 2, 3],
            public_key_hex: "010203".to_string(),
            signer_did: "did:key:zSigner".to_string(),
            service_identity,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime"),
        }
    }

    fn decode_jwt_payload(token: &str) -> serde_json::Value {
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT should contain three segments");
        let payload = URL_SAFE_NO_PAD
            .decode(parts[1])
            .expect("payload should base64url decode");
        serde_json::from_slice(&payload).expect("payload should be JSON")
    }

    #[test]
    fn create_bearer_token_embeds_policy_authorization_claims() {
        let client = make_test_client();
        let auth = SigningAuthorization::Policy {
            policy_id: "policy-1".to_string(),
            resource: "transcript".to_string(),
            object_id: "transcript".to_string(),
            permission: "writer".to_string(),
        };

        let token = client
            .create_bearer_token(b"hello".to_vec(), Some(&auth))
            .expect("token should build");
        let parsed = identity::from_token(token.as_bytes()).expect("token should verify");
        assert_eq!(
            parsed.did().expect("token DID").as_str(),
            client
                .service_identity
                .did()
                .expect("service identity DID")
                .as_str()
        );

        let payload = decode_jwt_payload(&token);
        assert_eq!(payload["ring_id"], "ring-123");
        assert_eq!(payload["policy_id"], "policy-1");
        assert_eq!(payload["resource"], "transcript");
        assert_eq!(payload["object_id"], "transcript");
        assert_eq!(payload["permission"], "writer");
        assert_eq!(
            payload["message"],
            serde_json::json!([104, 101, 108, 108, 111])
        );
        assert_eq!(
            payload["derivation"],
            serde_json::json!([112, 108, 97, 116, 102, 111, 114, 109])
        );
    }

    #[test]
    fn build_sign_request_populates_authorization_fields() {
        let client = make_test_client();
        let auth = SigningAuthorization::Policy {
            policy_id: "policy-1".to_string(),
            resource: "transcript".to_string(),
            object_id: "transcript".to_string(),
            permission: "writer".to_string(),
        };

        let request = client.build_sign_request(b"hello", Some(&auth));
        assert_eq!(request.ring_id, "ring-123");
        assert_eq!(request.message, b"hello");
        assert_eq!(request.derivation, b"platform");
        assert_eq!(request.policy_id, "policy-1");
        assert_eq!(request.resource, "transcript");
        assert_eq!(request.object_id, "transcript");
        assert_eq!(request.permission, "writer");
        assert!(request.decision_id.is_empty());
    }
}
