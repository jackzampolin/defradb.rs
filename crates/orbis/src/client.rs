//! Orbis gRPC client for threshold BLS signing.
//!
//! Delegates document signing to an Orbis ring's UtilityService.
//! The client maintains a dedicated single-threaded tokio runtime
//! to allow synchronous `sign_sync()` calls from the block builder
//! (which runs inside `spawn_blocking` → `Handle::block_on`).

use std::sync::Arc;
use std::time::{Duration, Instant};

use crypto::PublicKey;
use defra_core::signing::SigningAuthorization;
use serde::Serialize;
use thiserror::Error;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tracing::info;

use defra_core::signing::RemoteSigner;

use crate::proto::utility_service_client::UtilityServiceClient;
use crate::proto::{DerivePublicKeyRequest, SignRequest};

#[derive(Debug, Error)]
pub enum OrbisClientError {
    #[error("invalid Orbis endpoint: {0}")]
    InvalidEndpoint(String),

    #[error("failed to connect to Orbis at {endpoint}: {source}")]
    Connect {
        endpoint: String,
        #[source]
        source: tonic::transport::Error,
    },

    #[error("DerivePublicKey failed: {0}")]
    DerivePublicKey(#[source] tonic::Status),

    #[error("DerivePublicKey returned empty public key")]
    EmptyPublicKey,

    #[error("DerivePublicKey returned invalid BLS public key: {0}")]
    InvalidPublicKey(String),

    #[error("failed to derive BLS DID: {0}")]
    DeriveDid(String),

    #[error("failed to create Orbis runtime: {0}")]
    RuntimeBuild(std::io::Error),

    #[error("failed to create JWT for Orbis auth: {0}")]
    CreateBearerToken(identity::Error),

    #[error("JWT is not valid UTF-8: {0}")]
    InvalidBearerTokenUtf8(std::string::FromUtf8Error),

    #[error("Orbis Sign RPC failed: {0}")]
    Sign(#[source] tonic::Status),

    #[error("Orbis Sign returned empty signature")]
    EmptySignature,

    #[error("Orbis sign_sync worker thread panicked")]
    WorkerThreadPanicked,
}

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
    ) -> Result<Self, OrbisClientError> {
        let derivation_bytes = derivation.into_bytes();

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| OrbisClientError::InvalidEndpoint(e.to_string()))?
            .connect()
            .await
            .map_err(|source| OrbisClientError::Connect {
                endpoint: endpoint.clone(),
                source,
            })?;

        let mut client = UtilityServiceClient::new(channel);

        let resp = client
            .derive_public_key(DerivePublicKeyRequest {
                ring_id: ring_id.clone(),
                derivation: derivation_bytes.clone(),
            })
            .await
            .map_err(OrbisClientError::DerivePublicKey)?;

        let public_key_bytes = resp.into_inner().public_key;
        if public_key_bytes.is_empty() {
            return Err(OrbisClientError::EmptyPublicKey);
        }

        let public_key = crypto::BlsPublicKey::from_bytes(&public_key_bytes)
            .map_err(|e| OrbisClientError::InvalidPublicKey(e.to_string()))?;
        let public_key_hex = hex::encode(&public_key_bytes);

        let signer_did = public_key
            .did()
            .map_err(|e| OrbisClientError::DeriveDid(e.to_string()))?;

        info!(
            ring_id = %ring_id,
            signer_did = %signer_did,
            "Orbis signer initialized"
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(OrbisClientError::RuntimeBuild)?;

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
            Some(_) => (
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
    ) -> Result<String, OrbisClientError> {
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
            Some(_) => (
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
        .map_err(OrbisClientError::CreateBearerToken)?;

        String::from_utf8(token_bytes).map_err(OrbisClientError::InvalidBearerTokenUtf8)
    }

    /// Sign data via the Orbis ring (async).
    async fn sign_async(
        &self,
        data: &[u8],
        authorization: Option<SigningAuthorization>,
    ) -> Result<Vec<u8>, OrbisClientError> {
        let connect_start = Instant::now();
        let channel = Channel::from_shared(self.endpoint.clone())
            .map_err(|e| OrbisClientError::InvalidEndpoint(e.to_string()))?
            .connect()
            .await
            .map_err(|source| OrbisClientError::Connect {
                endpoint: self.endpoint.clone(),
                source,
            })?;
        info!(
            ring_id = %self.ring_id,
            elapsed = ?connect_start.elapsed(),
            "Orbis gRPC channel connected"
        );

        match authorization.as_ref() {
            Some(SigningAuthorization::Decision { decision_id }) => {
                info!(
                    ring_id = %self.ring_id,
                    signer_did = %self.signer_did,
                    decision_id = %decision_id,
                    "Orbis sign request using access decision authorization"
                );
            }
            Some(SigningAuthorization::Policy {
                policy_id,
                resource,
                object_id,
                permission,
            }) => {
                info!(
                    ring_id = %self.ring_id,
                    signer_did = %self.signer_did,
                    policy_id = %policy_id,
                    resource = %resource,
                    object_id = %object_id,
                    permission = %permission,
                    "Orbis sign request using direct policy authorization"
                );
            }
            None => {
                info!(
                    ring_id = %self.ring_id,
                    signer_did = %self.signer_did,
                    "Orbis sign request has no authorization context"
                );
            }
            Some(_) => {}
        }

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
            .map_err(OrbisClientError::Sign)?;
        info!(
            ring_id = %self.ring_id,
            elapsed = ?sign_rpc_start.elapsed(),
            "Orbis Sign RPC completed"
        );

        let signature = resp.into_inner().signature;
        if signature.is_empty() {
            return Err(OrbisClientError::EmptySignature);
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
        let authorization = authorization.cloned();

        if tokio::runtime::Handle::try_current().is_ok() {
            // `sign_sync` can be called while Defra is already executing on a Tokio
            // runtime thread (for example, the HTTP GraphQL path). Running
            // `Runtime::block_on` directly there panics, so hop to a plain OS thread
            // and use the dedicated Orbis runtime from that synchronous context.
            std::thread::scope(|scope| {
                scope
                    .spawn(|| self.runtime.block_on(self.sign_async(data, authorization)))
                    .join()
                    .map_err(|_| OrbisClientError::WorkerThreadPanicked)?
            })
            .map_err(|e| e.to_string())
        } else {
            self.runtime
                .block_on(self.sign_async(data, authorization))
                .map_err(|e| e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use identity::Identity;
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    use crate::proto::utility_service_server::{UtilityService, UtilityServiceServer};
    use crate::proto::{DerivePublicKeyResponse, SignResponse};

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

    #[derive(Clone)]
    struct MockUtilityService {
        derive_public_key_response: DerivePublicKeyResponse,
        sign_response: SignResponse,
    }

    #[tonic::async_trait]
    impl UtilityService for MockUtilityService {
        async fn derive_public_key(
            &self,
            _request: Request<DerivePublicKeyRequest>,
        ) -> Result<Response<DerivePublicKeyResponse>, Status> {
            Ok(Response::new(self.derive_public_key_response.clone()))
        }

        async fn sign(
            &self,
            _request: Request<SignRequest>,
        ) -> Result<Response<SignResponse>, Status> {
            Ok(Response::new(self.sign_response.clone()))
        }
    }

    struct TestServer {
        endpoint: String,
        shutdown: Option<oneshot::Sender<()>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl TestServer {
        async fn start(service: MockUtilityService) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test server");
            let addr = listener.local_addr().expect("local addr");
            listener
                .set_nonblocking(true)
                .expect("set nonblocking test server");
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            let incoming = TcpListenerStream::new(listener);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();

            let task = tokio::spawn(async move {
                let shutdown = async {
                    let _ = shutdown_rx.await;
                };

                Server::builder()
                    .add_service(UtilityServiceServer::new(service))
                    .serve_with_incoming_shutdown(incoming, shutdown)
                    .await
                    .expect("test server should run");
            });

            Self {
                endpoint: format!("http://{}", addr),
                shutdown: Some(shutdown_tx),
                task,
            }
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            self.task.abort();
        }
    }

    fn make_test_service_identity() -> Arc<identity::RawIdentity> {
        let private_key = crypto::generate_secp256k1().expect("should generate secp256k1 key");
        Arc::new(identity::RawIdentity::from_secp256k1(private_key).expect("identity"))
    }

    fn valid_bls_public_key_bytes() -> Vec<u8> {
        let ikm = [7u8; 32];
        blst::min_pk::SecretKey::key_gen(&ikm, &[])
            .expect("deterministic BLS secret key")
            .sk_to_pk()
            .compress()
            .to_vec()
    }

    #[tokio::test]
    async fn new_returns_error_for_empty_public_key_response() {
        let server = TestServer::start(MockUtilityService {
            derive_public_key_response: DerivePublicKeyResponse {
                public_key: Vec::new(),
                algorithm: 0,
            },
            sign_response: SignResponse {
                signature: vec![1],
                algorithm: 0,
                public_key: vec![],
                metadata: Default::default(),
            },
        })
        .await;

        let result = OrbisClient::new(
            server.endpoint.clone(),
            "ring-123".to_string(),
            "platform".to_string(),
            make_test_service_identity(),
        )
        .await;

        match result {
            Ok(_) => panic!("empty public key should fail"),
            Err(err) => assert!(matches!(err, OrbisClientError::EmptyPublicKey)),
        }
    }

    #[tokio::test]
    async fn new_returns_error_for_invalid_public_key_response() {
        let server = TestServer::start(MockUtilityService {
            derive_public_key_response: DerivePublicKeyResponse {
                public_key: vec![1, 2, 3],
                algorithm: 0,
            },
            sign_response: SignResponse {
                signature: vec![1],
                algorithm: 0,
                public_key: vec![],
                metadata: Default::default(),
            },
        })
        .await;

        let result = OrbisClient::new(
            server.endpoint.clone(),
            "ring-123".to_string(),
            "platform".to_string(),
            make_test_service_identity(),
        )
        .await;

        match result {
            Ok(_) => panic!("invalid public key bytes should fail"),
            Err(err) => assert!(matches!(err, OrbisClientError::InvalidPublicKey(_))),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sign_sync_returns_error_for_empty_signature_response() {
        let server = TestServer::start(MockUtilityService {
            derive_public_key_response: DerivePublicKeyResponse {
                public_key: valid_bls_public_key_bytes(),
                algorithm: 0,
            },
            sign_response: SignResponse {
                signature: Vec::new(),
                algorithm: 0,
                public_key: vec![],
                metadata: Default::default(),
            },
        })
        .await;

        let client = OrbisClient::new(
            server.endpoint.clone(),
            "ring-123".to_string(),
            "platform".to_string(),
            make_test_service_identity(),
        )
        .await
        .expect("client should build");

        let err = tokio::task::spawn_blocking(move || {
            client
                .sign_sync(b"hello", None)
                .expect_err("empty signature should fail")
        })
        .await
        .expect("blocking task should join");

        assert!(err.contains("Orbis Sign returned empty signature"));
    }
}
