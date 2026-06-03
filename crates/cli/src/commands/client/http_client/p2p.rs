//! P2P HTTP client methods

use defra_http::router::{ExplicitReplayCapabilityInput, RemoteManageOp, RemoteManageQueryOp};
use serde::{Deserialize, Serialize};

use super::HttpClient;
use crate::error::Result;

/// Body for a relayed mutating management request (mirrors `ManageRequestBody`).
#[derive(Debug, Serialize)]
struct ManageRequestBody<'a> {
    #[serde(rename = "Target")]
    target: &'a str,
    #[serde(rename = "AuthToken")]
    auth_token: &'a str,
    #[serde(rename = "Op")]
    op: RemoteManageOp,
}

/// Body for a relayed read-only management query (mirrors `ManageQueryRequestBody`).
#[derive(Debug, Serialize)]
struct ManageQueryRequestBody<'a> {
    #[serde(rename = "Target")]
    target: &'a str,
    #[serde(rename = "AuthToken")]
    auth_token: &'a str,
    #[serde(rename = "Op")]
    op: RemoteManageQueryOp,
}

/// P2P node info
#[derive(Debug, Deserialize, Serialize)]
pub struct P2pInfo {
    /// Peer ID
    #[serde(rename = "id", alias = "ID", alias = "peerId", alias = "PeerID")]
    pub id: String,

    /// Listen addresses
    #[serde(rename = "addresses", alias = "Addresses", default)]
    pub addresses: Vec<String>,
}

/// P2P peer info
#[derive(Debug, Deserialize, Serialize)]
pub struct P2pPeerInfo {
    /// Peer ID
    #[serde(rename = "id", alias = "ID")]
    pub id: String,

    /// Peer address
    #[serde(rename = "address", alias = "Address", default)]
    pub address: Option<String>,
}

/// P2P replicator info
#[derive(Debug, Deserialize, Serialize)]
pub struct P2pReplicatorInfo {
    /// Peer ID
    #[serde(rename = "id", alias = "ID", default)]
    pub id: Option<String>,

    /// Collections being replicated
    #[serde(
        rename = "collections",
        alias = "Collections",
        alias = "CollectionIDs",
        default
    )]
    pub collections: Vec<String>,

    /// Peer address
    #[serde(rename = "address", alias = "Address", default)]
    pub address: Option<String>,

    /// Active=0, Inactive=1.
    #[serde(rename = "status", alias = "Status", default)]
    pub status: Option<u8>,

    /// Last time the replicator status changed.
    #[serde(
        rename = "lastStatusChange",
        alias = "LastStatusChange",
        alias = "last_status_change",
        default
    )]
    pub last_status_change: Option<String>,
}

/// P2P replicator request (Go-compatible format).
#[derive(Debug, Serialize)]
pub struct P2pReplicatorRequest {
    #[serde(rename = "Collections")]
    pub collections: Vec<String>,
    #[serde(rename = "Addresses")]
    pub addresses: Vec<String>,
    #[serde(
        rename = "ExplicitReplayCapabilities",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub explicit_replay_capabilities: Vec<ExplicitReplayCapabilityInput>,
}

/// P2P collection request
#[derive(Debug, Serialize)]
pub struct P2pCollectionRequest {
    pub collections: Vec<String>,
}

/// P2P peer add request
#[derive(Debug, Serialize)]
pub struct P2pPeerAddRequest {
    pub address: String,
}

/// P2P connect request
#[derive(Debug, Serialize)]
pub struct P2pConnectRequest {
    pub addresses: Vec<String>,
}

/// P2P document request
#[derive(Debug, Serialize)]
pub struct P2pDocumentRequest {
    #[serde(rename = "docIDs", skip_serializing_if = "Vec::is_empty")]
    pub doc_ids: Vec<String>,
    #[serde(rename = "schemaIDs", skip_serializing_if = "Vec::is_empty")]
    pub schema_ids: Vec<String>,
}

impl HttpClient {
    pub async fn p2p_peers_add(&self, address: &str) -> Result<()> {
        let url = format!("{}/api/v0/p2p/peers", self.base_url);
        let body = serde_json::to_string(&P2pPeerAddRequest {
            address: address.to_string(),
        })?;
        self.request_void("POST", &url, Some(&body)).await
    }

    pub async fn p2p_replicator_add(
        &self,
        collections: &[String],
        address: Option<&str>,
        explicit_replay_capabilities: &[ExplicitReplayCapabilityInput],
    ) -> Result<()> {
        let url = format!("{}/api/v0/p2p/replicator", self.base_url);
        let addresses = address.map(|s| vec![s.to_string()]).unwrap_or_default();
        let body = serde_json::to_string(&P2pReplicatorRequest {
            collections: collections.to_vec(),
            addresses,
            explicit_replay_capabilities: explicit_replay_capabilities.to_vec(),
        })?;
        self.request_void("POST", &url, Some(&body)).await
    }

    pub async fn p2p_replicator_delete(
        &self,
        collections: &[String],
        address: Option<&str>,
    ) -> Result<()> {
        let url = format!("{}/api/v0/p2p/replicator", self.base_url);
        let body = serde_json::to_string(&serde_json::json!({
            "ID": address,
            "Collections": collections,
        }))?;
        self.request_void("DELETE", &url, Some(&body)).await
    }

    pub async fn p2p_collection_add(&self, collections: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/collections", self.base_url);
        let body = serde_json::to_string(&collections)?;
        self.request_void("POST", &url, Some(&body)).await
    }

    pub async fn p2p_collection_remove(&self, collections: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/collections", self.base_url);
        let body = serde_json::to_string(&collections)?;
        self.request_void("DELETE", &url, Some(&body)).await
    }

    pub async fn p2p_connect(&self, addresses: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/connect", self.base_url);
        let body = serde_json::to_string(&addresses)?;
        self.request_void("POST", &url, Some(&body)).await
    }

    pub async fn p2p_document_add(&self, doc_ids: &[String], _schema_ids: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/documents", self.base_url);
        let body = serde_json::to_string(&doc_ids)?;
        self.request_void("POST", &url, Some(&body)).await
    }

    pub async fn p2p_document_remove(
        &self,
        doc_ids: &[String],
        _schema_ids: &[String],
    ) -> Result<()> {
        let url = format!("{}/api/v0/p2p/documents", self.base_url);
        let body = serde_json::to_string(&doc_ids)?;
        self.request_void("DELETE", &url, Some(&body)).await
    }

    pub async fn p2p_document_sync(&self, collection_name: &str, doc_ids: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/documents/sync", self.base_url);
        let body = serde_json::to_string(&serde_json::json!({
            "collectionName": collection_name,
            "docIDs": doc_ids,
        }))?;
        self.request_void("POST", &url, Some(&body)).await
    }

    pub async fn p2p_collection_sync_versions(&self, version_ids: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/collections/sync-versions", self.base_url);
        let body = serde_json::to_string(&serde_json::json!({
            "versionIDs": version_ids,
        }))?;
        self.request_void("POST", &url, Some(&body)).await
    }

    pub async fn p2p_collection_sync_branchable(&self, collection_id: &str) -> Result<()> {
        let url = format!("{}/api/v0/p2p/collections/sync-branchable", self.base_url);
        let body = serde_json::to_string(&serde_json::json!({
            "collectionID": collection_id,
        }))?;
        self.request_void("POST", &url, Some(&body)).await
    }

    /// Relay a mutating management request to a P2P-only peer via this node.
    ///
    /// `target` is the peer address this node dials; `auth_token` is the
    /// caller-minted JWT (`aud` = target peer-id).
    pub async fn p2p_manage(
        &self,
        target: &str,
        auth_token: &str,
        op: RemoteManageOp,
    ) -> Result<()> {
        let url = format!("{}/api/v0/p2p/manage", self.base_url);
        let body = serde_json::to_string(&ManageRequestBody {
            target,
            auth_token,
            op,
        })?;
        self.request_void("POST", &url, Some(&body)).await
    }

    /// Relay a read-only management query to a P2P-only peer via this node.
    pub async fn p2p_manage_query(
        &self,
        target: &str,
        auth_token: &str,
        op: RemoteManageQueryOp,
    ) -> Result<ManageQueryResultResponse> {
        let url = format!("{}/api/v0/p2p/manage/query", self.base_url);
        let body = serde_json::to_string(&ManageQueryRequestBody {
            target,
            auth_token,
            op,
        })?;
        self.request_json("POST", &url, Some(&body)).await
    }
}

/// Deserializable mirror of [`RemoteManageQueryResult`] for client responses.
///
/// The server-side `RemoteManageQueryResult` is serialize-only (it embeds a
/// response type), so the client needs its own deserializable shape.
#[derive(Debug, Deserialize)]
#[serde(tag = "Kind")]
pub enum ManageQueryResultResponse {
    Replicators { replicators: Vec<P2pReplicatorInfo> },
    Strings { values: Vec<String> },
}

/// Mint an actor JWT for a P2P management request.
///
/// `private_key_hex` is the caller's identity private key (32-byte secp256k1 or
/// 64-byte ed25519, hex-encoded). `target_peer_id` becomes the token audience so
/// the target node (B) can reject tokens minted for any other peer.
pub fn mint_manage_token(private_key_hex: &str, target_peer_id: &str) -> Result<String> {
    let key_bytes = hex::decode(private_key_hex)
        .map_err(|e| crate::error::Error::InvalidIdentity(format!("invalid hex: {e}")))?;
    let id = super::super::raw_identity_from_key_bytes("manage", &key_bytes)?;

    let token_bytes = identity::new_token(
        &id,
        std::time::Duration::from_secs(15 * 60),
        Some(target_peer_id.to_string()),
        None,
    )
    .map_err(|e| {
        crate::error::Error::InvalidIdentity(format!("failed to mint manage token: {e}"))
    })?;

    String::from_utf8(token_bytes)
        .map_err(|e| crate::error::Error::InvalidIdentity(format!("token is not valid UTF-8: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicator_info_accepts_go_status_fields() {
        let json = r#"{"ID":"peer-1","Addresses":["/ip4/127.0.0.1/tcp/9000"],"CollectionIDs":["Users"],"Status":1,"LastStatusChange":"2026-04-26T10:00:00Z"}"#;
        let info: P2pReplicatorInfo = serde_json::from_str(json).unwrap();

        assert_eq!(info.id.as_deref(), Some("peer-1"));
        assert_eq!(info.collections, vec!["Users"]);
        assert_eq!(info.status, Some(1));
        assert_eq!(
            info.last_status_change.as_deref(),
            Some("2026-04-26T10:00:00Z")
        );
    }

    #[test]
    fn replicator_info_accepts_snake_case_last_status_change() {
        // The manage-query (ReplicatorList) path embeds the raw http
        // `ReplicatorInfo`, which serializes `last_status_change` as snake_case.
        let json = r#"{"id":"peer-1","collections":["Users"],"status":0,"last_status_change":"2026-04-26T10:00:00Z"}"#;
        let info: P2pReplicatorInfo = serde_json::from_str(json).unwrap();

        assert_eq!(
            info.last_status_change.as_deref(),
            Some("2026-04-26T10:00:00Z")
        );
    }
}
