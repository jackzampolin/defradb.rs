//! P2P HTTP client methods

use defra_http::router::ExplicitReplayCapabilityInput;
use serde::{Deserialize, Serialize};

use super::HttpClient;
use crate::error::Result;

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
    #[serde(rename = "collections", alias = "Collections", default)]
    pub collections: Vec<String>,

    /// Peer address
    #[serde(rename = "address", alias = "Address", default)]
    pub address: Option<String>,
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
}
