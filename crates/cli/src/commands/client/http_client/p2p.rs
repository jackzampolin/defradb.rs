//! P2P HTTP client methods

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use urlencoding::encode;

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

/// P2P replicator request
#[derive(Debug, Serialize)]
pub struct P2pReplicatorRequest {
    pub collections: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
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

/// P2P document sync request
#[derive(Debug, Serialize)]
pub struct P2pDocumentSyncRequest {
    #[serde(rename = "docID", skip_serializing_if = "Option::is_none")]
    pub doc_id: Option<String>,
    #[serde(rename = "schemaID", skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<String>,
}

impl HttpClient {
    /// Get P2P node info
    pub async fn p2p_info(&self) -> Result<P2pInfo> {
        let url = format!("{}/api/v0/p2p/info", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let info: P2pInfo = response.json().await?;
        Ok(info)
    }

    /// List connected peers
    pub async fn p2p_peers_list(&self) -> Result<Vec<P2pPeerInfo>> {
        let url = format!("{}/api/v0/p2p/peers", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let peers: Vec<P2pPeerInfo> = response.json().await?;
        Ok(peers)
    }

    /// Connect to a peer
    pub async fn p2p_peers_add(&self, address: &str) -> Result<()> {
        let url = format!("{}/api/v0/p2p/peers", self.base_url);
        let request = P2pPeerAddRequest {
            address: address.to_string(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// List replicators
    pub async fn p2p_replicator_list(&self) -> Result<Vec<P2pReplicatorInfo>> {
        let url = format!("{}/api/v0/p2p/replicator", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let replicators: Vec<P2pReplicatorInfo> = response.json().await?;
        Ok(replicators)
    }

    /// Add a replicator
    pub async fn p2p_replicator_add(
        &self,
        collections: &[String],
        address: Option<&str>,
    ) -> Result<()> {
        let url = format!("{}/api/v0/p2p/replicator", self.base_url);
        let request = P2pReplicatorRequest {
            collections: collections.to_vec(),
            address: address.map(|s| s.to_string()),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Delete a replicator
    pub async fn p2p_replicator_delete(
        &self,
        collections: &[String],
        address: Option<&str>,
    ) -> Result<()> {
        let mut url = format!("{}/api/v0/p2p/replicator", self.base_url);

        // Build query parameters with URL encoding
        let mut params = Vec::new();
        for col in collections {
            params.push(format!("collections={}", encode(col)));
        }
        if let Some(addr) = address {
            params.push(format!("address={}", encode(addr)));
        }
        if !params.is_empty() {
            url = format!("{}?{}", url, params.join("&"));
        }

        let response = self.send_with_retry("DELETE", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// List P2P collections
    pub async fn p2p_collection_list(&self) -> Result<Vec<String>> {
        let url = format!("{}/api/v0/p2p/collections", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let collections: Vec<String> = response.json().await?;
        Ok(collections)
    }

    /// Add collections to P2P
    pub async fn p2p_collection_add(&self, collections: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/collections", self.base_url);
        let request = P2pCollectionRequest {
            collections: collections.to_vec(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Remove collections from P2P
    pub async fn p2p_collection_remove(&self, collections: &[String]) -> Result<()> {
        let mut url = format!("{}/api/v0/p2p/collections", self.base_url);

        // Build query parameters with URL encoding
        let params: Vec<String> = collections
            .iter()
            .map(|c| format!("collections={}", encode(c)))
            .collect();
        if !params.is_empty() {
            url = format!("{}?{}", url, params.join("&"));
        }

        let response = self.send_with_retry("DELETE", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Get active peers
    pub async fn p2p_active_peers(&self) -> Result<JsonValue> {
        let url = format!("{}/api/v0/p2p/active-peers", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Connect to peer addresses
    pub async fn p2p_connect(&self, addresses: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/connect", self.base_url);
        let request = P2pConnectRequest {
            addresses: addresses.to_vec(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Add documents to P2P sync
    pub async fn p2p_document_add(&self, doc_ids: &[String], schema_ids: &[String]) -> Result<()> {
        let url = format!("{}/api/v0/p2p/documents", self.base_url);
        let request = P2pDocumentRequest {
            doc_ids: doc_ids.to_vec(),
            schema_ids: schema_ids.to_vec(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Remove documents from P2P sync
    pub async fn p2p_document_remove(
        &self,
        doc_ids: &[String],
        schema_ids: &[String],
    ) -> Result<()> {
        let url = format!("{}/api/v0/p2p/documents", self.base_url);
        let request = P2pDocumentRequest {
            doc_ids: doc_ids.to_vec(),
            schema_ids: schema_ids.to_vec(),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("DELETE", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// List P2P synced documents
    pub async fn p2p_document_list(&self) -> Result<JsonValue> {
        let url = format!("{}/api/v0/p2p/documents", self.base_url);
        let response = self.send_with_retry("GET", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        let result: JsonValue = response.json().await?;
        Ok(result)
    }

    /// Sync a P2P document
    pub async fn p2p_document_sync(
        &self,
        doc_id: Option<&str>,
        schema_id: Option<&str>,
    ) -> Result<()> {
        let url = format!("{}/api/v0/p2p/documents/sync", self.base_url);
        let request = P2pDocumentSyncRequest {
            doc_id: doc_id.map(|s| s.to_string()),
            schema_id: schema_id.map(|s| s.to_string()),
        };
        let body = serde_json::to_string(&request)?;
        let response = self.send_with_retry("POST", &url, Some(&body)).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }

    /// Sync P2P collection versions
    pub async fn p2p_collection_sync(&self) -> Result<()> {
        let url = format!("{}/api/v0/p2p/collections/sync", self.base_url);
        let response = self.send_with_retry("POST", &url, None).await?;

        if !response.status().is_success() {
            return Err(Self::extract_error(response).await);
        }

        Ok(())
    }
}
