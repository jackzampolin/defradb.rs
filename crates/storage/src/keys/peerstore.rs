/// Peerstore keys for peer and replication metadata
///
/// These keys are prefixed with 'p' at the store level and handle:
/// - Replicator configuration and state
/// - Replication retry tracking
/// - Search engine retry tracking

use crate::corekv::Key;

/// ReplicatorKey: Stores replicator configuration and state
///
/// Structure: /rep/id/[ReplicatorID]
/// Example: /rep/id/replicator_user_collection_peer1
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorKey {
    /// Unique replicator identifier
    pub replicator_id: String,
}

impl ReplicatorKey {
    /// Create a new ReplicatorKey
    pub fn new(replicator_id: impl Into<String>) -> Self {
        Self {
            replicator_id: replicator_id.into(),
        }
    }

    /// Create a prefix for all replicators
    pub fn replicator_prefix() -> Vec<u8> {
        b"/rep/id/".to_vec()
    }
}

impl Key for ReplicatorKey {
    fn bytes(&self) -> Vec<u8> {
        format!("/rep/id/{}", self.replicator_id).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/rep/id/{}", self.replicator_id)
    }
}

/// ReplicatorRetryIDKey: Tracks failed replication attempts by peer
///
/// Structure: /rep/retry/id/[PeerID]
/// Example: /rep/retry/id/QmXxxx...
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRetryIDKey {
    /// Peer network identifier
    pub peer_id: String,
}

impl ReplicatorRetryIDKey {
    /// Create a new ReplicatorRetryIDKey
    pub fn new(peer_id: impl Into<String>) -> Self {
        Self {
            peer_id: peer_id.into(),
        }
    }

    /// Create a prefix for all replication retry entries
    pub fn retry_prefix() -> Vec<u8> {
        b"/rep/retry/id/".to_vec()
    }
}

impl Key for ReplicatorRetryIDKey {
    fn bytes(&self) -> Vec<u8> {
        format!("/rep/retry/id/{}", self.peer_id).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/rep/retry/id/{}", self.peer_id)
    }
}

/// ReplicatorRetryDocIDKey: Tracks document-specific replication failures
///
/// Structure: /rep/retry/doc/[PeerID]/[DocID]
/// Example: /rep/retry/doc/QmXxxx.../bae123456789abcdef0123456789abcdef012345
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRetryDocIDKey {
    /// Peer network identifier
    pub peer_id: String,
    /// Document identifier
    pub doc_id: String,
}

impl ReplicatorRetryDocIDKey {
    /// Create a new ReplicatorRetryDocIDKey
    pub fn new(peer_id: impl Into<String>, doc_id: impl Into<String>) -> Self {
        Self {
            peer_id: peer_id.into(),
            doc_id: doc_id.into(),
        }
    }

    /// Create a prefix for all document retry entries
    pub fn retry_doc_prefix() -> Vec<u8> {
        b"/rep/retry/doc/".to_vec()
    }

    /// Create a prefix for a specific peer's document retries
    pub fn peer_prefix(peer_id: impl Into<String>) -> Vec<u8> {
        let peer_id = peer_id.into();
        format!("/rep/retry/doc/{}/", peer_id).into_bytes()
    }
}

impl Key for ReplicatorRetryDocIDKey {
    fn bytes(&self) -> Vec<u8> {
        format!("/rep/retry/doc/{}/{}", self.peer_id, self.doc_id).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/rep/retry/doc/{}/{}", self.peer_id, self.doc_id)
    }
}

/// PeerstoreSERetry: Tracks search engine indexing failures on peer
///
/// Structure: /se-retry/[PeerID]/[CollectionID]/[DocID]
/// Example: /se-retry/QmXxxx.../users/bae123456789abcdef0123456789abcdef012345
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerstoreSERetry {
    /// Peer network identifier
    pub peer_id: String,
    /// Collection identifier
    pub collection_id: String,
    /// Document identifier
    pub doc_id: String,
}

impl PeerstoreSERetry {
    /// Create a new PeerstoreSERetry key
    pub fn new(
        peer_id: impl Into<String>,
        collection_id: impl Into<String>,
        doc_id: impl Into<String>,
    ) -> Self {
        Self {
            peer_id: peer_id.into(),
            collection_id: collection_id.into(),
            doc_id: doc_id.into(),
        }
    }

    /// Create a prefix for all search engine retry entries
    pub fn se_retry_prefix() -> Vec<u8> {
        b"/se-retry/".to_vec()
    }

    /// Create a prefix for a specific peer's SE retry entries
    pub fn peer_prefix(peer_id: impl Into<String>) -> Vec<u8> {
        let peer_id = peer_id.into();
        format!("/se-retry/{}/", peer_id).into_bytes()
    }

    /// Create a prefix for a specific peer and collection
    pub fn peer_collection_prefix(
        peer_id: impl Into<String>,
        collection_id: impl Into<String>,
    ) -> Vec<u8> {
        let peer_id = peer_id.into();
        let collection_id = collection_id.into();
        format!("/se-retry/{}/{}/", peer_id, collection_id).into_bytes()
    }
}

impl Key for PeerstoreSERetry {
    fn bytes(&self) -> Vec<u8> {
        format!("/se-retry/{}/{}/{}", self.peer_id, self.collection_id, self.doc_id).into_bytes()
    }

    fn to_string(&self) -> String {
        format!("/se-retry/{}/{}/{}", self.peer_id, self.collection_id, self.doc_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replicator_key() {
        let key = ReplicatorKey::new("replicator_user_collection_peer1");
        assert_eq!(
            key.to_string(),
            "/rep/id/replicator_user_collection_peer1"
        );
        assert_eq!(key.bytes(), key.to_string().as_bytes());

        let prefix = ReplicatorKey::replicator_prefix();
        assert_eq!(prefix, b"/rep/id/");
    }

    #[test]
    fn test_replicator_retry_id_key() {
        let peer_id = "QmXxxx123456789";
        let key = ReplicatorRetryIDKey::new(peer_id);
        assert_eq!(key.to_string(), format!("/rep/retry/id/{}", peer_id));
        assert_eq!(key.bytes(), key.to_string().as_bytes());

        let prefix = ReplicatorRetryIDKey::retry_prefix();
        assert_eq!(prefix, b"/rep/retry/id/");
    }

    #[test]
    fn test_replicator_retry_doc_id_key() {
        let peer_id = "QmXxxx123456789";
        let doc_id = "bae123456789abcdef0123456789abcdef012345";
        let key = ReplicatorRetryDocIDKey::new(peer_id, doc_id);
        assert_eq!(
            key.to_string(),
            format!("/rep/retry/doc/{}/{}", peer_id, doc_id)
        );
        assert_eq!(key.bytes(), key.to_string().as_bytes());

        let prefix = ReplicatorRetryDocIDKey::retry_doc_prefix();
        assert_eq!(prefix, b"/rep/retry/doc/");

        let prefix = ReplicatorRetryDocIDKey::peer_prefix(peer_id);
        assert_eq!(prefix, format!("/rep/retry/doc/{}/", peer_id).as_bytes());
    }

    #[test]
    fn test_peerstore_se_retry() {
        let peer_id = "QmXxxx123456789";
        let collection_id = "users";
        let doc_id = "bae123456789abcdef0123456789abcdef012345";

        let key = PeerstoreSERetry::new(peer_id, collection_id, doc_id);
        assert_eq!(
            key.to_string(),
            format!("/se-retry/{}/{}/{}", peer_id, collection_id, doc_id)
        );
        assert_eq!(key.bytes(), key.to_string().as_bytes());

        let prefix = PeerstoreSERetry::se_retry_prefix();
        assert_eq!(prefix, b"/se-retry/");

        let prefix = PeerstoreSERetry::peer_prefix(peer_id);
        assert_eq!(prefix, format!("/se-retry/{}/", peer_id).as_bytes());

        let prefix = PeerstoreSERetry::peer_collection_prefix(peer_id, collection_id);
        assert_eq!(
            prefix,
            format!("/se-retry/{}/{}/", peer_id, collection_id).as_bytes()
        );
    }
}
