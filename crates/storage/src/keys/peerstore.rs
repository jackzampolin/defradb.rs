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
    /// Unique replicator identifier (private to prevent construction of invalid keys)
    replicator_id: String,
}

/// The prefix used for all replicator keys.
const REPLICATOR_PREFIX: &str = "/rep/id/";

impl ReplicatorKey {
    /// Create a new ReplicatorKey.
    ///
    /// # Panics
    ///
    /// Panics if `replicator_id` is empty. Use `try_new` for fallible construction.
    pub fn new(replicator_id: impl Into<String>) -> Self {
        let id = replicator_id.into();
        assert!(!id.is_empty(), "replicator_id cannot be empty");
        Self { replicator_id: id }
    }

    /// Try to create a new ReplicatorKey, returning None if the ID is empty.
    pub fn try_new(replicator_id: impl Into<String>) -> Option<Self> {
        let id = replicator_id.into();
        if id.is_empty() {
            None
        } else {
            Some(Self { replicator_id: id })
        }
    }

    /// Parse a ReplicatorKey from its byte representation.
    ///
    /// Returns None if the bytes don't represent a valid replicator key.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(bytes).ok()?;
        let id = s.strip_prefix(REPLICATOR_PREFIX)?;
        if id.is_empty() {
            None
        } else {
            Some(Self {
                replicator_id: id.to_string(),
            })
        }
    }

    /// Get the replicator ID.
    pub fn replicator_id(&self) -> &str {
        &self.replicator_id
    }

    /// Create a prefix for all replicators
    pub fn replicator_prefix() -> Vec<u8> {
        REPLICATOR_PREFIX.as_bytes().to_vec()
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

/// ReplicatorRetryCommitKey: tracks COLLECTION-COMMIT replication failures.
///
/// Structure: /rep/retry/commit/[PeerID]/[CollectionID]/[CID]
///
/// Collection commits are doc-less: their DAG obligation is CID-scoped, not
/// document-scoped, so they cannot be keyed by `ReplicatorRetryDocIDKey` (whose
/// terminal segment is the document id — an empty doc id would collapse every
/// commit for a peer into one slot). Before this key existed, a failed
/// collection-commit push had nowhere to be recorded and failed PERMANENTLY:
/// receivers kept heads whose parents never arrived, so their pending-DAG
/// registrations could never complete (defradb#1113, source-inc/gents#696).
///
/// One record per (peer, collection, CID): collection-commit DAGs chain, so a
/// newer commit does NOT make an older undelivered one redundant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatorRetryCommitKey {
    /// Peer network identifier
    pub peer_id: String,
    /// Collection identifier
    pub collection_id: String,
    /// Root CID of the collection commit
    pub cid: String,
}

impl ReplicatorRetryCommitKey {
    /// Create a new ReplicatorRetryCommitKey
    pub fn new(
        peer_id: impl Into<String>,
        collection_id: impl Into<String>,
        cid: impl Into<String>,
    ) -> Self {
        Self {
            peer_id: peer_id.into(),
            collection_id: collection_id.into(),
            cid: cid.into(),
        }
    }

    /// Prefix for all collection-commit retry entries
    pub fn retry_commit_prefix() -> Vec<u8> {
        b"/rep/retry/commit/".to_vec()
    }

    /// Prefix for a specific peer's collection-commit retries
    pub fn peer_prefix(peer_id: impl Into<String>) -> Vec<u8> {
        let peer_id = peer_id.into();
        format!("/rep/retry/commit/{}/", peer_id).into_bytes()
    }
}

impl Key for ReplicatorRetryCommitKey {
    fn bytes(&self) -> Vec<u8> {
        format!(
            "/rep/retry/commit/{}/{}/{}",
            self.peer_id, self.collection_id, self.cid
        )
        .into_bytes()
    }

    fn to_string(&self) -> String {
        format!(
            "/rep/retry/commit/{}/{}/{}",
            self.peer_id, self.collection_id, self.cid
        )
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
        format!(
            "/se-retry/{}/{}/{}",
            self.peer_id, self.collection_id, self.doc_id
        )
        .into_bytes()
    }

    fn to_string(&self) -> String {
        format!(
            "/se-retry/{}/{}/{}",
            self.peer_id, self.collection_id, self.doc_id
        )
    }
}

#[cfg(test)]
#[path = "peerstore_tests.rs"]
mod tests;
