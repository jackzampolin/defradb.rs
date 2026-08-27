//! GossipSub topic definitions for DefraDB.
//!
//! Topics follow Go implementation naming conventions:
//! - `doc-sync`: General document synchronization (pubsub-RPC, #828)
//! - `sync-branchable`: Branchable-collection sync (pubsub-RPC, #828)
//! - `encryption`: Encryption key exchange
//! - `{collection_id}`: Collection-specific updates
//! - `{doc_id}`: Document-specific updates

#[cfg(feature = "libp2p-transport")]
use libp2p::gossipsub::IdentTopic;

/// Well-known topic for general document synchronization.
pub const DOC_SYNC_TOPIC: &str = "doc-sync";

/// Well-known topic for branchable-collection sync. Matches Go's
/// `syncBranchableCollectionTopic` at
/// `defradb/internal/db/p2p/sync_branchable_col.go:35`.
pub const SYNC_BRANCHABLE_TOPIC: &str = "sync-branchable";

/// Well-known topic for encryption key exchange.
pub const ENCRYPTION_TOPIC: &str = "encryption";

/// Topic type for GossipSub subscriptions.
///
/// DefraDB uses several types of topics for different purposes:
/// - Fixed topics like `doc-sync` and `encryption` for system-wide operations
/// - Dynamic topics based on collection or document IDs for targeted updates
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DefraTopic {
    /// General document synchronization topic.
    DocSync,

    /// Branchable-collection sync topic (Go parity: `sync-branchable`).
    SyncBranchable,

    /// Encryption key exchange topic.
    Encryption,

    /// Collection-specific topic for updates to documents in a collection.
    Collection(String),

    /// Document-specific topic for updates to a single document.
    Document(String),

    /// Custom topic for other use cases.
    Custom(String),
}

impl DefraTopic {
    /// Convert to libp2p IdentTopic.
    #[cfg(feature = "libp2p-transport")]
    pub fn to_ident_topic(&self) -> IdentTopic {
        IdentTopic::new(self.topic_string())
    }

    /// Get the topic string representation.
    pub fn topic_string(&self) -> String {
        match self {
            DefraTopic::DocSync => DOC_SYNC_TOPIC.to_string(),
            DefraTopic::SyncBranchable => SYNC_BRANCHABLE_TOPIC.to_string(),
            DefraTopic::Encryption => ENCRYPTION_TOPIC.to_string(),
            DefraTopic::Collection(id) => id.clone(),
            DefraTopic::Document(id) => id.clone(),
            DefraTopic::Custom(name) => name.clone(),
        }
    }

    /// Create a collection topic from collection ID.
    ///
    /// # Example
    ///
    /// ```
    /// use p2p::DefraTopic;
    ///
    /// let topic = DefraTopic::collection("bafkreih3x2qgxr4gpx7qd5kqj7gg6ukipvxc32e3ihdpkwmv5fvnz6wuui");
    /// assert_eq!(topic.topic_string(), "bafkreih3x2qgxr4gpx7qd5kqj7gg6ukipvxc32e3ihdpkwmv5fvnz6wuui");
    /// ```
    pub fn collection(collection_id: impl Into<String>) -> Self {
        DefraTopic::Collection(collection_id.into())
    }

    /// Create a document topic from document ID.
    ///
    /// # Example
    ///
    /// ```
    /// use p2p::DefraTopic;
    ///
    /// let topic = DefraTopic::document("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
    /// assert_eq!(topic.topic_string(), "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi");
    /// ```
    pub fn document(doc_id: impl Into<String>) -> Self {
        DefraTopic::Document(doc_id.into())
    }
}

impl From<&str> for DefraTopic {
    fn from(s: &str) -> Self {
        match s {
            DOC_SYNC_TOPIC => DefraTopic::DocSync,
            SYNC_BRANCHABLE_TOPIC => DefraTopic::SyncBranchable,
            ENCRYPTION_TOPIC => DefraTopic::Encryption,
            other => DefraTopic::Custom(other.to_string()),
        }
    }
}

impl From<String> for DefraTopic {
    fn from(s: String) -> Self {
        DefraTopic::from(s.as_str())
    }
}

impl std::fmt::Display for DefraTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.topic_string())
    }
}
