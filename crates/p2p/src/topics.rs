// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! GossipSub topic definitions for DefraDB.
//!
//! Topics follow Go implementation naming conventions:
//! - `doc-sync`: General document synchronization
//! - `encryption`: Encryption key exchange
//! - `{collection_id}`: Collection-specific updates
//! - `{doc_id}`: Document-specific updates

use libp2p::gossipsub::IdentTopic;

/// Well-known topic for general document synchronization.
pub const DOC_SYNC_TOPIC: &str = "doc-sync";

/// Well-known topic for encryption key exchange.
pub const ENCRYPTION_TOPIC: &str = "encryption";

/// Topic type for GossipSub subscriptions.
///
/// DefraDB uses several types of topics for different purposes:
/// - Fixed topics like `doc-sync` and `encryption` for system-wide operations
/// - Dynamic topics based on collection or document IDs for targeted updates
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DefraTopic {
    /// General document synchronization topic.
    DocSync,

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
    pub fn to_ident_topic(&self) -> IdentTopic {
        IdentTopic::new(self.topic_string())
    }

    /// Get the topic string representation.
    pub fn topic_string(&self) -> String {
        match self {
            DefraTopic::DocSync => DOC_SYNC_TOPIC.to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doc_sync_topic() {
        let topic = DefraTopic::DocSync;
        assert_eq!(topic.topic_string(), "doc-sync");
        assert_eq!(topic.to_string(), "doc-sync");
    }

    #[test]
    fn test_encryption_topic() {
        let topic = DefraTopic::Encryption;
        assert_eq!(topic.topic_string(), "encryption");
    }

    #[test]
    fn test_collection_topic() {
        let collection_id = "bafkreih3x2qgxr4gpx7qd5kqj7gg6ukipvxc32e3ihdpkwmv5fvnz6wuui";
        let topic = DefraTopic::collection(collection_id);
        assert_eq!(topic.topic_string(), collection_id);
    }

    #[test]
    fn test_document_topic() {
        let doc_id = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let topic = DefraTopic::document(doc_id);
        assert_eq!(topic.topic_string(), doc_id);
    }

    #[test]
    fn test_from_str() {
        assert_eq!(DefraTopic::from("doc-sync"), DefraTopic::DocSync);
        assert_eq!(DefraTopic::from("encryption"), DefraTopic::Encryption);
        assert_eq!(
            DefraTopic::from("custom-topic"),
            DefraTopic::Custom("custom-topic".to_string())
        );
    }

    #[test]
    fn test_to_ident_topic() {
        let topic = DefraTopic::DocSync;
        let ident = topic.to_ident_topic();
        // Verify the topic hash is generated
        assert!(!ident.hash().to_string().is_empty());
    }

    #[test]
    fn test_equality() {
        assert_eq!(DefraTopic::DocSync, DefraTopic::DocSync);
        assert_eq!(
            DefraTopic::collection("abc"),
            DefraTopic::Collection("abc".to_string())
        );
        assert_ne!(DefraTopic::DocSync, DefraTopic::Encryption);
    }
}
