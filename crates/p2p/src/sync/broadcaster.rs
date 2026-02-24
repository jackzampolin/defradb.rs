//! Broadcasting functionality for P2P sync.
//!
//! This module handles broadcasting block updates to the network via GossipSub.
//! Updates are published to both document-specific and collection-specific topics.
//!
//! # Go Compatibility
//!
//! This matches Go's `SendUpdate()` in `p2p.go:532-563` which publishes to both
//! DocID and CollectionID topics.

use cid::Cid;

use crate::error::{Error, Result};
use crate::message::PushLogBroadcast;
use crate::topics::DefraTopic;
use crate::transport::P2PTransport;

/// Result of a broadcast operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcastResult {
    /// Both document and collection topics received the message.
    Success,
    /// Only the document topic received the message.
    PartialDocumentOnly {
        /// Error from the collection topic publish.
        collection_error: String,
    },
    /// Only the collection topic received the message.
    PartialCollectionOnly {
        /// Error from the document topic publish.
        document_error: String,
    },
}

/// Broadcaster for sending block updates to the P2P network.
#[derive(Clone)]
pub struct Broadcaster<T: P2PTransport> {
    transport: T,
}

impl<T: P2PTransport> Broadcaster<T> {
    /// Create a new broadcaster.
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// Subscribe to a collection topic.
    pub async fn subscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let topic = DefraTopic::collection(collection_id);
        self.transport.subscribe(topic).await
    }

    /// Subscribe to a specific document topic.
    pub async fn subscribe_document(&self, doc_id: &str) -> Result<bool> {
        let topic = DefraTopic::document(doc_id);
        self.transport.subscribe(topic).await
    }

    /// Unsubscribe from a collection topic.
    pub async fn unsubscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let topic = DefraTopic::collection(collection_id);
        self.transport.unsubscribe(topic).await
    }

    /// Unsubscribe from a document topic.
    pub async fn unsubscribe_document(&self, doc_id: &str) -> Result<bool> {
        let topic = DefraTopic::document(doc_id);
        self.transport.unsubscribe(topic).await
    }

    /// Broadcast an update to the network.
    ///
    /// This publishes to both the document-specific and collection-specific topics,
    /// matching Go's `SendUpdate()` behavior.
    pub async fn broadcast_update(&self, broadcast: &PushLogBroadcast) -> Result<BroadcastResult> {
        let doc_topic = DefraTopic::document(&broadcast.doc_id);
        let collection_topic = DefraTopic::collection(&broadcast.collection_id);

        let doc_result = self.transport.publish(doc_topic, broadcast.clone()).await;
        let collection_result = self
            .transport
            .publish(collection_topic, broadcast.clone())
            .await;

        match (&doc_result, &collection_result) {
            (Ok(doc_msg_id), Ok(col_msg_id)) => {
                tracing::debug!(
                    doc_id = %broadcast.doc_id,
                    collection_id = %broadcast.collection_id,
                    ?doc_msg_id,
                    ?col_msg_id,
                    "Broadcast to both topics"
                );
                Ok(BroadcastResult::Success)
            }
            (Ok(_), Err(e)) => {
                tracing::warn!(
                    doc_id = %broadcast.doc_id,
                    collection_id = %broadcast.collection_id,
                    error = %e,
                    "Partial broadcast: document topic succeeded, collection topic failed - \
                     some peers may not receive this update"
                );
                Ok(BroadcastResult::PartialDocumentOnly {
                    collection_error: e.to_string(),
                })
            }
            (Err(e), Ok(_)) => {
                tracing::warn!(
                    doc_id = %broadcast.doc_id,
                    collection_id = %broadcast.collection_id,
                    error = %e,
                    "Partial broadcast: collection topic succeeded, document topic failed - \
                     some peers may not receive this update"
                );
                Ok(BroadcastResult::PartialCollectionOnly {
                    document_error: e.to_string(),
                })
            }
            (Err(doc_err), Err(col_err)) => {
                tracing::error!(
                    doc_id = %broadcast.doc_id,
                    collection_id = %broadcast.collection_id,
                    doc_error = %doc_err,
                    collection_error = %col_err,
                    "Failed to broadcast to both topics"
                );
                Err(Error::GossipSubPublish(format!(
                    "failed to publish to both topics: doc={}, collection={}",
                    doc_err, col_err
                )))
            }
        }
    }

    /// Create a PushLogBroadcast from block data.
    pub fn create_broadcast(
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator: &str,
    ) -> PushLogBroadcast {
        PushLogBroadcast::new(
            doc_id.to_string(),
            cid.to_bytes(),
            collection_id.to_string(),
            creator.to_string(),
            block.to_vec(),
        )
    }

    /// Get the underlying transport reference.
    pub fn transport(&self) -> &T {
        &self.transport
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_broadcast() {
        use std::str::FromStr;
        let cid =
            Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
        let block = b"test block data";
        let doc_id = "bae-123";
        let collection_id = "users";
        let creator = "12D3KooWPeer";

        let broadcast = Broadcaster::<crate::host::Libp2pTransport>::create_broadcast(
            &cid,
            block,
            doc_id,
            collection_id,
            creator,
        );

        assert_eq!(broadcast.doc_id, doc_id);
        assert_eq!(broadcast.collection_id, collection_id);
        assert_eq!(broadcast.creator, creator);
        assert_eq!(broadcast.block, block.to_vec());
        assert_eq!(broadcast.cid, cid.to_bytes());
    }
}
