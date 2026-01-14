// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
use crate::host::P2PHostHandle;
use crate::message::PushLogBroadcast;
use crate::topics::DefraTopic;

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
///
/// # Usage
///
/// ```ignore
/// let broadcaster = Broadcaster::new(host_handle);
///
/// // Subscribe to topics for a collection
/// broadcaster.subscribe_collection("users").await?;
///
/// // Broadcast an update
/// broadcaster.broadcast_update(&broadcast).await?;
/// ```
#[derive(Clone)]
pub struct Broadcaster {
    host: P2PHostHandle,
}

impl Broadcaster {
    /// Create a new broadcaster.
    pub fn new(host: P2PHostHandle) -> Self {
        Self { host }
    }

    /// Subscribe to a collection topic.
    ///
    /// This enables receiving updates for all documents in the collection.
    pub async fn subscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let topic = DefraTopic::collection(collection_id);
        self.host.subscribe(topic).await
    }

    /// Subscribe to a specific document topic.
    ///
    /// This enables receiving updates for a specific document.
    pub async fn subscribe_document(&self, doc_id: &str) -> Result<bool> {
        let topic = DefraTopic::document(doc_id);
        self.host.subscribe(topic).await
    }

    /// Unsubscribe from a collection topic.
    pub async fn unsubscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let topic = DefraTopic::collection(collection_id);
        self.host.unsubscribe(topic).await
    }

    /// Unsubscribe from a document topic.
    pub async fn unsubscribe_document(&self, doc_id: &str) -> Result<bool> {
        let topic = DefraTopic::document(doc_id);
        self.host.unsubscribe(topic).await
    }

    /// Broadcast an update to the network.
    ///
    /// This publishes to both the document-specific and collection-specific topics,
    /// matching Go's `SendUpdate()` behavior.
    ///
    /// # Arguments
    ///
    /// * `broadcast` - The PushLogBroadcast message to send
    ///
    /// # Returns
    ///
    /// Returns `Ok(BroadcastResult)` indicating full or partial success.
    /// Returns an error only if both publishes fail.
    pub async fn broadcast_update(&self, broadcast: &PushLogBroadcast) -> Result<BroadcastResult> {
        let doc_topic = DefraTopic::document(&broadcast.doc_id);
        let collection_topic = DefraTopic::collection(&broadcast.collection_id);

        // Try to publish to both topics
        let doc_result = self.host.publish(doc_topic, broadcast.clone()).await;
        let collection_result = self.host.publish(collection_topic, broadcast.clone()).await;

        // Return appropriate result based on what succeeded
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
    ///
    /// # Arguments
    ///
    /// * `cid` - The CID of the block
    /// * `block` - The raw block data
    /// * `doc_id` - The document ID
    /// * `collection_id` - The collection ID
    /// * `creator` - The peer ID of the creator
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

    /// Get the underlying host handle.
    pub fn host(&self) -> &P2PHostHandle {
        &self.host
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full integration tests require a running P2PHost
    // See tests/integration.rs for end-to-end tests

    #[test]
    fn test_create_broadcast() {
        use std::str::FromStr;
        let cid =
            Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap();
        let block = b"test block data";
        let doc_id = "bae-123";
        let collection_id = "users";
        let creator = "12D3KooWPeer";

        let broadcast = Broadcaster::create_broadcast(&cid, block, doc_id, collection_id, creator);

        assert_eq!(broadcast.doc_id, doc_id);
        assert_eq!(broadcast.collection_id, collection_id);
        assert_eq!(broadcast.creator, creator);
        assert_eq!(broadcast.block, block.to_vec());
        assert_eq!(broadcast.cid, cid.to_bytes());
    }
}
