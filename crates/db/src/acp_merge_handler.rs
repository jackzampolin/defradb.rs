//! ACP-aware MergeHandler implementation.
//!
//! This module provides a MergeHandler that enforces document-level access control
//! before allowing CRDT merges from P2P sync.
//!
//! # Security Model
//!
//! When receiving blocks from P2P sync, the merge handler:
//! 1. Extracts the creator identity from the BlockMetadata
//! 2. Converts the creator PeerId to a DID (if configured)
//! 3. Checks if the creator has UPDATE permission on the document
//! 4. If permission denied, skips the merge with a reason
//! 5. If permission granted, delegates to the underlying merge logic
//!
//! # PeerId to DID Mapping
//!
//! The P2P layer identifies peers by their libp2p PeerId. To map this to
//! an ACP identity (DID), the system can use:
//! - Direct DID derivation from the peer's public key
//! - A pre-configured mapping table
//!
//! If no mapping exists, the merge is treated as anonymous and will be
//! rejected for protected documents.

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity};
use async_trait::async_trait;
use cid::Cid;
use identity::Did;
use p2p::sync::{BlockMetadata, MergeHandler, MergeOutcome};

use crate::collection_acp::check_doc_permission;
use crate::CollectionCache;

/// Type alias for peer-to-DID conversion function.
pub type PeerToDidMapper = dyn Fn(&str) -> Option<Did> + Send + Sync;

/// Error type for ACP merge handler operations.
#[derive(Debug, thiserror::Error)]
pub enum AcpMergeError {
    /// ACP permission check failed.
    #[error("ACP check failed: {0}")]
    AcpError(#[from] acp::Error),

    /// Collection not found.
    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    /// Missing metadata during non-recovery operation.
    #[error("missing metadata: {0}")]
    MissingMetadata(String),

    /// Failed to decode block.
    #[error("block decode failed: {0}")]
    BlockDecode(String),

    /// Failed to convert peer to DID.
    #[error("peer to DID conversion failed: {0}")]
    PeerConversion(String),
}

/// ACP-aware merge handler that enforces document-level access control.
///
/// This handler wraps the actual merge logic and adds permission checks
/// using the ACP system before allowing blocks to be merged.
pub struct AcpMergeHandler<H> {
    /// The underlying merge handler that does the actual work.
    inner: H,
    /// DocumentACP for permission checks.
    acp: Arc<dyn DocumentACP>,
    /// Collection cache for looking up collection metadata.
    collections: CollectionCache,
    /// Optional function to convert PeerId to DID.
    /// If None, peer identity cannot be verified and merges to protected docs will fail.
    peer_to_did: Option<Arc<PeerToDidMapper>>,
}

impl<H> AcpMergeHandler<H> {
    /// Create a new ACP merge handler.
    ///
    /// # Arguments
    ///
    /// * `inner` - The underlying merge handler
    /// * `acp` - DocumentACP for permission checks
    /// * `collections` - Collection cache for metadata lookup
    pub fn new(inner: H, acp: Arc<dyn DocumentACP>, collections: CollectionCache) -> Self {
        Self {
            inner,
            acp,
            collections,
            peer_to_did: None,
        }
    }

    /// Set a custom peer to DID conversion function.
    ///
    /// This function is called to convert a PeerId string to a DID for
    /// ACP permission checks. If not set, peer identity cannot be verified
    /// and merges to protected documents will fail.
    pub fn with_peer_to_did<F>(mut self, f: F) -> Self
    where
        F: Fn(&str) -> Option<Did> + Send + Sync + 'static,
    {
        self.peer_to_did = Some(Arc::new(f));
        self
    }

    /// Convert a peer ID to a DID if possible.
    ///
    /// If peer identity cannot be determined, logs a warning and returns Anonymous.
    /// This warning helps operators detect:
    /// - Misconfigured peer_to_did mappings
    /// - Unknown peers attempting to sync with protected documents
    fn peer_to_identity(&self, peer_id: &str) -> Identity {
        match &self.peer_to_did {
            Some(f) => match f(peer_id) {
                Some(did) => Identity::Authenticated(did),
                None => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        "No DID mapping for peer - treating as anonymous. \
                         Protected documents will reject this peer's updates."
                    );
                    Identity::Anonymous
                }
            },
            None => {
                tracing::warn!(
                    peer_id = %peer_id,
                    "No peer_to_did function configured - treating all peers as anonymous. \
                     Configure peer identity mapping for authenticated P2P sync."
                );
                Identity::Anonymous
            }
        }
    }

    /// Check if a merge operation is permitted.
    async fn check_merge_permission(
        &self,
        creator: &str,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<bool, AcpMergeError> {
        // Get collection metadata
        let collection = self
            .collections
            .get(collection_id)
            .ok_or_else(|| AcpMergeError::CollectionNotFound(collection_id.to_string()))?;

        // Convert peer to identity
        let identity = self.peer_to_identity(creator);

        // Check UPDATE permission (merges are effectively updates)
        let permitted = check_doc_permission(
            self.acp.as_ref(),
            &identity,
            DocumentPermission::Update,
            collection.schema(),
            doc_id,
        )
        .await?;

        if !permitted {
            tracing::info!(
                creator = %creator,
                collection = %collection_id,
                doc_id = %doc_id,
                "P2P merge denied: identity lacks UPDATE permission"
            );
        }

        Ok(permitted)
    }
}

#[async_trait]
impl<H> MergeHandler for AcpMergeHandler<H>
where
    H: MergeHandler,
    H::Error: Into<AcpMergeError>,
{
    type Error = AcpMergeError;

    async fn handle_block(
        &self,
        cid: &Cid,
        block_data: &[u8],
        metadata: BlockMetadata<'_>,
    ) -> Result<MergeOutcome, Self::Error> {
        // During recovery, metadata is unavailable - delegate to inner handler
        // The inner handler is responsible for extracting metadata from block_data
        if metadata.is_recovery {
            tracing::debug!(
                cid = %cid,
                "Recovery mode: delegating to inner handler without ACP check"
            );
            return self
                .inner
                .handle_block(cid, block_data, metadata)
                .await
                .map_err(Into::into);
        }

        // For normal operations, metadata must be present
        let (creator, collection_id, doc_id) =
            match (metadata.creator, metadata.collection_id, metadata.doc_id) {
                (Some(c), Some(col), Some(d)) => (c, col, d),
                _ => {
                    return Err(AcpMergeError::MissingMetadata(
                        "creator, collection_id, and doc_id required for non-recovery merge"
                            .to_string(),
                    ));
                }
            };

        // Check ACP permission before merging
        let permitted = self
            .check_merge_permission(creator, collection_id, doc_id)
            .await?;

        if !permitted {
            return Ok(MergeOutcome::skipped(format!(
                "ACP denied: {} lacks UPDATE permission on {}/{}",
                creator, collection_id, doc_id
            )));
        }

        // Permission granted, delegate to inner handler
        self.inner
            .handle_block(cid, block_data, metadata)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Mock merge handler for testing.
    struct MockMergeHandler {
        called: AtomicBool,
    }

    impl MockMergeHandler {
        fn new() -> Self {
            Self {
                called: AtomicBool::new(false),
            }
        }

        fn was_called(&self) -> bool {
            self.called.load(Ordering::SeqCst)
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("mock error")]
    struct MockError;

    impl From<MockError> for AcpMergeError {
        fn from(_: MockError) -> Self {
            AcpMergeError::BlockDecode("mock error".to_string())
        }
    }

    #[async_trait]
    impl MergeHandler for MockMergeHandler {
        type Error = MockError;

        async fn handle_block(
            &self,
            _cid: &Cid,
            _block_data: &[u8],
            _metadata: BlockMetadata<'_>,
        ) -> Result<MergeOutcome, Self::Error> {
            self.called.store(true, Ordering::SeqCst);
            Ok(MergeOutcome::Merged)
        }
    }

    #[test]
    fn test_peer_to_identity_no_mapping() {
        let mock = MockMergeHandler::new();
        let acp = Arc::new(acp::MemoryAcpStore::new());
        let local_acp = Arc::new(acp::LocalDocumentACP::new(acp));
        let collections = CollectionCache::new();

        let handler = AcpMergeHandler::new(mock, local_acp, collections);

        // Without peer_to_did function, should return anonymous
        let identity = handler.peer_to_identity("12D3KooWTest");
        assert!(matches!(identity, Identity::Anonymous));
    }

    #[test]
    fn test_peer_to_identity_with_mapping() {
        let mock = MockMergeHandler::new();
        let acp = Arc::new(acp::MemoryAcpStore::new());
        let local_acp = Arc::new(acp::LocalDocumentACP::new(acp));
        let collections = CollectionCache::new();

        let test_did =
            Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();
        let did_clone = test_did.clone();

        let handler =
            AcpMergeHandler::new(mock, local_acp, collections).with_peer_to_did(move |peer_id| {
                if peer_id == "12D3KooWTest" {
                    Some(did_clone.clone())
                } else {
                    None
                }
            });

        // Should return authenticated identity for known peer
        let identity = handler.peer_to_identity("12D3KooWTest");
        assert!(matches!(identity, Identity::Authenticated(did) if did == test_did));

        // Should return anonymous for unknown peer
        let identity = handler.peer_to_identity("12D3KooWUnknown");
        assert!(matches!(identity, Identity::Anonymous));
    }
}
