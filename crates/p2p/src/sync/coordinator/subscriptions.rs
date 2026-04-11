//! Collection and document subscription management.

use blockstore::Blockstore;
use cid::Cid;

use super::SyncCoordinator;
use crate::error::Result;
use crate::transport::P2PTransport;

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Subscribe to a collection for sync.
    ///
    /// Uses a write lock for the entire operation to prevent concurrent
    /// subscribe calls from racing past the contains-check.
    pub async fn subscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let mut subscribed_collections = self.subscriptions.subscribed_collections.write().await;

        if subscribed_collections.contains(collection_id) {
            return Ok(false);
        }

        self.subscriptions
            .collection_store
            .add_collection(collection_id)
            .await?;

        let result = self
            .runtime
            .broadcaster
            .subscribe_collection(collection_id)
            .await;

        match result {
            Ok(subscribed) => {
                subscribed_collections.insert(collection_id.to_string());

                if subscribed {
                    tracing::debug!(collection_id = %collection_id, "Subscribed to collection (persisted)");
                }
                Ok(subscribed)
            }
            Err(e) => {
                if let Err(remove_err) = self
                    .subscriptions
                    .collection_store
                    .remove_collection(collection_id)
                    .await
                {
                    tracing::error!(
                        collection_id = %collection_id,
                        subscribe_error = %e,
                        remove_error = %remove_err,
                        "Failed to rollback storage after GossipSub subscription failure"
                    );
                }
                Err(e)
            }
        }
    }

    /// Subscribe to a specific document for sync.
    pub async fn subscribe_document(&self, doc_id: &str) -> Result<bool> {
        self.runtime.broadcaster.subscribe_document(doc_id).await
    }

    /// Unsubscribe from a collection.
    pub async fn unsubscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let result = self
            .runtime
            .broadcaster
            .unsubscribe_collection(collection_id)
            .await?;
        if result {
            self.subscriptions
                .collection_store
                .remove_collection(collection_id)
                .await?;

            self.subscriptions
                .subscribed_collections
                .write()
                .await
                .remove(collection_id);

            tracing::debug!(collection_id = %collection_id, "Unsubscribed from collection (persisted)");
        }
        Ok(result)
    }

    /// Unsubscribe from a document.
    pub async fn unsubscribe_document(&self, doc_id: &str) -> Result<bool> {
        self.runtime.broadcaster.unsubscribe_document(doc_id).await
    }

    /// Get the list of subscribed collection IDs.
    pub async fn get_subscribed_collections(&self) -> Result<Vec<String>> {
        let collections = self.subscriptions.subscribed_collections.read().await;
        Ok(collections.iter().cloned().collect())
    }

    /// Load and subscribe to all persisted P2P collections.
    pub async fn load_p2p_collections(&self) -> Result<usize> {
        let collections = self
            .subscriptions
            .collection_store
            .get_all_collections()
            .await?;
        let count = collections.len();

        if count == 0 {
            tracing::debug!("No persisted P2P collections to load");
            return Ok(0);
        }

        tracing::info!(count = count, "Loading persisted P2P collections");

        let mut loaded = 0;
        for collection_id in collections {
            match self
                .runtime
                .broadcaster
                .subscribe_collection(&collection_id)
                .await
            {
                Ok(true) => {
                    self.subscriptions
                        .subscribed_collections
                        .write()
                        .await
                        .insert(collection_id.clone());
                    loaded += 1;
                    tracing::debug!(collection_id = %collection_id, "Loaded P2P collection subscription");
                }
                Ok(false) => {
                    self.subscriptions
                        .subscribed_collections
                        .write()
                        .await
                        .insert(collection_id.clone());
                    loaded += 1;
                    tracing::debug!(collection_id = %collection_id, "P2P collection already subscribed");
                }
                Err(e) => {
                    tracing::warn!(
                        collection_id = %collection_id,
                        error = %e,
                        "Failed to subscribe to persisted P2P collection"
                    );
                }
            }
        }

        tracing::info!(loaded = loaded, "Finished loading P2P collections");
        Ok(loaded)
    }

    /// Mark a block as merged.
    pub async fn mark_as_merged(&self, cid: &Cid) -> Result<()> {
        self.manager.mark_as_merged(cid).await
    }

    /// Mark multiple blocks as merged in a single transaction.
    pub async fn mark_batch_as_merged(&self, cids: &[Cid]) -> Result<()> {
        self.manager.mark_batch_as_merged(cids).await
    }

    /// Check if a block is merged.
    pub async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        self.manager.is_merged(cid).await
    }

    /// Get all unmerged block CIDs.
    pub async fn get_unmerged(&self) -> Result<Vec<Cid>> {
        self.manager.get_unmerged().await
    }
}
