//! Collection and document subscription management.

use blockstore::Blockstore;
use cid::Cid;

use super::SyncCoordinator;
use crate::error::Result;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Subscribe to a collection for sync.
    ///
    /// After subscribing, updates to any document in the collection will be
    /// received and processed. The subscription is persisted to storage.
    ///
    /// # Ordering
    ///
    /// Storage is persisted BEFORE subscribing to GossipSub to ensure consistency.
    /// If storage fails, we don't subscribe (avoiding inconsistent state where
    /// we receive messages for a collection we haven't recorded).
    pub async fn subscribe_collection(&self, collection_id: &str) -> Result<bool> {
        // Check if already subscribed in cache (fast path)
        if self
            .subscribed_collections
            .read()
            .await
            .contains(collection_id)
        {
            return Ok(false);
        }

        // Persist to storage FIRST (before GossipSub subscription)
        // This ensures we don't end up in an inconsistent state where we're
        // subscribed to the topic but haven't recorded it in storage.
        self.collection_store.add_collection(collection_id).await?;

        // Now subscribe to GossipSub
        let result = self.broadcaster.subscribe_collection(collection_id).await;

        match result {
            Ok(subscribed) => {
                // Update in-memory cache regardless of whether it's new or already subscribed
                self.subscribed_collections
                    .write()
                    .await
                    .insert(collection_id.to_string());

                if subscribed {
                    tracing::debug!(collection_id = %collection_id, "Subscribed to collection (persisted)");
                }
                Ok(subscribed)
            }
            Err(e) => {
                // GossipSub subscription failed - remove from storage to stay consistent
                if let Err(remove_err) =
                    self.collection_store.remove_collection(collection_id).await
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
        self.broadcaster.subscribe_document(doc_id).await
    }

    /// Unsubscribe from a collection.
    ///
    /// Removes the collection subscription from both memory and persistent storage.
    pub async fn unsubscribe_collection(&self, collection_id: &str) -> Result<bool> {
        let result = self
            .broadcaster
            .unsubscribe_collection(collection_id)
            .await?;
        if result {
            // Remove from persistent storage first
            self.collection_store
                .remove_collection(collection_id)
                .await?;

            // Update in-memory cache
            self.subscribed_collections
                .write()
                .await
                .remove(collection_id);

            tracing::debug!(collection_id = %collection_id, "Unsubscribed from collection (persisted)");
        }
        Ok(result)
    }

    /// Unsubscribe from a document.
    pub async fn unsubscribe_document(&self, doc_id: &str) -> Result<bool> {
        self.broadcaster.unsubscribe_document(doc_id).await
    }

    /// Get the list of subscribed collection IDs.
    pub async fn get_subscribed_collections(&self) -> Result<Vec<String>> {
        let collections = self.subscribed_collections.read().await;
        Ok(collections.iter().cloned().collect())
    }

    /// Load and subscribe to all persisted P2P collections.
    ///
    /// This should be called during startup to restore collection subscriptions
    /// from persistent storage. It loads collection IDs from storage, populates
    /// the in-memory cache, and subscribes to the GossipSub topics.
    ///
    /// Returns the number of collections loaded.
    pub async fn load_p2p_collections(&self) -> Result<usize> {
        let collections = self.collection_store.get_all_collections().await?;
        let count = collections.len();

        if count == 0 {
            tracing::debug!("No persisted P2P collections to load");
            return Ok(0);
        }

        tracing::info!(count = count, "Loading persisted P2P collections");

        let mut loaded = 0;
        for collection_id in collections {
            // Subscribe to the GossipSub topic
            match self.broadcaster.subscribe_collection(&collection_id).await {
                Ok(true) => {
                    // Update in-memory cache
                    self.subscribed_collections
                        .write()
                        .await
                        .insert(collection_id.clone());
                    loaded += 1;
                    tracing::debug!(collection_id = %collection_id, "Loaded P2P collection subscription");
                }
                Ok(false) => {
                    // Already subscribed (shouldn't happen on startup, but handle gracefully)
                    self.subscribed_collections
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
                    // Continue loading other collections
                }
            }
        }

        tracing::info!(loaded = loaded, "Finished loading P2P collections");
        Ok(loaded)
    }

    /// Mark a block as merged.
    ///
    /// Call this after successfully completing the CRDT merge for a block.
    pub async fn mark_as_merged(&self, cid: &Cid) -> Result<()> {
        self.manager.mark_as_merged(cid).await
    }

    /// Check if a block is merged.
    pub async fn is_merged(&self, cid: &Cid) -> Result<bool> {
        self.manager.is_merged(cid).await
    }

    /// Get all unmerged block CIDs.
    ///
    /// Useful for startup recovery - process any blocks that were stored
    /// but not yet merged.
    pub async fn get_unmerged(&self) -> Result<Vec<Cid>> {
        self.manager.get_unmerged().await
    }
}
