//! Replicator management for the sync coordinator.

use blockstore::Blockstore;
use libp2p::PeerId;

use super::result_types::{LoadReplicatorsResult, SetReplicatorResult};
use super::SyncCoordinator;
use crate::error::Result;
use crate::replicator::ReplicatorInfo;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Set (add/update) a replicator for the specified collections.
    ///
    /// This adds the peer to the replicator registry and optionally auto-subscribes
    /// to the collection topics so we can sync with them.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - The peer ID of the replicator
    /// * `collections` - Collections this peer should replicate
    /// * `auto_subscribe` - Whether to auto-subscribe to the collection topics
    ///
    /// # Returns
    ///
    /// Returns `Ok(SetReplicatorResult)` with details about subscription status.
    /// The replicator is registered even if some subscriptions fail.
    pub async fn set_replicator(
        &self,
        peer_id: PeerId,
        collections: Vec<String>,
        auto_subscribe: bool,
    ) -> Result<SetReplicatorResult> {
        // Update the registry via host command
        self.host
            .set_replicator(peer_id, collections.clone())
            .await?;

        let mut result = SetReplicatorResult {
            subscribed: Vec::new(),
            failed_subscriptions: Vec::new(),
        };

        // Auto-subscribe to collection topics so we receive updates
        if auto_subscribe {
            for collection_id in &collections {
                match self.subscribe_collection(collection_id).await {
                    Ok(_) => {
                        result.subscribed.push(collection_id.clone());
                    }
                    Err(e) => {
                        tracing::warn!(
                            collection_id = %collection_id,
                            error = %e,
                            "Failed to auto-subscribe to collection for replicator"
                        );
                        result
                            .failed_subscriptions
                            .push((collection_id.clone(), e.to_string()));
                    }
                }
            }
        }

        if result.has_failures() {
            tracing::warn!(
                peer_id = %peer_id,
                subscribed = ?result.subscribed,
                failed = ?result.failed_subscriptions,
                "Set replicator with subscription failures"
            );
        } else {
            tracing::info!(
                peer_id = %peer_id,
                collections = ?collections,
                "Set replicator"
            );
        }

        Ok(result)
    }

    /// Delete a replicator.
    ///
    /// Removes the peer from the replicator registry and unsubscribes from
    /// collection topics that no longer have any replicators.
    pub async fn delete_replicator(&self, peer_id: PeerId) -> Result<()> {
        // Get collections BEFORE deleting so we know what to potentially unsubscribe
        let removed_collections = match self.host.get_replicator(peer_id).await? {
            Some(info) => info.collections,
            None => Vec::new(),
        };

        self.host.delete_replicator(peer_id).await?;
        tracing::info!(peer_id = %peer_id, "Deleted replicator");

        // Unsubscribe from collections that no longer have any replicators
        self.unsubscribe_orphaned_collections(&removed_collections)
            .await;

        Ok(())
    }

    /// Remove specific collections from a replicator.
    ///
    /// This matches Go DefraDB's partial removal behavior:
    /// - If `collections` is empty: deletes the entire replicator (all collections)
    /// - If `collections` is non-empty: removes only those collections, keeping the
    ///   replicator if other collections remain
    ///
    /// Returns `true` if the replicator was fully deleted (no collections remain).
    pub async fn remove_replicator_collections(
        &self,
        peer_id: PeerId,
        collections: Vec<String>,
    ) -> Result<bool> {
        // Go behavior: empty collections = delete all
        if collections.is_empty() {
            // Get collections BEFORE deleting
            let removed_collections = match self.host.get_replicator(peer_id).await? {
                Some(info) => info.collections,
                None => Vec::new(),
            };

            self.host.delete_replicator(peer_id).await?;
            tracing::info!(peer_id = %peer_id, "Deleted replicator (empty collections = delete all)");

            self.unsubscribe_orphaned_collections(&removed_collections)
                .await;

            return Ok(true);
        }

        // Partial removal
        let fully_deleted = self
            .host
            .remove_replicator_collections(peer_id, collections.clone())
            .await?;

        if fully_deleted {
            tracing::info!(
                peer_id = %peer_id,
                collections = ?collections,
                "Replicator fully deleted (no collections remain after removal)"
            );
        } else {
            tracing::info!(
                peer_id = %peer_id,
                collections = ?collections,
                "Removed collections from replicator (replicator still has other collections)"
            );
        }

        // Unsubscribe from removed collections that no longer have replicators
        self.unsubscribe_orphaned_collections(&collections).await;

        Ok(fully_deleted)
    }

    /// Unsubscribe from collection topics that no longer have any replicators.
    async fn unsubscribe_orphaned_collections(&self, collections: &[String]) {
        for collection_id in collections {
            // Check if any remaining replicators use this collection
            let remaining = match self.host.get_all_replicators().await {
                Ok(reps) => reps.iter().any(|r| r.collections.contains(collection_id)),
                Err(_) => true, // conservative: don't unsubscribe if we can't check
            };

            if !remaining {
                if let Err(e) = self.unsubscribe_collection(collection_id).await {
                    tracing::warn!(
                        collection_id = %collection_id,
                        error = %e,
                        "Failed to unsubscribe from orphaned collection"
                    );
                }
            }
        }
    }

    /// Get all registered replicators.
    pub async fn get_all_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
        self.host.get_all_replicators().await
    }

    /// Get replicator info for a specific peer.
    ///
    /// Returns None if the peer is not a replicator.
    pub async fn get_replicator(&self, peer_id: PeerId) -> Result<Option<ReplicatorInfo>> {
        self.host.get_replicator(peer_id).await
    }

    /// Load replicators from stored ReplicatorInfo records.
    ///
    /// This is typically called during startup to restore replicator state
    /// from persistent storage.
    ///
    /// # Arguments
    ///
    /// * `infos` - ReplicatorInfo records loaded from storage
    /// * `auto_subscribe` - Whether to auto-subscribe to collection topics
    ///
    /// # Returns
    ///
    /// Returns a `LoadReplicatorsResult` with details about what was loaded
    /// and any failures that occurred. Unlike individual `set_replicator` calls,
    /// this method continues loading remaining replicators even if some fail.
    pub async fn load_replicators(
        &self,
        infos: &[ReplicatorInfo],
        auto_subscribe: bool,
    ) -> LoadReplicatorsResult {
        let mut result = LoadReplicatorsResult::default();

        for info in infos {
            if let Some(peer_id) = info.peer_id() {
                match self
                    .set_replicator(peer_id, info.collections.clone(), auto_subscribe)
                    .await
                {
                    Ok(set_result) => {
                        result.loaded += 1;
                        // Collect any subscription failures
                        result
                            .failed_subscriptions
                            .extend(set_result.failed_subscriptions);
                    }
                    Err(e) => {
                        tracing::error!(
                            peer_id = %peer_id,
                            error = %e,
                            "Failed to load replicator"
                        );
                        result.failed.push((peer_id.to_string(), e.to_string()));
                    }
                }
            } else {
                tracing::warn!(
                    peer_id_str = %info.peer_id_str(),
                    "Skipping replicator with invalid peer ID"
                );
                result
                    .skipped_invalid_ids
                    .push(info.peer_id_str().to_string());
            }
        }

        if result.failed.is_empty() && result.skipped_invalid_ids.is_empty() {
            tracing::info!(
                loaded = result.loaded,
                auto_subscribe = auto_subscribe,
                "Loaded replicators from storage"
            );
        } else {
            tracing::warn!(
                loaded = result.loaded,
                skipped = result.skipped_invalid_ids.len(),
                failed = result.failed.len(),
                failed_subscriptions = result.failed_subscriptions.len(),
                auto_subscribe = auto_subscribe,
                "Loaded replicators from storage with some failures"
            );
        }

        result
    }
}
