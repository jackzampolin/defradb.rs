//! Replicator management for the sync coordinator.

use blockstore::Blockstore;

use super::result_types::{CreateReplicatorResult, LoadReplicatorsResult};
use super::SyncCoordinator;
use crate::error::Result;
use crate::replicator::{ReplicationFilters, ReplicatorInfo};
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Set (add/update) a replicator for the specified collections.
    pub async fn create_replicator(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
        auto_subscribe: bool,
    ) -> Result<CreateReplicatorResult> {
        let info = ReplicatorInfo::from_raw(peer_id.to_string(), collections, Vec::new());
        self.create_replicator_info(peer_id, info, auto_subscribe)
            .await
    }

    /// Set (add/update) a replicator with per-collection filters.
    pub async fn create_replicator_with_filters(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
        filters: ReplicationFilters,
        auto_subscribe: bool,
    ) -> Result<CreateReplicatorResult> {
        let info = ReplicatorInfo::from_raw_with_filters(
            peer_id.to_string(),
            collections,
            Vec::new(),
            filters,
        );
        self.create_replicator_info(peer_id, info, auto_subscribe)
            .await
    }

    /// Set (add/update) a replicator from a full metadata record.
    pub async fn create_replicator_info(
        &self,
        peer_id: &PeerId,
        mut info: ReplicatorInfo,
        auto_subscribe: bool,
    ) -> Result<CreateReplicatorResult> {
        info.id = peer_id.to_string();
        let collections = info.collections.clone();
        self.runtime
            .transport
            .create_replicator_info(peer_id, info.clone())
            .await?;

        let filtered_collections: Vec<String> = info.filters.keys().cloned().collect();
        self.register_replicator_access(info);

        if !filtered_collections.is_empty() {
            if let Ok(subscribed) = self.get_subscribed_collections().await {
                let bypassed: Vec<&String> = filtered_collections
                    .iter()
                    .filter(|collection_id| subscribed.contains(collection_id))
                    .collect();
                if !bypassed.is_empty() {
                    tracing::warn!(
                        peer_id = %peer_id,
                        collections = ?bypassed,
                        "filtered replicator added for collection(s) this node also gossip-subscribes; \
                         filtered replication is push-path selectivity, not an access boundary \
                         (subscribed peers receive all documents) — use ACP/encryption for confidentiality"
                    );
                }
            }
        }

        let mut result = CreateReplicatorResult {
            subscribed: Vec::new(),
            failed_subscriptions: Vec::new(),
        };

        if auto_subscribe {
            for collection_id in &collections {
                if self
                    .access
                    .replicators
                    .is_filtered_replicator(collection_id, peer_id.as_str())
                {
                    continue;
                }
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
                "Create replicator with subscription failures"
            );
        } else {
            tracing::info!(
                peer_id = %peer_id,
                collections = ?collections,
                "Created replicator"
            );
        }

        Ok(result)
    }

    /// Delete a replicator.
    pub async fn delete_replicator(&self, peer_id: &PeerId) -> Result<()> {
        let removed_collections = match self.runtime.transport.get_replicator(peer_id).await? {
            Some(info) => info.collections,
            None => Vec::new(),
        };

        self.runtime.transport.delete_replicator(peer_id).await?;
        self.access.replicators.remove_peer(peer_id.as_str());
        tracing::info!(peer_id = %peer_id, "Deleted replicator");

        self.unsubscribe_orphaned_collections(&removed_collections)
            .await;

        Ok(())
    }

    /// Remove specific collections from a replicator.
    pub async fn remove_replicator_collections(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
    ) -> Result<bool> {
        if collections.is_empty() {
            let removed_collections = match self.runtime.transport.get_replicator(peer_id).await? {
                Some(info) => info.collections,
                None => Vec::new(),
            };

            self.runtime.transport.delete_replicator(peer_id).await?;
            self.access.replicators.remove_peer(peer_id.as_str());
            tracing::info!(peer_id = %peer_id, "Deleted replicator (empty collections = delete all)");

            self.unsubscribe_orphaned_collections(&removed_collections)
                .await;

            return Ok(true);
        }

        let fully_deleted = self
            .runtime
            .transport
            .remove_replicator_collections(peer_id, collections.clone())
            .await?;

        for collection_id in &collections {
            self.access
                .replicators
                .remove_replicator(collection_id, peer_id.as_str());
        }

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

        self.unsubscribe_orphaned_collections(&collections).await;

        Ok(fully_deleted)
    }

    /// Unsubscribe from collection topics that no longer have any replicators.
    async fn unsubscribe_orphaned_collections(&self, collections: &[String]) {
        for collection_id in collections {
            let remaining = match self.runtime.transport.list_replicators().await {
                Ok(reps) => reps.iter().any(|r| r.collections.contains(collection_id)),
                Err(e) => {
                    tracing::warn!(
                        collection_id = %collection_id,
                        error = %e,
                        "Failed to list replicators while checking orphaned collections, \
                         keeping subscription as a safety measure"
                    );
                    true
                }
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
    pub async fn list_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
        self.runtime.transport.list_replicators().await
    }

    /// Get replicator info for a specific peer.
    pub async fn get_replicator(&self, peer_id: &PeerId) -> Result<Option<ReplicatorInfo>> {
        self.runtime.transport.get_replicator(peer_id).await
    }

    /// Load replicators from stored ReplicatorInfo records.
    pub async fn load_replicators(
        &self,
        infos: &[ReplicatorInfo],
        auto_subscribe: bool,
    ) -> LoadReplicatorsResult {
        let mut result = LoadReplicatorsResult::default();

        for info in infos {
            let peer_id_str = info.peer_id_str();
            if peer_id_str.is_empty() {
                result.skipped_invalid_ids.push(peer_id_str.to_string());
                continue;
            }

            let peer_id = PeerId::new(peer_id_str.to_string());
            match self
                .create_replicator_info(&peer_id, info.clone(), auto_subscribe)
                .await
            {
                Ok(set_result) => {
                    result.loaded += 1;
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

    fn register_replicator_access(&self, info: ReplicatorInfo) {
        self.access.replicators.set_replicator_info(info);
    }
}
