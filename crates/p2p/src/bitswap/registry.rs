//! Replicator registry for tracking authorized peers per collection.
//!
//! Uses string-based peer IDs so the registry works with both libp2p and iroh transports.

use parking_lot::RwLock;

use crate::replicator::ReplicatorInfo;

/// Tracks which peers are authorized replicators for which collections.
///
/// This is the fast-path access check used by Go DefraDB. Replicators
/// automatically have access to all blocks in their subscribed collections.
///
/// Peer IDs are stored as strings to support both libp2p PeerIds and
/// iroh EndpointIds without coupling to either transport.
#[derive(Debug, Default)]
pub struct ReplicatorRegistry {
    /// Map of peer ID string -> persisted replicator metadata.
    replicators: RwLock<std::collections::BTreeMap<String, ReplicatorInfo>>,
}

impl ReplicatorRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            replicators: RwLock::new(std::collections::BTreeMap::new()),
        }
    }

    /// Register a peer as a replicator for a collection.
    pub fn add_replicator(&self, collection_id: &str, peer_id: &str) {
        let mut replicators = self.replicators.write();
        let info = replicators.entry(peer_id.to_string()).or_insert_with(|| {
            ReplicatorInfo::from_raw(peer_id.to_string(), Vec::new(), Vec::new())
        });
        if !info.collections.iter().any(|id| id == collection_id) {
            info.collections.push(collection_id.to_string());
        }
    }

    /// Replace the full set of collections replicated by a peer.
    pub fn set_peer_collections(&self, peer_id: &str, collections: &[String]) {
        let mut info =
            ReplicatorInfo::from_raw(peer_id.to_string(), collections.to_vec(), Vec::new());
        if let Some(existing) = self.replicators.read().get(peer_id) {
            info.addresses = existing.addresses.clone();
            info.status = existing.status;
            info.last_status_change = existing.last_status_change;
        }
        self.set_replicator_info(info);
    }

    /// Replace the full metadata record for a peer.
    pub fn set_replicator_info(&self, info: ReplicatorInfo) {
        let peer_id = info.peer_id_str().to_string();
        if peer_id.is_empty() {
            return;
        }
        if info.collections.is_empty() {
            self.replicators.write().remove(&peer_id);
            return;
        }
        self.replicators.write().insert(peer_id, info);
    }

    /// Remove a peer as a replicator for a collection.
    pub fn remove_replicator(&self, collection_id: &str, peer_id: &str) {
        let mut replicators = self.replicators.write();
        if let Some(info) = replicators.get_mut(peer_id) {
            info.collections.retain(|id| id != collection_id);
            info.filters
                .retain(|collection, _| collection != collection_id);
            if info.collections.is_empty() {
                replicators.remove(peer_id);
            }
        }
    }

    /// Remove a peer from all collections.
    pub fn remove_peer(&self, peer_id: &str) {
        self.replicators.write().remove(peer_id);
    }

    /// Remove a peer from specific collections. Returns true if the peer no
    /// longer replicates any collections afterwards.
    pub fn remove_peer_collections(&self, peer_id: &str, collections: &[String]) -> bool {
        let mut replicators = self.replicators.write();
        let Some(info) = replicators.get_mut(peer_id) else {
            return true;
        };

        for collection_id in collections {
            info.collections.retain(|id| id != collection_id);
            info.filters
                .retain(|collection, _| collection != collection_id);
        }

        if info.collections.is_empty() {
            replicators.remove(peer_id);
            true
        } else {
            false
        }
    }

    /// Check if a peer is a replicator for a collection.
    pub fn is_replicator(&self, collection_id: &str, peer_id: &str) -> bool {
        self.replicators
            .read()
            .get(peer_id)
            .map(|info| info.collections.iter().any(|id| id == collection_id))
            .unwrap_or(false)
    }

    /// Check if a peer has a filter for a collection it replicates.
    pub fn is_filtered_replicator(&self, collection_id: &str, peer_id: &str) -> bool {
        self.replicators
            .read()
            .get(peer_id)
            .map(|info| info.is_filtered_for_collection(collection_id))
            .unwrap_or(false)
    }

    /// Check if a peer is a replicator for any collection.
    pub fn is_any_replicator(&self, peer_id: &str) -> bool {
        self.replicators.read().contains_key(peer_id)
    }

    /// Get all replicator peer ID strings for a collection.
    pub fn get_replicators(&self, collection_id: &str) -> Vec<String> {
        self.replicators
            .read()
            .iter()
            .filter(|(_, info)| info.collections.iter().any(|id| id == collection_id))
            .map(|(peer_id, _)| peer_id.clone())
            .collect()
    }

    /// Get all collections a peer is replicating.
    pub fn get_collections(&self, peer_id: &str) -> Vec<String> {
        self.replicators
            .read()
            .get(peer_id)
            .map(|info| info.collections.clone())
            .unwrap_or_default()
    }

    /// Get all registered replicators as ReplicatorInfo.
    pub fn list_replicator_info(&self) -> Vec<ReplicatorInfo> {
        self.replicators.read().values().cloned().collect()
    }

    /// Load replicators from ReplicatorInfo records.
    ///
    /// Existing state is cleared before loading. Accepts any peer ID format
    /// (libp2p or iroh) since we store as strings.
    pub fn load_from_infos(&self, infos: &[ReplicatorInfo]) -> (usize, usize) {
        let mut replicators = self.replicators.write();
        replicators.clear();

        let mut loaded = 0;
        let mut skipped = 0;

        for info in infos {
            let peer_id_str = info.peer_id_str();
            if peer_id_str.is_empty() {
                tracing::warn!(
                    collections = ?info.collections,
                    "Skipping replicator with empty peer ID during load"
                );
                skipped += 1;
                continue;
            }

            if !info.collections.is_empty() {
                replicators.insert(peer_id_str.to_string(), info.clone());
            }
            loaded += 1;
        }

        (loaded, skipped)
    }

    /// Get replicator info for a specific peer.
    pub fn get_replicator_info(&self, peer_id: &str) -> Option<ReplicatorInfo> {
        self.replicators.read().get(peer_id).cloned()
    }

    /// Get all unique peer ID strings that are replicators.
    pub fn get_all_peer_ids(&self) -> Vec<String> {
        self.replicators.read().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::replicator::{ReplicationFilter, ReplicationFilters};

    fn random_peer_id() -> String {
        static NEXT_PEER_ID: AtomicU64 = AtomicU64::new(0);
        format!("test-peer-{}", NEXT_PEER_ID.fetch_add(1, Ordering::Relaxed))
    }

    #[test]
    fn test_replicator_registry_add_remove() {
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();

        registry.add_replicator("users", &peer);
        assert!(registry.is_replicator("users", &peer));
        assert!(!registry.is_replicator("posts", &peer));

        registry.remove_replicator("users", &peer);
        assert!(!registry.is_replicator("users", &peer));
    }

    #[test]
    fn test_replicator_registry_multiple_collections() {
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();

        registry.add_replicator("users", &peer);
        registry.add_replicator("posts", &peer);

        assert!(registry.is_replicator("users", &peer));
        assert!(registry.is_replicator("posts", &peer));
        assert!(registry.is_any_replicator(&peer));

        let collections = registry.get_collections(&peer);
        assert_eq!(collections.len(), 2);
    }

    #[test]
    fn test_replicator_registry_remove_peer() {
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();

        registry.add_replicator("users", &peer);
        registry.add_replicator("posts", &peer);

        registry.remove_peer(&peer);

        assert!(!registry.is_replicator("users", &peer));
        assert!(!registry.is_replicator("posts", &peer));
        assert!(!registry.is_any_replicator(&peer));
    }

    #[test]
    fn test_replicator_registry_add_same_peer_twice() {
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();

        registry.add_replicator("users", &peer);
        registry.add_replicator("users", &peer);

        let replicators = registry.get_replicators("users");
        assert_eq!(replicators.len(), 1);
        assert!(replicators.contains(&peer));
    }

    #[test]
    fn test_replicator_registry_remove_nonexistent() {
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();

        registry.remove_replicator("nonexistent", &peer);

        let other_peer = random_peer_id();
        registry.add_replicator("users", &other_peer);
        registry.remove_replicator("users", &peer);

        assert!(registry.is_replicator("users", &other_peer));
    }

    #[test]
    fn test_replicator_registry_concurrent_modifications() {
        use std::thread;

        let registry = std::sync::Arc::new(ReplicatorRegistry::new());
        let mut handles = vec![];

        for i in 0..10 {
            let registry_clone = std::sync::Arc::clone(&registry);
            let handle = thread::spawn(move || {
                let peer = random_peer_id();
                let collection = format!("collection_{}", i % 3);

                registry_clone.add_replicator(&collection, &peer);
                assert!(registry_clone.is_any_replicator(&peer));

                if i % 2 == 0 {
                    registry_clone.remove_replicator(&collection, &peer);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        let _ = registry.get_replicators("collection_0");
        let _ = registry.get_replicators("collection_1");
        let _ = registry.get_replicators("collection_2");
    }

    #[test]
    fn test_replicator_registry_list_replicator_info() {
        let registry = ReplicatorRegistry::new();
        let peer1 = random_peer_id();
        let peer2 = random_peer_id();

        registry.add_replicator("users", &peer1);
        registry.add_replicator("posts", &peer1);
        registry.add_replicator("users", &peer2);

        let infos = registry.list_replicator_info();
        assert_eq!(infos.len(), 2);

        let peer1_info = infos.iter().find(|i| i.peer_id_str() == peer1).unwrap();
        assert_eq!(peer1_info.collections.len(), 2);
        assert!(peer1_info.collections.contains(&"users".to_string()));
        assert!(peer1_info.collections.contains(&"posts".to_string()));

        let peer2_info = infos.iter().find(|i| i.peer_id_str() == peer2).unwrap();
        assert_eq!(peer2_info.collections.len(), 1);
        assert!(peer2_info.collections.contains(&"users".to_string()));
    }

    #[test]
    fn test_replicator_registry_load_from_infos() {
        let registry = ReplicatorRegistry::new();
        let peer1 = random_peer_id();
        let peer2 = random_peer_id();

        let infos = vec![
            ReplicatorInfo::from_raw(
                peer1.clone(),
                vec!["users".to_string(), "posts".to_string()],
                vec![],
            ),
            ReplicatorInfo::from_raw(peer2.clone(), vec!["comments".to_string()], vec![]),
        ];

        let (loaded, skipped) = registry.load_from_infos(&infos);
        assert_eq!(loaded, 2);
        assert_eq!(skipped, 0);

        assert!(registry.is_replicator("users", &peer1));
        assert!(registry.is_replicator("posts", &peer1));
        assert!(!registry.is_replicator("comments", &peer1));

        assert!(registry.is_replicator("comments", &peer2));
        assert!(!registry.is_replicator("users", &peer2));
    }

    #[test]
    fn test_replicator_registry_load_clears_existing() {
        let registry = ReplicatorRegistry::new();
        let peer1 = random_peer_id();
        let peer2 = random_peer_id();

        registry.add_replicator("users", &peer1);
        assert!(registry.is_replicator("users", &peer1));

        let infos = vec![ReplicatorInfo::from_raw(
            peer2.clone(),
            vec!["comments".to_string()],
            vec![],
        )];
        let (loaded, skipped) = registry.load_from_infos(&infos);
        assert_eq!(loaded, 1);
        assert_eq!(skipped, 0);

        assert!(!registry.is_replicator("users", &peer1));
        assert!(!registry.is_any_replicator(&peer1));

        assert!(registry.is_replicator("comments", &peer2));
    }

    #[test]
    fn test_replicator_registry_get_replicator_info() {
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();

        assert!(registry.get_replicator_info(&peer).is_none());

        registry.add_replicator("users", &peer);
        registry.add_replicator("posts", &peer);

        let info = registry.get_replicator_info(&peer).unwrap();
        assert_eq!(info.peer_id_str(), peer);
        assert_eq!(info.collections.len(), 2);
        assert!(info.collections.contains(&"users".to_string()));
        assert!(info.collections.contains(&"posts".to_string()));
    }

    #[test]
    fn test_replicator_registry_preserves_filters() {
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();
        let mut filters = ReplicationFilters::new();
        filters.insert(
            "users".to_string(),
            ReplicationFilter::new("agent_did", serde_json::json!("did:key:z6M")),
        );

        registry.set_replicator_info(ReplicatorInfo::from_raw_with_filters(
            peer.clone(),
            vec!["users".to_string(), "posts".to_string()],
            vec![],
            filters.clone(),
        ));

        assert!(registry.is_replicator("users", &peer));
        assert!(registry.is_filtered_replicator("users", &peer));
        assert!(!registry.is_filtered_replicator("posts", &peer));

        let info = registry.get_replicator_info(&peer).unwrap();
        assert_eq!(info.filters, filters);

        registry.remove_replicator("users", &peer);
        let info = registry.get_replicator_info(&peer).unwrap();
        assert!(info.filters.is_empty());
        assert!(registry.is_replicator("posts", &peer));
    }

    #[test]
    fn test_remove_peer_collections_by_name_vs_cid() {
        // The push registry is keyed by collection CID. Removing by the collection
        // NAME must NOT clear the peer (it never matches the stored CID) — this is
        // exactly the divergence the adapter's name->CID resolution fixes. Removing
        // by the CID does clear the peer. Regression guard for #1038.
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();
        let cid = "bafyCollectionCID".to_string();
        let name = "AgentDoc".to_string();

        registry.add_replicator(&cid, &peer);
        assert!(registry.is_replicator(&cid, &peer));

        // Buggy path: caller passed the NAME -> no match -> peer stays in push list.
        let fully_removed = registry.remove_peer_collections(&peer, &[name]);
        assert!(!fully_removed);
        assert!(registry.is_replicator(&cid, &peer));
        assert!(registry.get_replicators(&cid).contains(&peer));

        // Fixed path: caller passed the resolved CID -> peer removed from push list.
        let fully_removed = registry.remove_peer_collections(&peer, std::slice::from_ref(&cid));
        assert!(fully_removed);
        assert!(!registry.is_replicator(&cid, &peer));
        assert!(registry.get_replicators(&cid).is_empty());
    }

    #[test]
    fn test_replicator_registry_get_all_peer_ids() {
        let registry = ReplicatorRegistry::new();
        let peer1 = random_peer_id();
        let peer2 = random_peer_id();
        let peer3 = random_peer_id();

        registry.add_replicator("users", &peer1);
        registry.add_replicator("users", &peer2);
        registry.add_replicator("posts", &peer2);
        registry.add_replicator("comments", &peer3);

        let peer_ids = registry.get_all_peer_ids();
        assert_eq!(peer_ids.len(), 3);
        assert!(peer_ids.contains(&peer1));
        assert!(peer_ids.contains(&peer2));
        assert!(peer_ids.contains(&peer3));
    }

    #[test]
    fn test_replicator_registry_load_accepts_iroh_peer_ids() {
        let registry = ReplicatorRegistry::new();

        let infos = vec![
            ReplicatorInfo::from_raw(
                "iroh-endpoint-id-abc123".to_string(),
                vec!["users".to_string()],
                vec![],
            ),
            ReplicatorInfo::from_raw(
                "iroh-endpoint-id-def456".to_string(),
                vec!["posts".to_string()],
                vec![],
            ),
        ];

        let (loaded, skipped) = registry.load_from_infos(&infos);
        assert_eq!(loaded, 2);
        assert_eq!(skipped, 0);

        assert!(registry.is_replicator("users", "iroh-endpoint-id-abc123"));
        assert!(registry.is_replicator("posts", "iroh-endpoint-id-def456"));
    }

    #[test]
    fn test_replicator_registry_load_empty_collections() {
        let registry = ReplicatorRegistry::new();
        let peer = random_peer_id();

        let infos = vec![ReplicatorInfo::from_raw(peer.clone(), vec![], vec![])];

        let (loaded, skipped) = registry.load_from_infos(&infos);
        assert_eq!(loaded, 1);
        assert_eq!(skipped, 0);

        assert!(!registry.is_any_replicator(&peer));
        assert!(registry.get_all_peer_ids().is_empty());
    }

    #[test]
    fn test_replicator_registry_roundtrip() {
        let registry1 = ReplicatorRegistry::new();
        let peer1 = random_peer_id();
        let peer2 = random_peer_id();

        registry1.add_replicator("users", &peer1);
        registry1.add_replicator("posts", &peer1);
        registry1.add_replicator("users", &peer2);
        registry1.add_replicator("comments", &peer2);

        let infos = registry1.list_replicator_info();

        let registry2 = ReplicatorRegistry::new();
        let (loaded, skipped) = registry2.load_from_infos(&infos);
        assert_eq!(loaded, 2);
        assert_eq!(skipped, 0);

        assert_eq!(
            registry1.is_replicator("users", &peer1),
            registry2.is_replicator("users", &peer1)
        );
        assert_eq!(
            registry1.is_replicator("posts", &peer1),
            registry2.is_replicator("posts", &peer1)
        );
        assert_eq!(
            registry1.is_replicator("users", &peer2),
            registry2.is_replicator("users", &peer2)
        );
        assert_eq!(
            registry1.is_replicator("comments", &peer2),
            registry2.is_replicator("comments", &peer2)
        );
    }
}
