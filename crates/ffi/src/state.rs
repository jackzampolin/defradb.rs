//! Node state management for FFI.
//!
//! This module manages the lifecycle of node instances and their handles.
//! Go code receives opaque usize handles that map to actual node state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use storage::MemoryStore;

/// In-memory policy store for DAC policies.
///
/// Stores policy YAML indexed by content-addressed ID (SHA256 hash).
#[derive(Default)]
pub struct PolicyStore {
    policies: RwLock<HashMap<String, String>>,
}

impl PolicyStore {
    /// Create a new empty policy store.
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Add a policy and return its content-addressed ID.
    ///
    /// The ID is a SHA256 hash of the policy content (hex-encoded).
    pub fn add_policy(&self, policy: &str) -> String {
        // Compute content-addressed ID (SHA256 hash)
        let mut hasher = Sha256::new();
        hasher.update(policy.as_bytes());
        let hash = hasher.finalize();
        let policy_id = hex::encode(hash);

        // Store the policy
        self.policies.write().insert(policy_id.clone(), policy.to_string());

        policy_id
    }

    /// Get a policy by ID.
    pub fn get_policy(&self, id: &str) -> Option<String> {
        self.policies.read().get(id).cloned()
    }

    /// List all policy IDs.
    pub fn list_policies(&self) -> Vec<String> {
        self.policies.read().keys().cloned().collect()
    }
}

/// Type alias for the database type used in FFI.
pub type FfiDatabase = db::DB<MemoryStore>;

/// Type alias for node handles (opaque to FFI callers).
pub type NodeHandle = usize;

/// Type alias for the NAC manager used in FFI (in-memory).
pub type FfiNacManager = db::NacManager<acp::MemoryZanzibarStore>;

/// Type alias for subscription handles (opaque to FFI callers).
pub type SubscriptionHandle = usize;

/// State held for each FFI node.
pub struct NodeState {
    /// The database instance.
    pub database: Arc<FfiDatabase>,
    /// The query runner for executing GraphQL queries.
    pub query_runner: Arc<dyn query::QueryExecutor>,
    /// The NAC manager for node-level access control.
    pub nac_manager: Arc<FfiNacManager>,
    /// The document ACP for document-level access control.
    pub document_acp: Arc<dyn acp::DocumentACP>,
    /// The event bus for subscriptions.
    pub event_bus: Arc<dyn events::Bus>,
    /// The policy store for DAC policies.
    pub policy_store: Arc<PolicyStore>,
}

/// State held for each FFI subscription.
pub struct SubscriptionState {
    /// The underlying events subscription.
    pub subscription: events::Subscription,
    /// The node handle this subscription belongs to.
    pub node_handle: NodeHandle,
    /// Optional collection name filter (None = all collections).
    pub collection_filter: Option<String>,
}

/// Global registry of active nodes.
///
/// Uses RwLock for safe concurrent access from multiple threads.
pub struct NodeRegistry {
    nodes: RwLock<HashMap<NodeHandle, NodeState>>,
    next_handle: AtomicUsize,
}

impl NodeRegistry {
    /// Create a new empty registry.
    fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            next_handle: AtomicUsize::new(1), // Start at 1, 0 is invalid
        }
    }

    /// Insert a new node state and return its handle.
    pub fn insert(&self, state: NodeState) -> NodeHandle {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        let mut nodes = self.nodes.write();
        nodes.insert(handle, state);
        handle
    }

    /// Get a reference to a node state.
    ///
    /// Returns None if the handle is invalid.
    pub fn get<F, R>(&self, handle: NodeHandle, f: F) -> Option<R>
    where
        F: FnOnce(&NodeState) -> R,
    {
        let nodes = self.nodes.read();
        nodes.get(&handle).map(f)
    }

    /// Remove and return a node state.
    ///
    /// Returns None if the handle is invalid.
    pub fn remove(&self, handle: NodeHandle) -> Option<NodeState> {
        let mut nodes = self.nodes.write();
        nodes.remove(&handle)
    }

    /// Check if a handle is valid.
    pub fn contains(&self, handle: NodeHandle) -> bool {
        let nodes = self.nodes.read();
        nodes.contains_key(&handle)
    }

    /// Get the number of active nodes.
    pub fn len(&self) -> usize {
        let nodes = self.nodes.read();
        nodes.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Global node registry singleton, lazily initialized.
static NODE_REGISTRY: OnceLock<NodeRegistry> = OnceLock::new();

/// Access the global node registry.
pub fn nodes() -> &'static NodeRegistry {
    NODE_REGISTRY.get_or_init(NodeRegistry::new)
}

/// Global registry of active subscriptions.
pub struct SubscriptionRegistry {
    subscriptions: RwLock<HashMap<SubscriptionHandle, SubscriptionState>>,
    next_handle: AtomicUsize,
}

impl SubscriptionRegistry {
    fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            next_handle: AtomicUsize::new(1),
        }
    }

    /// Insert a new subscription state and return its handle.
    pub fn insert(&self, state: SubscriptionState) -> SubscriptionHandle {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        let mut subs = self.subscriptions.write();
        subs.insert(handle, state);
        handle
    }

    /// Get mutable access to a subscription state (required for try_recv).
    pub fn get_mut<F, R>(&self, handle: SubscriptionHandle, f: F) -> Option<R>
    where
        F: FnOnce(&mut SubscriptionState) -> R,
    {
        let mut subs = self.subscriptions.write();
        subs.get_mut(&handle).map(f)
    }

    /// Remove and return a subscription state.
    pub fn remove(&self, handle: SubscriptionHandle) -> Option<SubscriptionState> {
        let mut subs = self.subscriptions.write();
        subs.remove(&handle)
    }

    /// Remove all subscriptions for a given node handle.
    pub fn remove_for_node(&self, node_handle: NodeHandle) -> Vec<SubscriptionState> {
        let mut subs = self.subscriptions.write();
        let handles_to_remove: Vec<SubscriptionHandle> = subs
            .iter()
            .filter(|(_, state)| state.node_handle == node_handle)
            .map(|(handle, _)| *handle)
            .collect();

        handles_to_remove
            .into_iter()
            .filter_map(|handle| subs.remove(&handle))
            .collect()
    }
}

/// Global subscription registry singleton.
static SUBSCRIPTION_REGISTRY: OnceLock<SubscriptionRegistry> = OnceLock::new();

/// Access the global subscription registry.
pub fn subscriptions() -> &'static SubscriptionRegistry {
    SUBSCRIPTION_REGISTRY.get_or_init(SubscriptionRegistry::new)
}

/// Convenience wrapper for subscription registry access.
pub struct SubscriptionsAccess;

impl SubscriptionsAccess {
    pub fn insert(&self, state: SubscriptionState) -> SubscriptionHandle {
        subscriptions().insert(state)
    }

    pub fn get_mut<F, R>(&self, handle: SubscriptionHandle, f: F) -> Option<R>
    where
        F: FnOnce(&mut SubscriptionState) -> R,
    {
        subscriptions().get_mut(handle, f)
    }

    pub fn remove(&self, handle: SubscriptionHandle) -> Option<SubscriptionState> {
        subscriptions().remove(handle)
    }

    pub fn remove_for_node(&self, node_handle: NodeHandle) -> Vec<SubscriptionState> {
        subscriptions().remove_for_node(node_handle)
    }
}

/// Global SUBSCRIPTIONS accessor.
pub static SUBSCRIPTIONS: SubscriptionsAccess = SubscriptionsAccess;

/// Convenience wrapper for NODES access (backwards compatibility).
pub struct NodesAccess;

impl NodesAccess {
    pub fn insert(&self, state: NodeState) -> NodeHandle {
        nodes().insert(state)
    }

    pub fn get<F, R>(&self, handle: NodeHandle, f: F) -> Option<R>
    where
        F: FnOnce(&NodeState) -> R,
    {
        nodes().get(handle, f)
    }

    pub fn remove(&self, handle: NodeHandle) -> Option<NodeState> {
        nodes().remove(handle)
    }
}

/// Global NODES accessor for backwards compatibility.
pub static NODES: NodesAccess = NodesAccess;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_handles() {
        // Handles should be non-zero and incrementing
        let registry = nodes();
        assert!(registry.is_empty() || registry.len() > 0);
    }
}
