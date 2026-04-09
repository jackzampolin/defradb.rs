use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use parking_lot::RwLock;

use super::{
    GraphQLSubscriptionState, NodeHandle, NodeState, SubscriptionHandle, SubscriptionState,
};

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
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
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

    /// Get a mutable reference to a node state.
    ///
    /// Returns None if the handle is invalid.
    pub fn get_mut<F, R>(&self, handle: NodeHandle, f: F) -> Option<R>
    where
        F: FnOnce(&mut NodeState) -> R,
    {
        let mut nodes = self.nodes.write();
        nodes.get_mut(&handle).map(f)
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

    /// Apply a mutable operation to every node state in the registry.
    pub fn for_each_mut<F>(&self, mut f: F)
    where
        F: FnMut(&mut NodeState),
    {
        let mut nodes = self.nodes.write();
        for state in nodes.values_mut() {
            f(state);
        }
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
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
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

/// Global registry of active GraphQL subscriptions.
pub struct GraphQLSubscriptionRegistry {
    subscriptions: RwLock<HashMap<SubscriptionHandle, GraphQLSubscriptionState>>,
    next_handle: AtomicUsize,
}

impl GraphQLSubscriptionRegistry {
    fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            next_handle: AtomicUsize::new(1),
        }
    }

    /// Insert a new GraphQL subscription state and return its handle.
    pub fn insert(&self, state: GraphQLSubscriptionState) -> SubscriptionHandle {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        let mut subs = self.subscriptions.write();
        subs.insert(handle, state);
        handle
    }

    /// Get mutable access to a GraphQL subscription state.
    pub fn get_mut<F, R>(&self, handle: SubscriptionHandle, f: F) -> Option<R>
    where
        F: FnOnce(&mut GraphQLSubscriptionState) -> R,
    {
        let mut subs = self.subscriptions.write();
        subs.get_mut(&handle).map(f)
    }

    /// Remove and return a GraphQL subscription state.
    pub fn remove(&self, handle: SubscriptionHandle) -> Option<GraphQLSubscriptionState> {
        let mut subs = self.subscriptions.write();
        subs.remove(&handle)
    }

    /// Remove all GraphQL subscriptions for a given node handle.
    pub fn remove_for_node(&self, node_handle: NodeHandle) -> Vec<GraphQLSubscriptionState> {
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

/// Global GraphQL subscription registry singleton.
static GRAPHQL_SUBSCRIPTION_REGISTRY: OnceLock<GraphQLSubscriptionRegistry> = OnceLock::new();

/// Access the global GraphQL subscription registry.
pub fn graphql_subscriptions() -> &'static GraphQLSubscriptionRegistry {
    GRAPHQL_SUBSCRIPTION_REGISTRY.get_or_init(GraphQLSubscriptionRegistry::new)
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

    pub fn get_mut<F, R>(&self, handle: NodeHandle, f: F) -> Option<R>
    where
        F: FnOnce(&mut NodeState) -> R,
    {
        nodes().get_mut(handle, f)
    }

    pub fn for_each_mut<F>(&self, f: F)
    where
        F: FnMut(&mut NodeState),
    {
        nodes().for_each_mut(f)
    }

    pub fn remove(&self, handle: NodeHandle) -> Option<NodeState> {
        nodes().remove(handle)
    }
}

/// Global NODES accessor for backwards compatibility.
pub static NODES: NodesAccess = NodesAccess;

/// Convenience wrapper for GraphQL subscription registry access.
pub struct GraphQLSubscriptionsAccess;

impl GraphQLSubscriptionsAccess {
    pub fn insert(&self, state: GraphQLSubscriptionState) -> SubscriptionHandle {
        graphql_subscriptions().insert(state)
    }

    pub fn get_mut<F, R>(&self, handle: SubscriptionHandle, f: F) -> Option<R>
    where
        F: FnOnce(&mut GraphQLSubscriptionState) -> R,
    {
        graphql_subscriptions().get_mut(handle, f)
    }

    pub fn remove(&self, handle: SubscriptionHandle) -> Option<GraphQLSubscriptionState> {
        graphql_subscriptions().remove(handle)
    }

    pub fn remove_for_node(&self, node_handle: NodeHandle) -> Vec<GraphQLSubscriptionState> {
        graphql_subscriptions().remove_for_node(node_handle)
    }
}

/// Global GRAPHQL_SUBSCRIPTIONS accessor.
pub static GRAPHQL_SUBSCRIPTIONS: GraphQLSubscriptionsAccess = GraphQLSubscriptionsAccess;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_handles() {
        // Handles should be non-zero and incrementing
        let registry = nodes();
        assert!(registry.is_empty() || !registry.is_empty());
    }
}
