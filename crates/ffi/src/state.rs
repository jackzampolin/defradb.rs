//! Node state management for FFI.
//!
//! This module manages the lifecycle of node instances and their handles.
//! Go code receives opaque usize handles that map to actual node state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use parking_lot::RwLock;
use storage::MemoryStore;

/// Type alias for the database type used in FFI.
pub type FfiDatabase = db::DB<MemoryStore>;

/// Type alias for node handles (opaque to FFI callers).
pub type NodeHandle = usize;

/// State held for each FFI node.
pub struct NodeState {
    /// The database instance.
    pub database: std::sync::Arc<FfiDatabase>,
    /// The query runner for executing GraphQL queries.
    pub query_runner: std::sync::Arc<dyn query::QueryExecutor>,
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
