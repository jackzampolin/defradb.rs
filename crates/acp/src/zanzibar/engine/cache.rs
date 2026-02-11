//! Cache and cycle detection types for permission evaluation.

use std::collections::{HashMap, HashSet};

use async_lock::RwLock;
use identity::Did;
use sha2::{Digest, Sha256};

/// Node identifier for cycle detection.
///
/// Uniquely identifies a permission check node in the evaluation tree.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(String);

impl NodeId {
    /// Create a node ID from (resource, object_id, relation).
    pub(crate) fn new(resource: &str, object_id: &str, relation: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(resource.as_bytes());
        hasher.update(b"/");
        hasher.update(object_id.as_bytes());
        hasher.update(b"#");
        hasher.update(relation.as_bytes());
        let result = hasher.finalize();
        Self(format!("{:x}", result))
    }
}

/// Trail of visited nodes for cycle detection.
///
/// Tracks the path through the evaluation tree to detect cycles.
///
/// # Performance Note
/// The trail is cloned on each recursive call to maintain independent paths.
/// For very deep permission hierarchies, this could become expensive (O(n) per clone
/// where n is the depth). If this becomes a bottleneck, consider using the `im` crate
/// for persistent data structures which provide O(log n) cloning.
#[derive(Debug, Clone, Default)]
pub(crate) struct NodeTrail {
    visited: HashSet<NodeId>,
}

impl NodeTrail {
    pub(crate) fn new() -> Self {
        Self {
            visited: HashSet::new(),
        }
    }

    /// Check if a node has been visited (would create a cycle).
    pub(crate) fn contains(&self, node: &NodeId) -> bool {
        self.visited.contains(node)
    }

    /// Add a node to the trail.
    pub(crate) fn insert(&mut self, node: NodeId) {
        self.visited.insert(node);
    }

    /// Create a new trail with an additional node.
    pub(crate) fn with_node(&self, node: NodeId) -> Self {
        let mut new_trail = self.clone();
        new_trail.insert(node);
        new_trail
    }
}

/// Request-scoped cache for permission check results.
///
/// Caches the result of permission evaluations within a single top-level check
/// to avoid redundant computations when the same (resource, object_id, relation)
/// is checked multiple times during recursive evaluation.
///
/// The cache key includes the subject DID to ensure correct behavior when
/// checking permissions for different subjects.
#[derive(Debug, Default)]
pub(crate) struct CheckCache {
    /// Cache: (resource, object_id, relation, subject_hash) -> result
    results: RwLock<HashMap<String, bool>>,
}

impl CheckCache {
    pub(crate) fn new() -> Self {
        Self {
            results: RwLock::new(HashMap::new()),
        }
    }

    /// Generate a cache key for a permission check.
    fn cache_key(resource: &str, object_id: &str, relation: &str, subject: &Did) -> String {
        let mut hasher = Sha256::new();
        hasher.update(resource.as_bytes());
        hasher.update(b"/");
        hasher.update(object_id.as_bytes());
        hasher.update(b"#");
        hasher.update(relation.as_bytes());
        hasher.update(b"@");
        hasher.update(subject.to_string().as_bytes());
        let result = hasher.finalize();
        format!("{:x}", result)
    }

    /// Get a cached result if available.
    pub(crate) async fn get(
        &self,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
    ) -> Option<bool> {
        let key = Self::cache_key(resource, object_id, relation, subject);
        self.results.read().await.get(&key).copied()
    }

    /// Store a result in the cache.
    pub(crate) async fn set(
        &self,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &Did,
        result: bool,
    ) {
        let key = Self::cache_key(resource, object_id, relation, subject);
        self.results.write().await.insert(key, result);
    }
}
