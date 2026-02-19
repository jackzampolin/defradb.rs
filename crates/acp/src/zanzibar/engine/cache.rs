use std::collections::{HashMap, HashSet};

use async_lock::RwLock;
use identity::Did;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NodeId(String);

impl NodeId {
    pub(crate) fn new(resource: &str, object_id: &str, relation: &str) -> Self {
        Self(format!("{resource}/{object_id}#{relation}"))
    }
}

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

    pub(crate) fn contains(&self, node: &NodeId) -> bool {
        self.visited.contains(node)
    }

    pub(crate) fn insert(&mut self, node: NodeId) {
        self.visited.insert(node);
    }

    pub(crate) fn with_node(&self, node: NodeId) -> Self {
        let mut new_trail = self.clone();
        new_trail.insert(node);
        new_trail
    }
}

#[derive(Debug, Default)]
pub(crate) struct CheckCache {
    results: RwLock<HashMap<String, bool>>,
}

impl CheckCache {
    pub(crate) fn new() -> Self {
        Self {
            results: RwLock::new(HashMap::new()),
        }
    }

    fn cache_key(resource: &str, object_id: &str, relation: &str, subject: &Did) -> String {
        format!("{resource}/{object_id}#{relation}@{subject}")
    }

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
