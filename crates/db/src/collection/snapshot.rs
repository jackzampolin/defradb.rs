//! Immutable collection snapshot for transaction isolation.

use std::collections::HashMap;
use std::sync::Arc;

use crate::collection::Collection;

/// An immutable snapshot of collection definitions at a point in time.
///
/// This type provides snapshot isolation for transactions - each transaction
/// sees a consistent view of collections throughout its lifetime, even if
/// collections are created or deleted concurrently.
///
/// The snapshot is wrapped in `Arc` internally for efficient cloning and sharing.
#[derive(Debug, Clone)]
pub struct CollectionSnapshot {
    pub collections: Arc<HashMap<String, Collection>>,
}

impl CollectionSnapshot {
    /// True when both snapshots share one allocation, i.e. the clone was cheap.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.collections, &other.collections)
    }

    /// Create a new collection snapshot from a HashMap.
    pub fn new(collections: HashMap<String, Collection>) -> Self {
        Self {
            collections: Arc::new(collections),
        }
    }

    /// Get a collection by name.
    pub fn get(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }

    /// Check if a collection exists in the snapshot.
    pub fn contains(&self, name: &str) -> bool {
        self.collections.contains_key(name)
    }

    /// Get the number of collections in the snapshot.
    pub fn len(&self) -> usize {
        self.collections.len()
    }

    /// Check if the snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.collections.is_empty()
    }

    /// Get collection names as a vector.
    pub fn names(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    /// Get an iterator over the collections.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Collection)> {
        self.collections.iter()
    }
}

impl From<HashMap<String, Collection>> for CollectionSnapshot {
    fn from(collections: HashMap<String, Collection>) -> Self {
        Self::new(collections)
    }
}
