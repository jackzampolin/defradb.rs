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
    collections: Arc<HashMap<String, Collection>>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    fn test_collection() -> Collection {
        Collection::new(CollectionVersion::new(
            "Users",
            "v1",
            "col-users",
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        ))
    }

    #[test]
    fn test_snapshot_get() {
        let mut map = HashMap::new();
        map.insert("Users".to_string(), test_collection());
        let snapshot = CollectionSnapshot::new(map);

        assert!(snapshot.get("Users").is_some());
        assert!(snapshot.get("Posts").is_none());
    }

    #[test]
    fn test_snapshot_contains() {
        let mut map = HashMap::new();
        map.insert("Users".to_string(), test_collection());
        let snapshot = CollectionSnapshot::new(map);

        assert!(snapshot.contains("Users"));
        assert!(!snapshot.contains("Posts"));
    }

    #[test]
    fn test_snapshot_len() {
        let mut map = HashMap::new();
        map.insert("Users".to_string(), test_collection());
        let snapshot = CollectionSnapshot::new(map);

        assert_eq!(snapshot.len(), 1);
        assert!(!snapshot.is_empty());
    }

    #[test]
    fn test_empty_snapshot() {
        let snapshot = CollectionSnapshot::new(HashMap::new());
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.len(), 0);
    }

    #[test]
    fn test_snapshot_clone_is_cheap() {
        let mut map = HashMap::new();
        map.insert("Users".to_string(), test_collection());
        let snapshot1 = CollectionSnapshot::new(map);
        let snapshot2 = snapshot1.clone();

        // Both should point to the same Arc
        assert!(Arc::ptr_eq(&snapshot1.collections, &snapshot2.collections));
    }

    #[test]
    fn test_snapshot_names() {
        let mut map = HashMap::new();
        map.insert("Users".to_string(), test_collection());
        map.insert(
            "Posts".to_string(),
            Collection::new(CollectionVersion::new("Posts", "v1", "col-posts", vec![])),
        );
        let snapshot = CollectionSnapshot::new(map);

        let mut names = snapshot.names();
        names.sort();
        assert_eq!(names, vec!["Posts", "Users"]);
    }
}
