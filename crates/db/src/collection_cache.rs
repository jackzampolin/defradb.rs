//! Transaction-scoped collection cache.
//!
//! This module provides a per-transaction cache for collection metadata,
//! matching the Go DefraDB pattern. Each transaction gets its own cache
//! that is populated lazily from the SystemStore.

use std::collections::HashMap;

use crate::collection::Collection;

/// A transaction-scoped cache for collection metadata.
///
/// This cache lives within a transaction and provides:
/// - O(1) average-case lookups by collection name (HashMap-backed)
/// - Lazy loading from SystemStore on cache miss
/// - Isolation between transactions (each txn has its own cache)
///
/// The cache is populated lazily - individual collections are loaded on
/// first access, and the full collection list is loaded when needed.
#[derive(Debug, Clone)]
pub struct CollectionCache {
    /// Collections indexed by name
    by_name: HashMap<String, Collection>,

    /// Whether the full collection set has been loaded from store
    is_fully_populated: bool,
}

impl CollectionCache {
    /// Create a new empty collection cache.
    pub fn new() -> Self {
        Self {
            by_name: HashMap::new(),
            is_fully_populated: false,
        }
    }

    /// Get a collection by name.
    ///
    /// Returns `None` if the collection is not in the cache.
    /// Note: A `None` result does not mean the collection doesn't exist -
    /// it may not have been loaded yet. Use the transaction's lazy loading
    /// methods to check the store.
    pub fn get(&self, name: &str) -> Option<&Collection> {
        self.by_name.get(name)
    }

    /// Check if a collection exists in the cache.
    ///
    /// Note: `false` does not mean the collection doesn't exist - it may
    /// not have been loaded yet.
    pub fn contains(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// Add a collection to the cache, keyed by its name.
    ///
    /// The key is derived from `collection.name()` to prevent key-name mismatches.
    pub fn add(&mut self, collection: Collection) {
        self.by_name
            .insert(collection.name().to_string(), collection);
    }

    /// Remove a collection from the cache.
    ///
    /// Returns the removed collection if it existed.
    /// Note: This resets the `is_fully_populated` flag since the cache
    /// no longer contains all collections.
    pub fn remove(&mut self, name: &str) -> Option<Collection> {
        let removed = self.by_name.remove(name);
        if removed.is_some() {
            self.is_fully_populated = false;
        }
        removed
    }

    /// Get all collection names in the cache.
    pub fn names(&self) -> Vec<String> {
        self.by_name.keys().cloned().collect()
    }

    /// Get the number of collections in the cache.
    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// Check if the cache has been fully populated from the store.
    pub fn is_fully_populated(&self) -> bool {
        self.is_fully_populated
    }

    /// Populate the cache with a full set of collections.
    ///
    /// This replaces any existing cache contents, marks the cache as fully
    /// populated, and derives keys from each collection's name.
    pub fn populate(&mut self, collections: impl IntoIterator<Item = Collection>) {
        self.by_name = collections
            .into_iter()
            .map(|c| (c.name().to_string(), c))
            .collect();
        self.is_fully_populated = true;
    }
}

impl Default for CollectionCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    fn test_collection(name: &str) -> Collection {
        Collection::new(CollectionVersion::new(
            name,
            "v1",
            format!("col-{}", name.to_lowercase()),
            vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())],
        ))
    }

    #[test]
    fn test_cache_new_is_empty() {
        let cache = CollectionCache::new();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        assert!(!cache.is_fully_populated());
    }

    #[test]
    fn test_cache_add_and_get() {
        let mut cache = CollectionCache::new();
        let col = test_collection("Users");

        cache.add(col);

        assert!(cache.contains("Users"));
        assert!(!cache.contains("Posts"));
        assert_eq!(cache.len(), 1);

        let retrieved = cache.get("Users");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "Users");
    }

    #[test]
    fn test_cache_remove() {
        let mut cache = CollectionCache::new();
        cache.add(test_collection("Users"));
        cache.add(test_collection("Posts"));

        assert_eq!(cache.len(), 2);

        let removed = cache.remove("Users");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name(), "Users");
        assert_eq!(cache.len(), 1);
        assert!(!cache.contains("Users"));
        assert!(cache.contains("Posts"));
    }

    #[test]
    fn test_cache_remove_resets_fully_populated() {
        let mut cache = CollectionCache::new();
        cache.populate(vec![test_collection("Users"), test_collection("Posts")]);

        assert!(cache.is_fully_populated());
        assert_eq!(cache.len(), 2);

        cache.remove("Users");

        assert!(
            !cache.is_fully_populated(),
            "Cache should no longer be fully populated after remove"
        );
    }

    #[test]
    fn test_cache_names() {
        let mut cache = CollectionCache::new();
        cache.add(test_collection("Users"));
        cache.add(test_collection("Posts"));

        let mut names = cache.names();
        names.sort();
        assert_eq!(names, vec!["Posts", "Users"]);
    }

    #[test]
    fn test_cache_populate() {
        let mut cache = CollectionCache::new();
        assert!(!cache.is_fully_populated());

        cache.populate(vec![test_collection("Users"), test_collection("Posts")]);

        assert!(cache.is_fully_populated());
        assert_eq!(cache.len(), 2);
        assert!(cache.contains("Users"));
        assert!(cache.contains("Posts"));
    }
}
