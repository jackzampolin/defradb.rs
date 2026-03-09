/// Content cache for virtual files and document JSON.
///
/// Caches generated content (e.g. `_view.json`, `_schema.graphql`) to avoid
/// regenerating on every FUSE read/getattr/lookup call. Entries expire
/// after a TTL. Write operations invalidate the relevant collection.
///
/// Key convention:
/// - `"col:{name}:_view.json"` — collection materialized view
/// - `"col:{name}:_schema.graphql"` — collection SDL
/// - `"root:_schema.graphql"` — root-level combined schema
/// - `"root:_collections.json"` — root-level collection listing
use std::collections::HashMap;
use std::time::{Duration, Instant};

const CACHE_TTL: Duration = Duration::from_secs(5);

struct Entry {
    content: Vec<u8>,
    cached_at: Instant,
}

impl Entry {
    fn is_valid(&self) -> bool {
        self.cached_at.elapsed() < CACHE_TTL
    }
}

pub struct ContentCache {
    entries: HashMap<String, Entry>,
}

impl ContentCache {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&[u8]> {
        self.entries
            .get(key)
            .filter(|e| e.is_valid())
            .map(|e| e.content.as_slice())
    }

    pub fn insert(&mut self, key: String, content: Vec<u8>) {
        self.entries.insert(
            key,
            Entry {
                content,
                cached_at: Instant::now(),
            },
        );
    }

    /// Invalidate all entries for a collection and root-level aggregates.
    pub fn invalidate_collection(&mut self, collection: &str) {
        let prefix = format!("col:{}:", collection);
        self.entries.retain(|k, _| !k.starts_with(&prefix));
        self.entries.retain(|k, _| !k.starts_with("root:"));
    }
}

pub fn col_key(collection: &str, filename: &str) -> String {
    format!("col:{}:{}", collection, filename)
}

pub fn root_key(filename: &str) -> String {
    format!("root:{}", filename)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_none_for_missing_key() {
        let cache = ContentCache::new();
        assert!(cache.get("missing").is_none());
    }

    #[test]
    fn insert_then_get_returns_content() {
        let mut cache = ContentCache::new();
        cache.insert("k".into(), b"hello".to_vec());
        assert_eq!(cache.get("k"), Some(b"hello".as_slice()));
    }

    #[test]
    fn invalidate_collection_removes_matching_entries() {
        let mut cache = ContentCache::new();
        cache.insert(col_key("Users", "_view.json"), b"data".to_vec());
        cache.insert(col_key("Users", "_schema.graphql"), b"sdl".to_vec());
        cache.insert(col_key("Posts", "_view.json"), b"other".to_vec());
        cache.insert(root_key("_schema.graphql"), b"root".to_vec());

        cache.invalidate_collection("Users");

        assert!(cache.get(&col_key("Users", "_view.json")).is_none());
        assert!(cache.get(&col_key("Users", "_schema.graphql")).is_none());
        // Root files are also invalidated (they aggregate all collections)
        assert!(cache.get(&root_key("_schema.graphql")).is_none());
        // Other collections are untouched
        assert!(cache.get(&col_key("Posts", "_view.json")).is_some());
    }

    #[test]
    fn key_helpers() {
        assert_eq!(col_key("Users", "_view.json"), "col:Users:_view.json");
        assert_eq!(root_key("_schema.graphql"), "root:_schema.graphql");
    }
}
