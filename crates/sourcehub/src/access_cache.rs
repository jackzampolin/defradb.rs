use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(PartialEq, Eq, Hash)]
struct CacheKey {
    actor_did: String,
    policy_id: String,
    resource: String,
    doc_id: String,
    permission: String,
}

struct CachedDecision {
    allowed: bool,
    cached_at: Instant,
}

/// In-memory cache for ACP access decisions.
///
/// Caches the result of `verify_access` calls keyed by
/// `(actor, policy, resource, doc_id, permission)`. Entries expire
/// after a configurable TTL. Relationship mutations eagerly invalidate
/// all entries for their policy so indirect grants cannot remain cached.
pub(crate) struct AccessCache {
    ttl: Duration,
    entries: Mutex<HashMap<CacheKey, CachedDecision>>,
}

fn cache_key(
    actor_did: &str,
    policy_id: &str,
    resource: &str,
    doc_id: &str,
    permission: &str,
) -> CacheKey {
    CacheKey {
        actor_did: actor_did.to_string(),
        policy_id: policy_id.to_string(),
        resource: resource.to_string(),
        doc_id: doc_id.to_string(),
        permission: permission.to_string(),
    }
}

impl AccessCache {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn get(
        &self,
        actor_did: &str,
        policy_id: &str,
        resource: &str,
        doc_id: &str,
        permission: &str,
    ) -> Option<bool> {
        let key = cache_key(actor_did, policy_id, resource, doc_id, permission);
        let entries = self.entries.lock().ok()?;
        let entry = entries.get(&key)?;
        if entry.cached_at.elapsed() > self.ttl {
            None
        } else {
            Some(entry.allowed)
        }
    }

    pub(crate) fn set(
        &self,
        actor_did: &str,
        policy_id: &str,
        resource: &str,
        doc_id: &str,
        permission: &str,
        allowed: bool,
    ) {
        let key = cache_key(actor_did, policy_id, resource, doc_id, permission);
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(
                key,
                CachedDecision {
                    allowed,
                    cached_at: Instant::now(),
                },
            );
        }
    }

    /// Invalidate ALL cached decisions for a specific document.
    ///
    /// Called on document registration and archival mutations. Invalidates all
    /// actors and permissions for the affected document.
    pub(crate) fn invalidate_object(&self, policy_id: &str, resource: &str, doc_id: &str) -> usize {
        if let Ok(mut entries) = self.entries.lock() {
            let previous_len = entries.len();
            entries.retain(|key, _| {
                key.policy_id != policy_id || key.resource != resource || key.doc_id != doc_id
            });
            previous_len - entries.len()
        } else {
            0
        }
    }

    pub(crate) fn invalidate_policy(&self, policy_id: &str) -> usize {
        if let Ok(mut entries) = self.entries.lock() {
            let previous_len = entries.len();
            entries.retain(|key, _| key.policy_id != policy_id);
            previous_len - entries.len()
        } else {
            0
        }
    }

    pub(crate) fn clear(&self) -> usize {
        if let Ok(mut entries) = self.entries.lock() {
            let previous_len = entries.len();
            entries.clear();
            previous_len
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_hit_returns_stored_value() {
        let cache = AccessCache::new(Duration::from_secs(300));
        cache.set("did:key:alice", "policy1", "users", "doc1", "read", true);

        assert_eq!(
            cache.get("did:key:alice", "policy1", "users", "doc1", "read"),
            Some(true)
        );
    }

    #[test]
    fn cache_miss_returns_none() {
        let cache = AccessCache::new(Duration::from_secs(300));

        assert_eq!(
            cache.get("did:key:alice", "policy1", "users", "doc1", "read"),
            None
        );
    }

    #[test]
    fn expired_entry_returns_none() {
        let cache = AccessCache::new(Duration::from_millis(1));
        cache.set("did:key:alice", "policy1", "users", "doc1", "read", true);

        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(
            cache.get("did:key:alice", "policy1", "users", "doc1", "read"),
            None
        );
    }

    #[test]
    fn invalidate_object_clears_all_entries_for_document() {
        let cache = AccessCache::new(Duration::from_secs(300));
        cache.set("did:key:alice", "p1", "users", "doc1", "read", true);
        cache.set("did:key:alice", "p1", "users", "doc1", "update", true);
        cache.set("did:key:bob", "p1", "users", "doc1", "read", false);

        cache.invalidate_object("p1", "users", "doc1");

        assert_eq!(
            cache.get("did:key:alice", "p1", "users", "doc1", "read"),
            None
        );
        assert_eq!(
            cache.get("did:key:alice", "p1", "users", "doc1", "update"),
            None
        );
        assert_eq!(
            cache.get("did:key:bob", "p1", "users", "doc1", "read"),
            None
        );
    }

    #[test]
    fn invalidate_object_preserves_other_documents() {
        let cache = AccessCache::new(Duration::from_secs(300));
        cache.set("did:key:alice", "p1", "users", "doc1", "read", true);
        cache.set("did:key:alice", "p1", "users", "doc2", "read", true);

        cache.invalidate_object("p1", "users", "doc1");

        assert_eq!(
            cache.get("did:key:alice", "p1", "users", "doc1", "read"),
            None
        );
        assert_eq!(
            cache.get("did:key:alice", "p1", "users", "doc2", "read"),
            Some(true)
        );
    }

    #[test]
    fn invalidate_object_uses_exact_key_components() {
        let cache = AccessCache::new(Duration::from_secs(300));
        cache.set("did:key:alice", "p1", "users", "doc1", "read", true);
        cache.set("did:key:alice", "p1", "users|doc1", "other", "read", true);

        cache.invalidate_object("p1", "users", "doc1");

        assert_eq!(
            cache.get("did:key:alice", "p1", "users|doc1", "other", "read"),
            Some(true)
        );
    }

    #[test]
    fn invalidate_policy_preserves_other_policies() {
        let cache = AccessCache::new(Duration::from_secs(300));
        cache.set("did:key:alice", "p1", "users", "doc1", "read", true);
        cache.set("did:key:alice", "p2", "users", "doc1", "read", true);

        assert_eq!(cache.invalidate_policy("p1"), 1);
        assert_eq!(
            cache.get("did:key:alice", "p1", "users", "doc1", "read"),
            None
        );
        assert_eq!(
            cache.get("did:key:alice", "p2", "users", "doc1", "read"),
            Some(true)
        );
    }

    #[test]
    fn clear_removes_every_entry() {
        let cache = AccessCache::new(Duration::from_secs(300));
        cache.set("did:key:alice", "p1", "users", "doc1", "read", true);
        cache.set("did:key:bob", "p2", "books", "doc2", "update", true);

        assert_eq!(cache.clear(), 2);
        assert_eq!(
            cache.get("did:key:alice", "p1", "users", "doc1", "read"),
            None
        );
        assert_eq!(
            cache.get("did:key:bob", "p2", "books", "doc2", "update"),
            None
        );
    }
}
