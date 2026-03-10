use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct CachedDecision {
    allowed: bool,
    cached_at: Instant,
}

/// In-memory cache for ACP access decisions.
///
/// Caches the result of `verify_access` calls keyed by
/// `(actor, policy, resource, doc_id, permission)`. Entries expire
/// after a configurable TTL. Relationship mutations eagerly invalidate
/// all entries for the affected document.
pub(crate) struct AccessCache {
    ttl: Duration,
    entries: Mutex<HashMap<String, CachedDecision>>,
}

fn cache_key(
    actor_did: &str,
    policy_id: &str,
    resource: &str,
    doc_id: &str,
    permission: &str,
) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        actor_did, policy_id, resource, doc_id, permission
    )
}

fn object_prefix(policy_id: &str, resource: &str, doc_id: &str) -> String {
    format!("|{}|{}|{}|", policy_id, resource, doc_id)
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
    /// Called on relationship mutations (grant/revoke) and document
    /// unregistration. Invalidates all actors and permissions for the
    /// document to avoid subtle races where permission changes affect
    /// multiple actors through indirect relations.
    pub(crate) fn invalidate_object(&self, policy_id: &str, resource: &str, doc_id: &str) {
        let prefix = object_prefix(policy_id, resource, doc_id);
        if let Ok(mut entries) = self.entries.lock() {
            entries.retain(|key, _| !key.contains(&prefix));
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
}
