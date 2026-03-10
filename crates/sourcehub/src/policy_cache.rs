use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub(crate) struct CachedPolicy {
    pub id: String,
    pub name: String,
    pub cached_at: Instant,
}

/// In-memory cache for SourceHub policy metadata.
///
/// Entries expire after the configured TTL. A cache miss (absent or expired
/// entry) must always fall back to an on-chain query rather than treating
/// the miss as "no policy exists". This ensures stale or absent cache state
/// never silently denies or allows access incorrectly.
pub(crate) struct PolicyCache {
    ttl: Duration,
    entries: Mutex<HashMap<String, CachedPolicy>>,
}

impl PolicyCache {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Return a cached entry only if it is present and not expired.
    ///
    /// Returns `None` on cache miss or expiry — callers must fall back to
    /// an on-chain query in that case.
    pub(crate) fn get(&self, policy_id: &str) -> Option<CachedPolicy> {
        let entries = self.entries.lock().ok()?;
        let entry = entries.get(policy_id)?;
        if entry.cached_at.elapsed() > self.ttl {
            None
        } else {
            Some(entry.clone())
        }
    }

    /// Insert or refresh a policy entry.
    pub(crate) fn insert(&self, policy_id: &str, name: String) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.insert(
                policy_id.to_string(),
                CachedPolicy {
                    id: policy_id.to_string(),
                    name,
                    cached_at: Instant::now(),
                },
            );
        }
    }
}
