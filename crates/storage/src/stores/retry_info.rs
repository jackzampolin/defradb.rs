/// Retry information for failed replicator pushes.
///
/// Tracks the number of retries and the next retry time using exponential
/// backoff intervals matching Go DefraDB's replicator retry behavior.
use std::time::Duration;
use std::{hash::Hash, hash::Hasher};
use web_time::{SystemTime, UNIX_EPOCH};

/// Exponential backoff intervals in seconds, matching Go's seconds-to-hours retry ladder.
pub const RETRY_INTERVALS_SECS: &[u64] = &[
    30, 60, 120, 240, 480, 960, 1920, 3600, 7200, 14400, 28800, 43200,
];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryInfo {
    pub num_retries: u32,
    pub next_retry_unix: u64,
}

/// What kind of push obligation a retry record represents.
///
/// Document heads replay by document id; collection commits are doc-less and
/// replay by CID. Persisting the scope lets the ledger re-derive the correct
/// store key from a record alone, and lets the replay loop dispatch to the
/// right executor. Defaults to `Document` so records written before the
/// collection-commit keyspace existed still decode (defradb#1113).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum RetryScope {
    #[default]
    Document,
    CollectionCommit,
}

/// Durable newest-head state for one `(peer, document, CID)` pair — or, for a
/// collection commit, one `(peer, collection, CID)` triple. It is either a
/// dormant ordering watermark for an active live send or a pending retry.
/// `doc_id` is encoded with the value as a self-contained migration-safe record
/// even though it is also present in the store key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedPushRetry {
    pub doc_id: String,
    pub collection_id: String,
    pub cid: String,
    pub priority: u64,
    /// Whether this head is eligible for the retry loop. A newer live enqueue
    /// stores a dormant watermark first so an older in-flight failure cannot
    /// recreate stale retry work or race the active send.
    #[serde(default = "default_pending")]
    pub pending: bool,
    /// Document head vs collection commit. `#[serde(default)]` keeps records
    /// written before this field existed decoding as `Document`.
    #[serde(default)]
    pub scope: RetryScope,
    pub retry_info: RetryInfo,
}

impl PersistedPushRetry {
    /// Whether this record is a doc-less collection-commit obligation.
    pub fn is_collection_commit(&self) -> bool {
        matches!(self.scope, RetryScope::CollectionCommit)
    }

    pub fn new_observed(
        doc_id: impl Into<String>,
        collection_id: impl Into<String>,
        cid: impl Into<String>,
        priority: u64,
    ) -> Self {
        Self {
            doc_id: doc_id.into(),
            collection_id: collection_id.into(),
            cid: cid.into(),
            priority,
            pending: false,
            scope: RetryScope::Document,
            retry_info: RetryInfo::new_initial(),
        }
    }

    /// Dormant watermark for a doc-less collection-commit push.
    pub fn new_observed_commit(
        collection_id: impl Into<String>,
        cid: impl Into<String>,
        priority: u64,
    ) -> Self {
        Self {
            doc_id: String::new(),
            collection_id: collection_id.into(),
            cid: cid.into(),
            priority,
            pending: false,
            scope: RetryScope::CollectionCommit,
            retry_info: RetryInfo::new_initial(),
        }
    }

    /// Activate a live-send failure with the first 15–30 second jittered
    /// interval. The in-memory backlog already made the immediate attempt;
    /// delaying durable fanout prevents failed peers retrying in lockstep.
    pub fn activate(&mut self, retry_key: &str) {
        self.pending = true;
        self.retry_info = RetryInfo::new_initial();
        self.retry_info.bump_for(retry_key);
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        serde_cbor::to_vec(self)
            .map_err(|error| format!("failed to serialize persisted push retry: {error}"))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        serde_cbor::from_slice(bytes)
            .map_err(|error| format!("failed to deserialize persisted push retry: {error}"))
    }
}

fn default_pending() -> bool {
    true
}

impl RetryInfo {
    /// Create retry state that is due immediately.
    ///
    /// Restart promotion uses this directly because the volatile live send
    /// no longer exists. Fresh live failures call `PersistedPushRetry::activate`
    /// and advance to the first jittered interval instead.
    pub fn new_initial() -> Self {
        Self {
            num_retries: 0,
            next_retry_unix: 0,
        }
    }

    /// Returns true if a retry is due (current time >= next_retry_unix).
    pub fn is_due(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.next_retry_unix
    }

    /// Bump the retry counter and schedule the next retry with exponential backoff.
    pub fn bump(&mut self) {
        self.bump_for("");
    }

    /// Advance with deterministic bounded jitter derived from the exact
    /// `(peer, CID)` key and attempt. This preserves the exponential cap while
    /// preventing a fan-out failure from retrying every peer in lockstep.
    pub fn bump_for(&mut self, retry_key: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let idx = (self.num_retries as usize).min(RETRY_INTERVALS_SECS.len() - 1);
        let cap = RETRY_INTERVALS_SECS[idx];
        let floor = (cap / 2).max(1);
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        retry_key.hash(&mut hasher);
        self.num_retries.hash(&mut hasher);
        let delay = floor + hasher.finish() % (cap - floor + 1);
        self.next_retry_unix = now + delay;
        self.num_retries += 1;
    }

    /// Schedule another attempt without recording a delivery failure.
    pub fn defer_for(&mut self, delay: Duration) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.next_retry_unix = now.saturating_add(delay.as_secs());
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        serde_cbor::to_vec(self).map_err(|e| format!("failed to serialize RetryInfo: {}", e))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        serde_cbor::from_slice(bytes).map_err(|e| format!("failed to deserialize RetryInfo: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_initial_is_due() {
        let info = RetryInfo::new_initial();
        assert!(info.is_due());
        assert_eq!(info.num_retries, 0);
    }

    #[test]
    fn test_bump_advances_retry() {
        let mut info = RetryInfo::new_initial();
        info.bump();
        assert_eq!(info.num_retries, 1);
        assert!(!info.is_due());
    }

    #[test]
    fn test_retry_intervals_reach_hours_scale_cap() {
        assert_eq!(
            RETRY_INTERVALS_SECS,
            &[30, 60, 120, 240, 480, 960, 1920, 3600, 7200, 14400, 28800, 43200]
        );
    }

    #[test]
    fn test_roundtrip() {
        let info = RetryInfo::new_initial();
        let bytes = info.to_bytes().unwrap();
        let restored = RetryInfo::from_bytes(&bytes).unwrap();
        assert_eq!(restored.num_retries, info.num_retries);
        assert_eq!(restored.next_retry_unix, info.next_retry_unix);
    }

    #[test]
    fn persisted_retry_without_pending_field_defaults_to_pending() {
        #[derive(serde::Serialize)]
        struct LegacyPersistedPushRetry<'a> {
            doc_id: &'a str,
            collection_id: &'a str,
            cid: &'a str,
            priority: u64,
            retry_info: RetryInfo,
        }

        let bytes = serde_cbor::to_vec(&LegacyPersistedPushRetry {
            doc_id: "doc",
            collection_id: "collection",
            cid: "cid",
            priority: 1,
            retry_info: RetryInfo::new_initial(),
        })
        .unwrap();
        let restored = PersistedPushRetry::from_bytes(&bytes).unwrap();

        assert!(restored.pending);
        assert_eq!(restored.doc_id, "doc");
    }
}
