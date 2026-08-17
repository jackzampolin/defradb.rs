/// Retry information for failed replicator pushes.
///
/// Tracks the number of retries and the next retry time using exponential
/// backoff intervals matching Go DefraDB's replicator retry behavior.
use std::time::Duration;
use web_time::{SystemTime, UNIX_EPOCH};

/// Go-compatible document retry ladder: 30s through a 32-minute cap.
pub const RETRY_INTERVALS_SECS: &[u64] = &[30, 60, 120, 240, 480, 960, 1920];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryInfo {
    pub num_retries: u32,
    pub next_retry_unix: u64,
}

/// Durable sender obligations surfaced through P2P sync diagnostics.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PushRetryMarkerStats {
    pub document_markers: usize,
    pub collection_markers: usize,
    pub scheduled_peers: usize,
    pub oldest_scheduled_retry_unix: Option<u64>,
}

/// What kind of push obligation a retry record represents.
///
/// Document heads replay by document id; collection commits replay by
/// collection id. Defaults to `Document` so legacy payload records decode for
/// one-way marker migration.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Default,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum RetryScope {
    #[default]
    Document,
    CollectionCommit,
}

/// Legacy payload record and transient retry-sweep view.
///
/// New durable state stores empty scope markers, never this CID-bearing value.
/// The fields remain decodable solely for one-way migration and to avoid
/// duplicating the sweep dispatch shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedPushRetry {
    pub doc_id: String,
    pub collection_id: String,
    pub cid: String,
    pub priority: u64,
    /// Legacy eligibility flag; synthesized marker views are always pending.
    #[serde(default = "default_pending")]
    pub pending: bool,
    /// Document vs collection scope. `#[serde(default)]` keeps old records
    /// decoding as `Document` for migration.
    #[serde(default)]
    pub scope: RetryScope,
    pub retry_info: RetryInfo,
}

impl PersistedPushRetry {
    /// Whether this record is a doc-less collection-commit obligation.
    pub fn is_collection_commit(&self) -> bool {
        matches!(self.scope, RetryScope::CollectionCommit)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        defra_core::cbor::to_vec(self)
            .map_err(|error| format!("failed to serialize persisted push retry: {error}"))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        defra_core::cbor::from_slice(bytes)
            .map_err(|error| format!("failed to deserialize persisted push retry: {error}"))
    }
}

fn default_pending() -> bool {
    true
}

impl RetryInfo {
    /// Create retry state that is due immediately.
    ///
    /// Fresh scope registration advances the peer clock to the first interval.
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

    /// Advance on the Go-compatible document retry ladder. The schedule is
    /// peer-scoped, so all dirty scope markers share this one redrive clock.
    pub fn bump_for(&mut self, retry_key: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let idx = (self.num_retries as usize).min(RETRY_INTERVALS_SECS.len() - 1);
        let cap = RETRY_INTERVALS_SECS[idx];
        let _ = retry_key;
        self.next_retry_unix = now + cap;
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
        defra_core::cbor::to_vec(self).map_err(|e| format!("failed to serialize RetryInfo: {}", e))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        defra_core::cbor::from_slice(bytes)
            .map_err(|e| format!("failed to deserialize RetryInfo: {}", e))
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
    fn test_retry_intervals_match_go_document_ladder() {
        assert_eq!(RETRY_INTERVALS_SECS, &[30, 60, 120, 240, 480, 960, 1920]);
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

        let bytes = defra_core::cbor::to_vec(&LegacyPersistedPushRetry {
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
