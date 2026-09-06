use std::borrow::Cow;
/// Retry information for failed replicator pushes.
///
/// Tracks the number of retries and the next retry time using exponential
/// backoff intervals matching Go DefraDB's replicator retry behavior.
use std::time::Duration;
use std::{hash::Hash, hash::Hasher};
use web_time::{SystemTime, UNIX_EPOCH};

/// Go-compatible document retry ladder: 30s through a 32-minute cap.
///
/// Source parity: Go `cli/config/config.go`, default
/// `replicator.retryintervals`.
pub const RETRY_INTERVALS_SECS: &[u64] = &[30, 60, 120, 240, 480, 960, 1920];

/// Non-empty retry intervals in seconds. The final interval is the retry cap.
#[derive(Debug, Clone)]
pub struct RetrySchedule(Cow<'static, [u64]>);

impl RetrySchedule {
    pub fn new(intervals: Vec<u64>) -> Result<Self, String> {
        if intervals.is_empty() || intervals.contains(&0) {
            return Err(
                "replicator retry intervals must be a non-empty list of positive integers".into(),
            );
        }
        Ok(Self(Cow::Owned(intervals)))
    }

    fn cap(&self, attempt: u32) -> u64 {
        self.0[(attempt as usize).min(self.0.len() - 1)]
    }
}

impl Default for RetrySchedule {
    fn default() -> Self {
        Self(Cow::Borrowed(RETRY_INTERVALS_SECS))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryInfo {
    pub num_retries: u32,
    pub next_retry_unix: u64,
    /// Durable round-robin start for the peer's presence-only scope markers.
    ///
    /// All scopes share this peer clock.  Persisting the cursor prevents a
    /// bounded consumer from retrying the same failing lexical prefix after
    /// every sweep or process restart.
    #[serde(default)]
    pub dispatch_cursor: u64,
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

/// Presence-only retry scope returned to the sender retry sweep.
///
/// Durable marker values remain empty. The peer-scoped `RetryInfo` schedule is
/// joined onto each marker only while dispatching a sweep.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PushRetryMarker {
    pub doc_id: String,
    pub collection_id: String,
    pub scope: RetryScope,
    pub retry_info: RetryInfo,
}

impl PushRetryMarker {
    /// Whether this record is a doc-less collection-commit obligation.
    pub fn is_collection_commit(&self) -> bool {
        matches!(self.scope, RetryScope::CollectionCommit)
    }
}

/// Pre-stage-3 payload record, decoded only while migrating old stores.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LegacyPersistedPushRetry {
    doc_id: String,
    collection_id: String,
    cid: String,
    priority: u64,
    #[serde(default = "default_pending")]
    pending: bool,
    #[serde(default)]
    scope: RetryScope,
    retry_info: RetryInfo,
}

/// Rewrite an embedded document ID in a legacy retry payload during the
/// document-short-ID migration. Marker records have empty values and return
/// `Ok(None)` without decoding.
pub fn rewrite_legacy_push_retry_doc_id(
    bytes: &[u8],
    old_doc_id: &str,
    canonical_doc_id: &str,
) -> Result<Option<Vec<u8>>, String> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let mut retry: LegacyPersistedPushRetry = defra_core::cbor::from_slice(bytes)
        .map_err(|error| format!("failed to deserialize legacy push retry: {error}"))?;
    if retry.doc_id != old_doc_id {
        return Ok(None);
    }
    retry.doc_id = canonical_doc_id.to_string();
    defra_core::cbor::to_vec(&retry)
        .map(Some)
        .map_err(|error| format!("failed to serialize legacy push retry: {error}"))
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
            dispatch_cursor: 0,
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

    /// Advance with deterministic bounded jitter derived from the peer and
    /// retry rung.  The published Go-compatible ladder remains the cap while
    /// peers do not wake in lockstep after a fleet-wide outage.
    pub fn bump_for(&mut self, retry_key: &str) {
        self.bump_with_schedule(retry_key, &RetrySchedule::default());
    }

    /// Advance using the configured ladder, preserving peer-scoped jitter.
    pub fn bump_with_schedule(&mut self, retry_key: &str, schedule: &RetrySchedule) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let cap = schedule.cap(self.num_retries);
        let floor = (cap / 2).max(1);
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        retry_key.hash(&mut hasher);
        self.num_retries.hash(&mut hasher);
        let delay = floor + hasher.finish() % (cap - floor + 1);
        self.next_retry_unix = now.saturating_add(delay);
        self.num_retries = self.num_retries.saturating_add(1);
    }

    /// Move the next bounded pass to a different lexical marker prefix.
    pub fn advance_dispatch_cursor(&mut self) {
        self.dispatch_cursor = self.dispatch_cursor.wrapping_add(1);
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

    #[derive(serde::Serialize)]
    struct LegacyRetryInfo {
        num_retries: u32,
        next_retry_unix: u64,
    }

    #[test]
    fn test_new_initial_is_due() {
        let info = RetryInfo::new_initial();
        assert!(info.is_due());
        assert_eq!(info.num_retries, 0);
    }

    #[test]
    fn legacy_retry_info_decodes_with_zero_dispatch_cursor() {
        let bytes = defra_core::cbor::to_vec(&LegacyRetryInfo {
            num_retries: 4,
            next_retry_unix: 42,
        })
        .unwrap();
        let decoded = RetryInfo::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.num_retries, 4);
        assert_eq!(decoded.next_retry_unix, 42);
        assert_eq!(decoded.dispatch_cursor, 0);
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
        struct LegacyRetryFixture<'a> {
            doc_id: &'a str,
            collection_id: &'a str,
            cid: &'a str,
            priority: u64,
            retry_info: RetryInfo,
        }

        let bytes = defra_core::cbor::to_vec(&LegacyRetryFixture {
            doc_id: "doc",
            collection_id: "collection",
            cid: "cid",
            priority: 1,
            retry_info: RetryInfo::new_initial(),
        })
        .unwrap();
        let rewritten = rewrite_legacy_push_retry_doc_id(&bytes, "doc", "canonical")
            .unwrap()
            .unwrap();
        let restored: LegacyPersistedPushRetry = defra_core::cbor::from_slice(&rewritten).unwrap();

        assert!(restored.pending);
        assert_eq!(restored.doc_id, "canonical");
    }
}
