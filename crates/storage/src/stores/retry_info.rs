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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let idx = (self.num_retries as usize).min(RETRY_INTERVALS_SECS.len() - 1);
        let cap = RETRY_INTERVALS_SECS[idx];
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
