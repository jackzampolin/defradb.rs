/// Retry information for failed replicator pushes.
///
/// Tracks the number of retries and the next retry time using exponential
/// backoff intervals matching Go DefraDB's replicator retry behavior.
use std::time::{SystemTime, UNIX_EPOCH};

/// Exponential backoff intervals in seconds, matching Go's seconds-to-hours retry ladder.
pub const RETRY_INTERVALS_SECS: &[u64] = &[
    30, 60, 120, 240, 480, 960, 1920, 3600, 7200, 14400, 28800, 43200,
];

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetryInfo {
    pub num_retries: u32,
    pub next_retry_unix: u64,
}

impl RetryInfo {
    /// Create a new RetryInfo with the first retry scheduled immediately.
    ///
    /// The first retry fires on the next 2-second tick rather than waiting
    /// the full 30s, so temporarily-offline peers recover quickly.
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
        self.next_retry_unix = now + RETRY_INTERVALS_SECS[idx];
        self.num_retries += 1;
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
}
