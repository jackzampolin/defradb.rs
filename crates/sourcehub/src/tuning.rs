use std::time::Duration;

/// Tuning parameters for ACP network calls, caching, and circuit breaking.
pub struct AcpTuning {
    pub request_timeout: Duration,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_reset_timeout: Duration,
    pub cache_ttl: Duration,
    pub receipt_timeout: Duration,
}

impl Default for AcpTuning {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(5),
            circuit_breaker_threshold: 3,
            circuit_breaker_reset_timeout: Duration::from_secs(30),
            cache_ttl: Duration::from_secs(300),
            receipt_timeout: Duration::from_secs(30),
        }
    }
}
