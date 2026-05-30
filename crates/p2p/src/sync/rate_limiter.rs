//! Per-peer token-bucket rate limiter for P2P event dispatch.
//!
//! Limits the rate at which any single peer can drive expensive coordinator
//! operations (DocSync requests, PushLog broadcasts, etc.).  Each peer starts
//! with a full bucket of tokens; one token is consumed per allowed event.
//! Tokens refill at a constant rate up to the bucket capacity.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::transport::PeerId;

use super::manager::{
    default_rate_limit_backoff, DEFAULT_RATE_LIMIT_BURST, DEFAULT_RATE_LIMIT_RATE,
};

/// Maximum number of peer buckets to retain.
///
/// Disconnected peers that have not generated traffic for a while are evicted
/// lazily on the next insertion when this limit is hit.
const MAX_TRACKED_PEERS: usize = 10_000;

/// A single token-bucket for one peer.
#[derive(Debug)]
struct Bucket {
    /// Current token count (may be fractional internally, stored as f64).
    tokens: f64,
    /// When tokens were last refilled.
    last_refill: Instant,
    /// Consecutive rate-limit refusals since the last allowed event.
    consecutive_failures: u32,
    /// Earliest time this peer should be retried after a refusal.
    next_retry_after: Option<Instant>,
}

impl Bucket {
    fn new(capacity: u32) -> Self {
        Self {
            tokens: capacity as f64,
            last_refill: Instant::now(),
            consecutive_failures: 0,
            next_retry_after: None,
        }
    }

    /// Refill tokens based on elapsed time and return whether one token is
    /// available to consume.
    fn try_consume(
        &mut self,
        now: Instant,
        capacity: u32,
        refill_rate: f64,
        backoff_steps: &[Duration],
    ) -> RateLimitDecision {
        if let Some(next_retry_after) = self.next_retry_after {
            if next_retry_after > now {
                return RateLimitDecision::Limited {
                    retry_after: next_retry_after.duration_since(now),
                    consecutive_failures: self.consecutive_failures,
                };
            }
            self.next_retry_after = None;
        }

        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * refill_rate).min(capacity as f64);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            self.consecutive_failures = 0;
            RateLimitDecision::Allowed
        } else {
            self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            let retry_after = backoff_for_failure(backoff_steps, self.consecutive_failures);
            self.next_retry_after = Some(now + retry_after);
            RateLimitDecision::Limited {
                retry_after,
                consecutive_failures: self.consecutive_failures,
            }
        }
    }
}

fn backoff_for_failure(backoff_steps: &[Duration], consecutive_failures: u32) -> Duration {
    let index = consecutive_failures.saturating_sub(1) as usize;
    backoff_steps
        .get(index)
        .or_else(|| backoff_steps.last())
        .copied()
        .unwrap_or_else(|| Duration::from_secs(1))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RateLimitDecision {
    Allowed,
    Limited {
        retry_after: Duration,
        consecutive_failures: u32,
    },
}

/// Per-peer rate limiter backed by token buckets.
///
/// Thread-safe via an internal `Mutex`; designed to be held behind an `Arc`
/// and shared across event-handler invocations.
///
/// Uses string-based peer IDs to support both libp2p and iroh transports.
pub struct PeerRateLimiter {
    buckets: Mutex<HashMap<String, Bucket>>,
    capacity: u32,
    refill_rate: f64,
    backoff_steps: Vec<Duration>,
}

impl Default for PeerRateLimiter {
    fn default() -> Self {
        Self::new(DEFAULT_RATE_LIMIT_BURST, DEFAULT_RATE_LIMIT_RATE)
    }
}

impl PeerRateLimiter {
    /// Create a new limiter with the given capacity and refill rate.
    ///
    /// * `capacity`    – Maximum tokens per peer (burst size).
    /// * `refill_rate` – Tokens added per second per peer.
    pub fn new(capacity: u32, refill_rate: f64) -> Self {
        Self::with_backoff_steps(capacity, refill_rate, default_rate_limit_backoff())
    }

    /// Create a new limiter with explicit rate-limit backoff steps.
    pub fn with_backoff_steps(
        capacity: u32,
        refill_rate: f64,
        backoff_steps: Vec<Duration>,
    ) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            capacity,
            refill_rate,
            backoff_steps,
        }
    }

    /// Attempt to consume one token for `peer`.
    ///
    /// Returns backoff metadata when the peer is rate-limited.
    pub(crate) fn check(&self, peer: &PeerId) -> RateLimitDecision {
        let key = peer.as_str().to_string();
        let mut buckets = self.buckets.lock();

        // Lazy eviction when the map is too large.
        if buckets.len() >= MAX_TRACKED_PEERS && !buckets.contains_key(&key) {
            // Remove the entry with the oldest last_refill time.
            if let Some(oldest) = buckets
                .iter()
                .min_by_key(|(_, b)| b.last_refill)
                .map(|(k, _)| k.clone())
            {
                buckets.remove(&oldest);
            }
        }

        let capacity = self.capacity;
        let refill_rate = self.refill_rate;
        let now = Instant::now();
        buckets
            .entry(key)
            .or_insert_with(|| Bucket::new(capacity))
            .try_consume(now, capacity, refill_rate, &self.backoff_steps)
    }

    /// Discard the bucket for `peer` (called on disconnect to free memory).
    pub fn remove_peer(&self, peer: &PeerId) {
        self.buckets.lock().remove(peer.as_str());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limited(decision: RateLimitDecision) -> (Duration, u32) {
        match decision {
            RateLimitDecision::Allowed => panic!("expected rate-limited decision"),
            RateLimitDecision::Limited {
                retry_after,
                consecutive_failures,
            } => (retry_after, consecutive_failures),
        }
    }

    #[test]
    fn peer_backoff_grows_after_repeated_rate_limit_windows() {
        let limiter = PeerRateLimiter::with_backoff_steps(
            0,
            0.0,
            vec![
                Duration::from_millis(1),
                Duration::from_millis(2),
                Duration::from_millis(4),
            ],
        );
        let peer = PeerId::new("peer-1".to_string());

        let (_, failures) = limited(limiter.check(&peer));
        assert_eq!(failures, 1);

        std::thread::sleep(Duration::from_millis(2));
        let (_, failures) = limited(limiter.check(&peer));
        assert_eq!(failures, 2);

        std::thread::sleep(Duration::from_millis(3));
        let (_, failures) = limited(limiter.check(&peer));
        assert_eq!(failures, 3);
    }

    #[test]
    fn peer_backoff_resets_after_allowed_request() {
        let limiter = PeerRateLimiter::with_backoff_steps(
            1,
            1_000.0,
            vec![Duration::from_millis(1), Duration::from_millis(2)],
        );
        let peer = PeerId::new("peer-1".to_string());

        assert_eq!(limiter.check(&peer), RateLimitDecision::Allowed);
        let (_, failures) = limited(limiter.check(&peer));
        assert_eq!(failures, 1);

        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(limiter.check(&peer), RateLimitDecision::Allowed);
        let (_, failures) = limited(limiter.check(&peer));
        assert_eq!(failures, 1);
    }
}
