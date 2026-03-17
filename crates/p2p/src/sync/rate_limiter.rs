//! Per-peer token-bucket rate limiter for P2P event dispatch.
//!
//! Limits the rate at which any single peer can drive expensive coordinator
//! operations (DocSync requests, PushLog broadcasts, etc.).  Each peer starts
//! with a full bucket of tokens; one token is consumed per allowed event.
//! Tokens refill at a constant rate up to the bucket capacity.

use std::collections::HashMap;
use std::time::Instant;

use parking_lot::Mutex;

use crate::transport::PeerId;

/// Default bucket capacity (burst allowance).
///
/// P2P sync is naturally bursty — a peer reconnecting after a restart will
/// send many TwoStreamRequests in rapid succession to catch up.  The burst
/// must be large enough to absorb that initial flood.
const DEFAULT_CAPACITY: u32 = 500;

/// Default refill rate: tokens per second.
///
/// Sustained request rate allowed per peer.  50/s is generous enough for
/// normal sync traffic while still capping a misbehaving peer.
const DEFAULT_REFILL_RATE: f64 = 50.0;

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
}

impl Bucket {
    fn new(capacity: u32) -> Self {
        Self {
            tokens: capacity as f64,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time and return whether one token is
    /// available to consume.
    fn try_consume(&mut self, capacity: u32, refill_rate: f64) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * refill_rate).min(capacity as f64);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
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
}

impl Default for PeerRateLimiter {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY, DEFAULT_REFILL_RATE)
    }
}

impl PeerRateLimiter {
    /// Create a new limiter with the given capacity and refill rate.
    ///
    /// * `capacity`    – Maximum tokens per peer (burst size).
    /// * `refill_rate` – Tokens added per second per peer.
    pub fn new(capacity: u32, refill_rate: f64) -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            capacity,
            refill_rate,
        }
    }

    /// Attempt to consume one token for `peer`.
    ///
    /// Returns `true` if the event is allowed, `false` if the peer is rate-limited.
    pub fn check(&self, peer: &PeerId) -> bool {
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
        buckets
            .entry(key)
            .or_insert_with(|| Bucket::new(capacity))
            .try_consume(capacity, refill_rate)
    }

    /// Discard the bucket for `peer` (called on disconnect to free memory).
    pub fn remove_peer(&self, peer: &PeerId) {
        self.buckets.lock().remove(peer.as_str());
    }
}
