use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use p2p::message::PushLogReply;
use p2p::transport::PeerId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Maximum concurrent per-document push tasks during initial replay.
///
/// Lower than the coordinator's live push limit (32) because initial replay
/// is background work that shouldn't starve real-time sync traffic.
pub(crate) const MAX_CONCURRENT_REPLAY_TASKS: usize = 8;

/// Maximum concurrent outbound PushLog requests across replay tasks.
pub const DEFAULT_MAX_CONCURRENT_REPLAY_SENDS: usize = 8;

/// Default per-peer replay burst. Mirrors the sync manager's live rate limiter.
pub const DEFAULT_REPLAY_RATE_LIMIT_BURST: u32 = p2p::sync::DEFAULT_RATE_LIMIT_BURST;

/// Default per-peer replay refill rate. Mirrors the sync manager's live rate limiter.
pub const DEFAULT_REPLAY_RATE_LIMIT_RATE: f64 = p2p::sync::DEFAULT_RATE_LIMIT_RATE;

/// Default timeout for a single replay PushLog request.
pub const DEFAULT_REPLAY_SEND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct ReplayPushConfig {
    /// Maximum number of documents whose DAG blocks may be replayed concurrently.
    pub max_concurrent_document_tasks: usize,

    /// Maximum number of outbound PushLog requests in flight across all replay tasks.
    pub max_concurrent_outbound_pushes: usize,

    /// Number of per-peer replay tokens available for short bursts.
    pub per_peer_rate_limit_burst: u32,

    /// Number of per-peer replay tokens refilled per second.
    pub per_peer_rate_limit_rate: f64,

    /// Timeout for one outbound replay PushLog request.
    pub send_timeout: Duration,
}

impl Default for ReplayPushConfig {
    fn default() -> Self {
        Self {
            max_concurrent_document_tasks: MAX_CONCURRENT_REPLAY_TASKS,
            max_concurrent_outbound_pushes: DEFAULT_MAX_CONCURRENT_REPLAY_SENDS,
            per_peer_rate_limit_burst: DEFAULT_REPLAY_RATE_LIMIT_BURST,
            per_peer_rate_limit_rate: DEFAULT_REPLAY_RATE_LIMIT_RATE,
            send_timeout: DEFAULT_REPLAY_SEND_TIMEOUT,
        }
    }
}

#[derive(Debug)]
pub(crate) enum ReplayPushSendError {
    SemaphoreClosed,
    Timeout { timeout: Duration },
    Transport(p2p::Error),
}

impl ReplayPushSendError {
    pub(crate) fn is_connection_like(&self) -> bool {
        matches!(self, Self::Transport(error) if error.is_connection_like())
    }
}

impl fmt::Display for ReplayPushSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SemaphoreClosed => f.write_str("replay send semaphore closed"),
            Self::Timeout { timeout } => {
                write!(f, "replay PushLog timed out after {}s", timeout.as_secs())
            }
            Self::Transport(error) => write!(f, "{error}"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReplayPushGate {
    document_task_semaphore: Arc<Semaphore>,
    outbound_push_semaphore: Arc<Semaphore>,
    peer_pacer: ReplayPeerPacer,
    rate_retry_delay: Duration,
    send_timeout: Duration,
}

impl ReplayPushGate {
    pub(crate) fn new(config: ReplayPushConfig) -> Self {
        let rate = if config.per_peer_rate_limit_rate.is_finite()
            && config.per_peer_rate_limit_rate > 0.0
        {
            config.per_peer_rate_limit_rate
        } else {
            DEFAULT_REPLAY_RATE_LIMIT_RATE
        };

        Self {
            document_task_semaphore: Arc::new(Semaphore::new(
                config.max_concurrent_document_tasks.max(1),
            )),
            outbound_push_semaphore: Arc::new(Semaphore::new(
                config.max_concurrent_outbound_pushes.max(1),
            )),
            peer_pacer: ReplayPeerPacer::new(config.per_peer_rate_limit_burst.max(1), rate),
            rate_retry_delay: Duration::from_secs_f64((1.0 / rate).clamp(0.001, 1.0)),
            send_timeout: config.send_timeout.max(Duration::from_millis(1)),
        }
    }

    pub(crate) async fn acquire_document_task(
        &self,
    ) -> Result<OwnedSemaphorePermit, ReplayPushSendError> {
        self.document_task_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ReplayPushSendError::SemaphoreClosed)
    }

    pub(crate) async fn send_pushlog<F>(
        &self,
        peer_id: &PeerId,
        send: F,
    ) -> Result<PushLogReply, ReplayPushSendError>
    where
        F: Future<Output = p2p::Result<PushLogReply>>,
    {
        while !self.peer_pacer.try_consume(peer_id.as_str()) {
            tokio::time::sleep(self.rate_retry_delay).await;
        }

        let _permit = self
            .outbound_push_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ReplayPushSendError::SemaphoreClosed)?;

        tokio::time::timeout(self.send_timeout, send)
            .await
            .map_err(|_| ReplayPushSendError::Timeout {
                timeout: self.send_timeout,
            })?
            .map_err(ReplayPushSendError::Transport)
    }
}

#[derive(Debug)]
struct ReplayPeerPacer {
    buckets: parking_lot::Mutex<HashMap<String, ReplayPeerBucket>>,
    capacity: u32,
    refill_rate: f64,
}

impl ReplayPeerPacer {
    fn new(capacity: u32, refill_rate: f64) -> Self {
        Self {
            buckets: parking_lot::Mutex::new(HashMap::new()),
            capacity,
            refill_rate,
        }
    }

    fn try_consume(&self, peer_id: &str) -> bool {
        let mut buckets = self.buckets.lock();
        buckets
            .entry(peer_id.to_string())
            .or_insert_with(|| ReplayPeerBucket::new(self.capacity))
            .try_consume(self.capacity, self.refill_rate)
    }
}

#[derive(Debug)]
struct ReplayPeerBucket {
    tokens: f64,
    last_refill: Instant,
}

impl ReplayPeerBucket {
    fn new(capacity: u32) -> Self {
        Self {
            tokens: capacity as f64,
            last_refill: Instant::now(),
        }
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn replay_push_gate_caps_concurrent_sends() {
        let gate = Arc::new(ReplayPushGate::new(ReplayPushConfig {
            max_concurrent_document_tasks: 8,
            max_concurrent_outbound_pushes: 2,
            per_peer_rate_limit_burst: 100,
            per_peer_rate_limit_rate: 100.0,
            send_timeout: Duration::from_secs(1),
        }));
        let current = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let peer = PeerId::new("peer-1".to_string());

        let mut handles = Vec::new();
        for _ in 0..8 {
            let gate = gate.clone();
            let current = current.clone();
            let max_seen = max_seen.clone();
            let peer = peer.clone();
            handles.push(tokio::spawn(async move {
                gate.send_pushlog(&peer, async move {
                    let active = current.fetch_add(1, Ordering::SeqCst) + 1;
                    record_max(&max_seen, active);
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    current.fetch_sub(1, Ordering::SeqCst);
                    Ok(PushLogReply::success("message"))
                })
                .await
                .unwrap();
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(max_seen.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn replay_push_gate_paces_after_peer_burst() {
        let gate = ReplayPushGate::new(ReplayPushConfig {
            max_concurrent_document_tasks: 1,
            max_concurrent_outbound_pushes: 1,
            per_peer_rate_limit_burst: 1,
            per_peer_rate_limit_rate: 10.0,
            send_timeout: Duration::from_secs(1),
        });
        let peer = PeerId::new("peer-1".to_string());

        let start = Instant::now();
        for _ in 0..3 {
            gate.send_pushlog(&peer, async { Ok(PushLogReply::success("message")) })
                .await
                .unwrap();
        }

        assert!(start.elapsed() >= Duration::from_millis(150));
    }

    fn record_max(max_seen: &AtomicUsize, value: usize) {
        let mut observed = max_seen.load(Ordering::SeqCst);
        while value > observed {
            match max_seen.compare_exchange(observed, value, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return,
                Err(current) => observed = current,
            }
        }
    }
}
