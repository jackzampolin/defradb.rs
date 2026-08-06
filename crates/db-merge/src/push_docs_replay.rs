use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
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

/// Retries to make immediately before durable replay takes ownership.
const MAX_RATE_LIMITED_RETRIES: u32 = 6;
/// Ceiling on the per-attempt rate-limit backoff.
const RATE_LIMIT_RETRY_BACKOFF_CAP: Duration = Duration::from_secs(5);

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplayDocumentFailure {
    pub(crate) doc_id: String,
    pub(crate) collection_id: String,
}

/// Persist documents that did not finish their initial replay so the normal
/// jittered retry sweep owns them after the bounded in-memory attempt.
pub(crate) async fn persist_replay_failures<S: storage::corekv::Store>(
    store: Arc<S>,
    peer_id: &PeerId,
    failures: &[ReplayDocumentFailure],
) -> Result<(), String> {
    if failures.is_empty() {
        return Ok(());
    }

    let peerstore = storage::stores::Peerstore::new(store);
    let Some(_retry_guard) = peerstore
        .acquire_replicator_retry_guard(peer_id.as_str())
        .await
        .map_err(|error| format!("failed to coordinate replay failure recording: {error}"))?
    else {
        tracing::debug!(
            peer_id = %peer_id,
            failure_count = failures.len(),
            "Replicator was removed before replay failures could be recorded"
        );
        return Ok(());
    };
    let retry_info = storage::stores::RetryInfo::new_initial()
        .to_bytes()
        .map_err(|error| format!("failed to serialize replay retry state: {error}"))?;

    for failure in failures {
        // Initial replay resolves the current document heads again on retry, so
        // this is deliberately versionless. If a live-send watermark exists,
        // record_push_failure activates that exact newer version instead.
        peerstore
            .record_push_failure(
                peer_id.as_str(),
                &failure.doc_id,
                &failure.collection_id,
                "",
                0,
                &retry_info,
            )
            .await
            .map_err(|error| {
                format!(
                    "failed to persist replay retry for document {}: {error}",
                    failure.doc_id
                )
            })?;
    }

    // Match the live-push failure recorder's observable state. Retry records
    // remain authoritative even if legacy/invalid replicator bytes prevent the
    // status update.
    if let Some(bytes) = peerstore
        .get_replicator(peer_id.as_str())
        .await
        .map_err(|error| format!("failed to load persisted replicator status: {error}"))?
    {
        match p2p::ReplicatorInfo::from_bytes(&bytes) {
            Ok(mut info) => {
                if info.set_status_if_changed_now(p2p::ReplicatorStatus::Inactive) {
                    let bytes = info.to_bytes().map_err(|error| {
                        format!("failed to encode inactive replicator status: {error}")
                    })?;
                    peerstore
                        .create_replicator(peer_id.as_str(), &bytes)
                        .await
                        .map_err(|error| {
                            format!("failed to persist inactive replicator status: {error}")
                        })?;
                }
            }
            Err(error) => {
                tracing::warn!(
                    peer_id = %peer_id,
                    %error,
                    "Replay retries were recorded but replicator status could not be decoded"
                );
            }
        }
    }

    tracing::warn!(
        peer_id = %peer_id,
        failure_count = failures.len(),
        "Initial replay left unfinished documents; deferred them to persisted retry"
    );
    Ok(())
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
        while let Some(delay) = self.peer_pacer.consume_or_delay(peer_id.as_str()) {
            tokio::time::sleep(delay).await;
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

    /// `send_pushlog`, but a `RATE_LIMITED_MESSAGE` nack is retried with
    /// exponential backoff instead of being surfaced to the caller.
    ///
    /// The peer's admission control says, verbatim, "retry later" — the
    /// replay paths previously treated that nack like a hard rejection. Only
    /// the rate-limit sentinel is retried: hard rejections and transport errors
    /// surface unchanged on the first occurrence. A nack also pauses and drains
    /// the shared peer bucket, so concurrent document tasks cannot keep sending
    /// through the receiver's peer-wide cooldown. After the bounded immediate
    /// attempts, callers hand the document to persisted retry.
    pub(crate) async fn send_pushlog_with_rate_limit_retry<F, Fut>(
        &self,
        peer_id: &PeerId,
        mut make_send: F,
    ) -> Result<PushLogReply, ReplayPushSendError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = p2p::Result<PushLogReply>>,
    {
        let mut attempt: u32 = 0;
        loop {
            let reply = self.send_pushlog(peer_id, make_send()).await?;
            let rate_limited = reply
                .err_message
                .as_deref()
                .is_some_and(p2p::error::is_rate_limited_message);
            if !rate_limited || attempt >= MAX_RATE_LIMITED_RETRIES {
                return Ok(reply);
            }
            attempt += 1;
            let backoff = self.rate_limited_backoff(peer_id, attempt);
            self.peer_pacer.defer(peer_id.as_str(), backoff);
            tracing::debug!(
                peer_id = %peer_id,
                attempt,
                backoff_ms = backoff.as_millis() as u64,
                "Replay push rate-limited by peer; pausing peer sends before retry"
            );
        }
    }

    fn rate_limited_backoff(&self, peer_id: &PeerId, attempt: u32) -> Duration {
        let base = self
            .rate_retry_delay
            .saturating_mul(1u32 << attempt.min(MAX_RATE_LIMITED_RETRIES))
            .min(RATE_LIMIT_RETRY_BACKOFF_CAP);
        let jitter_window = base / 4;
        let jitter_micros = jitter_window.as_micros().min(u64::MAX as u128) as u64;
        if jitter_micros == 0 || base >= RATE_LIMIT_RETRY_BACKOFF_CAP {
            return base;
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        peer_id.as_str().hash(&mut hasher);
        attempt.hash(&mut hasher);
        base.saturating_add(Duration::from_micros(hasher.finish() % (jitter_micros + 1)))
            .min(RATE_LIMIT_RETRY_BACKOFF_CAP)
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

    fn consume_or_delay(&self, peer_id: &str) -> Option<Duration> {
        let mut buckets = self.buckets.lock();
        buckets
            .entry(peer_id.to_string())
            .or_insert_with(|| ReplayPeerBucket::new(self.capacity))
            .consume_or_delay(self.capacity, self.refill_rate)
    }

    fn defer(&self, peer_id: &str, delay: Duration) {
        let mut buckets = self.buckets.lock();
        buckets
            .entry(peer_id.to_string())
            .or_insert_with(|| ReplayPeerBucket::new(self.capacity))
            .defer(delay);
    }
}

#[derive(Debug)]
struct ReplayPeerBucket {
    tokens: f64,
    last_refill: Instant,
    blocked_until: Option<Instant>,
}

impl ReplayPeerBucket {
    fn new(capacity: u32) -> Self {
        Self {
            tokens: capacity as f64,
            last_refill: Instant::now(),
            blocked_until: None,
        }
    }

    fn consume_or_delay(&mut self, capacity: u32, refill_rate: f64) -> Option<Duration> {
        let now = Instant::now();
        if let Some(blocked_until) = self.blocked_until {
            if blocked_until > now {
                return Some(blocked_until.duration_since(now));
            }
            self.blocked_until = None;
        }

        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * refill_rate).min(capacity as f64);
        self.last_refill = now;

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            None
        } else {
            let refill_delay = (1.0 - self.tokens) / refill_rate;
            Some(Duration::from_secs_f64(refill_delay.clamp(0.001, 1.0)))
        }
    }

    fn defer(&mut self, delay: Duration) {
        let now = Instant::now();
        let blocked_until = now + delay;
        self.blocked_until = Some(
            self.blocked_until
                .map_or(blocked_until, |current| current.max(blocked_until)),
        );
        // The sender's mirrored bucket may still be full while unrelated
        // traffic has drained the receiver. Forget that stale credit after an
        // explicit nack and resume at the configured refill rate.
        self.tokens = 0.0;
        self.last_refill = now;
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

#[cfg(test)]
mod rate_limit_retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn gate() -> ReplayPushGate {
        ReplayPushGate::new(ReplayPushConfig {
            max_concurrent_document_tasks: 4,
            max_concurrent_outbound_pushes: 4,
            per_peer_rate_limit_burst: 1000,
            per_peer_rate_limit_rate: 1000.0,
            send_timeout: Duration::from_secs(1),
        })
    }

    fn rate_limited_reply() -> PushLogReply {
        PushLogReply::error("nacked", p2p::error::RATE_LIMITED_MESSAGE)
    }

    #[tokio::test]
    async fn rate_limited_nacks_are_retried_until_success() {
        let g = gate();
        let peer = PeerId::new("peer-rl".to_string());
        let attempts = AtomicUsize::new(0);

        let reply = g
            .send_pushlog_with_rate_limit_retry(&peer, || {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 3 {
                        Ok(rate_limited_reply())
                    } else {
                        Ok(PushLogReply::success("merged"))
                    }
                }
            })
            .await
            .unwrap();

        assert!(
            reply.err_message.is_none(),
            "retry must surface the success"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            4,
            "three rate-limited nacks then the success"
        );
    }

    #[tokio::test]
    async fn exhausted_rate_limit_retries_surface_the_nack_for_durable_handoff() {
        let g = gate();
        let peer = PeerId::new("peer-rl-exhaust".to_string());
        let attempts = AtomicUsize::new(0);

        let reply = g
            .send_pushlog_with_rate_limit_retry(&peer, || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Ok(rate_limited_reply()) }
            })
            .await
            .unwrap();

        assert_eq!(
            reply.err_message.as_deref(),
            Some(p2p::error::RATE_LIMITED_MESSAGE),
            "after exhaustion the caller's existing rejected-reply handling must fire"
        );
        assert_eq!(
            attempts.load(Ordering::SeqCst) as u32,
            MAX_RATE_LIMITED_RETRIES + 1,
            "initial attempt plus the bounded retries"
        );
    }

    #[tokio::test]
    async fn hard_rejections_are_not_retried() {
        let g = gate();
        let peer = PeerId::new("peer-hard".to_string());
        let attempts = AtomicUsize::new(0);

        let reply = g
            .send_pushlog_with_rate_limit_retry(&peer, || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Ok(PushLogReply::error("nacked", "schema mismatch")) }
            })
            .await
            .unwrap();

        assert_eq!(reply.err_message.as_deref(), Some("schema mismatch"));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "a hard rejection must surface on the first attempt"
        );
    }

    #[tokio::test]
    async fn observed_nack_pauses_and_drains_the_shared_peer_bucket() {
        let g = gate();
        let peer = PeerId::new("peer-shared-cooldown".to_string());
        g.peer_pacer
            .defer(peer.as_str(), Duration::from_millis(100));
        let attempts = Arc::new(AtomicUsize::new(0));
        let task_attempts = attempts.clone();
        let task = tokio::spawn(async move {
            g.send_pushlog(&peer, async move {
                task_attempts.fetch_add(1, Ordering::SeqCst);
                Ok(PushLogReply::success("accepted"))
            })
            .await
            .unwrap();
        });

        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        tokio::time::sleep(Duration::from_millis(90)).await;
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        task.await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn unfinished_replay_is_persisted_and_marks_replicator_inactive() {
        use storage::backends::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let peerstore = storage::stores::Peerstore::new(store.clone());
        let peer = PeerId::new("peer-durable".to_string());
        let info = p2p::ReplicatorInfo::from_raw(
            peer.to_string(),
            vec!["collection".to_string()],
            Vec::new(),
        );
        peerstore
            .create_replicator(peer.as_str(), &info.to_bytes().unwrap())
            .await
            .unwrap();

        persist_replay_failures(
            store,
            &peer,
            &[ReplayDocumentFailure {
                doc_id: "doc-1".to_string(),
                collection_id: "collection".to_string(),
            }],
        )
        .await
        .unwrap();

        let retries = peerstore.get_retry_documents(peer.as_str()).await.unwrap();
        assert_eq!(retries.len(), 1);
        assert_eq!(retries[0].doc_id, "doc-1");
        assert!(retries[0].pending);
        assert!(!retries[0].retry_info.is_due());
        let saved = peerstore
            .get_replicator(peer.as_str())
            .await
            .unwrap()
            .unwrap();
        let saved = p2p::ReplicatorInfo::from_bytes(&saved).unwrap();
        assert_eq!(saved.status, p2p::ReplicatorStatus::Inactive);
    }
}
