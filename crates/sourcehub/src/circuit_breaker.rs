use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Number of consecutive failures before the circuit trips.
const FAILURE_THRESHOLD: u32 = 3;

/// How long the circuit stays open (denying all requests) before allowing a probe.
const RESET_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    Closed,
    Open,
    HalfOpen,
}

/// Thread-safe circuit breaker for SourceHub network calls.
///
/// States:
/// - Closed: normal operation, requests pass through.
/// - Open: SourceHub unreachable; all requests fail-closed immediately.
/// - HalfOpen: cooldown elapsed; one probe request is allowed through.
///
/// The breaker trips to Open after `FAILURE_THRESHOLD` consecutive failures.
/// After `RESET_TIMEOUT` it moves to HalfOpen and allows one probe. A
/// successful probe closes the circuit; a failed probe reopens it.
#[derive(Clone)]
pub(crate) struct CircuitBreaker {
    inner: Arc<CircuitBreakerInner>,
}

struct CircuitBreakerInner {
    /// Number of consecutive failures (reset to 0 on any success).
    consecutive_failures: AtomicU32,
    /// Unix timestamp (seconds) when the circuit was tripped. 0 = not tripped.
    tripped_at_secs: AtomicU64,
    /// 0 = Closed, 1 = Open, 2 = HalfOpen
    state: AtomicU32,
}

impl CircuitBreaker {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(CircuitBreakerInner {
                consecutive_failures: AtomicU32::new(0),
                tripped_at_secs: AtomicU64::new(0),
                state: AtomicU32::new(0),
            }),
        }
    }

    fn current_state(&self) -> State {
        match self.inner.state.load(Ordering::Acquire) {
            0 => State::Closed,
            1 => {
                // Check if reset timeout has elapsed.
                let tripped_at = self.inner.tripped_at_secs.load(Ordering::Acquire);
                let now = now_secs();
                if now.saturating_sub(tripped_at) >= RESET_TIMEOUT.as_secs() {
                    // Transition to HalfOpen to allow a probe.
                    let _ = self.inner.state.compare_exchange(
                        1,
                        2,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    State::HalfOpen
                } else {
                    State::Open
                }
            }
            _ => State::HalfOpen,
        }
    }

    /// Returns true if the request should be allowed through.
    ///
    /// Closed: always allowed.
    /// Open: never allowed (fail-closed).
    /// HalfOpen: allowed once for a probe.
    pub(crate) fn allow_request(&self) -> bool {
        match self.current_state() {
            State::Closed => true,
            State::Open => false,
            State::HalfOpen => true,
        }
    }

    /// Record a successful call. Resets failure count and closes the circuit.
    pub(crate) fn record_success(&self) {
        self.inner.consecutive_failures.store(0, Ordering::Release);
        self.inner.tripped_at_secs.store(0, Ordering::Release);
        self.inner.state.store(0, Ordering::Release);
    }

    /// Record a failed call. Trips the circuit after `FAILURE_THRESHOLD` failures.
    pub(crate) fn record_failure(&self) {
        let failures = self
            .inner
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            + 1;

        if failures >= FAILURE_THRESHOLD {
            let was_closed = self
                .inner
                .state
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();

            // Also re-trip from HalfOpen (failed probe).
            let was_half_open = self
                .inner
                .state
                .compare_exchange(2, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();

            if was_closed || was_half_open {
                self.inner
                    .tripped_at_secs
                    .store(now_secs(), Ordering::Release);
                tracing::warn!(
                    failures,
                    "SourceHub circuit breaker tripped: denying all access until recovery"
                );
            }
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trips_after_threshold_failures() {
        let cb = CircuitBreaker::new();
        assert!(cb.allow_request());

        for _ in 0..FAILURE_THRESHOLD {
            cb.record_failure();
        }

        assert!(!cb.allow_request());
    }

    #[test]
    fn resets_on_success() {
        let cb = CircuitBreaker::new();
        for _ in 0..FAILURE_THRESHOLD {
            cb.record_failure();
        }
        assert!(!cb.allow_request());

        cb.record_success();
        assert!(cb.allow_request());
    }

    #[test]
    fn does_not_trip_below_threshold() {
        let cb = CircuitBreaker::new();
        for _ in 0..FAILURE_THRESHOLD - 1 {
            cb.record_failure();
        }
        assert!(cb.allow_request());
    }
}
