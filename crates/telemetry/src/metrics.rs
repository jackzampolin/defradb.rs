use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "otlp")]
use std::sync::{OnceLock, RwLock};

#[cfg(feature = "otlp")]
use opentelemetry::metrics::{Counter, Gauge, Histogram, MeterProvider as _};
#[cfg(feature = "otlp")]
use opentelemetry::KeyValue;

/// A retry loop that absorbs transaction conflicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum RetryLayer {
    HttpAutoCommit,
    EmbeddedExecute,
    Merge,
    PushLedger,
}

impl RetryLayer {
    const COUNT: usize = 4;

    #[cfg(feature = "otlp")]
    const fn as_str(self) -> &'static str {
        match self {
            Self::HttpAutoCommit => "http_auto_commit",
            Self::EmbeddedExecute => "embedded_execute",
            Self::Merge => "merge",
            Self::PushLedger => "push_ledger",
        }
    }
}

/// Process-lifetime retry counters for one layer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetryLayerSnapshot {
    /// Retries performed after an initial conflict.
    pub attempts: u64,
    /// Retry sequences whose final result was no longer a conflict.
    pub successes: u64,
    /// Retry sequences that returned a conflict after reaching their bound.
    pub exhaustions: u64,
}

/// Process-lifetime conflict retry and escape counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConflictMetricsSnapshot {
    pub http_auto_commit: RetryLayerSnapshot,
    pub embedded_execute: RetryLayerSnapshot,
    pub merge: RetryLayerSnapshot,
    pub push_ledger: RetryLayerSnapshot,
    /// Typed transaction conflicts returned through a client API.
    pub escaped_to_clients: u64,
}

struct RetryCounters {
    attempts: [AtomicU64; RetryLayer::COUNT],
    successes: [AtomicU64; RetryLayer::COUNT],
    exhaustions: [AtomicU64; RetryLayer::COUNT],
    escaped_to_clients: AtomicU64,
}

impl RetryCounters {
    const fn new() -> Self {
        Self {
            attempts: [const { AtomicU64::new(0) }; RetryLayer::COUNT],
            successes: [const { AtomicU64::new(0) }; RetryLayer::COUNT],
            exhaustions: [const { AtomicU64::new(0) }; RetryLayer::COUNT],
            escaped_to_clients: AtomicU64::new(0),
        }
    }

    fn layer(&self, layer: RetryLayer) -> RetryLayerSnapshot {
        let index = layer as usize;
        RetryLayerSnapshot {
            attempts: self.attempts[index].load(Ordering::Relaxed),
            successes: self.successes[index].load(Ordering::Relaxed),
            exhaustions: self.exhaustions[index].load(Ordering::Relaxed),
        }
    }
}

static RETRIES: RetryCounters = RetryCounters::new();

/// Capture process-lifetime conflict retry and escape counters.
pub fn conflict_metrics_snapshot() -> ConflictMetricsSnapshot {
    ConflictMetricsSnapshot {
        http_auto_commit: RETRIES.layer(RetryLayer::HttpAutoCommit),
        embedded_execute: RETRIES.layer(RetryLayer::EmbeddedExecute),
        merge: RETRIES.layer(RetryLayer::Merge),
        push_ledger: RETRIES.layer(RetryLayer::PushLedger),
        escaped_to_clients: RETRIES.escaped_to_clients.load(Ordering::Relaxed),
    }
}

pub fn record_retry_attempt(layer: RetryLayer) {
    RETRIES.attempts[layer as usize].fetch_add(1, Ordering::Relaxed);
    emit_retry_attempt(layer);
}

pub fn record_retry_success(layer: RetryLayer) {
    RETRIES.successes[layer as usize].fetch_add(1, Ordering::Relaxed);
    emit_retry_success(layer);
}

pub fn record_retry_exhaustion(layer: RetryLayer) {
    RETRIES.exhaustions[layer as usize].fetch_add(1, Ordering::Relaxed);
    emit_retry_exhaustion(layer);
}

pub fn record_escaped_conflict(surface: &'static str) {
    RETRIES.escaped_to_clients.fetch_add(1, Ordering::Relaxed);
    emit_escaped_conflict(surface);
}

pub fn record_storage_conflict(backend: &'static str, rule: &'static str) {
    emit_storage_conflict(backend, rule);
}

pub fn record_commit_gate_wait(backend: &'static str, seconds: f64) {
    emit_commit_gate_wait(backend, seconds);
}

pub fn record_conflict_tracker_size(
    backend: &'static str,
    committed: u64,
    pending: u64,
    active_snapshots: u64,
) {
    emit_conflict_tracker_size(backend, committed, pending, active_snapshots);
}

#[cfg(feature = "otlp")]
fn emit_retry_attempt(layer: RetryLayer) {
    with_instruments(|metrics| {
        metrics
            .retry_attempts
            .add(1, &[KeyValue::new("layer", layer.as_str())]);
    });
}

#[cfg(feature = "otlp")]
fn emit_retry_success(layer: RetryLayer) {
    with_instruments(|metrics| {
        metrics
            .retry_successes
            .add(1, &[KeyValue::new("layer", layer.as_str())]);
    });
}

#[cfg(feature = "otlp")]
fn emit_retry_exhaustion(layer: RetryLayer) {
    with_instruments(|metrics| {
        metrics
            .retry_exhaustions
            .add(1, &[KeyValue::new("layer", layer.as_str())]);
    });
}

#[cfg(feature = "otlp")]
fn emit_escaped_conflict(surface: &'static str) {
    with_instruments(|metrics| {
        metrics
            .escaped_conflicts
            .add(1, &[KeyValue::new("surface", surface)]);
    });
}

#[cfg(feature = "otlp")]
fn emit_storage_conflict(backend: &'static str, rule: &'static str) {
    with_instruments(|metrics| {
        metrics.storage_conflicts.add(
            1,
            &[
                KeyValue::new("backend", backend),
                KeyValue::new("rule", rule),
            ],
        );
    });
}

#[cfg(feature = "otlp")]
fn emit_commit_gate_wait(backend: &'static str, seconds: f64) {
    with_instruments(|metrics| {
        metrics
            .commit_gate_wait
            .record(seconds, &[KeyValue::new("backend", backend)]);
    });
}

#[cfg(feature = "otlp")]
fn emit_conflict_tracker_size(
    backend: &'static str,
    committed: u64,
    pending: u64,
    active_snapshots: u64,
) {
    with_instruments(|metrics| {
        for (state, value) in [
            ("committed", committed),
            ("pending", pending),
            ("active_snapshots", active_snapshots),
        ] {
            metrics.conflict_tracker_size.record(
                value,
                &[
                    KeyValue::new("backend", backend),
                    KeyValue::new("state", state),
                ],
            );
        }
    });
}

#[cfg(not(feature = "otlp"))]
fn emit_retry_attempt(_layer: RetryLayer) {}
#[cfg(not(feature = "otlp"))]
fn emit_retry_success(_layer: RetryLayer) {}
#[cfg(not(feature = "otlp"))]
fn emit_retry_exhaustion(_layer: RetryLayer) {}
#[cfg(not(feature = "otlp"))]
fn emit_escaped_conflict(_surface: &'static str) {}
#[cfg(not(feature = "otlp"))]
fn emit_storage_conflict(_backend: &'static str, _rule: &'static str) {}
#[cfg(not(feature = "otlp"))]
fn emit_commit_gate_wait(_backend: &'static str, _seconds: f64) {}
#[cfg(not(feature = "otlp"))]
fn emit_conflict_tracker_size(
    _backend: &'static str,
    _committed: u64,
    _pending: u64,
    _active_snapshots: u64,
) {
}

#[cfg(feature = "otlp")]
struct Instruments {
    storage_conflicts: Counter<u64>,
    retry_attempts: Counter<u64>,
    retry_successes: Counter<u64>,
    retry_exhaustions: Counter<u64>,
    escaped_conflicts: Counter<u64>,
    commit_gate_wait: Histogram<f64>,
    conflict_tracker_size: Gauge<u64>,
}

#[cfg(feature = "otlp")]
static NEXT_INSTALLATION: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "otlp")]
static INSTRUMENTS: OnceLock<RwLock<Vec<(u64, Instruments)>>> = OnceLock::new();

#[cfg(feature = "otlp")]
pub(crate) fn install(provider: &opentelemetry_sdk::metrics::SdkMeterProvider) -> u64 {
    let meter = provider.meter("defradb");
    let instruments = Instruments {
        storage_conflicts: meter
            .u64_counter("defradb.storage.transaction.conflicts")
            .with_description("Storage transaction conflicts")
            .build(),
        retry_attempts: meter
            .u64_counter("defradb.transaction.retry.attempts")
            .with_description("Transaction conflict retry attempts")
            .build(),
        retry_successes: meter
            .u64_counter("defradb.transaction.retry.successes")
            .with_description("Conflicts recovered by a retry layer")
            .build(),
        retry_exhaustions: meter
            .u64_counter("defradb.transaction.retry.exhaustions")
            .with_description("Transaction conflict retry loops exhausted")
            .build(),
        escaped_conflicts: meter
            .u64_counter("defradb.transaction.conflicts.escaped")
            .with_description("Transaction conflicts returned to clients")
            .build(),
        commit_gate_wait: meter
            .f64_histogram("defradb.storage.commit_gate.wait")
            .with_unit("s")
            .with_description("Time waiting to publish a committed storage version")
            .build(),
        conflict_tracker_size: meter
            .u64_gauge("defradb.storage.conflict_tracker.size")
            .with_description("Current conflict tracker entries")
            .build(),
    };

    let installation = NEXT_INSTALLATION.fetch_add(1, Ordering::Relaxed) + 1;
    let lock = INSTRUMENTS.get_or_init(|| RwLock::new(Vec::new()));
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push((installation, instruments));
    installation
}

#[cfg(feature = "otlp")]
pub(crate) fn uninstall(installation: u64) {
    if let Some(lock) = INSTRUMENTS.get() {
        lock.write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|(id, _)| *id != installation);
    }
}

#[cfg(feature = "otlp")]
fn with_instruments(record: impl FnOnce(&Instruments)) {
    let Some(lock) = INSTRUMENTS.get() else {
        return;
    };
    let guard = lock.read().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some((_, metrics)) = guard.last() {
        record(metrics);
    }
}
