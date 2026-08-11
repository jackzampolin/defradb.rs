//! OpenTelemetry exporter setup for DefraDB.
//!
//! Mirrors Go DefraDB's `internal/telemetry/otel.go`. The `otlp` feature
//! compiles in OTLP/HTTP trace and metric export; without it the crate provides a
//! no-op [`TelemetryHandle`] and the [`OtelDedupFilter`] (always available,
//! no-op when no `opentelemetry*` events exist).
//!
//! Connection-refused log spam from the OTEL SDK is deduped via
//! [`OtelDedupFilter`] — the Rust equivalent of Go's `sync.Once`-guarded
//! `otel.SetErrorHandler`. The global error handler API was removed from
//! `opentelemetry::global` in v0.27; SDK diagnostics now flow through
//! `tracing` at `target = "opentelemetry"*`, so a `tracing_subscriber`
//! filter is the natural hook.

mod config;
mod dedup;
mod handle;
mod metrics;
#[cfg(feature = "otlp")]
mod util;

pub use config::TelemetryConfig;
pub use dedup::OtelDedupFilter;
pub use handle::TelemetryHandle;
pub use metrics::{
    conflict_metrics_snapshot, record_commit_gate_wait, record_conflict_tracker_size,
    record_escaped_conflict, record_retry_attempt, record_retry_exhaustion, record_retry_success,
    record_storage_conflict, ConflictMetricsSnapshot, RetryLayer, RetryLayerSnapshot,
};

#[cfg(feature = "otlp")]
mod init;
#[cfg(feature = "otlp")]
pub use init::{init, otel_layer, InitError, Tracer};
