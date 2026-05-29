//! Quieting filter for noisy OTEL exporter errors.
//!
//! Ports Go DefraDB's `sync.Once`-guarded `otel.SetErrorHandler` (PR #4639,
//! commit `83af37a9`). Go suppresses the SDK's repeated collector-unreachable
//! export errors and emits a single actionable hint instead; other OTEL
//! errors log normally (Go's `else` branch).
//!
//! `opentelemetry::global::set_error_handler` was removed in opentelemetry-rust
//! v0.27, so SDK diagnostics now flow through `tracing` at
//! `target = "opentelemetry"*`. The hook is therefore a
//! `tracing_subscriber::layer::Filter`:
//!
//! - An event reporting an unreachable collector (matches a known needle) is
//!   suppressed, and Go's hint is emitted **once per process** — via a global
//!   `OnceLock` shared across every layer the filter is attached to, so the
//!   hint appears exactly once regardless of layer count.
//! - Any other `opentelemetry*` event passes through and logs normally
//!   (matching Go's `else` branch — genuine errors stay visible).
//!
//! The hint is emitted with `eprintln!` rather than `tracing`: emitting a
//! `tracing` event from inside a layer's event hook risks re-entering the
//! subscriber mid-event. Plain stderr is re-entrancy-safe and matches the
//! crate's existing operator-warning style (see `handle.rs`).

use std::fmt;
use std::sync::OnceLock;

use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;

const OTEL_TARGET_PREFIX: &str = "opentelemetry";

/// Substrings that indicate the OTLP collector is *unreachable* (the only
/// case Go special-cases). `"connection refused"` is the gRPC transport's
/// wording; `"network error"` is what `opentelemetry-otlp`'s HTTP transport
/// emits for connection/DNS failures (http/mod.rs:518).
///
/// Deliberately NOT `"HTTP export failed"`: that prefix also appears on
/// status-code rejections from a *reachable* collector
/// (`"HTTP export failed with status code: 401/429/503"`, http/mod.rs:540).
/// Those are genuine server-side errors Go keeps visible (its `else` branch),
/// so matching the prefix would wrongly suppress them behind the
/// unreachable-collector hint and latch them off for the rest of the process.
const UNREACHABLE_NEEDLES: &[&str] = &["connection refused", "network error"];

/// Operator-facing hint, matching Go's `internal/telemetry/otel.go`.
const UNREACHABLE_HINT: &str =
    "OpenTelemetry export failed, ensure your OTLP collector is running and reachable";

/// Guards the hint so it is emitted at most once per process, across every
/// layer this filter is attached to. Go uses a single `sync.Once` for the
/// same effect.
static HINT_ONCE: OnceLock<()> = OnceLock::new();

#[derive(Default)]
pub struct OtelDedupFilter;

impl OtelDedupFilter {
    pub fn new() -> Self {
        Self
    }

    /// True if this is an `opentelemetry*`-target event reporting an
    /// unreachable collector — the case Go suppresses. Pure and stateless,
    /// so it's unit-testable without a subscriber.
    fn is_collector_unreachable(target: &str, message: &str) -> bool {
        target.starts_with(OTEL_TARGET_PREFIX)
            && UNREACHABLE_NEEDLES.iter().any(|n| message.contains(n))
    }
}

impl<S> Filter<S> for OtelDedupFilter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn enabled(&self, _meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        true
    }

    fn event_enabled(&self, event: &Event<'_>, _cx: &Context<'_, S>) -> bool {
        let target = event.metadata().target();
        if !target.starts_with(OTEL_TARGET_PREFIX) {
            return true;
        }
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        if Self::is_collector_unreachable(target, &visitor.message) {
            // Suppress the raw, repeated export error; emit Go's hint once
            // process-wide (including the underlying detail, as Go's
            // `log.ErrorE(msg, err)` does).
            if HINT_ONCE.set(()).is_ok() {
                eprintln!("{UNREACHABLE_HINT}: {}", visitor.message);
            }
            return false;
        }
        // Genuine, non-unreachable OTEL error — log normally.
        true
    }
}

/// Concatenates every field of the event into one string. The OTel SDK
/// uses `otel_error!(name: "BatchSpanProcessor.ExportError", error = ...)`
/// with no format-string `message` field, so a "message-only" visitor sees
/// nothing.
#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl MessageVisitor {
    fn push(&mut self, name: &str, value: impl fmt::Display) {
        use std::fmt::Write;
        if !self.message.is_empty() {
            self.message.push(' ');
        }
        let _ = write!(self.message, "{name}={value}");
    }
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push(field.name(), format_args!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field.name(), value);
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.push(field.name(), value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_refused_is_unreachable() {
        assert!(OtelDedupFilter::is_collector_unreachable(
            "opentelemetry-otlp",
            "transport error: connection refused"
        ));
    }

    #[test]
    fn http_exporter_unreachable_is_unreachable() {
        // Real `opentelemetry_sdk` output for an unreachable collector
        // (http/mod.rs:518), captured by tools/otel-smoke/dedup.sh. Matches
        // via the "network error" needle.
        let msg = r#"name="BatchSpanProcessor.ExportError" error="Operation failed: HTTP export failed: network error""#;
        assert!(OtelDedupFilter::is_collector_unreachable(
            "opentelemetry_sdk",
            msg
        ));
    }

    #[test]
    fn http_status_code_error_is_not_unreachable() {
        // A *reachable* collector rejecting the export (http/mod.rs:540) is a
        // genuine server-side error Go keeps visible — it must NOT be treated
        // as unreachable (else it gets suppressed behind the hint and latched
        // off). Regression guard for the over-broad "HTTP export failed" needle.
        for status in ["401", "413", "429", "503"] {
            let msg = format!(
                r#"name="BatchSpanProcessor.ExportError" error="Operation failed: HTTP export failed with status code: {status}""#
            );
            assert!(
                !OtelDedupFilter::is_collector_unreachable("opentelemetry_sdk", &msg),
                "status {status} should pass through, not be suppressed"
            );
        }
    }

    #[test]
    fn network_error_is_unreachable() {
        assert!(OtelDedupFilter::is_collector_unreachable(
            "opentelemetry-otlp",
            "transport network error"
        ));
    }

    #[test]
    fn other_otel_errors_are_not_unreachable() {
        // Genuine non-collector-unreachable OTEL errors must pass through
        // (Go logs these via its `else` branch).
        assert!(!OtelDedupFilter::is_collector_unreachable(
            "opentelemetry-otlp",
            "invalid response from collector"
        ));
        assert!(!OtelDedupFilter::is_collector_unreachable(
            "opentelemetry_sdk",
            "batch send took too long"
        ));
        assert!(!OtelDedupFilter::is_collector_unreachable(
            "opentelemetry",
            "tls handshake failed"
        ));
    }

    #[test]
    fn non_otel_events_are_never_unreachable() {
        // A non-OTEL target that happens to contain a needle must not be
        // touched — only `opentelemetry*` targets are in scope.
        assert!(!OtelDedupFilter::is_collector_unreachable(
            "hyper::client",
            "connection refused"
        ));
        assert!(!OtelDedupFilter::is_collector_unreachable(
            "my_app::module",
            "hello"
        ));
    }
}
