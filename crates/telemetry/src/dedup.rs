//! Dedup filter for noisy OTEL exporter errors.
//!
//! When the OTLP collector is unreachable, `opentelemetry-otlp` emits a
//! `"connection refused"` `tracing::error!` per failed batch — fast enough to
//! flood logs. This filter suppresses all but the first such event per
//! process, matching Go DefraDB's `otel.SetErrorHandler + sync.Once` dedup
//! (PR #4639, commit `83af37a9`).
//!
//! `opentelemetry::global::set_error_handler` was removed in opentelemetry-rust
//! v0.27; SDK diagnostics now flow through `tracing` at `target = "opentelemetry"`
//! / `"opentelemetry_sdk"` / `"opentelemetry-otlp"`, so a
//! `tracing_subscriber::layer::Filter` is the natural hook.

use std::fmt;
use std::sync::OnceLock;

use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;

const OTEL_TARGET_PREFIX: &str = "opentelemetry";
/// Substrings that indicate "OTLP collector unreachable". The Go port matches
/// just `"connection refused"`, which is what the gRPC transport surfaces.
/// With `opentelemetry-otlp`'s HTTP+reqwest transport, the SDK wraps the
/// connection error as `"HTTP export failed: network error"`, so we match
/// either pattern. `"connection refused"` is kept first for the gRPC path
/// in case future builds switch transport.
const UNREACHABLE_NEEDLES: &[&str] = &["connection refused", "HTTP export failed", "network error"];

#[derive(Default)]
pub struct OtelDedupFilter {
    refused_logged: OnceLock<()>,
}

impl OtelDedupFilter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decide whether to emit. Returns `true` to forward the event,
    /// `false` to suppress it. Pure logic, separated from the `Filter`
    /// impl so it can be unit-tested without spinning up a subscriber.
    fn decide(&self, target: &str, message: &str) -> bool {
        if !target.starts_with(OTEL_TARGET_PREFIX) {
            return true;
        }
        if UNREACHABLE_NEEDLES.iter().any(|n| message.contains(n)) {
            // `OnceLock::set` returns Ok only on the first call; subsequent
            // calls return Err. That's exactly the "log once, then suppress"
            // semantics of Go's `sync.Once.Do`.
            return self.refused_logged.set(()).is_ok();
        }
        true
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
        self.decide(target, &visitor.message)
    }
}

/// Concatenates every field of the event into a single string so that
/// structured events (which the OTel SDK uses — e.g.
/// `otel_error!(name: "BatchSpanProcessor.ExportError", error = ...)`) are
/// matchable. An earlier "only the `message` field" version missed those
/// because the SDK does not include a format-string message.
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
    fn first_connection_refused_passes() {
        let f = OtelDedupFilter::new();
        assert!(f.decide("opentelemetry-otlp", "transport error: connection refused"));
    }

    #[test]
    fn subsequent_connection_refused_suppressed() {
        let f = OtelDedupFilter::new();
        assert!(f.decide("opentelemetry-otlp", "connection refused"));
        assert!(!f.decide("opentelemetry-otlp", "connection refused"));
        assert!(!f.decide("opentelemetry-otlp", "connection refused"));
    }

    #[test]
    fn other_otel_errors_pass_through() {
        let f = OtelDedupFilter::new();
        assert!(f.decide("opentelemetry-otlp", "invalid response from collector"));
        assert!(f.decide("opentelemetry_sdk", "batch send took too long"));
        assert!(f.decide("opentelemetry", "tls handshake failed"));
    }

    #[test]
    fn non_otel_events_pass_through_even_with_refused_text() {
        let f = OtelDedupFilter::new();
        assert!(f.decide("hyper::client", "connection refused"));
        assert!(f.decide("my_app::module", "hello"));
    }

    #[test]
    fn other_otel_errors_unaffected_after_refused_suppression() {
        let f = OtelDedupFilter::new();
        assert!(f.decide("opentelemetry-otlp", "connection refused"));
        assert!(!f.decide("opentelemetry-otlp", "connection refused"));
        // Non-refused OTEL errors still flow through.
        assert!(f.decide("opentelemetry-otlp", "different error"));
        assert!(f.decide("opentelemetry_sdk", "another problem"));
    }

    #[test]
    fn http_exporter_unreachable_pattern_dedups() {
        // Real-world output from `opentelemetry_sdk` when the HTTP exporter
        // can't reach the collector (captured by `tools/otel-smoke/dedup.sh`):
        //   name="BatchSpanProcessor.ExportError" error="Operation failed: HTTP export failed: network error"
        let f = OtelDedupFilter::new();
        let msg = r#"name="BatchSpanProcessor.ExportError" error="Operation failed: HTTP export failed: network error""#;
        assert!(f.decide("opentelemetry_sdk", msg));
        assert!(!f.decide("opentelemetry_sdk", msg));
    }

    #[test]
    fn network_error_pattern_dedups() {
        let f = OtelDedupFilter::new();
        assert!(f.decide("opentelemetry-otlp", "transport network error"));
        assert!(!f.decide("opentelemetry-otlp", "another network error case"));
    }

    #[test]
    fn dedup_state_is_per_instance() {
        let f1 = OtelDedupFilter::new();
        let f2 = OtelDedupFilter::new();
        assert!(f1.decide("opentelemetry-otlp", "connection refused"));
        assert!(!f1.decide("opentelemetry-otlp", "connection refused"));
        // Separate instance has its own state.
        assert!(f2.decide("opentelemetry-otlp", "connection refused"));
    }
}
