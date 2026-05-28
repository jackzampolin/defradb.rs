//! Dedup filter for noisy OTEL exporter errors.
//!
//! Ports Go DefraDB's `sync.Once`-guarded `otel.SetErrorHandler` (PR #4639,
//! commit `83af37a9`). Since `opentelemetry::global::set_error_handler` was
//! removed in opentelemetry-rust v0.27, SDK diagnostics now flow through
//! `tracing` at `target = "opentelemetry"*` and the natural hook is a
//! `tracing_subscriber::layer::Filter`.
//!
//! **Per-needle latching.** A single shared `OnceLock` would let the first
//! transient `"network error"` silence all subsequent `"connection refused"`
//! hours later (or vice versa). Each known unreachable-collector pattern
//! gets its own latch — one log per pattern per filter instance, then
//! suppress.
//!
//! **Per-layer instances.** `tracing_subscriber::layer::Filter` is a
//! per-layer hook. Each layer that ingests OTel events needs its own
//! filter instance; trying to share state via `Arc` interleaves the
//! per-event decisions (first layer wins the lock, second layer sees the
//! event as already-suppressed). The right model is each layer logs the
//! first occurrence and suppresses the rest, independently.

use std::fmt;
use std::sync::OnceLock;

use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::{Context, Filter};
use tracing_subscriber::registry::LookupSpan;

const OTEL_TARGET_PREFIX: &str = "opentelemetry";

#[derive(Default)]
pub struct OtelDedupFilter {
    refused_logged: OnceLock<()>,
    http_export_failed_logged: OnceLock<()>,
    network_error_logged: OnceLock<()>,
}

impl OtelDedupFilter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decide whether to emit. Pure logic, unit-testable without a subscriber.
    fn decide(&self, target: &str, message: &str) -> bool {
        if !target.starts_with(OTEL_TARGET_PREFIX) {
            return true;
        }
        // First match wins. Order doesn't affect correctness — different
        // needles have independent latches.
        for (needle, lock) in self.needles() {
            if message.contains(needle) {
                return lock.set(()).is_ok();
            }
        }
        true
    }

    fn needles(&self) -> [(&'static str, &OnceLock<()>); 3] {
        [
            ("connection refused", &self.refused_logged),
            ("HTTP export failed", &self.http_export_failed_logged),
            ("network error", &self.network_error_logged),
        ]
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

    #[test]
    fn needles_latch_independently() {
        // Critical: a transient `"network error"` early in the process must
        // NOT suppress later `"connection refused"` events from a different
        // failure mode. Single shared OnceLock would conflate the two.
        let f = OtelDedupFilter::new();
        assert!(f.decide("opentelemetry-otlp", "transient network error"));
        assert!(!f.decide("opentelemetry-otlp", "another network error"));
        // Different needle → its own latch, still passes once.
        assert!(f.decide("opentelemetry-otlp", "connection refused"));
        assert!(!f.decide("opentelemetry-otlp", "connection refused"));
        // Third needle.
        assert!(f.decide("opentelemetry-otlp", "HTTP export failed: 503"));
        assert!(!f.decide("opentelemetry-otlp", "HTTP export failed: 502"));
    }
}
