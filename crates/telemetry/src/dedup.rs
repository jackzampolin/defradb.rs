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

/// Known "OTLP collector unreachable" substrings. `"connection refused"` is
/// the gRPC transport's wording; `"HTTP export failed"` / `"network error"`
/// are what `opentelemetry-otlp`'s HTTP+reqwest transport emits. Each gets
/// its own latch (indexed positionally by [`OtelDedupFilter::latches`]).
const NEEDLES: &[&str] = &["connection refused", "HTTP export failed", "network error"];

#[derive(Default)]
pub struct OtelDedupFilter {
    /// One latch per entry in [`NEEDLES`], positionally aligned. A single
    /// shared latch would let the first transient `"network error"` silence
    /// a later `"connection refused"` from a different failure mode.
    latches: [OnceLock<()>; NEEDLES.len()],
}

impl OtelDedupFilter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decide whether to emit. Pure logic, unit-testable without a subscriber.
    ///
    /// Every matching needle claims its own latch in one pass. The event is
    /// emitted if at least one matching needle was previously unclaimed, OR
    /// if no needle matched at all (non-unreachable-collector events always
    /// pass through). Claiming ALL matching needles — not just the first —
    /// means a real SDK message like `"HTTP export failed: network error"`
    /// (two needles in one line) consumes both latches, so a later
    /// `"network error"`-only event can't slip through as a "first
    /// occurrence".
    fn decide(&self, target: &str, message: &str) -> bool {
        if !target.starts_with(OTEL_TARGET_PREFIX) {
            return true;
        }
        let mut emit = false;
        let mut any_match = false;
        for (needle, lock) in NEEDLES.iter().zip(&self.latches) {
            if message.contains(needle) {
                any_match = true;
                if lock.set(()).is_ok() {
                    emit = true;
                }
            }
        }
        emit || !any_match
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
    fn combined_needles_in_single_message_claim_all_latches() {
        // Real SDK output contains BOTH "HTTP export failed" AND
        // "network error". Marking ALL matched needles (not just the
        // first iterated) means a later "network error"-only event can't
        // log as a "first occurrence" by riding a still-unclaimed latch.
        // Regression for the prior first-match-wins bug.
        let f = OtelDedupFilter::new();
        let combined = r#"error="Operation failed: HTTP export failed: network error""#;
        assert!(f.decide("opentelemetry_sdk", combined));
        // Same combined message — both latches already claimed, suppressed.
        assert!(!f.decide("opentelemetry_sdk", combined));
        // A subsequent "network error"-only event must ALSO be suppressed,
        // because its needle's latch was claimed by the combined message.
        assert!(!f.decide("opentelemetry-otlp", "transient network error"));
        // And a "HTTP export failed"-only event is also suppressed.
        assert!(!f.decide("opentelemetry-otlp", "HTTP export failed: 502"));
        // But a totally different needle (connection refused) still gets
        // its one shot — its latch is still free.
        assert!(f.decide("opentelemetry-otlp", "connection refused"));
        assert!(!f.decide("opentelemetry-otlp", "connection refused"));
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
