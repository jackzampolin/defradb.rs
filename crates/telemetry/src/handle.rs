//! Lifecycle handle returned by [`crate::init`].
//!
//! Go DefraDB never flushes provider state — buffered spans at SIGTERM are
//! lost. `TelemetryHandle` fixes that two ways: explicit [`shutdown`] for
//! callers that want a deterministic flush point, and a `Drop` impl that
//! flushes if the handle is dropped without one (panic, early return,
//! caller forgot, etc.).
//!
//! **Prefer explicit `shutdown`.** Drop is a safety net, not a primary path.
//! The opentelemetry-sdk 0.32 batch span processor + periodic metric reader
//! each spawn dedicated OS threads; `shutdown` joins them synchronously
//! with up to a 5 s timeout per signal (~10 s combined). Dropping a handle
//! on a Tokio worker stalls that worker for the duration; dropping on a
//! current-thread runtime stalls the whole reactor. The Drop impl also
//! wraps the join in `catch_unwind` so a panicked batch thread can't
//! propagate a second panic during stack unwinding (which would abort the
//! process).
//!
//! [`shutdown`]: TelemetryHandle::shutdown

pub struct TelemetryHandle {
    #[cfg(feature = "otlp")]
    pub(crate) inner: Option<OtelProviders>,
}

#[cfg(feature = "otlp")]
pub(crate) struct OtelProviders {
    pub tracer_provider: opentelemetry_sdk::trace::SdkTracerProvider,
    #[cfg(feature = "metrics")]
    pub meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl TelemetryHandle {
    /// Returns a handle that owns no providers — `shutdown` and `Drop` are no-ops.
    pub fn noop() -> Self {
        Self {
            #[cfg(feature = "otlp")]
            inner: None,
        }
    }

    /// Flush providers eagerly. Equivalent to letting the handle drop, but
    /// makes the flush point explicit and lets callers control ordering
    /// (e.g. shut down telemetry after the rest of the node stops emitting).
    /// Prefer this over relying on `Drop` — see the module docs for the
    /// blocking-thread + panic-safety reasons.
    pub fn shutdown(mut self) {
        self.flush();
    }

    fn flush(&mut self) {
        #[cfg(feature = "otlp")]
        if let Some(_p) = self.inner.take() {
            // `catch_unwind` keeps a panicked batch thread (which the SDK
            // exposes via `handle.join().unwrap()` in shutdown) from
            // turning a normal Drop into a double-panic → abort.
            // `AssertUnwindSafe` is acceptable: the providers we move in
            // are about to be dropped anyway, so post-panic logical state
            // doesn't matter.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _ = _p.tracer_provider.shutdown();
                #[cfg(feature = "metrics")]
                let _ = _p.meter_provider.shutdown();
            }));
        }
    }
}

/// Safety net: even if a caller forgets [`TelemetryHandle::shutdown`] or
/// drops the handle on an error path, the providers get flushed. Without
/// this, the fix for Go's "never calls shutdown" bug only worked on the
/// happy path. See module-level docs for the blocking behavior and why
/// explicit `shutdown` is preferred.
impl Drop for TelemetryHandle {
    fn drop(&mut self) {
        self.flush();
    }
}

impl Default for TelemetryHandle {
    fn default() -> Self {
        Self::noop()
    }
}
