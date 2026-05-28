//! Lifecycle handle returned by [`crate::init`].
//!
//! Go DefraDB never flushes provider state — buffered spans at SIGTERM are
//! lost. `TelemetryHandle` fixes that two ways: explicit [`shutdown`] for
//! callers that want a deterministic flush point, and a `Drop` impl that
//! flushes if the handle is dropped without one (panic, early return,
//! caller forgot, etc.).
//!
//! [`shutdown`]: TelemetryHandle::shutdown

pub struct TelemetryHandle {
    #[cfg(feature = "otlp")]
    pub(crate) inner: Option<OtelProviders>,
}

#[cfg(feature = "otlp")]
pub(crate) struct OtelProviders {
    pub tracer_provider: opentelemetry_sdk::trace::SdkTracerProvider,
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
    pub fn shutdown(mut self) {
        self.flush();
    }

    fn flush(&mut self) {
        #[cfg(feature = "otlp")]
        if let Some(p) = self.inner.take() {
            let _ = p.tracer_provider.shutdown();
            let _ = p.meter_provider.shutdown();
        }
    }
}

/// Safety net: even if a caller forgets [`TelemetryHandle::shutdown`] or
/// drops the handle on an error path, the providers get flushed. Without
/// this, the fix for Go's "never calls shutdown" bug only worked on the
/// happy path.
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
