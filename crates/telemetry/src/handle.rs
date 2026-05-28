//! Lifecycle handle returned by [`crate::init`]. Holds provider handles so
//! the caller can flush buffered spans/metrics on shutdown.
//!
//! Go DefraDB never calls provider shutdown (`internal/telemetry/otel.go`
//! has no Shutdown plumbing), so spans buffered at SIGTERM are lost. We
//! fix that here: the CLI's main path takes ownership of this handle and
//! calls [`TelemetryHandle::shutdown`] before exit.

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
    pub fn noop() -> Self {
        Self {
            #[cfg(feature = "otlp")]
            inner: None,
        }
    }

    pub fn shutdown(self) {
        #[cfg(feature = "otlp")]
        if let Some(p) = self.inner {
            let _ = p.tracer_provider.shutdown();
            let _ = p.meter_provider.shutdown();
        }
    }
}

impl Default for TelemetryHandle {
    fn default() -> Self {
        Self::noop()
    }
}
