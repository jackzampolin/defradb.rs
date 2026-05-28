//! OTLP exporter setup. Mirrors Go DefraDB's `internal/telemetry/otel.go`.
//!
//! - OTLP/HTTP transport (`reqwest-blocking-client`). Same wire protocol Go
//!   uses via `otlptracehttp`. Default endpoint is `http://localhost:4318`.
//! - Standard OTEL env vars are honored automatically by `opentelemetry-otlp`
//!   0.32: `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`,
//!   `OTEL_EXPORTER_OTLP_PROTOCOL`, signal-specific overrides, etc.
//! - Resource attributes: `service.name`, `service.version`, `os.type`,
//!   `host.arch`, `process.pid`, `process.executable.name`. Mirrors Go's
//!   `resource.WithOS()` + `resource.WithProcess()` (which we approximate
//!   with `std::env::consts` + `std::process` to avoid an extra dep).
//!
//! Traces are always exported. Metrics export is off until the `metrics`
//! feature is enabled — nothing in defradb.rs records metric instruments
//! today, so leaving the metric pipeline running would burn an empty
//! periodic export every 60 s.

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use thiserror::Error;
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::registry::LookupSpan;

#[cfg(feature = "metrics")]
use opentelemetry_otlp::MetricExporter;
#[cfg(feature = "metrics")]
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

use crate::config::TelemetryConfig;
use crate::handle::{OtelProviders, TelemetryHandle};

pub use opentelemetry_sdk::trace::SdkTracer as Tracer;

#[derive(Debug, Error)]
pub enum InitError {
    #[error("failed to build OTLP span exporter: {0}")]
    SpanExporter(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[cfg(feature = "metrics")]
    #[error("failed to build OTLP metric exporter: {0}")]
    MetricExporter(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Returns the lifecycle handle and a configured [`SdkTracer`]. The caller
/// wraps the tracer with `tracing_opentelemetry::layer().with_tracer(...)`
/// at its subscriber-composition site so type inference can pick the right
/// `S` parameter for `OpenTelemetryLayer<S, _>`.
///
/// Safe to call from any context (no Tokio runtime required): with the
/// `reqwest-blocking-client` transport selected in this crate's `Cargo.toml`,
/// both `BatchSpanProcessor` and `PeriodicReader` (opentelemetry_sdk 0.32)
/// spawn dedicated OS threads via `std::thread`. No `Handle::current()`
/// call happens at init.
///
/// By default `init` installs the providers in the process-wide
/// `opentelemetry::global` slot. Set [`TelemetryConfig::install_global`] to
/// `false` (or call [`TelemetryConfig::without_global`]) to skip that —
/// useful when the host process already runs its own OTel stack and would
/// otherwise see its globals silently replaced.
pub fn init(config: TelemetryConfig) -> Result<(TelemetryHandle, SdkTracer), InitError> {
    let executable_name = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_default();
    let resource = Resource::builder()
        .with_service_name(config.service_name.clone())
        .with_attribute(KeyValue::new("service.version", config.service_version))
        .with_attribute(KeyValue::new("os.type", std::env::consts::OS))
        .with_attribute(KeyValue::new("host.arch", std::env::consts::ARCH))
        .with_attribute(KeyValue::new("process.pid", i64::from(std::process::id())))
        .with_attribute(KeyValue::new("process.executable.name", executable_name))
        .build();

    let span_exporter = SpanExporter::builder()
        .with_http()
        .build()
        .map_err(|e| InitError::SpanExporter(Box::new(e)))?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    #[cfg(feature = "metrics")]
    let meter_provider = {
        let metric_exporter = MetricExporter::builder()
            .with_http()
            .build()
            .map_err(|e| InitError::MetricExporter(Box::new(e)))?;
        let reader = PeriodicReader::builder(metric_exporter).build();
        SdkMeterProvider::builder()
            .with_reader(reader)
            .with_resource(resource)
            .build()
    };
    #[cfg(not(feature = "metrics"))]
    let _ = resource; // moved into trace provider only; silence unused-binding

    if config.install_global {
        global::set_tracer_provider(tracer_provider.clone());
        #[cfg(feature = "metrics")]
        global::set_meter_provider(meter_provider.clone());
    }

    let tracer = tracer_provider.tracer(config.service_name);

    let handle = TelemetryHandle {
        inner: Some(OtelProviders {
            tracer_provider,
            #[cfg(feature = "metrics")]
            meter_provider,
        }),
    };

    Ok((handle, tracer))
}

/// Build the `tracing` ↔ OTEL bridge layer for a given subscriber type.
/// Call this at the subscriber-composition site so `S` is inferred from the
/// inner subscriber at that point — the layer's `S` parameter must match.
pub fn otel_layer<S>(tracer: SdkTracer) -> OpenTelemetryLayer<S, SdkTracer>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_opentelemetry::layer().with_tracer(tracer)
}
