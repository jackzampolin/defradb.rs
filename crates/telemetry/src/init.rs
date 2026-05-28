//! OTLP exporter setup. Mirrors Go DefraDB's `internal/telemetry/otel.go`.
//!
//! - OTLP/HTTP transport (`reqwest`), traces + metrics signals. Same wire
//!   protocol Go uses via `otlptracehttp` / `otlpmetrichttp`. Default
//!   endpoint is `http://localhost:4318`.
//! - Standard OTEL env vars are honored automatically by `opentelemetry-otlp`
//!   0.32: `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`,
//!   `OTEL_EXPORTER_OTLP_PROTOCOL`, signal-specific overrides, etc.
//! - Resource attributes: `service.name`, `service.version`, `os.type`,
//!   `host.arch`, `process.pid`, `process.executable.name`. Mirrors Go's
//!   `resource.WithOS()` + `resource.WithProcess()` (which we approximate
//!   with `std::env::consts` + `std::process` to avoid an extra dep).
//!
//! Requires a Tokio runtime: `reqwest-client` needs one for the async
//! HTTP exporter.

use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use thiserror::Error;
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::registry::LookupSpan;

use crate::config::TelemetryConfig;
use crate::handle::{OtelProviders, TelemetryHandle};

pub use opentelemetry_sdk::trace::SdkTracer as Tracer;

#[derive(Debug, Error)]
pub enum InitError {
    #[error("failed to build OTLP span exporter: {0}")]
    SpanExporter(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("failed to build OTLP metric exporter: {0}")]
    MetricExporter(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Returns the lifecycle handle and a configured [`SdkTracer`]. The caller
/// wraps the tracer with `tracing_opentelemetry::layer().with_tracer(...)`
/// at its subscriber-composition site so type inference can pick the right
/// `S` parameter for `OpenTelemetryLayer<S, _>`.
pub fn init(config: TelemetryConfig) -> Result<(TelemetryHandle, SdkTracer), InitError> {
    // Resource attributes mirror Go's `resource.WithSchemaURL + WithOS + WithProcess`.
    // We use `std::env::consts` / `std::process` instead of pulling in
    // `opentelemetry-resource-detectors`, which keeps the dep tree small —
    // the attribute set matches what those detectors produce for the common
    // fields (os.type, host.arch, process.pid, process.executable.name).
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

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .build()
        .map_err(|e| InitError::MetricExporter(Box::new(e)))?;
    let reader = PeriodicReader::builder(metric_exporter).build();
    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let tracer = tracer_provider.tracer(config.service_name);

    let handle = TelemetryHandle {
        inner: Some(OtelProviders {
            tracer_provider,
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
