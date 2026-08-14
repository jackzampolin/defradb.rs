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
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithHttpConfig};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use thiserror::Error;
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::registry::LookupSpan;

use crate::config::TelemetryConfig;
use crate::handle::TelemetryHandle;
use crate::util::{otel_timeout, panic_message};

pub use opentelemetry_sdk::trace::SdkTracer as Tracer;

#[derive(Debug, Error)]
pub enum InitError {
    #[error("failed to build OTLP span exporter: {0}")]
    SpanExporter(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("failed to build OTLP metric exporter: {0}")]
    MetricExporter(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("failed to spawn the telemetry HTTP client init thread: {0}")]
    HttpClientThreadSpawn(#[source] std::io::Error),

    #[error("the telemetry HTTP client init thread panicked: {0}")]
    HttpClientThreadPanic(String),

    #[error("failed to build the telemetry HTTP client: {0}")]
    HttpClientBuild(#[source] reqwest::Error),
}

/// Returns the lifecycle handle and a configured [`SdkTracer`]. The caller
/// wraps the tracer with `tracing_opentelemetry::layer().with_tracer(...)`
/// at its subscriber-composition site so type inference can pick the right
/// `S` parameter for `OpenTelemetryLayer<S, _>`.
///
/// Safe to call from any context (no Tokio runtime required): with the
/// `reqwest-blocking-client` transport selected in this crate's `Cargo.toml`,
/// `BatchSpanProcessor` spawns a dedicated OS thread via `std::thread`. No
/// `Handle::current()` call happens at init.
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
        .with_schema_url(
            // Declare which semantic-conventions version these attributes
            // follow, matching Go's `resource.WithSchemaURL(semconv.SchemaURL)`.
            // The attributes themselves are passed via `.with_*` below, so the
            // attribute iterator here is empty.
            std::iter::empty::<KeyValue>(),
            opentelemetry_semantic_conventions::SCHEMA_URL,
        )
        .with_service_name(config.service_name.clone())
        .with_attribute(KeyValue::new("service.version", config.service_version))
        .with_attribute(KeyValue::new("os.type", std::env::consts::OS))
        .with_attribute(KeyValue::new("host.arch", std::env::consts::ARCH))
        .with_attribute(KeyValue::new("process.pid", i64::from(std::process::id())))
        .with_attribute(KeyValue::new("process.executable.name", executable_name))
        .build();

    let http_client = std::thread::Builder::new()
        .name("telemetry-http-client-init".into())
        .spawn(move || {
            reqwest::blocking::Client::builder()
                .timeout(otel_timeout())
                .build()
        })
        .map_err(InitError::HttpClientThreadSpawn)?
        .join()
        .map_err(|payload| InitError::HttpClientThreadPanic(panic_message(&payload)))?
        .map_err(InitError::HttpClientBuild)?;

    let span_exporter = SpanExporter::builder()
        .with_http()
        .with_http_client(http_client.clone())
        .build()
        .map_err(|e| InitError::SpanExporter(Box::new(e)))?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    let metric_exporter = MetricExporter::builder()
        .with_http()
        .with_http_client(http_client.clone())
        .build()
        .map_err(|e| InitError::MetricExporter(Box::new(e)))?;

    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource)
        .build();
    let metric_installation = crate::metrics::install(&meter_provider);

    if config.install_global {
        global::set_tracer_provider(tracer_provider.clone());
        global::set_meter_provider(meter_provider.clone());
    }

    let tracer = tracer_provider.tracer(config.service_name);

    let handle = TelemetryHandle {
        tracer_provider: Some(tracer_provider),
        meter_provider: Some(meter_provider),
        metric_installation: Some(metric_installation),
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
