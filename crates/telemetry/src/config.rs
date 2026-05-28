//! Telemetry configuration.
//!
//! Standard OTEL env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
//! `OTEL_RESOURCE_ATTRIBUTES`) are honored automatically by `opentelemetry-otlp`
//! 0.32, so this struct only carries the few DefraDB-specific defaults.

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub service_name: String,
    pub service_version: String,
}

impl TelemetryConfig {
    pub fn new(service_name: impl Into<String>, service_version: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: service_version.into(),
        }
    }
}

impl Default for TelemetryConfig {
    /// Defaults to Go DefraDB's service name (`"DefraDB"`) and the telemetry
    /// crate's own version. Callers should override `service_version` with
    /// the actual defra binary version (e.g. from `defra_version`).
    fn default() -> Self {
        Self::new("DefraDB", env!("CARGO_PKG_VERSION"))
    }
}
