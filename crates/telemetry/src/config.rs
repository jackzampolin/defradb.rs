//! Telemetry configuration.
//!
//! Standard OTEL env vars (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`,
//! `OTEL_RESOURCE_ATTRIBUTES`) are honored automatically by `opentelemetry-otlp`
//! 0.32, so this struct only carries the few DefraDB-specific defaults.

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub service_name: String,
    pub service_version: String,
    /// When `true` (default) `init` registers the providers as the
    /// process-wide globals via `opentelemetry::global::set_*`. Set to
    /// `false` for embedded use where the host process owns its own OTel
    /// globals and `init` would otherwise silently clobber them (e.g.
    /// Gents runs its own OTel stack and just wants DefraDB-emitted
    /// spans flushed at shutdown).
    pub install_global: bool,
}

impl TelemetryConfig {
    /// `service_name` should match what the operator's OTLP backend expects
    /// to dimension on (Go DefraDB uses literal `"DefraDB"`).
    /// `service_version` should be the actual binary version — sourcing it
    /// from `defra_version::VersionInfo::new().version` is the convention.
    pub fn new(service_name: impl Into<String>, service_version: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: service_version.into(),
            install_global: true,
        }
    }

    /// Don't touch `opentelemetry::global::set_*`. The returned handle still
    /// flushes its own providers on shutdown.
    pub fn without_global(mut self) -> Self {
        self.install_global = false;
        self
    }
}

// No `Default` impl: the only sensible default for `service_version` would
// be the telemetry crate's own `CARGO_PKG_VERSION`, which never matches the
// binary version the operator cares about. Forcing callers through `new`
// keeps that mismatch from happening silently.
