//! Logging initialization

use tracing::Level;
use tracing_subscriber::fmt::{self, format::FmtSpan};
use tracing_subscriber::layer::SubscriberExt;
#[cfg(feature = "profiling")]
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry};

#[cfg(feature = "profiling")]
use std::fs::File;
#[cfg(feature = "profiling")]
use std::path::PathBuf;
#[cfg(feature = "profiling")]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "profiling")]
use tracing_chrome::{ChromeLayerBuilder, FlushGuard};

use crate::config::{Config, LogFormat, LogLevel, LogOutput};
use crate::error::{Error, Result};

fn with_default_transport_noise_filters(filter: EnvFilter) -> EnvFilter {
    filter
        .add_directive(
            "iroh_quinn_proto::connection=error"
                .parse()
                .expect("valid tracing directive"),
        )
        .add_directive(
            "noq_proto::connection=error"
                .parse()
                .expect("valid tracing directive"),
        )
}

pub struct LoggingHandle {
    #[cfg(feature = "profiling")]
    profiling: Option<ProfilingTrace>,
    telemetry: telemetry::TelemetryHandle,
}

#[cfg(feature = "profiling")]
struct ProfilingTrace {
    path: PathBuf,
    guard: FlushGuard,
}

impl LoggingHandle {
    fn new(telemetry: telemetry::TelemetryHandle) -> Self {
        Self {
            #[cfg(feature = "profiling")]
            profiling: None,
            telemetry,
        }
    }

    #[cfg(feature = "profiling")]
    fn with_profile(profiling: ProfilingTrace, telemetry: telemetry::TelemetryHandle) -> Self {
        Self {
            profiling: Some(profiling),
            telemetry,
        }
    }

    pub fn finish(self) {
        #[cfg(feature = "profiling")]
        if let Some(profiling) = self.profiling {
            let path = profiling.path;
            drop(profiling.guard);
            eprintln!("Chrome trace written to {}", path.display());
        }
        self.telemetry.shutdown();
    }
}

/// Initialize logging based on configuration.
pub fn init(config: &Config, enable_profiling: bool) -> Result<LoggingHandle> {
    let level = match config.log.level {
        LogLevel::Debug => Level::DEBUG,
        LogLevel::Info => Level::INFO,
        LogLevel::Error => Level::ERROR,
        LogLevel::Fatal => Level::ERROR,
    };

    let filter = with_default_transport_noise_filters(
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.to_string())),
    );

    let builder = fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(config.log.source)
        .with_line_number(config.log.source)
        .with_ansi(!config.log.color_disabled);

    let builder = if config.log.stacktrace {
        builder.with_span_events(FmtSpan::CLOSE)
    } else {
        builder.with_span_events(FmtSpan::NONE)
    };

    match (config.log.format, config.log.output) {
        (LogFormat::Json, LogOutput::Stdout) => init_subscriber(
            filter,
            builder.json().with_writer(std::io::stdout),
            enable_profiling,
            config,
        ),
        (LogFormat::Json, LogOutput::Stderr) => init_subscriber(
            filter,
            builder.json().with_writer(std::io::stderr),
            enable_profiling,
            config,
        ),
        (LogFormat::Text, LogOutput::Stdout) => init_subscriber(
            filter,
            builder.with_writer(std::io::stdout),
            enable_profiling,
            config,
        ),
        (LogFormat::Text, LogOutput::Stderr) => init_subscriber(
            filter,
            builder.with_writer(std::io::stderr),
            enable_profiling,
            config,
        ),
    }
}

fn init_subscriber<L>(
    filter: EnvFilter,
    fmt_layer: L,
    enable_profiling: bool,
    config: &Config,
) -> Result<LoggingHandle>
where
    L: tracing_subscriber::Layer<Registry> + Send + Sync + 'static,
{
    #[cfg(not(feature = "profiling"))]
    if enable_profiling {
        return Err(Error::LoggingInit(
            "profiling support requires building the CLI with `--features profiling`".into(),
        ));
    }

    // Telemetry init. Mirrors Go's default-on behavior: when compiled with
    // `--features otel`, the exporter is active unless `--no-telemetry` /
    // `DEFRA_NO_TELEMETRY` / `telemetry_disabled` opts out. If exporter
    // construction fails (e.g. malformed env var), we log and continue with
    // no telemetry — matches Go's `log.ErrorContextE` + continue path.
    #[cfg(feature = "otel")]
    let (telemetry_handle, tracer) = if config.telemetry_disabled {
        (telemetry::TelemetryHandle::noop(), None)
    } else {
        match telemetry::init(telemetry::TelemetryConfig::default()) {
            Ok((handle, tracer)) => (handle, Some(tracer)),
            Err(err) => {
                eprintln!(
                    "warning: failed to configure OpenTelemetry, continuing without telemetry: {err}"
                );
                (telemetry::TelemetryHandle::noop(), None)
            }
        }
    };

    #[cfg(not(feature = "otel"))]
    let telemetry_handle = {
        let _ = config; // suppress unused-binding warning when otel is off
        telemetry::TelemetryHandle::noop()
    };

    // Suppress repeat "connection refused" spam from OTEL exporter target.
    // Only attach when otel feature is on (otherwise no events to dedup).
    #[cfg(feature = "otel")]
    let fmt_layer = fmt_layer.with_filter(telemetry::OtelDedupFilter::new());

    #[cfg(feature = "profiling")]
    if enable_profiling {
        let (chrome_layer, profiling) = build_chrome_layer()?;
        let registry = tracing_subscriber::registry()
            .with(fmt_layer)
            .with(chrome_layer);
        #[cfg(feature = "otel")]
        let registry = registry.with(tracer.map(telemetry::otel_layer));
        registry
            .with(filter)
            .try_init()
            .map_err(|error| Error::LoggingInit(error.to_string()))?;
        return Ok(LoggingHandle::with_profile(profiling, telemetry_handle));
    }

    let registry = tracing_subscriber::registry().with(fmt_layer);
    #[cfg(feature = "otel")]
    let registry = registry.with(tracer.map(telemetry::otel_layer));
    registry
        .with(filter)
        .try_init()
        .map_err(|error| Error::LoggingInit(error.to_string()))?;

    Ok(LoggingHandle::new(telemetry_handle))
}

#[cfg(feature = "profiling")]
fn build_chrome_layer<S>() -> Result<(tracing_chrome::ChromeLayer<S>, ProfilingTrace)>
where
    S: tracing::Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
{
    let path = trace_output_path()?;
    let file = File::create(&path).map_err(|error| {
        Error::LoggingInit(format!(
            "failed to create profiling trace file {}: {}",
            path.display(),
            error
        ))
    })?;
    let (layer, guard) = ChromeLayerBuilder::new()
        .writer(file)
        .include_args(true)
        .build();

    Ok((layer, ProfilingTrace { path, guard }))
}

#[cfg(feature = "profiling")]
fn trace_output_path() -> Result<PathBuf> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::LoggingInit(format!("failed to read system time: {}", error)))?
        .as_millis();

    Ok(std::env::current_dir()
        .map_err(Error::Io)?
        .join(format!("defra-trace-{}.json", timestamp)))
}
