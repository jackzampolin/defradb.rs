// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Logging initialization

use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use crate::config::{Config, LogFormat, LogLevel, LogOutput};
use crate::error::{Error, Result};

/// Initialize logging based on configuration
pub fn init(config: &Config) -> Result<()> {
    let level = match config.log.level {
        LogLevel::Debug => Level::DEBUG,
        LogLevel::Info => Level::INFO,
        LogLevel::Error => Level::ERROR,
        LogLevel::Fatal => Level::ERROR, // tracing doesn't have FATAL, use ERROR
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level.to_string()));

    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(config.log.source)
        .with_line_number(config.log.source)
        .with_ansi(!config.log.color_disabled);

    // Configure span events based on stacktrace setting
    let builder = if config.log.stacktrace {
        builder.with_span_events(FmtSpan::CLOSE)
    } else {
        builder.with_span_events(FmtSpan::NONE)
    };

    // Select output
    let result = match (config.log.format, config.log.output) {
        (LogFormat::Json, LogOutput::Stdout) => {
            builder.json().with_writer(std::io::stdout).try_init()
        }
        (LogFormat::Json, LogOutput::Stderr) => {
            builder.json().with_writer(std::io::stderr).try_init()
        }
        (LogFormat::Text, LogOutput::Stdout) => builder.with_writer(std::io::stdout).try_init(),
        (LogFormat::Text, LogOutput::Stderr) => builder.with_writer(std::io::stderr).try_init(),
    };

    result.map_err(|e| Error::LoggingInit(e.to_string()))
}
