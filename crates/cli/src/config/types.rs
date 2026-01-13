// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Configuration type enums

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// Log level options
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    #[default]
    Info,
    Error,
    Fatal,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Error => write!(f, "error"),
            LogLevel::Fatal => write!(f, "fatal"),
        }
    }
}

impl std::str::FromStr for LogLevel {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "debug" => Ok(LogLevel::Debug),
            "info" => Ok(LogLevel::Info),
            "error" => Ok(LogLevel::Error),
            "fatal" => Ok(LogLevel::Fatal),
            _ => Err(Error::InvalidLogLevel(s.to_string())),
        }
    }
}

/// Log format options
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    #[default]
    Text,
    Json,
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogFormat::Text => write!(f, "text"),
            LogFormat::Json => write!(f, "json"),
        }
    }
}

impl std::str::FromStr for LogFormat {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(LogFormat::Text),
            "json" => Ok(LogFormat::Json),
            _ => Err(Error::InvalidLogFormat(s.to_string())),
        }
    }
}

/// Log output options
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum LogOutput {
    #[default]
    Stderr,
    Stdout,
}

impl std::fmt::Display for LogOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogOutput::Stderr => write!(f, "stderr"),
            LogOutput::Stdout => write!(f, "stdout"),
        }
    }
}

impl std::str::FromStr for LogOutput {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "stderr" => Ok(LogOutput::Stderr),
            "stdout" => Ok(LogOutput::Stdout),
            _ => Err(Error::InvalidLogOutput(s.to_string())),
        }
    }
}

/// Keyring backend options
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum KeyringBackend {
    #[default]
    File,
    System,
}

impl std::fmt::Display for KeyringBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyringBackend::File => write!(f, "file"),
            KeyringBackend::System => write!(f, "system"),
        }
    }
}

impl std::str::FromStr for KeyringBackend {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "file" => Ok(KeyringBackend::File),
            "system" => Ok(KeyringBackend::System),
            _ => Err(Error::InvalidKeyringBackend(s.to_string())),
        }
    }
}

/// Datastore backend options
///
/// Note: "rocksdb" is accepted as an alias for "badger" for compatibility.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DatastoreType {
    #[default]
    Badger,
    Memory,
}

impl std::fmt::Display for DatastoreType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatastoreType::Badger => write!(f, "badger"),
            DatastoreType::Memory => write!(f, "memory"),
        }
    }
}

impl std::str::FromStr for DatastoreType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "badger" | "rocksdb" => Ok(DatastoreType::Badger),
            "memory" => Ok(DatastoreType::Memory),
            _ => Err(Error::InvalidDatastore(s.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // LogLevel tests
    #[test]
    fn test_log_level_from_str_valid() {
        assert_eq!("debug".parse::<LogLevel>().unwrap(), LogLevel::Debug);
        assert_eq!("info".parse::<LogLevel>().unwrap(), LogLevel::Info);
        assert_eq!("error".parse::<LogLevel>().unwrap(), LogLevel::Error);
        assert_eq!("fatal".parse::<LogLevel>().unwrap(), LogLevel::Fatal);
        // Case insensitive
        assert_eq!("DEBUG".parse::<LogLevel>().unwrap(), LogLevel::Debug);
        assert_eq!("Info".parse::<LogLevel>().unwrap(), LogLevel::Info);
    }

    #[test]
    fn test_log_level_from_str_invalid() {
        let result: Result<LogLevel, _> = "invalid".parse();
        assert!(matches!(result, Err(Error::InvalidLogLevel(s)) if s == "invalid"));

        let result: Result<LogLevel, _> = "trace".parse();
        assert!(matches!(result, Err(Error::InvalidLogLevel(s)) if s == "trace"));

        let result: Result<LogLevel, _> = "warn".parse();
        assert!(matches!(result, Err(Error::InvalidLogLevel(s)) if s == "warn"));
    }

    #[test]
    fn test_log_level_display_roundtrip() {
        for level in [
            LogLevel::Debug,
            LogLevel::Info,
            LogLevel::Error,
            LogLevel::Fatal,
        ] {
            let display = level.to_string();
            let parsed: LogLevel = display.parse().unwrap();
            assert_eq!(level, parsed);
        }
    }

    // LogFormat tests
    #[test]
    fn test_log_format_from_str_valid() {
        assert_eq!("text".parse::<LogFormat>().unwrap(), LogFormat::Text);
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("TEXT".parse::<LogFormat>().unwrap(), LogFormat::Text);
    }

    #[test]
    fn test_log_format_from_str_invalid() {
        let result: Result<LogFormat, _> = "yaml".parse();
        assert!(matches!(result, Err(Error::InvalidLogFormat(s)) if s == "yaml"));

        let result: Result<LogFormat, _> = "xml".parse();
        assert!(matches!(result, Err(Error::InvalidLogFormat(s)) if s == "xml"));
    }

    #[test]
    fn test_log_format_display_roundtrip() {
        for format in [LogFormat::Text, LogFormat::Json] {
            let display = format.to_string();
            let parsed: LogFormat = display.parse().unwrap();
            assert_eq!(format, parsed);
        }
    }

    // LogOutput tests
    #[test]
    fn test_log_output_from_str_valid() {
        assert_eq!("stderr".parse::<LogOutput>().unwrap(), LogOutput::Stderr);
        assert_eq!("stdout".parse::<LogOutput>().unwrap(), LogOutput::Stdout);
        assert_eq!("STDERR".parse::<LogOutput>().unwrap(), LogOutput::Stderr);
    }

    #[test]
    fn test_log_output_from_str_invalid() {
        let result: Result<LogOutput, _> = "file".parse();
        assert!(matches!(result, Err(Error::InvalidLogOutput(s)) if s == "file"));

        let result: Result<LogOutput, _> = "/var/log/defra.log".parse();
        assert!(matches!(result, Err(Error::InvalidLogOutput(_))));
    }

    #[test]
    fn test_log_output_display_roundtrip() {
        for output in [LogOutput::Stderr, LogOutput::Stdout] {
            let display = output.to_string();
            let parsed: LogOutput = display.parse().unwrap();
            assert_eq!(output, parsed);
        }
    }

    // KeyringBackend tests
    #[test]
    fn test_keyring_backend_from_str_valid() {
        assert_eq!(
            "file".parse::<KeyringBackend>().unwrap(),
            KeyringBackend::File
        );
        assert_eq!(
            "system".parse::<KeyringBackend>().unwrap(),
            KeyringBackend::System
        );
        assert_eq!(
            "FILE".parse::<KeyringBackend>().unwrap(),
            KeyringBackend::File
        );
    }

    #[test]
    fn test_keyring_backend_from_str_invalid() {
        let result: Result<KeyringBackend, _> = "vault".parse();
        assert!(matches!(result, Err(Error::InvalidKeyringBackend(s)) if s == "vault"));

        let result: Result<KeyringBackend, _> = "aws".parse();
        assert!(matches!(result, Err(Error::InvalidKeyringBackend(s)) if s == "aws"));
    }

    #[test]
    fn test_keyring_backend_display_roundtrip() {
        for backend in [KeyringBackend::File, KeyringBackend::System] {
            let display = backend.to_string();
            let parsed: KeyringBackend = display.parse().unwrap();
            assert_eq!(backend, parsed);
        }
    }

    // DatastoreType tests
    #[test]
    fn test_datastore_type_from_str_valid() {
        assert_eq!(
            "badger".parse::<DatastoreType>().unwrap(),
            DatastoreType::Badger
        );
        assert_eq!(
            "memory".parse::<DatastoreType>().unwrap(),
            DatastoreType::Memory
        );
        assert_eq!(
            "BADGER".parse::<DatastoreType>().unwrap(),
            DatastoreType::Badger
        );
    }

    #[test]
    fn test_datastore_type_accepts_rocksdb_alias() {
        // rocksdb is an alias for badger (Rust impl uses RocksDB)
        assert_eq!(
            "rocksdb".parse::<DatastoreType>().unwrap(),
            DatastoreType::Badger
        );
        assert_eq!(
            "RocksDB".parse::<DatastoreType>().unwrap(),
            DatastoreType::Badger
        );
    }

    #[test]
    fn test_datastore_type_from_str_invalid() {
        let result: Result<DatastoreType, _> = "postgres".parse();
        assert!(matches!(result, Err(Error::InvalidDatastore(s)) if s == "postgres"));

        let result: Result<DatastoreType, _> = "sqlite".parse();
        assert!(matches!(result, Err(Error::InvalidDatastore(s)) if s == "sqlite"));

        let result: Result<DatastoreType, _> = "bader".parse(); // typo
        assert!(matches!(result, Err(Error::InvalidDatastore(s)) if s == "bader"));
    }

    #[test]
    fn test_datastore_type_display_roundtrip() {
        for store in [DatastoreType::Badger, DatastoreType::Memory] {
            let display = store.to_string();
            let parsed: DatastoreType = display.parse().unwrap();
            assert_eq!(store, parsed);
        }
    }
}
