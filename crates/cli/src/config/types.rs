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
/// Note: "rocksdb" and "redb" are accepted as aliases for "badger" for compatibility.
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
            "badger" | "rocksdb" | "redb" => Ok(DatastoreType::Badger),
            "memory" => Ok(DatastoreType::Memory),
            _ => Err(Error::InvalidDatastore(s.to_string())),
        }
    }
}
