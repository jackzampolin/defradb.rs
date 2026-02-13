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
#[serde(rename_all = "kebab-case")]
pub enum KeyringBackend {
    #[default]
    File,
    System,
    SystemdCreds,
}

impl std::fmt::Display for KeyringBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyringBackend::File => write!(f, "file"),
            KeyringBackend::System => write!(f, "system"),
            KeyringBackend::SystemdCreds => write!(f, "systemd-creds"),
        }
    }
}

impl std::str::FromStr for KeyringBackend {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "file" => Ok(KeyringBackend::File),
            "system" => Ok(KeyringBackend::System),
            "systemd-creds" => Ok(KeyringBackend::SystemdCreds),
            _ => Err(Error::InvalidKeyringBackend(s.to_string())),
        }
    }
}

/// Datastore backend options
///
/// Note: "redb" is accepted as an alias for "badger" for compatibility.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DatastoreType {
    #[default]
    Badger,
    Memory,
    Fjall,
    RocksDb,
}

/// Document ACP (Access Control Policy) type options.
///
/// - `None`: No document-level access control (default)
/// - `Local`: Local Zanzibar-based ACP
/// - `SourceHub`: Remote SourceHub ACP (future)
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AcpDocumentType {
    #[default]
    None,
    Local,
    SourceHub,
}

impl std::fmt::Display for AcpDocumentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AcpDocumentType::None => write!(f, "none"),
            AcpDocumentType::Local => write!(f, "local"),
            AcpDocumentType::SourceHub => write!(f, "source-hub"),
        }
    }
}

impl std::str::FromStr for AcpDocumentType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().replace('-', "").as_str() {
            "none" | "" => Ok(AcpDocumentType::None),
            "local" => Ok(AcpDocumentType::Local),
            "sourcehub" => Ok(AcpDocumentType::SourceHub),
            _ => Err(Error::InvalidAcpType(s.to_string())),
        }
    }
}

impl std::fmt::Display for DatastoreType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatastoreType::Badger => write!(f, "badger"),
            DatastoreType::Memory => write!(f, "memory"),
            DatastoreType::Fjall => write!(f, "fjall"),
            DatastoreType::RocksDb => write!(f, "rocksdb"),
        }
    }
}

impl std::str::FromStr for DatastoreType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "badger" | "redb" => Ok(DatastoreType::Badger),
            "memory" => Ok(DatastoreType::Memory),
            "fjall" => Ok(DatastoreType::Fjall),
            "rocksdb" => Ok(DatastoreType::RocksDb),
            _ => Err(Error::InvalidDatastore(s.to_string())),
        }
    }
}
