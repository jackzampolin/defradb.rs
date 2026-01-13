// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Configuration system for DefraDB CLI
//!
//! Configuration is loaded in the following priority order (highest to lowest):
//! 1. CLI flags
//! 2. Environment variables (prefix: DEFRA_)
//! 3. Config file (config.yaml in rootdir)
//! 4. Default values

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cli::Cli;
use crate::error::{Error, Result};

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

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    pub level: LogLevel,
    pub output: LogOutput,
    pub format: LogFormat,
    pub stacktrace: bool,
    pub source: bool,
    pub color_disabled: bool,
    #[serde(default)]
    pub overrides: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            output: LogOutput::Stderr,
            format: LogFormat::Text,
            stacktrace: false,
            source: false,
            color_disabled: false,
            overrides: String::new(),
        }
    }
}

/// API configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub address: String,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default)]
    pub pubkey_path: String,
    #[serde(default)]
    pub privkey_path: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:9181".to_string(),
            allowed_origins: Vec::new(),
            pubkey_path: String::new(),
            privkey_path: String::new(),
        }
    }
}

/// Datastore configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatastoreConfig {
    pub store: DatastoreType,
    pub path: String,
    pub max_txn_retries: u32,
    pub valuelogfilesize: u64,
    pub no_encryption: bool,
    pub no_searchable_encryption: bool,
    pub no_signing: bool,
    pub default_key_type: String,
}

impl Default for DatastoreConfig {
    fn default() -> Self {
        Self {
            store: DatastoreType::Badger,
            path: "data".to_string(),
            max_txn_retries: 5,
            valuelogfilesize: 1 << 30, // 1GB
            no_encryption: false,
            no_searchable_encryption: false,
            no_signing: false,
            default_key_type: "secp256k1".to_string(),
        }
    }
}

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetConfig {
    pub p2p_disabled: bool,
    pub p2p_addresses: Vec<String>,
    #[serde(default)]
    pub peers: Vec<String>,
    pub pubsub_enabled: bool,
    pub relay_enabled: bool,
}

impl Default for NetConfig {
    fn default() -> Self {
        Self {
            p2p_disabled: false,
            p2p_addresses: vec!["/ip4/127.0.0.1/tcp/9171".to_string()],
            peers: Vec::new(),
            pubsub_enabled: true,
            relay_enabled: false,
        }
    }
}

/// Keyring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyringConfig {
    pub backend: KeyringBackend,
    pub path: String,
    pub namespace: String,
    pub disabled: bool,
}

impl Default for KeyringConfig {
    fn default() -> Self {
        Self {
            backend: KeyringBackend::File,
            path: "keys".to_string(),
            namespace: "defradb".to_string(),
            disabled: false,
        }
    }
}

/// Complete configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub rootdir: PathBuf,
    pub log: LogConfig,
    pub api: ApiConfig,
    pub datastore: DatastoreConfig,
    pub net: NetConfig,
    pub keyring: KeyringConfig,
    pub development: bool,
    pub secret_file: String,
    pub telemetry_disabled: bool,
    #[serde(default)]
    pub replicator_retry_intervals: Vec<u32>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rootdir: PathBuf::new(),
            log: LogConfig::default(),
            api: ApiConfig::default(),
            datastore: DatastoreConfig::default(),
            net: NetConfig::default(),
            keyring: KeyringConfig::default(),
            development: false,
            secret_file: ".env".to_string(),
            telemetry_disabled: false,
            replicator_retry_intervals: vec![30, 60, 120, 240, 480, 960, 1920],
        }
    }
}

impl Config {
    /// Load configuration from CLI args, environment, and config file
    pub fn load(cli: &Cli) -> Result<Self> {
        // Determine rootdir
        let rootdir = Self::resolve_rootdir(cli.rootdir.as_deref())?;

        // Load from config file if it exists, otherwise use defaults
        let config_path = rootdir.join("config.yaml");
        let mut config = if config_path.exists() {
            let mut cfg = Self::load_from_file(&config_path)?;
            cfg.rootdir = rootdir.clone();
            cfg
        } else {
            Self {
                rootdir: rootdir.clone(),
                ..Default::default()
            }
        };

        // Override with CLI flags (environment variables are handled by clap)
        config.apply_cli_flags(cli);

        // Make relative paths absolute
        config.resolve_paths();

        Ok(config)
    }

    /// Resolve the rootdir path
    fn resolve_rootdir(cli_rootdir: Option<&str>) -> Result<PathBuf> {
        if let Some(dir) = cli_rootdir {
            return Ok(PathBuf::from(dir));
        }

        // Check environment variable
        if let Ok(dir) = std::env::var("DEFRA_ROOTDIR") {
            return Ok(PathBuf::from(dir));
        }

        // Use default: $HOME/.defradb
        let home = dirs::home_dir().ok_or(Error::HomeDirectory)?;
        Ok(home.join(".defradb"))
    }

    /// Load configuration from a YAML file
    fn load_from_file(path: &Path) -> Result<Self> {
        let contents = fs::read_to_string(path).map_err(|e| Error::ReadConfig {
            path: path.to_path_buf(),
            source: e,
        })?;

        serde_yaml::from_str(&contents).map_err(|e| Error::ParseConfig {
            path: path.to_path_buf(),
            source: e,
        })
    }

    /// Apply CLI flags to config
    fn apply_cli_flags(&mut self, cli: &Cli) {
        // Logging
        if let Some(ref level) = cli.log_level {
            if let Ok(l) = level.parse() {
                self.log.level = l;
            }
        }
        if let Some(ref output) = cli.log_output {
            if let Ok(o) = output.parse() {
                self.log.output = o;
            }
        }
        if let Some(ref format) = cli.log_format {
            if let Ok(f) = format.parse() {
                self.log.format = f;
            }
        }
        if let Some(stacktrace) = cli.log_stacktrace {
            self.log.stacktrace = stacktrace;
        }
        if let Some(source) = cli.log_source {
            self.log.source = source;
        }
        if let Some(ref overrides) = cli.log_overrides {
            self.log.overrides = overrides.clone();
        }
        if let Some(no_color) = cli.no_log_color {
            self.log.color_disabled = no_color;
        }

        // API
        if let Some(ref url) = cli.url {
            self.api.address = url.clone();
        }

        // Keyring
        if let Some(ref namespace) = cli.keyring_namespace {
            self.keyring.namespace = namespace.clone();
        }
        if let Some(ref backend) = cli.keyring_backend {
            if let Ok(b) = backend.parse() {
                self.keyring.backend = b;
            }
        }
        if let Some(ref path) = cli.keyring_path {
            self.keyring.path = path.clone();
        }
        if let Some(no_keyring) = cli.no_keyring {
            self.keyring.disabled = no_keyring;
        }

        // Other
        if let Some(ref secret_file) = cli.secret_file {
            self.secret_file = secret_file.clone();
        }
    }

    /// Make relative paths absolute (relative to rootdir)
    fn resolve_paths(&mut self) {
        let rootdir = &self.rootdir;

        // Datastore path
        if !self.datastore.path.is_empty() && !Path::new(&self.datastore.path).is_absolute() {
            self.datastore.path = rootdir.join(&self.datastore.path).display().to_string();
        }

        // Keyring path
        if !self.keyring.path.is_empty() && !Path::new(&self.keyring.path).is_absolute() {
            self.keyring.path = rootdir.join(&self.keyring.path).display().to_string();
        }

        // API key paths
        if !self.api.pubkey_path.is_empty() && !Path::new(&self.api.pubkey_path).is_absolute() {
            self.api.pubkey_path = rootdir.join(&self.api.pubkey_path).display().to_string();
        }
        if !self.api.privkey_path.is_empty() && !Path::new(&self.api.privkey_path).is_absolute() {
            self.api.privkey_path = rootdir.join(&self.api.privkey_path).display().to_string();
        }
    }

    /// Create the config file with defaults if it doesn't exist
    pub fn create_if_missing(&self) -> Result<()> {
        // Create rootdir if it doesn't exist
        if !self.rootdir.exists() {
            fs::create_dir_all(&self.rootdir).map_err(|e| Error::CreateDirectory {
                path: self.rootdir.clone(),
                source: e,
            })?;
        }

        let config_path = self.rootdir.join("config.yaml");
        if !config_path.exists() {
            let yaml = serde_yaml::to_string(&Self::default())?;
            fs::write(&config_path, yaml).map_err(|e| Error::WriteConfig {
                path: config_path,
                source: e,
            })?;
        }
        Ok(())
    }

    /// Get the full data path
    pub fn data_path(&self) -> PathBuf {
        if Path::new(&self.datastore.path).is_absolute() {
            PathBuf::from(&self.datastore.path)
        } else {
            self.rootdir.join(&self.datastore.path)
        }
    }

    /// Get the full keyring path
    #[allow(dead_code)]
    pub fn keyring_path(&self) -> PathBuf {
        if Path::new(&self.keyring.path).is_absolute() {
            PathBuf::from(&self.keyring.path)
        } else {
            self.rootdir.join(&self.keyring.path)
        }
    }
}
