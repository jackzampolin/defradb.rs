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

mod sections;
mod types;

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cli::Cli;
use crate::error::{Error, Result};

// Re-export types and sections for external use
pub use sections::{ApiConfig, DatastoreConfig, KeyringConfig, LogConfig, NetConfig};
pub use types::{DatastoreType, LogFormat, LogLevel, LogOutput};
// KeyringBackend is available but not currently used externally
#[allow(unused_imports)]
pub use types::KeyringBackend;

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
        // This now returns an error if any flag value is invalid
        config.apply_cli_flags(cli)?;

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
    ///
    /// Returns an error if any flag value fails to parse.
    fn apply_cli_flags(&mut self, cli: &Cli) -> Result<()> {
        // Logging
        if let Some(ref level) = cli.log_level {
            self.log.level = level.parse()?;
        }
        if let Some(ref output) = cli.log_output {
            self.log.output = output.parse()?;
        }
        if let Some(ref format) = cli.log_format {
            self.log.format = format.parse()?;
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
            self.keyring.backend = backend.parse()?;
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

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command};
    use crate::commands::VersionArgs;
    use crate::error::Error;

    /// Helper to create a minimal Cli for testing apply_cli_flags
    fn cli_with_defaults() -> Cli {
        Cli {
            rootdir: None,
            log_level: None,
            log_output: None,
            log_format: None,
            log_stacktrace: None,
            log_source: None,
            log_overrides: None,
            no_log_color: None,
            url: None,
            keyring_namespace: None,
            keyring_backend: None,
            keyring_path: None,
            no_keyring: None,
            source_hub_address: None,
            secret_file: None,
            command: Command::Version(VersionArgs {
                format: "text".to_string(),
                full: false,
            }),
        }
    }

    #[test]
    fn test_apply_cli_flags_invalid_log_level_returns_error() {
        let mut config = Config::default();
        let mut cli = cli_with_defaults();
        cli.log_level = Some("invalid_level".to_string());

        let result = config.apply_cli_flags(&cli);
        assert!(matches!(result, Err(Error::InvalidLogLevel(s)) if s == "invalid_level"));
    }

    #[test]
    fn test_apply_cli_flags_invalid_log_output_returns_error() {
        let mut config = Config::default();
        let mut cli = cli_with_defaults();
        cli.log_output = Some("file".to_string());

        let result = config.apply_cli_flags(&cli);
        assert!(matches!(result, Err(Error::InvalidLogOutput(s)) if s == "file"));
    }

    #[test]
    fn test_apply_cli_flags_invalid_log_format_returns_error() {
        let mut config = Config::default();
        let mut cli = cli_with_defaults();
        cli.log_format = Some("xml".to_string());

        let result = config.apply_cli_flags(&cli);
        assert!(matches!(result, Err(Error::InvalidLogFormat(s)) if s == "xml"));
    }

    #[test]
    fn test_apply_cli_flags_invalid_keyring_backend_returns_error() {
        let mut config = Config::default();
        let mut cli = cli_with_defaults();
        cli.keyring_backend = Some("vault".to_string());

        let result = config.apply_cli_flags(&cli);
        assert!(matches!(result, Err(Error::InvalidKeyringBackend(s)) if s == "vault"));
    }

    #[test]
    fn test_apply_cli_flags_valid_values_succeed() {
        let mut config = Config::default();
        let mut cli = cli_with_defaults();
        cli.log_level = Some("debug".to_string());
        cli.log_output = Some("stdout".to_string());
        cli.log_format = Some("json".to_string());
        cli.keyring_backend = Some("system".to_string());
        cli.url = Some("0.0.0.0:8080".to_string());

        let result = config.apply_cli_flags(&cli);
        assert!(result.is_ok());
        assert_eq!(config.log.level, LogLevel::Debug);
        assert_eq!(config.log.output, LogOutput::Stdout);
        assert_eq!(config.log.format, LogFormat::Json);
        assert_eq!(config.keyring.backend, KeyringBackend::System);
        assert_eq!(config.api.address, "0.0.0.0:8080");
    }

    #[test]
    fn test_config_defaults() {
        let config = Config::default();
        assert!(config.rootdir.as_os_str().is_empty());
        assert_eq!(config.api.address, "127.0.0.1:9181");
        assert_eq!(config.datastore.store, DatastoreType::Badger);
        assert!(!config.development);
        assert_eq!(config.secret_file, ".env");
    }

    #[test]
    fn test_resolve_paths_relative_to_rootdir() {
        let mut config = Config::default();
        config.rootdir = PathBuf::from("/home/user/.defradb");
        config.datastore.path = "data".to_string();
        config.keyring.path = "keys".to_string();
        config.resolve_paths();

        assert_eq!(config.datastore.path, "/home/user/.defradb/data");
        assert_eq!(config.keyring.path, "/home/user/.defradb/keys");
    }

    #[test]
    fn test_resolve_paths_absolute_unchanged() {
        let mut config = Config::default();
        config.rootdir = PathBuf::from("/home/user/.defradb");
        config.datastore.path = "/custom/data/path".to_string();
        config.keyring.path = "/custom/keys/path".to_string();
        config.resolve_paths();

        assert_eq!(config.datastore.path, "/custom/data/path");
        assert_eq!(config.keyring.path, "/custom/keys/path");
    }

    #[test]
    fn test_data_path_relative() {
        let mut config = Config::default();
        config.rootdir = PathBuf::from("/root");
        config.datastore.path = "data".to_string();

        assert_eq!(config.data_path(), PathBuf::from("/root/data"));
    }

    #[test]
    fn test_data_path_absolute() {
        let mut config = Config::default();
        config.rootdir = PathBuf::from("/root");
        config.datastore.path = "/custom/data".to_string();

        assert_eq!(config.data_path(), PathBuf::from("/custom/data"));
    }

    #[test]
    fn test_keyring_path_relative() {
        let mut config = Config::default();
        config.rootdir = PathBuf::from("/root");
        config.keyring.path = "keys".to_string();

        assert_eq!(config.keyring_path(), PathBuf::from("/root/keys"));
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let original = Config::default();
        let yaml = serde_yaml::to_string(&original).unwrap();
        let deserialized: Config = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(original.log.level, deserialized.log.level);
        assert_eq!(original.datastore.store, deserialized.datastore.store);
        assert_eq!(original.api.address, deserialized.api.address);
        assert_eq!(original.keyring.backend, deserialized.keyring.backend);
    }
}
