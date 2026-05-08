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
pub use sections::{
    AcpConfig, ApiConfig, DatastoreConfig, EmbeddingConfig, KeyringConfig, LogConfig, NetConfig,
};
pub use types::{AcpDocumentType, DatastoreType, LogFormat, LogLevel, LogOutput, TransportType};
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
    #[serde(default)]
    pub embedding: EmbeddingConfig,
    pub datastore: DatastoreConfig,
    pub net: NetConfig,
    pub keyring: KeyringConfig,
    #[serde(default)]
    pub acp: AcpConfig,
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
            embedding: EmbeddingConfig::default(),
            datastore: DatastoreConfig::default(),
            net: NetConfig::default(),
            keyring: KeyringConfig::default(),
            acp: AcpConfig::default(),
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

        // Validate the final configuration
        config.validate()?;

        Ok(config)
    }

    /// Validate the complete configuration
    pub fn validate(&self) -> Result<()> {
        self.api.validate()?;
        self.net.validate()?;
        self.acp.validate()?;
        Ok(())
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
    pub fn apply_cli_flags(&mut self, cli: &Cli) -> Result<()> {
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
        if let Some(development) = cli.development {
            self.development = development;
        }

        // ACP
        if let Some(node_enable) = cli.acp_node_enable {
            self.acp.node_enable = node_enable;
        }
        if let Some(ref doc_type) = cli.acp_document_type {
            self.acp.document_type = doc_type.parse()?;
        }

        // SourceHub
        if let Some(ref addr) = cli.source_hub_address {
            self.acp.sourcehub_address = addr.clone();
        }
        if let Some(ref addr) = cli.source_hub_comet_address {
            self.acp.sourcehub_comet_address = addr.clone();
        }
        if let Some(ref id) = cli.source_hub_chain_id {
            self.acp.sourcehub_chain_id = id.clone();
        }

        // hub.rs
        if let Some(ref addr) = cli.hub_rs_address {
            self.acp.hub_rs_address = addr.clone();
        }

        Ok(())
    }

    /// Make relative paths absolute (relative to rootdir)
    pub fn resolve_paths(&mut self) {
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
