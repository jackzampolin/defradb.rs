// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Tests for the Config struct and configuration loading

use std::path::PathBuf;

use cli::cli::{Cli, Command};
use cli::commands::VersionArgs;
use cli::config::{Config, DatastoreType, KeyringBackend, LogFormat, LogLevel, LogOutput};
use cli::error::Error;

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
        acp_node_enable: None,
        acp_document_type: None,
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
