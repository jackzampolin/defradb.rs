//! Tests for the Config struct and configuration loading

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use clap::{CommandFactory, Parser};
use cli::cli::{Cli, Command};
use cli::commands::VersionArgs;
use cli::config::{
    AcpDocumentType, Config, DatastoreType, KeyringBackend, LogFormat, LogLevel, LogOutput,
};
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
        #[cfg(feature = "sourcehub")]
        source_hub_address: None,
        #[cfg(feature = "sourcehub")]
        source_hub_comet_address: None,
        #[cfg(feature = "sourcehub")]
        source_hub_events_ws: None,
        #[cfg(feature = "sourcehub")]
        source_hub_chain_id: None,
        #[cfg(feature = "sourcehub")]
        hub_rs_address: None,
        secret_file: None,
        no_telemetry: None,
        development: None,
        acp_node_enable: None,
        acp_document_type: None,
        command: Command::Version(VersionArgs {
            format: "text".to_string(),
            full: false,
        }),
    }
}

#[test]
fn test_apply_cli_flags_no_telemetry_sets_telemetry_disabled() {
    // Coverage for the global Cli::no_telemetry → config.telemetry_disabled
    // plumbing. Issue #977 deleted the duplicate StartArgs version (which
    // had its own assertion in start_tests.rs); this is the replacement
    // covering the canonical path.
    let mut config = Config::default();
    assert!(!config.telemetry_disabled, "default expected to be false");
    let mut cli = cli_with_defaults();
    cli.no_telemetry = Some(true);

    config
        .apply_cli_flags(&cli)
        .expect("apply_cli_flags should succeed");
    assert!(
        config.telemetry_disabled,
        "--no-telemetry / DEFRA_NO_TELEMETRY=true should flip config.telemetry_disabled"
    );
}

#[test]
fn test_apply_cli_flags_no_telemetry_false_re_enables_telemetry() {
    // Seed with telemetry_disabled = true (e.g. from a config file) so the
    // assertion actually exercises the write path: a CLI `--no-telemetry=false`
    // must be able to override a disabled config back to enabled.
    let mut config = Config {
        telemetry_disabled: true,
        ..Config::default()
    };
    let mut cli = cli_with_defaults();
    cli.no_telemetry = Some(false);

    config
        .apply_cli_flags(&cli)
        .expect("apply_cli_flags should succeed");
    assert!(
        !config.telemetry_disabled,
        "--no-telemetry=false should override a disabled config back to enabled"
    );
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
    cli.log_stacktrace = Some(true);
    cli.log_source = Some(true);
    cli.log_overrides = Some("db,level=debug".to_string());
    cli.no_log_color = Some(true);
    cli.keyring_backend = Some("system".to_string());
    cli.keyring_namespace = Some("test-defradb".to_string());
    cli.keyring_path = Some("/tmp/test-keys".to_string());
    cli.no_keyring = Some(true);
    cli.url = Some("0.0.0.0:8080".to_string());
    cli.secret_file = Some("test.env".to_string());
    cli.no_telemetry = Some(true);
    cli.development = Some(true);
    cli.acp_node_enable = Some(true);
    cli.acp_document_type = Some("local".to_string());
    #[cfg(feature = "sourcehub")]
    {
        cli.source_hub_address = Some("http://localhost:1317".to_string());
        cli.source_hub_comet_address = Some("http://localhost:26657".to_string());
        cli.source_hub_events_ws = Some("ws://localhost:26657/websocket".to_string());
        cli.source_hub_chain_id = Some("sourcehub-test".to_string());
        cli.hub_rs_address = Some("http://localhost:8545".to_string());
    }

    let result = config.apply_cli_flags(&cli);
    assert!(result.is_ok());
    assert_eq!(config.log.level, LogLevel::Debug);
    assert_eq!(config.log.output, LogOutput::Stdout);
    assert_eq!(config.log.format, LogFormat::Json);
    assert!(config.log.stacktrace);
    assert!(config.log.source);
    assert_eq!(config.log.overrides, "db,level=debug");
    assert!(config.log.color_disabled);
    assert_eq!(config.keyring.backend, KeyringBackend::System);
    assert_eq!(config.keyring.namespace, "test-defradb");
    assert_eq!(config.keyring.path, "/tmp/test-keys");
    assert!(config.keyring.disabled);
    assert_eq!(config.api.address, "0.0.0.0:8080");
    assert_eq!(config.secret_file, "test.env");
    assert!(config.telemetry_disabled);
    assert!(config.development);
    assert!(config.acp.node_enable);
    assert_eq!(config.acp.document_type, AcpDocumentType::Local);
    #[cfg(feature = "sourcehub")]
    {
        assert_eq!(config.acp.sourcehub_address, "http://localhost:1317");
        assert_eq!(config.acp.sourcehub_comet_address, "http://localhost:26657");
        assert_eq!(
            config.acp.sourcehub_events_ws,
            "ws://localhost:26657/websocket"
        );
        assert_eq!(config.acp.sourcehub_chain_id, "sourcehub-test");
        assert_eq!(config.acp.hub_rs_address, "http://localhost:8545");
    }
}

// These are all asserted above to change the typed node configuration.
const CONFIG_BACKED_GLOBAL_FLAGS: &[&str] = &[
    "log-level",
    "log-output",
    "log-format",
    "log-stacktrace",
    "log-source",
    "log-overrides",
    "no-log-color",
    "url",
    "keyring-namespace",
    "keyring-backend",
    "keyring-path",
    "no-keyring",
    #[cfg(feature = "sourcehub")]
    "source-hub-address",
    #[cfg(feature = "sourcehub")]
    "source-hub-comet-address",
    #[cfg(feature = "sourcehub")]
    "source-hub-events-ws",
    #[cfg(feature = "sourcehub")]
    "source-hub-chain-id",
    #[cfg(feature = "sourcehub")]
    "hub-rs-address",
    "secret-file",
    "no-telemetry",
    "development",
    "node-acp-enable",
    "document-acp-type",
];

// `rootdir` selects the config file before Config exists, so it cannot be
// covered by the apply-to-config assertion above.
const DIRECT_GLOBAL_FLAGS: &[(&str, fn())] = &[("rootdir", rootdir_selects_config_file)];

fn rootdir_selects_config_file() {
    let rootdir = tempfile::tempdir().unwrap();
    let mut stored = Config::default();
    stored.datastore.path = "selected-data".into();
    fs::write(
        rootdir.path().join("config.yaml"),
        serde_yaml::to_string(&stored).unwrap(),
    )
    .unwrap();
    let cli = Cli::try_parse_from([
        "defra",
        "--rootdir",
        rootdir.path().to_str().unwrap(),
        "--secret-file",
        rootdir.path().join("absent.env").to_str().unwrap(),
        "version",
    ])
    .unwrap();

    let config = Config::load(&cli).unwrap();
    assert_eq!(config.rootdir, rootdir.path());
    assert_eq!(config.data_path(), rootdir.path().join("selected-data"));
}

#[test]
fn every_global_config_flag_has_an_enforcement_path() {
    let mut command = Cli::command();
    command.build();
    let actual: BTreeSet<_> = command
        .get_arguments()
        .filter_map(|arg| arg.get_long())
        .filter(|name| !matches!(*name, "help" | "version"))
        .collect();

    let classified: BTreeSet<_> = CONFIG_BACKED_GLOBAL_FLAGS
        .iter()
        .copied()
        .chain(DIRECT_GLOBAL_FLAGS.iter().map(|(name, _)| *name))
        .collect();

    assert_eq!(
        classified.len(),
        CONFIG_BACKED_GLOBAL_FLAGS.len() + DIRECT_GLOBAL_FLAGS.len(),
        "global flag enforcement inventory contains a duplicate"
    );
    for (_, check) in DIRECT_GLOBAL_FLAGS {
        check();
    }
    assert_eq!(
        actual, classified,
        "classify every global config flag by its config assertion or direct enforcement path"
    );
}

#[test]
fn test_config_defaults() {
    let config = Config::default();
    assert!(config.rootdir.as_os_str().is_empty());
    assert_eq!(config.api.address, "127.0.0.1:9181");
    assert_eq!(config.embedding.url, "");
    assert_eq!(config.embedding.model, "");
    assert_eq!(config.embedding.api_key_env, "OPENAI_API_KEY");
    assert_eq!(config.datastore.store, DatastoreType::Regolith);
    assert!(!config.development);
    assert_eq!(config.secret_file, ".env");
}

#[test]
fn test_load_secret_file() {
    let rootdir = tempfile::tempdir().unwrap();
    let secret_file = rootdir.path().join("secrets.env");
    let variable = format!("DEFRA_SECRET_FILE_TEST_{}", std::process::id());
    fs::write(&secret_file, format!("{variable}=loaded\n")).unwrap();
    let mut cli = cli_with_defaults();
    cli.rootdir = Some(rootdir.path().display().to_string());
    cli.secret_file = Some(secret_file.display().to_string());

    Config::load(&cli).unwrap();

    assert_eq!(std::env::var(&variable).unwrap(), "loaded");
    std::env::remove_var(variable);
}

#[test]
fn test_missing_secret_file_is_ignored() {
    let rootdir = tempfile::tempdir().unwrap();
    let mut cli = cli_with_defaults();
    cli.rootdir = Some(rootdir.path().display().to_string());
    cli.secret_file = Some(rootdir.path().join("missing.env").display().to_string());

    Config::load(&cli).unwrap();
}

#[test]
fn test_invalid_secret_file_returns_error() {
    let rootdir = tempfile::tempdir().unwrap();
    let secret_file = rootdir.path().join("invalid.env");
    let secret = "must-not-appear";
    fs::write(&secret_file, format!("SECRET='{secret}\n")).unwrap();
    let mut cli = cli_with_defaults();
    cli.rootdir = Some(rootdir.path().display().to_string());
    cli.secret_file = Some(secret_file.display().to_string());

    let result = Config::load(&cli);

    let error = result.unwrap_err();
    assert!(matches!(
        &error,
        Error::ParseSecretFile { path, line: 1 } if path == &secret_file
    ));
    assert!(!error.to_string().contains(secret));
}

#[test]
fn test_resolve_paths_relative_to_rootdir() {
    let mut config = Config {
        rootdir: PathBuf::from("/home/user/.defradb"),
        ..Default::default()
    };
    config.datastore.path = "data".to_string();
    config.keyring.path = "keys".to_string();
    config.resolve_paths();

    assert_eq!(config.datastore.path, "/home/user/.defradb/data");
    assert_eq!(config.keyring.path, "/home/user/.defradb/keys");
}

#[test]
fn test_resolve_paths_absolute_unchanged() {
    let mut config = Config {
        rootdir: PathBuf::from("/home/user/.defradb"),
        ..Default::default()
    };
    config.datastore.path = "/custom/data/path".to_string();
    config.keyring.path = "/custom/keys/path".to_string();
    config.resolve_paths();

    assert_eq!(config.datastore.path, "/custom/data/path");
    assert_eq!(config.keyring.path, "/custom/keys/path");
}

#[test]
fn test_data_path_relative() {
    let mut config = Config {
        rootdir: PathBuf::from("/root"),
        ..Default::default()
    };
    config.datastore.path = "data".to_string();

    assert_eq!(config.data_path(), PathBuf::from("/root/data"));
}

#[test]
fn test_data_path_absolute() {
    let mut config = Config {
        rootdir: PathBuf::from("/root"),
        ..Default::default()
    };
    config.datastore.path = "/custom/data".to_string();

    assert_eq!(config.data_path(), PathBuf::from("/custom/data"));
}

#[test]
fn test_keyring_path_relative() {
    let mut config = Config {
        rootdir: PathBuf::from("/root"),
        ..Default::default()
    };
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
    assert_eq!(
        original.api.query_max_depth,
        deserialized.api.query_max_depth
    );
    assert_eq!(
        original.api.query_max_width,
        deserialized.api.query_max_width
    );
    assert_eq!(
        original.api.query_max_filter_depth,
        deserialized.api.query_max_filter_depth
    );
    assert_eq!(original.embedding.url, deserialized.embedding.url);
    assert_eq!(original.embedding.model, deserialized.embedding.model);
    assert_eq!(
        original.embedding.api_key_env,
        deserialized.embedding.api_key_env
    );
    assert_eq!(original.keyring.backend, deserialized.keyring.backend);
}
