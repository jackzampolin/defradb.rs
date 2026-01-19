// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Tests for configuration type enums

use cli::config::{DatastoreType, KeyringBackend, LogFormat, LogLevel, LogOutput};
use cli::error::Error;

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
