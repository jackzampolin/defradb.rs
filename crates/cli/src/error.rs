// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! CLI error types

use std::path::PathBuf;

use thiserror::Error;

/// CLI result type
pub type Result<T> = std::result::Result<T, Error>;

/// CLI errors
#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to determine home directory")]
    HomeDirectory,

    #[error("failed to create directory {path}: {source}")]
    CreateDirectory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to read config file {path}: {source}")]
    ReadConfig {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to parse config file {path}: {source}")]
    ParseConfig {
        path: PathBuf,
        source: serde_yaml::Error,
    },

    #[error("failed to write config file {path}: {source}")]
    WriteConfig {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to serialize config: {0}")]
    SerializeConfig(#[from] serde_yaml::Error),

    #[error("invalid log level: {0}")]
    InvalidLogLevel(String),

    #[error("invalid log format: {0}")]
    InvalidLogFormat(String),

    #[error("invalid log output: {0}")]
    InvalidLogOutput(String),

    #[error("invalid keyring backend: {0}")]
    InvalidKeyringBackend(String),

    #[error("invalid datastore type: {0}")]
    InvalidDatastore(String),

    #[error("invalid ACP type: {0}")]
    InvalidAcpType(String),

    #[error("storage error: {0}")]
    Storage(#[from] storage::Error),

    #[error("P2P error: {0}")]
    P2P(#[from] p2p::Error),

    #[error("crypto error: {0}")]
    Crypto(#[from] crypto::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("logging initialization failed: {0}")]
    LoggingInit(String),

    #[error("signal handling error: {0}")]
    Signal(String),

    #[error("invalid multiaddr: {0}")]
    InvalidMultiaddr(String),

    #[error("JSON serialization error: {0}")]
    JsonSerialization(#[from] serde_json::Error),

    #[error("invalid API address '{0}': {1}")]
    InvalidApiAddress(String, String),

    #[error(
        "incomplete TLS configuration: both pubkey_path and privkey_path must be set, or neither"
    )]
    IncompleteTlsConfig,

    #[error("keyring error: {0}")]
    Keyring(String),

    #[error("identity error: {0}")]
    Identity(#[from] identity::Error),

    #[error("invalid identity: {0}")]
    InvalidIdentity(String),

    #[error("HTTP request failed: {0}")]
    HttpRequest(#[from] reqwest::Error),

    #[error("server returned error: {0}")]
    Server(String),

    #[error("failed to read file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("missing required input: {0}")]
    MissingInput(String),

    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),

    #[error("failed to initialize HTTP client: {0}")]
    HttpClientInit(String),

    #[error("invalid URL '{0}': {1}")]
    InvalidUrl(String, String),
}
