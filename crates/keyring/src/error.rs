// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Keyring error types

use thiserror::Error;

/// Keyring-specific errors
#[derive(Debug, Error)]
pub enum Error {
    #[error("key not found: {0}")]
    NotFound(String),

    #[error("listing keys is not supported by OS keyring")]
    SystemKeyringListNotSupported,

    #[error("keyring secret not set: DEFRA_KEYRING_SECRET environment variable is required")]
    SecretNotSet,

    #[error("encryption failed: {0}")]
    Encryption(String),

    #[error("decryption failed: {0}")]
    Decryption(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for keyring operations
pub type Result<T> = std::result::Result<T, Error>;
