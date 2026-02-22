//! Error types for lens operations.

use thiserror::Error;

/// Result type for lens operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur during lens operations.
#[derive(Debug, Error)]
pub enum Error {
    /// WASM module failed to load.
    #[error("failed to load WASM module: {0}")]
    WasmLoad(String),

    /// WASM module execution failed.
    #[error("WASM transform execution failed: {0}")]
    WasmExecution(String),

    /// Transform not found.
    #[error("transform not found: {0}")]
    TransformNotFound(String),

    /// Invalid lens configuration.
    #[error("invalid lens configuration: {0}")]
    InvalidConfig(String),

    /// Schema version not found in history.
    #[error("schema version not found: {0}")]
    SchemaVersionNotFound(String),

    /// JSON serialization/deserialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// IO error (e.g., reading WASM file).
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Pipeline error during document transformation.
    #[error("pipeline error: {0}")]
    Pipeline(String),

    /// File path not allowed (path traversal or HTTP restriction).
    #[error("file path not allowed: {0}")]
    PathNotAllowed(String),
}
