//! Error types for the WASM client.
//!
//! All errors are converted to JsError for JavaScript interop.

use thiserror::Error;
use wasm_bindgen::JsValue;

/// Result type for WASM operations.
pub type Result<T> = std::result::Result<T, WasmError>;

/// Errors that can occur in the WASM client.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum WasmError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Document error: {0}")]
    Document(String),

    #[error("Verification error: {0}")]
    Verification(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Sync error: {0}")]
    Sync(String),

    #[error("Client not initialized")]
    NotInitialized,

    #[error("Client already closed")]
    Closed,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

impl From<WasmError> for JsValue {
    fn from(err: WasmError) -> Self {
        JsValue::from_str(&err.to_string())
    }
}

impl From<document::Error> for WasmError {
    fn from(err: document::Error) -> Self {
        WasmError::Document(err.to_string())
    }
}

impl From<defra_core::Error> for WasmError {
    fn from(err: defra_core::Error) -> Self {
        match err {
            defra_core::Error::Io(_)
            | defra_core::Error::Json(_)
            | defra_core::Error::DagCborEncode(_)
            | defra_core::Error::DagCborDecode(_)
            | defra_core::Error::Serialization(_)
            | defra_core::Error::IpldError(_) => WasmError::Serialization(err.to_string()),
            defra_core::Error::Cid(_) | defra_core::Error::InvalidCID(_) => {
                WasmError::InvalidArgument(err.to_string())
            }
            _ => WasmError::Verification(err.to_string()),
        }
    }
}

impl From<serde_json::Error> for WasmError {
    fn from(err: serde_json::Error) -> Self {
        WasmError::Serialization(err.to_string())
    }
}

impl From<serde_wasm_bindgen::Error> for WasmError {
    fn from(err: serde_wasm_bindgen::Error) -> Self {
        WasmError::Serialization(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::WasmError;

    #[test]
    fn defra_core_json_errors_map_to_serialization() {
        let err = serde_json::from_str::<serde_json::Value>("{").unwrap_err();
        let wasm_err = WasmError::from(defra_core::Error::from(err));
        assert!(matches!(wasm_err, WasmError::Serialization(_)));
    }

    #[test]
    fn defra_core_cid_errors_map_to_invalid_argument() {
        let err = cid::Cid::try_from("not-a-cid").unwrap_err();
        let wasm_err = WasmError::from(defra_core::Error::from(err));
        assert!(matches!(wasm_err, WasmError::InvalidArgument(_)));
    }
}
