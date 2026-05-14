//! Error type for cursor token codec.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CursorError {
    #[error("invalid cursor")]
    InvalidBase64(#[from] base64::DecodeError),

    #[error("invalid cursor")]
    InvalidJson(#[from] serde_json::Error),

    #[error("invalid cursor")]
    EmptyDocId,
}
