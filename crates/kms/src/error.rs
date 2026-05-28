//! Error types for the KMS.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum KmsError {
    #[error("key unavailable")]
    KeyUnavailable,

    #[error("access denied: {reason}")]
    AccessDenied { reason: String },

    #[error("no transport configured")]
    NoTransport,

    #[error("wire decode failed: {0}")]
    WireDecode(String),

    #[error("wire encode failed: {0}")]
    WireEncode(String),

    #[error("crypto failure: {0}")]
    Crypto(String),

    #[error("storage failure: {0}")]
    Storage(String),

    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),

    #[error("internal invariant violated: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, KmsError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_renders_each_variant() {
        assert_eq!(KmsError::KeyUnavailable.to_string(), "key unavailable");
        assert_eq!(
            KmsError::AccessDenied {
                reason: "no read grant".into()
            }
            .to_string(),
            "access denied: no read grant"
        );
    }
}
