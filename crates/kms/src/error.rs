//! Error types for the KMS.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Returned when a requested DEK cannot be found in any local KeyStore or
    /// via any KeyTransport.
    #[error("key unavailable")]
    KeyUnavailable,

    /// An AccessPolicy denied the release. `reason` carries a human-readable
    /// explanation suitable for debug logs (not for end users).
    #[error("access denied: {reason}")]
    AccessDenied { reason: String },

    /// No KeyTransport is configured, so remote fetch is impossible.
    #[error("no transport configured")]
    NoTransport,

    /// A wire message (request or reply) failed to decode.
    #[error("wire decode failed: {0}")]
    WireDecode(String),

    /// A wire message (request or reply) failed to encode.
    #[error("wire encode failed: {0}")]
    WireEncode(String),

    /// A cryptographic operation (wrap, unwrap, sign, verify) failed.
    #[error("crypto failure: {0}")]
    Crypto(String),

    /// A KeyStore backend (memory, keyring, enclave, threshold) failed.
    #[error("storage failure: {0}")]
    Storage(String),

    /// An operation is not supported by the current KMS configuration
    /// (e.g. threshold reshare on a memory store).
    #[error("unsupported operation: {0}")]
    Unsupported(&'static str),

    /// An internal invariant was violated. Indicates a bug in the KMS itself.
    #[error("internal invariant violated: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_renders_each_variant() {
        assert_eq!(Error::KeyUnavailable.to_string(), "key unavailable");
        assert_eq!(
            Error::AccessDenied {
                reason: "no read grant".into()
            }
            .to_string(),
            "access denied: no read grant"
        );
    }
}
