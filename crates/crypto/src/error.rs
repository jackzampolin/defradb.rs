//! Crypto error handling
//!
//! All crypto errors use the `defra_core::Error::Crypto(String)` variant
//! for consistency with the rest of the DefraDB ecosystem.

use defra_core::Error;

use crate::types::KeyType;

/// Creates a generic crypto error
pub fn crypto_error(msg: impl Into<String>) -> Error {
    Error::Crypto(msg.into())
}

/// Error: Ciphertext is too short to contain required components
pub fn ciphertext_too_short() -> Error {
    Error::Crypto("ciphertext is too short".to_string())
}

/// Error: HMAC verification failed
pub fn verification_with_hmac_failed() -> Error {
    Error::Crypto("verification with HMAC failed".to_string())
}

/// Error: No public key provided for decryption
pub fn no_public_key_for_decryption() -> Error {
    Error::Crypto("no public key for decryption".to_string())
}

/// Error: Invalid ECDSA private key bytes
pub fn invalid_ecdsa_private_key_bytes() -> Error {
    Error::Crypto("invalid ECDSA private key bytes".to_string())
}

/// Error: Nil key provided
pub fn nil_key() -> Error {
    Error::Crypto("nil key".to_string())
}

/// Error: Invalid ECDSA signature format
pub fn invalid_ecdsa_signature() -> Error {
    Error::Crypto("invalid ECDSA signature".to_string())
}

/// Error: Invalid ECDSA public key
pub fn invalid_ecdsa_public_key() -> Error {
    Error::Crypto("invalid ECDSA public key".to_string())
}

/// Error: Invalid Ed25519 private key length
pub fn invalid_ed25519_private_key_length(len: usize) -> Error {
    Error::Crypto(format!(
        "invalid Ed25519 private key length: expected 64 bytes, got {}",
        len
    ))
}

/// Error: Invalid Ed25519 public key length
pub fn invalid_ed25519_public_key_length(len: usize) -> Error {
    Error::Crypto(format!(
        "invalid Ed25519 public key length: expected 32 bytes, got {}",
        len
    ))
}

/// Error: Signature verification failed
pub fn signature_verification_failed() -> Error {
    Error::Crypto("signature verification failed".to_string())
}

/// Error: Unsupported private key type
pub fn unsupported_private_key_type() -> Error {
    Error::Crypto("unsupported private key type".to_string())
}

/// Error: Unsupported public key type
pub fn unsupported_public_key_type() -> Error {
    Error::Crypto("unsupported public key type".to_string())
}

/// Error: Unsupported key type
pub fn unsupported_key_type(key_type: KeyType) -> Error {
    Error::Crypto(format!("unsupported key type: {}", key_type))
}

/// Error: Failed to generate ephemeral key
pub fn failed_to_generate_ephemeral_key(inner: impl std::error::Error) -> Error {
    Error::Crypto(format!("failed to generate ephemeral key: {}", inner))
}

/// Error: ECDH operation failed
pub fn failed_ecdh_operation(inner: impl std::error::Error) -> Error {
    Error::Crypto(format!("failed ECDH operation: {}", inner))
}

/// Error: KDF operation failed for AES key
pub fn failed_kdf_operation_aes(inner: impl std::error::Error) -> Error {
    Error::Crypto(format!("failed KDF operation for AES key: {}", inner))
}

/// Error: KDF operation failed for HMAC key
pub fn failed_kdf_operation_hmac(inner: impl std::error::Error) -> Error {
    Error::Crypto(format!("failed KDF operation for HMAC key: {}", inner))
}

/// Error: Encryption failed
pub fn failed_to_encrypt(inner: impl std::error::Error) -> Error {
    Error::Crypto(format!("failed to encrypt: {}", inner))
}

/// Error: Failed to parse ephemeral public key
pub fn failed_to_parse_ephemeral_public_key(inner: impl std::error::Error) -> Error {
    Error::Crypto(format!("failed to parse ephemeral public key: {}", inner))
}

/// Error: Decryption failed
pub fn failed_to_decrypt(inner: impl std::error::Error) -> Error {
    Error::Crypto(format!("failed to decrypt: {}", inner))
}

/// Error: Failed to create DID key
pub fn failed_to_create_did_key(inner: impl std::error::Error) -> Error {
    Error::Crypto(format!("failed to create DID key: {}", inner))
}

/// Error: Invalid AES key size
pub fn invalid_aes_key_size(len: usize) -> Error {
    Error::Crypto(format!(
        "invalid AES key size: expected 32 bytes, got {}",
        len
    ))
}

/// Error: Invalid nonce size
pub fn invalid_nonce_size(len: usize) -> Error {
    Error::Crypto(format!(
        "invalid nonce size: expected 12 bytes, got {}",
        len
    ))
}

/// Error: Failed to generate random bytes
pub fn failed_to_generate_random() -> Error {
    Error::Crypto("failed to generate random bytes".to_string())
}

/// Error: Invalid key bytes
pub fn invalid_key_bytes(msg: impl Into<String>) -> Error {
    Error::Crypto(format!("invalid key bytes: {}", msg.into()))
}

/// Error: Invalid hex string
pub fn invalid_hex_string(inner: hex::FromHexError) -> Error {
    Error::Crypto(format!("invalid hex string: {}", inner))
}

/// Error: Invalid public key format
pub fn invalid_public_key_format(msg: impl Into<String>) -> Error {
    Error::Crypto(format!("invalid public key format: {}", msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crypto_error() {
        let err = crypto_error("test error");
        assert!(matches!(err, Error::Crypto(_)));
        if let Error::Crypto(msg) = err {
            assert_eq!(msg, "test error");
        }
    }

    #[test]
    fn test_error_messages() {
        let err = ciphertext_too_short();
        assert!(matches!(err, Error::Crypto(_)));

        let err = invalid_ed25519_private_key_length(32);
        assert!(matches!(err, Error::Crypto(_)));

        let err = unsupported_key_type(KeyType::Secp256k1);
        assert!(matches!(err, Error::Crypto(_)));
    }
}
