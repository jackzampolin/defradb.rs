//! AES-256-GCM encryption and decryption
//!
//! This module provides authenticated encryption using AES-256-GCM (Galois/Counter Mode).
//! AES-GCM provides both confidentiality and authenticity through authenticated encryption
//! with associated data (AEAD).

use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};

use defra_core::Result;

use crate::error::{ciphertext_too_short, invalid_aes_key_size, invalid_nonce_size};
use crate::types::{AES_KEY_SIZE, AES_NONCE_SIZE};

use super::nonce::generate_nonce;

/// Encrypt data using AES-256-GCM
///
/// # Parameters
/// * `plaintext` - The data to encrypt
/// * `key` - The AES-256 encryption key (must be 32 bytes)
/// * `additional_data` - Additional authenticated data (AAD) for authentication context
/// * `prepend_nonce` - If true, prepends the nonce to the ciphertext output
///
/// # Returns
/// A tuple containing:
/// - The ciphertext (with nonce prepended if `prepend_nonce` is true)
/// - The generated nonce (12 bytes)
///
/// # Example
/// ```ignore
/// let key = generate_aes256()?;
/// let plaintext = b"secret message";
/// let aad = b"context";
///
/// let (ciphertext, nonce) = encrypt_aes(plaintext, &key, aad, true)?;
/// ```
pub fn encrypt_aes(
    plaintext: &[u8],
    key: &[u8],
    additional_data: &[u8],
    prepend_nonce: bool,
) -> Result<(Vec<u8>, Vec<u8>)> {
    // Validate key size
    if key.len() != AES_KEY_SIZE {
        return Err(invalid_aes_key_size(key.len()));
    }

    // Generate nonce
    let nonce_bytes = generate_nonce()?;
    #[allow(deprecated)]
    let nonce = Nonce::from_slice(&nonce_bytes);

    // Create cipher
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| crate::error::crypto_error(format!("failed to create cipher: {:?}", e)))?;

    // Prepare payload with AAD
    let payload = Payload {
        msg: plaintext,
        aad: additional_data,
    };

    // Encrypt
    let ciphertext = cipher
        .encrypt(nonce, payload)
        .map_err(|e| crate::error::crypto_error(format!("encryption failed: {:?}", e)))?;

    // Prepend nonce if requested
    let output = if prepend_nonce {
        let mut result = Vec::with_capacity(AES_NONCE_SIZE + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);
        result
    } else {
        ciphertext
    };

    Ok((output, nonce_bytes.to_vec()))
}

/// Decrypt data using AES-256-GCM
///
/// # Parameters
/// * `nonce` - The nonce used for encryption (if None, extracts from ciphertext)
/// * `ciphertext` - The encrypted data
/// * `key` - The AES-256 decryption key (must be 32 bytes)
/// * `additional_data` - Additional authenticated data (must match encryption AAD)
///
/// # Returns
/// The decrypted plaintext
///
/// # Example
/// ```ignore
/// let key = generate_aes256()?;
/// let (ciphertext, nonce) = encrypt_aes(plaintext, &key, aad, false)?;
/// let decrypted = decrypt_aes(Some(&nonce), &ciphertext, &key, aad)?;
/// assert_eq!(plaintext, decrypted);
/// ```
pub fn decrypt_aes(
    nonce: Option<&[u8]>,
    ciphertext: &[u8],
    key: &[u8],
    additional_data: &[u8],
) -> Result<Vec<u8>> {
    // Validate key size
    if key.len() != AES_KEY_SIZE {
        return Err(invalid_aes_key_size(key.len()));
    }

    // Extract or validate nonce
    let (nonce_bytes, ciphertext_data) = if let Some(nonce_data) = nonce {
        // Nonce provided separately
        if nonce_data.len() != AES_NONCE_SIZE {
            return Err(invalid_nonce_size(nonce_data.len()));
        }
        (nonce_data, ciphertext)
    } else {
        // Nonce prepended to ciphertext
        if ciphertext.len() < AES_NONCE_SIZE {
            return Err(ciphertext_too_short());
        }
        let (nonce_data, ct) = ciphertext.split_at(AES_NONCE_SIZE);
        (nonce_data, ct)
    };

    #[allow(deprecated)]
    let nonce = Nonce::from_slice(nonce_bytes);

    // Create cipher
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| crate::error::crypto_error(format!("failed to create cipher: {:?}", e)))?;

    // Prepare payload with AAD
    let payload = Payload {
        msg: ciphertext_data,
        aad: additional_data,
    };

    // Decrypt
    let plaintext = cipher
        .decrypt(nonce, payload)
        .map_err(|e| crate::error::crypto_error(format!("decryption failed: {:?}", e)))?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::generation::generate_aes256;
    use crate::encryption::nonce::USE_DETERMINISTIC_NONCE;

    #[test]
    fn test_encrypt_decrypt_round_trip() {
        let key = generate_aes256().unwrap();
        let plaintext = b"test message";
        let aad = b"additional data";

        // Encrypt with nonce not prepended
        let (ciphertext, nonce) = encrypt_aes(plaintext, &key, aad, false).unwrap();

        // Decrypt with separate nonce
        let decrypted = decrypt_aes(Some(&nonce), &ciphertext, &key, aad).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_encrypt_decrypt_with_prepended_nonce() {
        let key = generate_aes256().unwrap();
        let plaintext = b"test message";
        let aad = b"context";

        // Encrypt with nonce prepended
        let (ciphertext, _nonce) = encrypt_aes(plaintext, &key, aad, true).unwrap();

        // Decrypt with nonce from ciphertext
        let decrypted = decrypt_aes(None, &ciphertext, &key, aad).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_encrypt_decrypt_empty_aad() {
        let key = generate_aes256().unwrap();
        let plaintext = b"test message";

        let (ciphertext, nonce) = encrypt_aes(plaintext, &key, &[], false).unwrap();
        let decrypted = decrypt_aes(Some(&nonce), &ciphertext, &key, &[]).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_decrypt_with_wrong_key_fails() {
        let key1 = generate_aes256().unwrap();
        let key2 = generate_aes256().unwrap();
        let plaintext = b"test message";

        let (ciphertext, nonce) = encrypt_aes(plaintext, &key1, &[], false).unwrap();
        let result = decrypt_aes(Some(&nonce), &ciphertext, &key2, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_with_wrong_aad_fails() {
        let key = generate_aes256().unwrap();
        let plaintext = b"test message";
        let aad1 = b"correct aad";
        let aad2 = b"wrong aad";

        let (ciphertext, nonce) = encrypt_aes(plaintext, &key, aad1, false).unwrap();
        let result = decrypt_aes(Some(&nonce), &ciphertext, &key, aad2);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_with_tampered_ciphertext_fails() {
        let key = generate_aes256().unwrap();
        let plaintext = b"test message";

        let (mut ciphertext, nonce) = encrypt_aes(plaintext, &key, &[], false).unwrap();

        // Tamper with ciphertext
        if !ciphertext.is_empty() {
            ciphertext[0] ^= 0xFF;
        }

        let result = decrypt_aes(Some(&nonce), &ciphertext, &key, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_encrypt_with_invalid_key_size() {
        let key = vec![0u8; 16]; // Wrong size (should be 32)
        let plaintext = b"test";

        let result = encrypt_aes(plaintext, &key, &[], false);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_with_invalid_key_size() {
        let key = vec![0u8; 16]; // Wrong size
        let ciphertext = vec![0u8; 32];

        let result = decrypt_aes(None, &ciphertext, &key, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_ciphertext_too_short() {
        let key = generate_aes256().unwrap();
        let ciphertext = vec![0u8; 5]; // Too short to contain nonce

        let result = decrypt_aes(None, &ciphertext, &key, &[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_deterministic_encryption() {
        // Enable deterministic nonces for reproducibility
        USE_DETERMINISTIC_NONCE.store(true, std::sync::atomic::Ordering::Relaxed);

        let key = vec![1u8; 32];
        let plaintext = b"test message";
        let aad = b"context";

        let (ct1, nonce1) = encrypt_aes(plaintext, &key, aad, false).unwrap();
        let (ct2, nonce2) = encrypt_aes(plaintext, &key, aad, false).unwrap();

        // With deterministic nonces, encryption should be reproducible
        assert_eq!(nonce1, nonce2);
        assert_eq!(ct1, ct2);

        // Restore random mode
        USE_DETERMINISTIC_NONCE.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    #[test]
    fn test_nonce_prepending() {
        let key = generate_aes256().unwrap();
        let plaintext = b"test";

        let (ciphertext_with_nonce, nonce) = encrypt_aes(plaintext, &key, &[], true).unwrap();

        // Nonce should be prepended (first 12 bytes)
        assert_eq!(&ciphertext_with_nonce[..12], &nonce[..]);

        // Should be able to decrypt
        let decrypted = decrypt_aes(None, &ciphertext_with_nonce, &key, &[]).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }
}

