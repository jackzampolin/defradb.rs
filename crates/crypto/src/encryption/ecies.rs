//! ECIES (Elliptic Curve Integrated Encryption Scheme) implementation
//!
//! This module provides hybrid encryption using:
//! - X25519 for Elliptic Curve Diffie-Hellman key agreement
//! - HKDF-SHA256 for key derivation
//! - AES-256-GCM for symmetric encryption
//! - HMAC-SHA256 for authentication

use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

use defra_core::Result;

use crate::encryption::aes::{decrypt_aes, encrypt_aes};
use crate::error::{
    crypto_error, failed_to_parse_ephemeral_public_key, verification_with_hmac_failed,
};
use crate::types::{AES_KEY_SIZE, HMAC_SIZE, X25519_PUBLIC_KEY_SIZE};

/// Options for ECIES encryption/decryption
#[derive(Default)]
pub struct EciesOptions {
    /// Additional authenticated data (included in AAD for AES-GCM)
    pub associated_data: Option<Vec<u8>>,
    /// Custom private key (for non-prepended scenarios)
    pub private_key: Option<StaticSecret>,
    /// Ephemeral public key bytes (for decryption when not prepended)
    pub public_key_bytes: Option<Vec<u8>>,
    /// Whether to prepend the ephemeral public key to the output
    pub prepend_public_key: bool,
}

impl EciesOptions {
    /// Create a new options builder
    pub fn builder() -> EciesOptionsBuilder {
        EciesOptionsBuilder::default()
    }
}

/// Builder for ECIES options
#[derive(Default)]
pub struct EciesOptionsBuilder {
    associated_data: Option<Vec<u8>>,
    private_key: Option<StaticSecret>,
    public_key_bytes: Option<Vec<u8>>,
    prepend_public_key: bool,
}

impl EciesOptionsBuilder {
    /// Set additional authenticated data
    pub fn with_aad(mut self, aad: Vec<u8>) -> Self {
        self.associated_data = Some(aad);
        self
    }

    /// Set a custom private key (for sender)
    pub fn with_private_key(mut self, key: StaticSecret) -> Self {
        self.private_key = Some(key);
        self
    }

    /// Set ephemeral public key bytes (for decryption)
    pub fn with_public_key_bytes(mut self, bytes: Vec<u8>) -> Self {
        self.public_key_bytes = Some(bytes);
        self
    }

    /// Set whether to prepend public key to output
    pub fn prepend_public_key(mut self, prepend: bool) -> Self {
        self.prepend_public_key = prepend;
        self
    }

    /// Build the options
    pub fn build(self) -> EciesOptions {
        EciesOptions {
            associated_data: self.associated_data,
            private_key: self.private_key,
            public_key_bytes: self.public_key_bytes,
            prepend_public_key: self.prepend_public_key,
        }
    }
}

/// Encrypt data using ECIES
///
/// # Parameters
/// * `plaintext` - The data to encrypt
/// * `public_key` - The recipient's X25519 public key
/// * `options` - ECIES encryption options
///
/// # Returns
/// The encrypted data in format: `[ephemeral_pub | ciphertext | HMAC]`
/// (ephemeral_pub is optional based on prepend_public_key option)
///
/// # Example
/// ```ignore
/// let recipient_key = generate_x25519()?;
/// let recipient_pub = PublicKey::from(&recipient_key);
///
/// let options = EciesOptions::builder()
///     .with_aad(b"context".to_vec())
///     .prepend_public_key(true)
///     .build();
///
/// let ciphertext = encrypt_ecies(b"secret", &recipient_pub, options)?;
/// ```
pub fn encrypt_ecies(
    plaintext: &[u8],
    public_key: &PublicKey,
    options: EciesOptions,
) -> Result<Vec<u8>> {
    // 1. Generate or use provided ephemeral key pair
    let ephemeral_private = options
        .private_key
        .unwrap_or_else(|| StaticSecret::random_from_rng(rand::rngs::OsRng));
    let ephemeral_public = PublicKey::from(&ephemeral_private);

    // 2. ECDH: compute shared secret
    let shared_secret = ephemeral_private.diffie_hellman(public_key);

    // 3. HKDF-SHA256: derive AES and HMAC keys (RFC 5869)
    // We expand 64 bytes and split: first 32 for AES, next 32 for HMAC.
    // This matches Go's sequential hkdf.Read() calls for P2P compatibility.
    // Empty salt and info parameters match the Go implementation.
    let hkdf = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());

    let mut keys = [0u8; AES_KEY_SIZE + AES_KEY_SIZE];
    hkdf.expand(&[], &mut keys)
        .map_err(|e| crypto_error(format!("HKDF expansion failed: {}", e)))?;

    let aes_key: [u8; AES_KEY_SIZE] = keys[..AES_KEY_SIZE].try_into().unwrap();
    let hmac_key: [u8; AES_KEY_SIZE] = keys[AES_KEY_SIZE..].try_into().unwrap();

    // 4. Build AAD: ephemeral public key + optional additional data
    let mut aad = ephemeral_public.as_bytes().to_vec();
    if let Some(extra_aad) = options.associated_data {
        aad.extend_from_slice(&extra_aad);
    }

    // 5. Encrypt with AES-GCM (nonce prepended to ciphertext)
    let (encrypted_data, _nonce) = encrypt_aes(plaintext, &aes_key, &aad, true)?;

    // 6. HMAC over the encrypted data
    let mut mac = Hmac::<Sha256>::new_from_slice(&hmac_key)
        .map_err(|e| crypto_error(format!("failed to create HMAC: {:?}", e)))?;
    mac.update(&encrypted_data);
    let mac_tag = mac.finalize().into_bytes();

    // 7. Assemble output
    let mut result = Vec::new();
    if options.prepend_public_key {
        result.extend_from_slice(ephemeral_public.as_bytes());
    }
    result.extend_from_slice(&encrypted_data);
    result.extend_from_slice(&mac_tag);

    Ok(result)
}

/// Decrypt data using ECIES
///
/// # Parameters
/// * `ciphertext` - The encrypted data
/// * `private_key` - Our X25519 private key
/// * `options` - ECIES decryption options
///
/// # Returns
/// The decrypted plaintext
///
/// # Example
/// ```ignore
/// let our_key = generate_x25519()?;
/// let plaintext = decrypt_ecies(&ciphertext, &our_key, options)?;
/// ```
pub fn decrypt_ecies(
    ciphertext: &[u8],
    private_key: &StaticSecret,
    options: EciesOptions,
) -> Result<Vec<u8>> {
    // 1. Extract or use provided ephemeral public key
    let (ephemeral_public_bytes, remaining) = if let Some(pub_key_bytes) = options.public_key_bytes
    {
        // Public key provided separately
        if pub_key_bytes.len() != X25519_PUBLIC_KEY_SIZE {
            return Err(crypto_error("invalid ephemeral public key size"));
        }
        (pub_key_bytes, ciphertext)
    } else {
        // Public key prepended to ciphertext
        if ciphertext.len() < X25519_PUBLIC_KEY_SIZE + HMAC_SIZE {
            return Err(crypto_error(format!(
                "ciphertext too short: got {} bytes, expected at least {} bytes (ephemeral_pub: {} + hmac: {} + encrypted data)",
                ciphertext.len(),
                X25519_PUBLIC_KEY_SIZE + HMAC_SIZE,
                X25519_PUBLIC_KEY_SIZE,
                HMAC_SIZE
            )));
        }
        let (pub_bytes, rest) = ciphertext.split_at(X25519_PUBLIC_KEY_SIZE);
        (pub_bytes.to_vec(), rest)
    };

    // Parse ephemeral public key
    let ephemeral_public_array: [u8; X25519_PUBLIC_KEY_SIZE] =
        ephemeral_public_bytes.as_slice().try_into().map_err(|_| {
            failed_to_parse_ephemeral_public_key(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid key length",
            ))
        })?;
    let ephemeral_public = PublicKey::from(ephemeral_public_array);

    // 2. Extract HMAC and encrypted data
    if remaining.len() < HMAC_SIZE {
        return Err(crypto_error(format!(
            "ciphertext too short for HMAC: got {} bytes after ephemeral key, expected at least {} bytes for HMAC",
            remaining.len(),
            HMAC_SIZE
        )));
    }
    let (encrypted_data, received_mac) = remaining.split_at(remaining.len() - HMAC_SIZE);

    // 3. ECDH: compute shared secret
    let shared_secret = private_key.diffie_hellman(&ephemeral_public);

    // 4. HKDF-SHA256: derive AES and HMAC keys (RFC 5869)
    // We expand 64 bytes and split: first 32 for AES, next 32 for HMAC.
    // This matches Go's sequential hkdf.Read() calls for P2P compatibility.
    let hkdf = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());

    let mut keys = [0u8; AES_KEY_SIZE + AES_KEY_SIZE];
    hkdf.expand(&[], &mut keys)
        .map_err(|e| crypto_error(format!("HKDF expansion failed: {}", e)))?;

    let aes_key: [u8; AES_KEY_SIZE] = keys[..AES_KEY_SIZE].try_into().unwrap();
    let hmac_key: [u8; AES_KEY_SIZE] = keys[AES_KEY_SIZE..].try_into().unwrap();

    // 5. Verify HMAC
    let mut mac = Hmac::<Sha256>::new_from_slice(&hmac_key)
        .map_err(|e| crypto_error(format!("failed to create HMAC: {:?}", e)))?;
    mac.update(encrypted_data);
    mac.verify_slice(received_mac)
        .map_err(|_| verification_with_hmac_failed())?;

    // 6. Build AAD: ephemeral public key + optional additional data
    let mut aad = ephemeral_public_bytes;
    if let Some(extra_aad) = options.associated_data {
        aad.extend_from_slice(&extra_aad);
    }

    // 7. Decrypt with AES-GCM (nonce is prepended in encrypted_data)
    let plaintext = decrypt_aes(None, encrypted_data, &aes_key, &aad)?;

    Ok(plaintext)
}
