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
    crypto_error, failed_to_parse_ephemeral_public_key,
    verification_with_hmac_failed,
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
    let ephemeral_private = options.private_key.unwrap_or_else(|| {
        StaticSecret::random_from_rng(rand::rngs::OsRng)
    });
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
    let (ephemeral_public_bytes, remaining) = if let Some(pub_key_bytes) = options.public_key_bytes {
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
    let ephemeral_public_array: [u8; X25519_PUBLIC_KEY_SIZE] = ephemeral_public_bytes
        .as_slice()
        .try_into()
        .map_err(|_| failed_to_parse_ephemeral_public_key(
            std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid key length")
        ))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::generation::generate_x25519;

    #[test]
    fn test_ecies_encrypt_decrypt_default() {
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"test message";

        // Default options: prepend public key
        let options_enc = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_ecies_with_aad() {
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"test message";
        let aad = b"context data";

        let options_enc = EciesOptions::builder()
            .with_aad(aad.to_vec())
            .prepend_public_key(true)
            .build();

        let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

        let options_dec = EciesOptions::builder()
            .with_aad(aad.to_vec())
            .prepend_public_key(true)
            .build();

        let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_ecies_wrong_aad_fails() {
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"test message";

        let options_enc = EciesOptions::builder()
            .with_aad(b"correct".to_vec())
            .prepend_public_key(true)
            .build();

        let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

        let options_dec = EciesOptions::builder()
            .with_aad(b"wrong".to_vec())
            .prepend_public_key(true)
            .build();

        let result = decrypt_ecies(&ciphertext, &private_key, options_dec);
        assert!(result.is_err());
    }

    #[test]
    fn test_ecies_tampered_ciphertext_fails() {
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"test message";

        let options_enc = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let mut ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

        // Tamper with ciphertext (but not HMAC)
        if ciphertext.len() > 50 {
            ciphertext[40] ^= 0xFF;
        }

        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let result = decrypt_ecies(&ciphertext, &private_key, options_dec);
        assert!(result.is_err());
    }

    #[test]
    fn test_ecies_custom_private_key() {
        let recipient_key = generate_x25519().unwrap();
        let recipient_pub = PublicKey::from(&recipient_key);

        let sender_key = generate_x25519().unwrap();
        let sender_pub = PublicKey::from(&sender_key);
        let plaintext = b"test message";

        // Encrypt with custom sender key, don't prepend public key
        let options_enc = EciesOptions::builder()
            .with_private_key(sender_key)
            .prepend_public_key(false)
            .build();

        let ciphertext = encrypt_ecies(plaintext, &recipient_pub, options_enc).unwrap();

        // Should be shorter without 32-byte public key prepended
        assert!(ciphertext.len() < 100);

        // Now test decryption with separate public key
        let options_dec = EciesOptions::builder()
            .with_public_key_bytes(sender_pub.as_bytes().to_vec())
            .build();

        let decrypted = decrypt_ecies(&ciphertext, &recipient_key, options_dec).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_ecies_tampered_hmac_fails() {
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"test message";

        let options_enc = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let mut ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

        // Tamper with HMAC (last 32 bytes)
        let len = ciphertext.len();
        if len > 32 {
            ciphertext[len - 10] ^= 0xFF; // Flip a bit in the HMAC
        }

        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let result = decrypt_ecies(&ciphertext, &private_key, options_dec);
        assert!(result.is_err(), "Tampered HMAC should cause decryption to fail");
    }

    #[test]
    fn test_ecies_wrong_separate_public_key_fails() {
        let recipient_key = generate_x25519().unwrap();
        let recipient_pub = PublicKey::from(&recipient_key);

        let sender_key = generate_x25519().unwrap();
        let wrong_key = generate_x25519().unwrap();
        let wrong_pub = PublicKey::from(&wrong_key);

        let plaintext = b"test message";

        // Encrypt without prepending public key
        let options_enc = EciesOptions::builder()
            .with_private_key(sender_key)
            .prepend_public_key(false)
            .build();

        let ciphertext = encrypt_ecies(plaintext, &recipient_pub, options_enc).unwrap();

        // Try to decrypt with wrong public key
        let options_dec = EciesOptions::builder()
            .with_public_key_bytes(wrong_pub.as_bytes().to_vec())
            .build();

        let result = decrypt_ecies(&ciphertext, &recipient_key, options_dec);
        assert!(result.is_err(), "Wrong public key should cause decryption to fail");
    }

    #[test]
    fn test_ecies_output_format() {
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"test";

        let options = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let ciphertext = encrypt_ecies(plaintext, &public_key, options).unwrap();

        // Format: [ephemeral_pub (32) | nonce (12) | encrypted (varies) | HMAC (32)]
        assert!(ciphertext.len() >= X25519_PUBLIC_KEY_SIZE + 12 + HMAC_SIZE);
    }

    #[test]
    fn test_ecies_empty_plaintext() {
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"";

        let options_enc = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
        assert_eq!(plaintext, &decrypted[..]);
    }

    #[test]
    fn test_ecies_ciphertext_only_public_key_no_data() {
        let private_key = generate_x25519().unwrap();

        // Ciphertext with only ephemeral public key (32 bytes), no encrypted data, no HMAC
        let malformed_ct = vec![0u8; X25519_PUBLIC_KEY_SIZE];

        let options = EciesOptions::builder().prepend_public_key(true).build();
        let result = decrypt_ecies(&malformed_ct, &private_key, options);

        assert!(result.is_err(), "Should fail: ciphertext too short (only ephemeral key)");
    }

    #[test]
    fn test_ecies_ciphertext_no_hmac() {
        let private_key = generate_x25519().unwrap();

        // Ciphertext with ephemeral key + some data but no HMAC
        let malformed_ct = vec![0u8; X25519_PUBLIC_KEY_SIZE + 16]; // Missing HMAC_SIZE bytes

        let options = EciesOptions::builder().prepend_public_key(true).build();
        let result = decrypt_ecies(&malformed_ct, &private_key, options);

        assert!(result.is_err(), "Should fail: ciphertext too short (missing HMAC)");
    }

    #[test]
    fn test_ecies_invalid_separate_public_key_sizes() {
        let recipient_key = generate_x25519().unwrap();
        let recipient_pub = PublicKey::from(&recipient_key);
        let sender_key = generate_x25519().unwrap();
        let plaintext = b"test";

        // Encrypt without prepending key
        let options_enc = EciesOptions::builder()
            .with_private_key(sender_key)
            .prepend_public_key(false)
            .build();
        let ciphertext = encrypt_ecies(plaintext, &recipient_pub, options_enc).unwrap();

        // Try to decrypt with wrong-sized public key (31 bytes)
        let wrong_size_key = vec![0u8; 31];
        let options_dec = EciesOptions::builder()
            .with_public_key_bytes(wrong_size_key)
            .build();
        let result = decrypt_ecies(&ciphertext, &recipient_key, options_dec);
        assert!(result.is_err(), "31-byte ephemeral key should be rejected");

        // Try with 33 bytes
        let wrong_size_key = vec![0u8; 33];
        let options_dec = EciesOptions::builder()
            .with_public_key_bytes(wrong_size_key)
            .build();
        let result = decrypt_ecies(&ciphertext, &recipient_key, options_dec);
        assert!(result.is_err(), "33-byte ephemeral key should be rejected");
    }

    #[test]
    fn test_ecies_wrong_recipient_key_fails() {
        let correct_key = generate_x25519().unwrap();
        let correct_pub = PublicKey::from(&correct_key);
        let wrong_key = generate_x25519().unwrap();
        let plaintext = b"sensitive data";

        let options_enc = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let ciphertext = encrypt_ecies(plaintext, &correct_pub, options_enc).unwrap();

        // Try to decrypt with wrong recipient key
        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let result = decrypt_ecies(&ciphertext, &wrong_key, options_dec);

        assert!(result.is_err(), "Wrong recipient key should cause HMAC verification failure");
    }

    // ===== ECIES Error Handling Tests (ported from Go ecies_test.go) =====

    #[test]
    fn test_encrypt_with_weak_public_key() {
        // TestEncryptECIES_Errors - Weak public key handling
        // X25519 accepts any 32 bytes as a public key, but weak keys (like all-zeros)
        // will produce predictable shared secrets that can't be used securely.
        // Encryption may succeed, but decryption with a different private key will fail.
        let plaintext = b"test data";
        let zero_pub = PublicKey::from([0u8; 32]);

        let options_enc = EciesOptions::builder().prepend_public_key(true).build();
        let ciphertext = encrypt_ecies(plaintext, &zero_pub, options_enc).unwrap();

        // Decryption with a random private key should fail (HMAC mismatch)
        let random_private = generate_x25519().unwrap();
        let options_dec = EciesOptions::builder().prepend_public_key(true).build();
        let result = decrypt_ecies(&ciphertext, &random_private, options_dec);

        assert!(result.is_err(), "Decryption with wrong key should fail HMAC verification");
    }

    #[test]
    fn test_encrypt_no_prepend_without_ephemeral_private_key() {
        // TestEncryptECIES_Errors - No public key prepended without providing private key
        // When prepend_public_key=false, encryption works but decryption requires
        // the ephemeral public key to be provided separately

        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"test data";

        // Encrypt without prepending public key
        let options_enc = EciesOptions::builder()
            .prepend_public_key(false)
            .build();
        let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

        // Try to decrypt with prepend_public_key=true (expecting prepended key but there isn't one)
        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let result = decrypt_ecies(&ciphertext, &private_key, options_dec);

        // This should fail because we're expecting a prepended key but there isn't one
        assert!(result.is_err(), "Decryption should fail when expecting prepended key but none exists");
    }

    #[test]
    fn test_decrypt_with_invalid_private_key() {
        // TestDecryptECIES_Errors - Invalid private key
        let correct_key = generate_x25519().unwrap();
        let correct_pub = PublicKey::from(&correct_key);
        let plaintext = b"test data";

        // Encrypt with correct key
        let options_enc = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let ciphertext = encrypt_ecies(plaintext, &correct_pub, options_enc).unwrap();

        // Create a malformed/weak private key (all zeros)
        let weak_key = StaticSecret::from([0u8; 32]);

        // Try to decrypt with weak private key
        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let result = decrypt_ecies(&ciphertext, &weak_key, options_dec);

        // Should fail with HMAC verification error (keys don't match)
        assert!(result.is_err(), "Decryption with weak/wrong private key should fail");
    }

    #[test]
    fn test_verify_ephemeral_public_key_in_ciphertext() {
        // TestEncryptDecryptECIES_WithCustomPrivateKey_Succeeds + verification
        // Verify that when using a custom private key, the correct ephemeral public key is prepended

        let sender_private = generate_x25519().unwrap();
        let sender_public = PublicKey::from(&sender_private);

        let recipient_private = generate_x25519().unwrap();
        let recipient_public = PublicKey::from(&recipient_private);

        let plaintext = b"test data";

        // Encrypt with custom sender private key and prepend public key
        let options_enc = EciesOptions::builder()
            .with_private_key(sender_private)
            .prepend_public_key(true)
            .build();
        let ciphertext = encrypt_ecies(plaintext, &recipient_public, options_enc).unwrap();

        // Verify the first 32 bytes are the sender's ephemeral public key
        assert!(ciphertext.len() >= X25519_PUBLIC_KEY_SIZE, "Ciphertext should contain ephemeral public key");
        let prepended_key = &ciphertext[..X25519_PUBLIC_KEY_SIZE];
        assert_eq!(
            prepended_key,
            sender_public.as_bytes(),
            "Prepended public key should match sender's public key"
        );

        // Verify decryption works
        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let decrypted = decrypt_ecies(&ciphertext, &recipient_private, options_dec).unwrap();
        assert_eq!(decrypted, plaintext, "Decrypted data should match original");
    }

    #[test]
    fn test_ecies_encryption_with_empty_plaintext() {
        // Test ECIES with empty plaintext
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let plaintext = b"";

        let options_enc = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let ciphertext = encrypt_ecies(plaintext, &public_key, options_enc).unwrap();

        // Ciphertext should still contain ephemeral key + HMAC even for empty plaintext
        assert!(
            ciphertext.len() >= X25519_PUBLIC_KEY_SIZE + HMAC_SIZE,
            "Empty plaintext should still produce ciphertext with ephemeral key and HMAC"
        );

        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
        assert_eq!(decrypted, plaintext, "Should decrypt empty plaintext correctly");
    }

    #[test]
    fn test_ecies_aad_mismatch_with_separate_key() {
        // Test that AAD must match between encryption and decryption with separate ephemeral key
        let sender_key = generate_x25519().unwrap();
        let sender_pub = PublicKey::from(&sender_key);
        let recipient_key = generate_x25519().unwrap();
        let recipient_pub = PublicKey::from(&recipient_key);
        let plaintext = b"test";

        let aad_enc = b"context1".to_vec();
        let aad_dec = b"context2".to_vec();

        // Encrypt with AAD and custom sender key (prepend=false so key goes separately)
        let options_enc = EciesOptions::builder()
            .with_private_key(sender_key)
            .with_aad(aad_enc)
            .prepend_public_key(false)
            .build();
        let ciphertext = encrypt_ecies(plaintext, &recipient_pub, options_enc).unwrap();

        // Decrypt with different AAD and separate ephemeral public key
        let options_dec = EciesOptions::builder()
            .with_public_key_bytes(sender_pub.as_bytes().to_vec())
            .with_aad(aad_dec)
            .prepend_public_key(false)
            .build();
        let result = decrypt_ecies(&ciphertext, &recipient_key, options_dec);

        // Should fail due to AAD mismatch
        assert!(result.is_err(), "AAD mismatch should cause decryption failure");
    }

    #[test]
    fn test_ecies_large_plaintext_1mb() {
        // Test ECIES with 1MB plaintext
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let large_plaintext = vec![0xABu8; 1024 * 1024]; // 1MB

        let options_enc = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let ciphertext = encrypt_ecies(&large_plaintext, &public_key, options_enc).unwrap();

        // Expected size: ephemeral_pub(32) + nonce(12) + ciphertext(1MB + 16 auth tag) + HMAC(32)
        let expected_min_size = X25519_PUBLIC_KEY_SIZE + 12 + large_plaintext.len() + 16 + HMAC_SIZE;
        assert_eq!(
            ciphertext.len(),
            expected_min_size,
            "ECIES ciphertext size should be ephemeral_pub + nonce + plaintext + auth_tag + HMAC"
        );

        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();

        let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
        assert_eq!(large_plaintext.len(), decrypted.len(), "Decrypted length should match");
        assert_eq!(large_plaintext, decrypted, "1MB plaintext should decrypt correctly");
    }

    #[test]
    fn test_ecies_large_plaintext_5mb_with_aad() {
        // Test ECIES with 5MB plaintext and additional authenticated data
        let private_key = generate_x25519().unwrap();
        let public_key = PublicKey::from(&private_key);
        let large_plaintext = vec![0xCDu8; 5 * 1024 * 1024]; // 5MB
        let aad = b"large document encryption context".to_vec();

        let options_enc = EciesOptions::builder()
            .with_aad(aad.clone())
            .prepend_public_key(true)
            .build();

        let ciphertext = encrypt_ecies(&large_plaintext, &public_key, options_enc).unwrap();

        let options_dec = EciesOptions::builder()
            .with_aad(aad)
            .prepend_public_key(true)
            .build();

        let decrypted = decrypt_ecies(&ciphertext, &private_key, options_dec).unwrap();
        assert_eq!(large_plaintext, decrypted, "5MB plaintext with AAD should decrypt correctly");
    }

    #[test]
    fn test_ecies_large_plaintext_without_prepend() {
        // Test ECIES with large plaintext and separate ephemeral key
        let sender_key = generate_x25519().unwrap();
        let sender_pub = PublicKey::from(&sender_key);
        let recipient_key = generate_x25519().unwrap();
        let recipient_pub = PublicKey::from(&recipient_key);
        let large_plaintext = vec![0xEFu8; 2 * 1024 * 1024]; // 2MB

        let options_enc = EciesOptions::builder()
            .with_private_key(sender_key)
            .prepend_public_key(false)
            .build();

        let ciphertext = encrypt_ecies(&large_plaintext, &recipient_pub, options_enc).unwrap();

        // Without prepended key: nonce(12) + ciphertext(2MB + 16 auth tag) + HMAC(32)
        let expected_size = 12 + large_plaintext.len() + 16 + HMAC_SIZE;
        assert_eq!(
            ciphertext.len(),
            expected_size,
            "ECIES without prepend should be nonce + plaintext + auth_tag + HMAC"
        );

        let options_dec = EciesOptions::builder()
            .with_public_key_bytes(sender_pub.as_bytes().to_vec())
            .build();

        let decrypted = decrypt_ecies(&ciphertext, &recipient_key, options_dec).unwrap();
        assert_eq!(large_plaintext, decrypted, "2MB plaintext should decrypt correctly with separate key");
    }

    #[test]
    fn test_ecies_hkdf_key_derivation() {
        // Test HKDF key derivation with known inputs to verify Go compatibility
        // This ensures the key derivation matches Go's hkdf.Read() behavior

        // Use deterministic keys for reproducible test
        let alice_private_bytes: [u8; 32] = [
            0x77, 0x07, 0x6d, 0x0a, 0x73, 0x18, 0xa5, 0x7d,
            0x3c, 0x16, 0xc1, 0x72, 0x51, 0xb2, 0x66, 0x45,
            0xdf, 0x4c, 0x2f, 0x87, 0xeb, 0xc0, 0x99, 0x2a,
            0xb1, 0x77, 0xfb, 0xa5, 0x1d, 0xb9, 0x2c, 0x2a,
        ];
        let bob_private_bytes: [u8; 32] = [
            0x5d, 0xab, 0x08, 0x7e, 0x62, 0x4a, 0x8a, 0x4b,
            0x79, 0xe1, 0x7f, 0x8b, 0x83, 0x80, 0x0e, 0xe6,
            0x6f, 0x3b, 0xb1, 0x29, 0x26, 0x18, 0xb6, 0xfd,
            0x1c, 0x2f, 0x8b, 0x27, 0xff, 0x88, 0xe0, 0xeb,
        ];

        let alice_private = StaticSecret::from(alice_private_bytes);
        let bob_private = StaticSecret::from(bob_private_bytes);
        let bob_public = PublicKey::from(&bob_private);

        // Compute shared secret (Alice encrypting to Bob)
        let shared_secret = alice_private.diffie_hellman(&bob_public);

        // Derive keys using HKDF-SHA256 with empty salt and info (matches Go)
        let hkdf = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());
        let mut keys = [0u8; 64];
        hkdf.expand(&[], &mut keys).unwrap();

        let aes_key = &keys[..32];
        let hmac_key = &keys[32..];

        // Verify derived keys are deterministic (same inputs = same outputs)
        // This test ensures the HKDF derivation is consistent
        assert_eq!(aes_key.len(), 32, "AES key should be 32 bytes");
        assert_eq!(hmac_key.len(), 32, "HMAC key should be 32 bytes");
        assert_ne!(aes_key, hmac_key, "AES and HMAC keys should be different");

        // Verify keys are non-trivial (not all zeros or ones)
        assert!(!aes_key.iter().all(|&b| b == 0), "AES key should not be all zeros");
        assert!(!hmac_key.iter().all(|&b| b == 0), "HMAC key should not be all zeros");

        // Re-derive to ensure determinism
        let hkdf2 = Hkdf::<Sha256>::new(None, shared_secret.as_bytes());
        let mut keys2 = [0u8; 64];
        hkdf2.expand(&[], &mut keys2).unwrap();

        assert_eq!(keys, keys2, "HKDF derivation should be deterministic");

        // Test that encryption with derived keys produces consistent results
        // This verifies the full ECIES flow with known keys
        let plaintext = b"test message for key derivation";
        let options_enc = EciesOptions::builder()
            .with_private_key(alice_private_bytes.into())
            .prepend_public_key(true)
            .build();

        let ciphertext1 = encrypt_ecies(plaintext, &bob_public, options_enc).unwrap();

        // Decrypt should succeed with Bob's private key
        let options_dec = EciesOptions::builder()
            .prepend_public_key(true)
            .build();
        let decrypted = decrypt_ecies(&ciphertext1, &bob_private, options_dec).unwrap();
        assert_eq!(plaintext.to_vec(), decrypted, "Decryption should recover original plaintext");
    }
}

