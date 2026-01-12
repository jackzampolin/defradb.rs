//! Ed25519 key implementations
//!
//! Ed25519 is a modern, high-security elliptic curve signature scheme.
//! - Public keys: 32 bytes
//! - Private keys: 64 bytes (includes public key)
//! - Signatures: 64 bytes

use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey, PUBLIC_KEY_LENGTH};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use defra_core::Result;

use crate::error::signature_verification_failed;
use crate::keys::{Key, PrivateKey, PublicKey};
use crate::types::KeyType;

/// Ed25519 private key wrapper
///
/// # Private Key Format
///
/// Ed25519 private keys in this implementation use a **64-byte representation**:
/// - **Bytes 0-31**: 32-byte seed (the actual private scalar)
/// - **Bytes 32-63**: 32-byte public key (derived from the seed)
///
/// This format matches the Go implementation and is compatible with many Ed25519
/// libraries. The 64-byte format allows storing both the private seed and public
/// key together for efficient operations.
///
/// When creating a key from bytes using `from_bytes()`, exactly 64 bytes must be
/// provided. The first 32 bytes are used as the seed to reconstruct the signing key.
///
/// # Compatibility Note
///
/// This 64-byte format is standard in many implementations including Go's `crypto/ed25519`,
/// libsodium's `crypto_sign_keypair`, and PyNaCl. Some libraries use only 32-byte seeds;
/// when interoperating with those, use only the first 32 bytes of this format.
#[derive(Clone)]
pub struct Ed25519PrivateKey {
    key: SigningKey,
}

impl PartialEq for Ed25519PrivateKey {
    fn eq(&self, other: &Self) -> bool {
        // Compare private keys using constant-time comparison to prevent timing attacks
        // Compare the 32-byte seed (not the full 64-byte representation which includes public key)
        self.key.to_bytes().ct_eq(&other.key.to_bytes()).into()
    }
}

impl Eq for Ed25519PrivateKey {}

impl std::fmt::Debug for Ed25519PrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't print key material for security
        f.debug_struct("Ed25519PrivateKey")
            .field("key_type", &KeyType::Ed25519)
            .finish_non_exhaustive()
    }
}

impl Ed25519PrivateKey {
    /// Create a new Ed25519 private key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 64-byte Ed25519 private key
    ///
    /// # Returns
    /// * `Some(Ed25519PrivateKey)` if the key is valid
    /// * `None` if the key is invalid (wrong length or nil)
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        // Ed25519 private keys stored as 64 bytes (32-byte seed + 32-byte public key)
        // NOTE: RFC 8032 defines Ed25519 private keys as 32 bytes (just the seed).
        // We use 64-byte format for compatibility with DefraDB Go implementation,
        // which stores both seed and derived public key together.
        if bytes.len() != 64 {
            return None;
        }

        // ed25519-dalek expects 32 bytes for SigningKey (the seed)
        // The first 32 bytes are the seed
        let seed: [u8; 32] = bytes[..32].try_into().ok()?;
        let key = SigningKey::from_bytes(&seed);

        Some(Self { key })
    }

    /// Get the underlying ed25519-dalek signing key
    pub fn underlying(&self) -> &SigningKey {
        &self.key
    }
}

impl Key for Ed25519PrivateKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Ed25519 {
            return false;
        }
        // Use constant-time comparison to prevent timing attacks
        let self_raw = self.raw();
        let other_raw = other.raw();
        if self_raw.len() != other_raw.len() {
            return false;
        }
        self_raw.ct_eq(&other_raw).into()
    }

    fn raw(&self) -> Vec<u8> {
        // Return 64 bytes: 32-byte seed + 32-byte public key
        // DefraDB format (not RFC 8032 standard) for Go compatibility
        let seed = self.key.to_bytes();
        let public = self.key.verifying_key().to_bytes();
        let mut result = Vec::with_capacity(64);
        result.extend_from_slice(&seed);
        result.extend_from_slice(&public);
        result
    }

    fn key_type(&self) -> KeyType {
        KeyType::Ed25519
    }
}

impl PrivateKey for Ed25519PrivateKey {
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        let signature = self.key.sign(data);
        Ok(signature.to_bytes().to_vec())
    }

    fn public_key(&self) -> Box<dyn PublicKey> {
        Box::new(Ed25519PublicKey {
            key: self.key.verifying_key(),
        })
    }
}

/// Ed25519 public key wrapper
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Ed25519PublicKey {
    #[serde(with = "ed25519_public_key_serde")]
    key: VerifyingKey,
}

impl Ed25519PublicKey {
    /// Create a new Ed25519 public key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 32-byte Ed25519 public key
    ///
    /// # Returns
    /// * `Some(Ed25519PublicKey)` if the key is valid
    /// * `None` if the key is invalid (wrong length or nil)
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        if bytes.len() != PUBLIC_KEY_LENGTH {
            return None;
        }

        let key_bytes: [u8; PUBLIC_KEY_LENGTH] = bytes.try_into().ok()?;
        let key = VerifyingKey::from_bytes(&key_bytes).ok()?;

        Some(Self { key })
    }

    /// Get the underlying ed25519-dalek verifying key
    pub fn underlying(&self) -> &VerifyingKey {
        &self.key
    }
}

impl Key for Ed25519PublicKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Ed25519 {
            return false;
        }
        // Use constant-time comparison to prevent timing attacks
        let self_raw = self.raw();
        let other_raw = other.raw();
        if self_raw.len() != other_raw.len() {
            return false;
        }
        self_raw.ct_eq(&other_raw).into()
    }

    fn raw(&self) -> Vec<u8> {
        self.key.to_bytes().to_vec()
    }

    fn key_type(&self) -> KeyType {
        KeyType::Ed25519
    }
}

impl PublicKey for Ed25519PublicKey {
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<bool> {
        if signature.len() != 64 {
            return Ok(false);
        }

        let sig_bytes: [u8; 64] = signature
            .try_into()
            .map_err(|_| signature_verification_failed())?;

        let signature = ed25519_dalek::Signature::from_bytes(&sig_bytes);

        match self.key.verify(data, &signature) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn did(&self) -> Result<String> {
        crate::did::create_did_key(KeyType::Ed25519, &self.raw())
    }
}

// Custom serde module for VerifyingKey
mod ed25519_public_key_serde {
    use ed25519_dalek::VerifyingKey;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(key: &VerifyingKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&key.to_bytes())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<VerifyingKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        if bytes.len() != 32 {
            return Err(serde::de::Error::custom("invalid Ed25519 public key length"));
        }
        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| serde::de::Error::custom("invalid Ed25519 public key bytes"))?;
        VerifyingKey::from_bytes(&key_bytes).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::generation::generate_ed25519;

    #[test]
    fn test_ed25519_key_generation() {
        let private_key = Ed25519PrivateKey::from_bytes(&[0u8; 64]);
        assert!(private_key.is_some());
    }

    #[test]
    fn test_ed25519_key_type() {
        let private_key = Ed25519PrivateKey::from_bytes(&[1u8; 64]).unwrap();
        assert_eq!(private_key.key_type(), KeyType::Ed25519);

        let public_key = private_key.public_key();
        assert_eq!(public_key.key_type(), KeyType::Ed25519);
    }

    #[test]
    fn test_ed25519_raw_bytes() {
        let private_key = Ed25519PrivateKey::from_bytes(&[2u8; 64]).unwrap();
        let raw = private_key.raw();
        assert_eq!(raw.len(), 64);
    }

    #[test]
    fn test_ed25519_public_key_from_private() {
        let private_key = Ed25519PrivateKey::from_bytes(&[3u8; 64]).unwrap();
        let public_key = private_key.public_key();
        let raw = public_key.raw();
        assert_eq!(raw.len(), 32);
    }

    #[test]
    fn test_ed25519_sign_verify() {
        let private_key = Ed25519PrivateKey::from_bytes(&[4u8; 64]).unwrap();
        let message = b"test message";

        let signature = private_key.sign(message).unwrap();
        assert_eq!(signature.len(), 64);

        let public_key = private_key.public_key();
        let valid = public_key.verify(message, &signature).unwrap();
        assert!(valid);
    }

    #[test]
    fn test_ed25519_verify_wrong_message() {
        let private_key = Ed25519PrivateKey::from_bytes(&[5u8; 64]).unwrap();
        let message = b"test message";
        let signature = private_key.sign(message).unwrap();

        let public_key = private_key.public_key();
        let wrong_message = b"wrong message";
        let valid = public_key.verify(wrong_message, &signature).unwrap();
        assert!(!valid);
    }

    #[test]
    fn test_ed25519_invalid_key_lengths() {
        // Invalid private key length
        let private_key = Ed25519PrivateKey::from_bytes(&[0u8; 32]);
        assert!(private_key.is_none());

        // Invalid public key length
        let public_key = Ed25519PublicKey::from_bytes(&[0u8; 16]);
        assert!(public_key.is_none());
    }

    #[test]
    fn test_ed25519_nil_keys() {
        let private_key = Ed25519PrivateKey::from_bytes(&[]);
        assert!(private_key.is_none());

        let public_key = Ed25519PublicKey::from_bytes(&[]);
        assert!(public_key.is_none());
    }

    #[test]
    fn test_ed25519_key_equality() {
        let key1 = Ed25519PrivateKey::from_bytes(&[6u8; 64]).unwrap();
        let key2 = Ed25519PrivateKey::from_bytes(&[6u8; 64]).unwrap();
        assert!(key1.equal(&key2 as &dyn Key));

        let key3 = Ed25519PrivateKey::from_bytes(&[7u8; 64]).unwrap();
        assert!(!key1.equal(&key3 as &dyn Key));
    }

    #[test]
    fn test_ed25519_partial_eq() {
        // Test PartialEq implementation for private keys
        let key1 = Ed25519PrivateKey::from_bytes(&[8u8; 64]).unwrap();
        let key2 = Ed25519PrivateKey::from_bytes(&[8u8; 64]).unwrap();
        assert_eq!(key1, key2, "Same keys should be equal");

        let key3 = Ed25519PrivateKey::from_bytes(&[9u8; 64]).unwrap();
        assert_ne!(key1, key3, "Different keys should not be equal");

        // Test PartialEq for public keys (already derived)
        let pub1 = key1.public_key();
        let pub2 = key2.public_key();
        let pub1_concrete = Ed25519PublicKey::from_bytes(&pub1.raw()).unwrap();
        let pub2_concrete = Ed25519PublicKey::from_bytes(&pub2.raw()).unwrap();
        assert_eq!(pub1_concrete, pub2_concrete, "Public keys from same private key should be equal");
    }

    // ===== Signature Tests (ported from Go signature_test.go) =====

    #[test]
    fn test_sign_verify_round_trip() {
        // TestSignEd25519_WithPrivateKeyStruct + TestVerifyEd25519_WithPublicKeyStruct
        let private_key = generate_ed25519().unwrap();
        let message = b"test message";

        let signature = private_key.sign(message).unwrap();
        assert_eq!(signature.len(), 64, "Ed25519 signature should be 64 bytes");

        let public_key = private_key.public_key();
        let verified = public_key.verify(message, &signature).unwrap();
        assert!(verified, "Signature should verify with correct key and message");
    }

    #[test]
    fn test_verify_tampered_message() {
        // TestVerifyEd25519_TamperedMessage
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();

        let original_message = b"original message";
        let signature = private_key.sign(original_message).unwrap();

        let tampered_message = b"tampered message";
        let result = public_key.verify(tampered_message, &signature).unwrap();

        assert!(!result, "Verification should fail with tampered message");
    }

    #[test]
    fn test_verify_tampered_signature() {
        // TestVerifyEd25519_TamperedSignature
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();
        let message = b"test message";

        let mut signature = private_key.sign(message).unwrap();

        // Tamper with signature by flipping bits in first byte
        signature[0] ^= 0xFF;

        let result = public_key.verify(message, &signature).unwrap();
        assert!(!result, "Verification should fail with tampered signature");
    }

    #[test]
    fn test_verify_wrong_public_key() {
        // TestVerifyEd25519_WrongPublicKey
        let correct_private = generate_ed25519().unwrap();
        let wrong_private = generate_ed25519().unwrap();
        let wrong_public = wrong_private.public_key();

        let message = b"test message";
        let signature = correct_private.sign(message).unwrap();

        let result = wrong_public.verify(message, &signature).unwrap();
        assert!(!result, "Verification should fail with wrong public key");
    }

    #[test]
    fn test_sign_verify_multiple_messages() {
        // Verify multiple different messages
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();

        let messages = vec![
            b"message 1".as_slice(),
            b"message 2".as_slice(),
            b"a longer message with more content".as_slice(),
            b"".as_slice(), // empty message
        ];

        for message in messages {
            let signature = private_key.sign(message).unwrap();
            assert_eq!(signature.len(), 64, "Ed25519 signature should be 64 bytes");

            let verified = public_key.verify(message, &signature).unwrap();
            assert!(verified, "Signature should verify for message: {:?}", std::str::from_utf8(message));
        }
    }

    #[test]
    fn test_signature_not_reusable_across_messages() {
        // Verify that a signature for one message doesn't verify for another
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();

        let message1 = b"first message";
        let message2 = b"second message";

        let signature1 = private_key.sign(message1).unwrap();

        // Signature for message1 should not verify for message2
        let result = public_key.verify(message2, &signature1).unwrap();
        assert!(!result, "Signature should not verify for different message");
    }

    #[test]
    fn test_verify_invalid_signature_lengths() {
        // Ed25519 signatures must be exactly 64 bytes
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();
        let message = b"test message";

        // Too short
        let short_sig = vec![0u8; 63];
        let result = public_key.verify(message, &short_sig).unwrap();
        assert!(!result, "Signature with wrong length (63 bytes) should fail");

        // Too long
        let long_sig = vec![0u8; 65];
        let result = public_key.verify(message, &long_sig).unwrap();
        assert!(!result, "Signature with wrong length (65 bytes) should fail");

        // Empty
        let empty_sig = vec![];
        let result = public_key.verify(message, &empty_sig).unwrap();
        assert!(!result, "Empty signature should fail");
    }

    #[test]
    fn test_ed25519_rejects_invalid_points() {
        // Test that ed25519-dalek validates public key bytes represent valid curve points
        // The library rejects bytes that don't decompress to valid Ed25519 points

        // Test that random invalid bytes are rejected
        // Most random 32-byte sequences are not valid Ed25519 public keys
        let invalid_bytes: [u8; 32] = [
            0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
            0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00,
        ];
        let result = Ed25519PublicKey::from_bytes(&invalid_bytes);
        assert!(result.is_none(), "Random invalid bytes should be rejected");

        // Note: all-zeros (identity point) is accepted by ed25519-dalek as it's
        // technically a valid curve point, though useless for signatures
    }

    #[test]
    fn test_ed25519_rejects_point_not_on_curve() {
        // Test specific bytes that are deterministically invalid curve points
        // These bytes fail decompression because no valid y² exists for the x coordinate

        // Pattern 1: specific bytes known to fail decompression
        let invalid_pattern1: [u8; 32] = [
            0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0,
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
            0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00,
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        ];
        assert!(
            Ed25519PublicKey::from_bytes(&invalid_pattern1).is_none(),
            "Invalid pattern 1 should be rejected"
        );

        // Pattern 2: alternating bits - fails decompression
        let invalid_pattern2: [u8; 32] = [
            0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55,
            0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55,
            0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55,
            0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55, 0xAA, 0x55,
        ];
        assert!(
            Ed25519PublicKey::from_bytes(&invalid_pattern2).is_none(),
            "Invalid pattern 2 should be rejected"
        );
    }

    #[test]
    fn test_ed25519_valid_key_is_accepted() {
        // Verify that valid Ed25519 public keys are accepted
        // Generate a valid key and verify it can be reconstructed from bytes
        let private_key = generate_ed25519().unwrap();
        let public_key = private_key.public_key();
        let public_bytes = public_key.raw();

        let reconstructed = Ed25519PublicKey::from_bytes(&public_bytes);
        assert!(
            reconstructed.is_some(),
            "Valid public key bytes should be accepted"
        );
        assert_eq!(
            reconstructed.unwrap().raw(),
            public_bytes,
            "Reconstructed key should match original"
        );
    }
}
