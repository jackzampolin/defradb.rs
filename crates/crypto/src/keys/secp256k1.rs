//! secp256k1 key implementations
//!
//! secp256k1 is the elliptic curve used by Bitcoin, Ethereum, and other blockchains.
//! - Public keys: 33 bytes (compressed format with 0x02/0x03 prefix)
//! - Private keys: 32 bytes
//! - Signatures: DER-encoded ECDSA signatures

use k256::ecdsa::{
    signature::DigestSigner, signature::DigestVerifier, Signature, SigningKey, VerifyingKey,
};
use k256::EncodedPoint;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use defra_core::Result;

use crate::keys::{Key, PrivateKey, PublicKey};
use crate::types::{KeyType, SECP256K1_PRIVATE_KEY_SIZE};

/// secp256k1 private key wrapper
#[derive(Clone)]
pub struct Secp256k1PrivateKey {
    key: SigningKey,
}

impl PartialEq for Secp256k1PrivateKey {
    fn eq(&self, other: &Self) -> bool {
        // Compare private keys using constant-time comparison to prevent timing attacks
        self.key.to_bytes().ct_eq(&other.key.to_bytes()).into()
    }
}

impl Eq for Secp256k1PrivateKey {}

impl std::fmt::Debug for Secp256k1PrivateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't print key material for security
        f.debug_struct("Secp256k1PrivateKey")
            .field("key_type", &KeyType::Secp256k1)
            .finish_non_exhaustive()
    }
}

impl Secp256k1PrivateKey {
    /// Create a new secp256k1 private key from raw bytes
    ///
    /// # Parameters
    /// * `bytes` - 32-byte secp256k1 private key
    ///
    /// # Returns
    /// * `Some(Secp256k1PrivateKey)` if the key is valid
    /// * `None` if the key is invalid (wrong length or nil)
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        if bytes.len() != SECP256K1_PRIVATE_KEY_SIZE {
            return None;
        }

        let key = SigningKey::from_slice(bytes).ok()?;
        Some(Self { key })
    }

    /// Get the underlying k256 signing key
    pub fn underlying(&self) -> &SigningKey {
        &self.key
    }
}

impl Key for Secp256k1PrivateKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Secp256k1 {
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
        KeyType::Secp256k1
    }
}

impl PrivateKey for Secp256k1PrivateKey {
    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        // Hash the message with SHA-256 (required for ECDSA)
        // Use DigestSigner to sign pre-hashed data (matches Go behavior)
        let mut hasher = Sha256::new();
        hasher.update(data);

        // Sign the pre-hashed digest using DigestSigner trait
        let signature: Signature = self.key.sign_digest(hasher);

        // Return DER-encoded signature (X.690 Distinguished Encoding Rules)
        // DER is the standard format for Bitcoin/blockchain ECDSA signatures because:
        // 1. Self-describing format with type and length fields
        // 2. Deterministic serialization (unlike raw r,s which have variable length)
        // 3. Required for DefraDB Go implementation compatibility
        Ok(signature.to_der().as_bytes().to_vec())
    }

    fn public_key(&self) -> Box<dyn PublicKey> {
        Box::new(Secp256k1PublicKey {
            key: *self.key.verifying_key(),
        })
    }
}

/// secp256k1 public key wrapper
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Secp256k1PublicKey {
    #[serde(with = "secp256k1_public_key_serde")]
    key: VerifyingKey,
}

impl Secp256k1PublicKey {
    /// Create a new secp256k1 public key from raw bytes
    ///
    /// Supports both compressed (33 bytes) and uncompressed (65 bytes) formats.
    ///
    /// # Parameters
    /// * `bytes` - Public key bytes (33 or 65 bytes)
    ///
    /// # Returns
    /// * `Some(Secp256k1PublicKey)` if the key is valid
    /// * `None` if the key is invalid
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() {
            return None;
        }

        // EncodedPoint handles both compressed (33) and uncompressed (65) formats
        let point = EncodedPoint::from_bytes(bytes).ok()?;
        let key = VerifyingKey::from_encoded_point(&point).ok()?;

        Some(Self { key })
    }

    /// Get the underlying k256 verifying key
    pub fn underlying(&self) -> &VerifyingKey {
        &self.key
    }
}

impl Key for Secp256k1PublicKey {
    fn equal(&self, other: &dyn Key) -> bool {
        if other.key_type() != KeyType::Secp256k1 {
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
        // Always return compressed format (33 bytes with 0x02/0x03 prefix)
        self.key.to_encoded_point(true).as_bytes().to_vec()
    }

    fn key_type(&self) -> KeyType {
        KeyType::Secp256k1
    }
}

impl PublicKey for Secp256k1PublicKey {
    fn verify(&self, data: &[u8], signature: &[u8]) -> Result<bool> {
        // Parse DER-encoded signature
        let sig = match Signature::from_der(signature) {
            Ok(s) => s,
            Err(_) => return Ok(false),
        };

        // Normalize S to low-S form if needed for Go compatibility
        // ECDSA signatures have the property that both (r, s) and (r, n-s) are valid.
        // Some implementations (like Go's dcrd) may produce high-S signatures,
        // while k256 verification expects low-S. Normalizing ensures compatibility.
        let sig = sig.normalize_s().unwrap_or(sig);

        // Hash the message with SHA-256 using DigestVerifier trait
        // This matches Go behavior which signs/verifies against SHA256(message)
        let mut hasher = Sha256::new();
        hasher.update(data);

        // Verify signature against the pre-hashed digest
        match self.key.verify_digest(hasher, &sig) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    fn did(&self) -> Result<String> {
        // Use uncompressed format (65 bytes) for DID generation to match Go implementation
        // Go uses SerializeUncompressed() which produces the full [0x04 | x | y] format
        let uncompressed = self.key.to_encoded_point(false);
        crate::did::create_did_key(KeyType::Secp256k1, uncompressed.as_bytes())
    }
}

// Custom serde module for VerifyingKey
mod secp256k1_public_key_serde {
    use k256::ecdsa::VerifyingKey;
    use k256::EncodedPoint;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(key: &VerifyingKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as compressed format
        let bytes = key.to_encoded_point(true);
        serializer.serialize_bytes(bytes.as_bytes())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<VerifyingKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let point = EncodedPoint::from_bytes(&bytes)
            .map_err(|e| serde::de::Error::custom(e.to_string()))?;
        VerifyingKey::from_encoded_point(&point).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::generation::generate_secp256k1;

    #[test]
    fn test_secp256k1_key_type() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[1u8; 32]).unwrap();
        assert_eq!(private_key.key_type(), KeyType::Secp256k1);

        let public_key = private_key.public_key();
        assert_eq!(public_key.key_type(), KeyType::Secp256k1);
    }

    #[test]
    fn test_secp256k1_raw_bytes() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[2u8; 32]).unwrap();
        let raw = private_key.raw();
        assert_eq!(raw.len(), 32);
    }

    #[test]
    fn test_secp256k1_public_key_compressed() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[3u8; 32]).unwrap();
        let public_key = private_key.public_key();
        let raw = public_key.raw();

        // Should be compressed format (33 bytes)
        assert_eq!(raw.len(), 33);
        // First byte should be 0x02 or 0x03
        assert!(raw[0] == 0x02 || raw[0] == 0x03);
    }

    #[test]
    fn test_secp256k1_sign_verify() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[4u8; 32]).unwrap();
        let message = b"test message";

        let signature = private_key.sign(message).unwrap();
        // DER signatures vary in length but are typically 70-72 bytes
        assert!(signature.len() >= 70 && signature.len() <= 73);

        let public_key = private_key.public_key();
        let valid = public_key.verify(message, &signature).unwrap();
        assert!(valid);
    }

    #[test]
    fn test_secp256k1_verify_wrong_message() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[5u8; 32]).unwrap();
        let message = b"test message";
        let signature = private_key.sign(message).unwrap();

        let public_key = private_key.public_key();
        let wrong_message = b"wrong message";
        let valid = public_key.verify(wrong_message, &signature).unwrap();
        assert!(!valid);
    }

    #[test]
    fn test_secp256k1_invalid_signature() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[6u8; 32]).unwrap();
        let public_key = private_key.public_key();

        let message = b"test message";
        let invalid_sig = vec![0u8; 64];
        let valid = public_key.verify(message, &invalid_sig).unwrap();
        assert!(!valid);
    }

    #[test]
    fn test_secp256k1_invalid_key_lengths() {
        // Invalid private key length
        let private_key = Secp256k1PrivateKey::from_bytes(&[0u8; 16]);
        assert!(private_key.is_none());

        // Invalid public key length
        let public_key = Secp256k1PublicKey::from_bytes(&[0u8; 10]);
        assert!(public_key.is_none());
    }

    #[test]
    fn test_secp256k1_nil_keys() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[]);
        assert!(private_key.is_none());

        let public_key = Secp256k1PublicKey::from_bytes(&[]);
        assert!(public_key.is_none());
    }

    #[test]
    fn test_secp256k1_key_equality() {
        let key1 = Secp256k1PrivateKey::from_bytes(&[7u8; 32]).unwrap();
        let key2 = Secp256k1PrivateKey::from_bytes(&[7u8; 32]).unwrap();
        assert!(key1.equal(&key2 as &dyn Key));

        let key3 = Secp256k1PrivateKey::from_bytes(&[8u8; 32]).unwrap();
        assert!(!key1.equal(&key3 as &dyn Key));
    }

    #[test]
    fn test_secp256k1_public_key_from_compressed() {
        // Create a compressed public key (33 bytes starting with 0x02 or 0x03)
        let private_key = Secp256k1PrivateKey::from_bytes(&[9u8; 32]).unwrap();
        let compressed = private_key.public_key().raw();

        // Should be able to parse compressed format
        let public_key = Secp256k1PublicKey::from_bytes(&compressed);
        assert!(public_key.is_some());
    }

    #[test]
    fn test_secp256k1_partial_eq() {
        // Test PartialEq implementation for private keys
        let key1 = Secp256k1PrivateKey::from_bytes(&[10u8; 32]).unwrap();
        let key2 = Secp256k1PrivateKey::from_bytes(&[10u8; 32]).unwrap();
        assert_eq!(key1, key2, "Same keys should be equal");

        let key3 = Secp256k1PrivateKey::from_bytes(&[11u8; 32]).unwrap();
        assert_ne!(key1, key3, "Different keys should not be equal");

        // Test PartialEq for public keys (already derived)
        let pub1 = key1.public_key();
        let pub2 = key2.public_key();
        let pub1_concrete = Secp256k1PublicKey::from_bytes(&pub1.raw()).unwrap();
        let pub2_concrete = Secp256k1PublicKey::from_bytes(&pub2.raw()).unwrap();
        assert_eq!(
            pub1_concrete, pub2_concrete,
            "Public keys from same private key should be equal"
        );
    }

    #[test]
    fn test_secp256k1_uncompressed_format() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[12u8; 32]).unwrap();
        let public_key = private_key.public_key();

        // Get uncompressed format (65 bytes: 0x04 + 32-byte X + 32-byte Y)
        let compressed = public_key.raw();
        let verifying_key = private_key.key.verifying_key();
        let uncompressed_point = verifying_key.to_encoded_point(false);
        let uncompressed = uncompressed_point.as_bytes();

        // Verify uncompressed format
        assert_eq!(
            uncompressed.len(),
            65,
            "Uncompressed key should be 65 bytes"
        );
        assert_eq!(
            uncompressed[0], 0x04,
            "Uncompressed key should start with 0x04"
        );

        // Should be able to parse uncompressed format
        let parsed = Secp256k1PublicKey::from_bytes(uncompressed);
        assert!(parsed.is_some(), "Should parse uncompressed format");

        // Parsed key should produce same compressed output
        let parsed_compressed = parsed.unwrap().raw();
        assert_eq!(
            compressed, parsed_compressed,
            "Parsed key should match original compressed"
        );
    }

    #[test]
    fn test_secp256k1_key_equality_through_trait_objects() {
        use crate::keys::Key;

        // Create two keys with same bytes
        let key1 = Secp256k1PrivateKey::from_bytes(&[13u8; 32]).unwrap();
        let key2 = Secp256k1PrivateKey::from_bytes(&[13u8; 32]).unwrap();

        // Test equality through trait objects
        let key1_trait: &dyn Key = &key1;
        let key2_trait: &dyn Key = &key2;
        assert!(
            key1_trait.equal(key2_trait),
            "Keys should be equal through trait objects"
        );

        // Test inequality
        let key3 = Secp256k1PrivateKey::from_bytes(&[14u8; 32]).unwrap();
        let key3_trait: &dyn Key = &key3;
        assert!(
            !key1_trait.equal(key3_trait),
            "Different keys should not be equal through trait objects"
        );
    }

    #[test]
    fn test_secp256k1_invalid_der_signatures() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[1u8; 32]).unwrap();
        let public_key = private_key.public_key();
        let message = b"test";

        // Empty signature
        let result = public_key.verify(message, &[]);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Empty signature should return false"
        );

        // Invalid DER: wrong sequence tag (should be 0x30)
        let invalid_der = vec![0xFF, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01];
        let result = public_key.verify(message, &invalid_der);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Invalid DER tag should return false"
        );

        // Invalid DER: length exceeds data
        let invalid_der = vec![0x30, 0xFF, 0x02, 0x01, 0x01];
        let result = public_key.verify(message, &invalid_der);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Invalid DER length should return false"
        );

        // Too short to be valid DER
        let invalid_der = vec![0x30, 0x02];
        let result = public_key.verify(message, &invalid_der);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Truncated DER should return false"
        );

        // Single byte (not valid DER)
        let invalid_der = vec![0x30];
        let result = public_key.verify(message, &invalid_der);
        assert!(
            result.is_ok() && !result.unwrap(),
            "Single byte should return false"
        );
    }

    #[test]
    fn test_secp256k1_signature_with_empty_message() {
        let private_key = Secp256k1PrivateKey::from_bytes(&[2u8; 32]).unwrap();
        let empty_message = b"";

        let signature = private_key.sign(empty_message).unwrap();
        assert!(
            signature.len() >= 8 && signature.len() <= 73,
            "DER signature should be 8-73 bytes"
        );

        let public_key = private_key.public_key();
        let valid = public_key.verify(empty_message, &signature).unwrap();
        assert!(valid, "Empty message signature should verify");
    }

    // ===== Signature Tests (ported from Go signature_test.go) =====

    #[test]
    fn test_sign_verify_round_trip() {
        // TestSignECDSA_WithPrivateKeyStruct + TestVerifyECDSA_WithPublicKeyStruct
        let private_key = generate_secp256k1().unwrap();
        let message = b"test message";

        let signature = private_key.sign(message).unwrap();
        let public_key = private_key.public_key();

        let verified = public_key.verify(message, &signature).unwrap();
        assert!(
            verified,
            "Signature should verify with correct key and message"
        );
    }

    #[test]
    fn test_verify_tampered_message() {
        // TestVerifyECDSA_TamperedMessage
        let private_key = generate_secp256k1().unwrap();
        let public_key = private_key.public_key();

        let original_message = b"original message";
        let signature = private_key.sign(original_message).unwrap();

        let tampered_message = b"tampered message";
        let result = public_key.verify(tampered_message, &signature).unwrap();

        assert!(!result, "Verification should fail with tampered message");
    }

    #[test]
    fn test_verify_tampered_signature() {
        // TestVerifyECDSA_TamperedSignature
        let private_key = generate_secp256k1().unwrap();
        let public_key = private_key.public_key();
        let message = b"test message";

        let mut signature = private_key.sign(message).unwrap();

        // Tamper with signature by flipping bits in the middle
        if signature.len() > 10 {
            let mid = signature.len() / 2;
            signature[mid] ^= 0xFF;
        }

        let result = public_key.verify(message, &signature).unwrap();
        assert!(!result, "Verification should fail with tampered signature");
    }

    #[test]
    fn test_verify_wrong_public_key() {
        // TestVerifyECDSA_WrongPublicKey
        let correct_private = generate_secp256k1().unwrap();
        let wrong_private = generate_secp256k1().unwrap();
        let wrong_public = wrong_private.public_key();

        let message = b"test message";
        let signature = correct_private.sign(message).unwrap();

        let result = wrong_public.verify(message, &signature).unwrap();
        assert!(!result, "Verification should fail with wrong public key");
    }

    #[test]
    fn test_sign_verify_multiple_messages() {
        // Additional test: verify multiple different messages
        let private_key = generate_secp256k1().unwrap();
        let public_key = private_key.public_key();

        let messages = vec![
            b"message 1".as_slice(),
            b"message 2".as_slice(),
            b"a longer message with more content".as_slice(),
            b"".as_slice(), // empty message
        ];

        for message in messages {
            let signature = private_key.sign(message).unwrap();
            let verified = public_key.verify(message, &signature).unwrap();
            assert!(
                verified,
                "Signature should verify for message: {:?}",
                std::str::from_utf8(message)
            );
        }
    }

    #[test]
    fn test_signature_not_reusable_across_messages() {
        // Verify that a signature for one message doesn't verify for another
        let private_key = generate_secp256k1().unwrap();
        let public_key = private_key.public_key();

        let message1 = b"first message";
        let message2 = b"second message";

        let signature1 = private_key.sign(message1).unwrap();

        // Signature for message1 should not verify for message2
        let result = public_key.verify(message2, &signature1).unwrap();
        assert!(!result, "Signature should not verify for different message");
    }

    #[test]
    fn test_secp256k1_invalid_curve_point() {
        // Test that invalid curve points are rejected

        // Valid length (33 bytes) but all zeros with a 0x02 prefix - invalid point
        let mut invalid_point = vec![0x02u8];
        invalid_point.extend_from_slice(&[0u8; 32]);
        let result = Secp256k1PublicKey::from_bytes(&invalid_point);
        assert!(result.is_none(), "All-zero X coordinate should be rejected");

        // Valid length with 0x03 prefix and all zeros - also invalid
        let mut invalid_point2 = vec![0x03u8];
        invalid_point2.extend_from_slice(&[0u8; 32]);
        let result = Secp256k1PublicKey::from_bytes(&invalid_point2);
        assert!(
            result.is_none(),
            "All-zero X coordinate with 0x03 prefix should be rejected"
        );

        // Valid length but X coordinate larger than field prime - invalid
        let mut invalid_point3 = vec![0x02u8];
        invalid_point3.extend_from_slice(&[0xFFu8; 32]); // All 0xFF exceeds field prime
        let result = Secp256k1PublicKey::from_bytes(&invalid_point3);
        assert!(
            result.is_none(),
            "X coordinate exceeding field prime should be rejected"
        );

        // Invalid prefix bytes that k256 rejects
        // 0x00 is invalid
        let mut invalid_prefix_00 = vec![0x00u8];
        invalid_prefix_00.extend_from_slice(&[1u8; 32]);
        let result = Secp256k1PublicKey::from_bytes(&invalid_prefix_00);
        assert!(result.is_none(), "Prefix 0x00 should be rejected");

        // 0x01 is invalid
        let mut invalid_prefix_01 = vec![0x01u8];
        invalid_prefix_01.extend_from_slice(&[1u8; 32]);
        let result = Secp256k1PublicKey::from_bytes(&invalid_prefix_01);
        assert!(result.is_none(), "Prefix 0x01 should be rejected");

        // 0x06 is invalid (0x06 and 0x07 are hybrid formats not supported)
        let mut invalid_prefix_06 = vec![0x06u8];
        invalid_prefix_06.extend_from_slice(&[1u8; 64]);
        let result = Secp256k1PublicKey::from_bytes(&invalid_prefix_06);
        assert!(result.is_none(), "Prefix 0x06 should be rejected");

        // Uncompressed format (65 bytes) but all zeros - invalid point at infinity
        let mut invalid_uncompressed = vec![0x04u8];
        invalid_uncompressed.extend_from_slice(&[0u8; 64]); // Both X and Y are zero
        let result = Secp256k1PublicKey::from_bytes(&invalid_uncompressed);
        assert!(result.is_none(), "Point at infinity should be rejected");
    }

    #[test]
    fn test_secp256k1_point_not_on_curve() {
        // Generate valid point and corrupt Y coordinate in uncompressed form
        let private_key = Secp256k1PrivateKey::from_bytes(&[7u8; 32]).unwrap();
        let _public_key = private_key.public_key();

        // Get uncompressed format and corrupt last byte (Y coordinate)
        let verifying_key = private_key.key.verifying_key();
        let mut uncompressed = verifying_key.to_encoded_point(false).as_bytes().to_vec();
        assert_eq!(uncompressed.len(), 65, "Uncompressed should be 65 bytes");

        // Flip last byte to make Y invalid for this X
        uncompressed[64] ^= 0xFF;

        let result = Secp256k1PublicKey::from_bytes(&uncompressed);
        assert!(
            result.is_none(),
            "Point with invalid Y for given X should be rejected"
        );
    }
}
