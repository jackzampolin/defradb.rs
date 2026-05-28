//! ECIES wrap/unwrap helpers for KMS pubsub replies.
//!
//! `wrap_for_requester` ECIES-encrypts the plaintext (an `Encryption`
//! block on the wire path) to the requester's X25519 ephemeral pubkey.
//! `crypto::encrypt_ecies` prepends a per-call ephemeral pubkey into the
//! envelope so the requester can derive the shared secret and unwrap with
//! its private key.

use crate::error::{Error, Result};

/// ECIES-encrypt `plaintext` for delivery to a requester whose 32-byte
/// X25519 public key is `requester_pub`. Returns the envelope bytes
/// suitable for inclusion in `FetchEncryptionKeyReply::blocks`.
pub fn wrap_for_requester(plaintext: &[u8], requester_pub: &[u8]) -> Result<Vec<u8>> {
    if requester_pub.len() != 32 {
        return Err(Error::Crypto(
            "requester ephemeral pubkey must be 32 bytes".into(),
        ));
    }
    let array: [u8; 32] = requester_pub.try_into().unwrap();
    let public = x25519_dalek::PublicKey::from(array);
    let options = crypto::EciesOptions::builder()
        .prepend_public_key(true)
        .build();
    crypto::encrypt_ecies(plaintext, &public, options).map_err(|e| Error::Crypto(e.to_string()))
}

/// ECIES-decrypt a reply envelope using the requester's X25519 private key.
pub fn unwrap_with_private(
    envelope: &[u8],
    private: &x25519_dalek::StaticSecret,
) -> Result<Vec<u8>> {
    let options = crypto::EciesOptions::builder()
        .prepend_public_key(true)
        .build();
    crypto::decrypt_ecies(envelope, private, options).map_err(|e| Error::Crypto(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wrap_then_unwrap_roundtrip() {
        let requester = crypto::generate_x25519().unwrap();
        let requester_pub = x25519_dalek::PublicKey::from(&requester);
        let plaintext = b"encryption-block-bytes-here";
        let envelope = wrap_for_requester(plaintext, requester_pub.as_bytes()).unwrap();
        let unwrapped = unwrap_with_private(&envelope, &requester).unwrap();
        assert_eq!(&unwrapped[..], plaintext);
    }

    #[test]
    fn unwrap_rejects_tampered_ciphertext() {
        let requester = crypto::generate_x25519().unwrap();
        let requester_pub = x25519_dalek::PublicKey::from(&requester);
        let mut env = wrap_for_requester(b"hi", requester_pub.as_bytes()).unwrap();
        let last = env.len() - 1;
        env[last] ^= 0x01;
        assert!(unwrap_with_private(&env, &requester).is_err());
    }

    #[test]
    fn unwrap_rejects_wrong_private_key() {
        let requester = crypto::generate_x25519().unwrap();
        let attacker = crypto::generate_x25519().unwrap();
        let requester_pub = x25519_dalek::PublicKey::from(&requester);
        let env = wrap_for_requester(b"secret", requester_pub.as_bytes()).unwrap();
        assert!(unwrap_with_private(&env, &attacker).is_err());
    }

    #[test]
    fn wrap_rejects_wrong_pubkey_length() {
        let result = wrap_for_requester(b"hi", &[0u8; 16]);
        assert!(matches!(result, Err(crate::Error::Crypto(_))));
    }
}
