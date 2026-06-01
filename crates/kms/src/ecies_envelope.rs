//! ECIES wrap/unwrap helpers for KMS pubsub replies.
//!
//! Byte-wire compatible with Go's `internal/kms/pubsub.go`:
//! - The ephemeral pubkey is NOT prepended into the ciphertext
//!   (Go's `WithPubKeyPrepended(false)`); the responder's ephemeral pubkey
//!   travels in the reply's separate `ephemeral_public_key` field.
//! - The AES-GCM tag binds `AAD = base64_std(requester_eph_pub ‖ peer_id_utf8)`
//!   (Go's `makeAssociatedData`), where `peer_id` is the responder's peer id.

use crate::error::{Error, Result};

/// Build the ECIES associated data exactly as Go's `makeAssociatedData`:
/// `base64_std(requester_ephemeral_pubkey_bytes ++ peer_id_utf8_bytes)`.
pub fn make_associated_data(requester_eph_pub: &[u8], peer_id: &str) -> Vec<u8> {
    use base64::Engine;
    let mut joined = Vec::with_capacity(requester_eph_pub.len() + peer_id.len());
    joined.extend_from_slice(requester_eph_pub);
    joined.extend_from_slice(peer_id.as_bytes());
    base64::engine::general_purpose::STANDARD
        .encode(joined)
        .into_bytes()
}

/// ECIES-encrypt `plaintext` for `requester_pub` using the responder's
/// ephemeral private key `responder_eph_priv` (Go's `WithPrivKey`), NOT
/// prepending the ephemeral pubkey (Go's `WithPubKeyPrepended(false)`), and
/// binding `aad`. The responder's ephemeral PUBLIC key must be carried in
/// the reply's `ephemeral_public_key` field by the caller.
pub fn wrap_for_requester(
    plaintext: &[u8],
    requester_pub: &[u8],
    responder_eph_priv: &x25519_dalek::StaticSecret,
    aad: &[u8],
) -> Result<Vec<u8>> {
    if requester_pub.len() != 32 {
        return Err(Error::Crypto(format!(
            "requester ephemeral pubkey must be 32 bytes, got {}",
            requester_pub.len()
        )));
    }
    let arr: [u8; 32] = requester_pub.try_into().unwrap();
    let public = x25519_dalek::PublicKey::from(arr);
    let options = crypto::EciesOptions::builder()
        .prepend_public_key(false)
        .with_private_key(responder_eph_priv.clone())
        .with_aad(aad.to_vec())
        .build();
    crypto::encrypt_ecies(plaintext, &public, options).map_err(|e| Error::Crypto(e.to_string()))
}

/// ECIES-decrypt a reply envelope with the requester's ephemeral private
/// key and the responder's ephemeral pubkey (from the reply field), binding `aad`.
pub fn unwrap_with_private(
    envelope: &[u8],
    requester_eph_priv: &x25519_dalek::StaticSecret,
    responder_eph_pub: &[u8],
    aad: &[u8],
) -> Result<Vec<u8>> {
    let options = crypto::EciesOptions::builder()
        .prepend_public_key(false)
        .with_public_key_bytes(responder_eph_pub.to_vec())
        .with_aad(aad.to_vec())
        .build();
    crypto::decrypt_ecies(envelope, requester_eph_priv, options)
        .map_err(|e| Error::Crypto(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pub_bytes(s: &x25519_dalek::StaticSecret) -> Vec<u8> {
        x25519_dalek::PublicKey::from(s).as_bytes().to_vec()
    }

    #[test]
    fn make_associated_data_matches_go_layout() {
        // base64_std( req_eph_pub ++ peer_id_utf8 )
        use base64::Engine;
        let req_pub = [7u8; 32];
        let peer = "peer-xyz";
        let mut joined = req_pub.to_vec();
        joined.extend_from_slice(peer.as_bytes());
        let expected = base64::engine::general_purpose::STANDARD
            .encode(&joined)
            .into_bytes();
        assert_eq!(make_associated_data(&req_pub, peer), expected);
    }

    #[test]
    fn wrap_then_unwrap_roundtrip() {
        let requester = crypto::generate_x25519().unwrap();
        let responder = crypto::generate_x25519().unwrap();
        let req_pub = pub_bytes(&requester);
        let resp_pub = pub_bytes(&responder);
        let aad = make_associated_data(&req_pub, "peer");
        let plaintext = b"encryption-block-bytes-here";
        let envelope = wrap_for_requester(plaintext, &req_pub, &responder, &aad).unwrap();
        let unwrapped = unwrap_with_private(&envelope, &requester, &resp_pub, &aad).unwrap();
        assert_eq!(&unwrapped[..], plaintext);
    }

    #[test]
    fn envelope_does_not_prepend_responder_pubkey() {
        // prepend=false ⇒ envelope = [AES-GCM(nonce|ct) | HMAC], no leading
        // 32-byte responder pubkey. Decrypting WITHOUT supplying the responder
        // pubkey (i.e. treating the leading bytes as a prepended key) must fail.
        let requester = crypto::generate_x25519().unwrap();
        let responder = crypto::generate_x25519().unwrap();
        let req_pub = pub_bytes(&requester);
        let aad = make_associated_data(&req_pub, "peer");
        let envelope = wrap_for_requester(b"hi", &req_pub, &responder, &aad).unwrap();

        // Attempt to decrypt as if the responder pubkey were prepended.
        let options = crypto::EciesOptions::builder()
            .prepend_public_key(true)
            .with_aad(aad.clone())
            .build();
        assert!(crypto::decrypt_ecies(&envelope, &requester, options).is_err());
    }

    #[test]
    fn unwrap_rejects_tampered_ciphertext() {
        let requester = crypto::generate_x25519().unwrap();
        let responder = crypto::generate_x25519().unwrap();
        let req_pub = pub_bytes(&requester);
        let resp_pub = pub_bytes(&responder);
        let aad = make_associated_data(&req_pub, "peer");
        let mut env = wrap_for_requester(b"hi", &req_pub, &responder, &aad).unwrap();
        let last = env.len() - 1;
        env[last] ^= 0x01;
        assert!(unwrap_with_private(&env, &requester, &resp_pub, &aad).is_err());
    }

    #[test]
    fn unwrap_rejects_wrong_aad() {
        let requester = crypto::generate_x25519().unwrap();
        let responder = crypto::generate_x25519().unwrap();
        let req_pub = pub_bytes(&requester);
        let resp_pub = pub_bytes(&responder);
        let aad = make_associated_data(&req_pub, "peer-a");
        let wrong_aad = make_associated_data(&req_pub, "peer-b");
        let env = wrap_for_requester(b"secret", &req_pub, &responder, &aad).unwrap();
        assert!(unwrap_with_private(&env, &requester, &resp_pub, &wrong_aad).is_err());
    }

    #[test]
    fn unwrap_rejects_wrong_private_key() {
        let requester = crypto::generate_x25519().unwrap();
        let responder = crypto::generate_x25519().unwrap();
        let attacker = crypto::generate_x25519().unwrap();
        let req_pub = pub_bytes(&requester);
        let resp_pub = pub_bytes(&responder);
        let aad = make_associated_data(&req_pub, "peer");
        let env = wrap_for_requester(b"secret", &req_pub, &responder, &aad).unwrap();
        assert!(unwrap_with_private(&env, &attacker, &resp_pub, &aad).is_err());
    }

    #[test]
    fn wrap_rejects_wrong_pubkey_length() {
        let responder = crypto::generate_x25519().unwrap();
        let result = wrap_for_requester(b"hi", &[0u8; 16], &responder, b"");
        assert!(matches!(result, Err(crate::Error::Crypto(_))));
    }
}
