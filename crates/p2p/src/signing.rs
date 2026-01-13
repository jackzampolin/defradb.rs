// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Message signing and verification for DefraDB P2P protocol.
//!
//! This module provides functions for signing outgoing messages and
//! verifying incoming messages, ensuring wire compatibility with the
//! Go implementation.
//!
//! # Wire Compatibility
//!
//! The signing algorithm matches Go's `signAndSetMetaData`:
//! 1. Generate UUID v4 for message_id (if empty)
//! 2. Set version to MESSAGE_VERSION
//! 3. Set sender_id to peer ID string
//! 4. Set pubkey to protobuf-encoded public key
//! 5. CBOR serialize with signature=None
//! 6. Sign the serialized bytes
//! 7. Set signature field
//!
//! Verification matches Go's `verifyMessage`:
//! 1. Decode public key from message
//! 2. Verify peer ID matches public key
//! 3. Clear signature, serialize, verify signature

use libp2p::identity::{Keypair, PublicKey};
use libp2p::PeerId;
use serde::{de::DeserializeOwned, Serialize};
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::message::Message;
use crate::protocol::MESSAGE_VERSION;

/// Sign a message with the given keypair.
///
/// This function populates all metadata fields and signs the message:
/// - Sets `message_id` to a new UUID v4 (if empty)
/// - Sets `version` to the current protocol version
/// - Sets `sender_id` to the peer ID derived from the keypair
/// - Sets `pubkey` to the protobuf-encoded public key
/// - Signs the CBOR-encoded message and sets `signature`
///
/// # Arguments
///
/// * `keypair` - The libp2p keypair to use for signing
/// * `msg` - The message to sign (must implement `Message + Serialize + Clone`)
///
/// # Returns
///
/// The signed message with all metadata populated, or an error if signing fails.
///
/// # Wire Compatibility
///
/// This function matches Go's `signAndSetMetaData` from `message/message.go`.
pub fn sign_message<M>(keypair: &Keypair, msg: &mut M) -> Result<()>
where
    M: Message + Serialize,
{
    let metadata = msg.metadata_mut();

    // Generate message ID if not already set
    if metadata.message_id.is_empty() {
        metadata.message_id = Uuid::new_v4().to_string();
    }

    // Set protocol version
    metadata.version = MESSAGE_VERSION.to_string();

    // Set sender ID from keypair
    let peer_id = keypair.public().to_peer_id();
    metadata.sender_id = peer_id.to_string();

    // Set public key (protobuf encoded, matching Go's libp2p)
    metadata.pubkey = keypair.public().encode_protobuf();

    // Clear signature before serializing for signing
    metadata.signature = None;

    // CBOR serialize the message
    let bytes = serde_cbor::to_vec(&msg).map_err(|e| Error::CborSerialization(e.to_string()))?;

    // Sign the serialized bytes
    let signature = keypair
        .sign(&bytes)
        .map_err(|e| Error::SigningFailed(e.to_string()))?;

    // Set the signature
    msg.metadata_mut().signature = Some(signature);

    Ok(())
}

/// Verify a message signature.
///
/// This function verifies that:
/// 1. The message has a signature
/// 2. The public key is valid
/// 3. The peer ID matches the public key
/// 4. The signature is valid for the message content
///
/// # Arguments
///
/// * `msg` - The message to verify (must implement `Message + Serialize + Clone`)
///
/// # Returns
///
/// `Ok(())` if the signature is valid, or an error describing why verification failed.
///
/// # Wire Compatibility
///
/// This function matches Go's `verifyMessage` from `message/message.go`.
pub fn verify_message<M>(msg: &M) -> Result<()>
where
    M: Message + Serialize + Clone,
{
    let metadata = msg.metadata();

    // Check that signature exists
    let signature = metadata.signature.as_ref().ok_or(Error::MissingSignature)?;

    // Decode public key from message
    let pubkey = PublicKey::try_decode_protobuf(&metadata.pubkey)
        .map_err(|e| Error::PublicKeyDecode(e.to_string()))?;

    // Derive peer ID from public key
    let id_from_key = pubkey.to_peer_id();

    // Parse sender ID as peer ID
    let sender_peer_id: PeerId = metadata
        .sender_id
        .parse()
        .map_err(|e: libp2p::identity::ParseError| Error::InvalidPeerId(e.to_string()))?;

    // Verify peer ID matches
    if id_from_key != sender_peer_id {
        return Err(Error::PubkeyPeerIdMismatch);
    }

    // Clone message and clear signature for verification
    let mut msg_for_verify = msg.clone();
    msg_for_verify.metadata_mut().signature = None;

    // CBOR serialize
    let bytes =
        serde_cbor::to_vec(&msg_for_verify).map_err(|e| Error::CborSerialization(e.to_string()))?;

    // Verify signature
    if !pubkey.verify(&bytes, signature) {
        return Err(Error::InvalidSignature);
    }

    Ok(())
}

/// Sign a message and return a new signed copy.
///
/// This is a convenience function that clones the message, signs it,
/// and returns the signed copy.
pub fn sign_message_cloned<M>(keypair: &Keypair, msg: &M) -> Result<M>
where
    M: Message + Serialize + Clone + DeserializeOwned,
{
    let mut signed = msg.clone();
    sign_message(keypair, &mut signed)?;
    Ok(signed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::PushLogRequest;

    fn create_test_message() -> PushLogRequest {
        PushLogRequest::new(
            "doc123".to_string(),
            vec![1, 2, 3, 4],
            "collection1".to_string(),
            "creator1".to_string(),
            vec![5, 6, 7, 8],
        )
    }

    #[test]
    fn test_sign_message_sets_all_fields() {
        let keypair = Keypair::generate_ed25519();
        let mut msg = create_test_message();

        // Before signing, metadata should have defaults
        assert!(msg.metadata.message_id.is_empty());
        assert!(msg.metadata.sender_id.is_empty());
        assert!(msg.metadata.pubkey.is_empty());
        assert!(msg.metadata.signature.is_none());

        // Sign the message
        sign_message(&keypair, &mut msg).expect("signing should succeed");

        // After signing, all fields should be populated
        assert!(!msg.metadata.message_id.is_empty());
        assert_eq!(msg.metadata.version, MESSAGE_VERSION);
        assert!(!msg.metadata.sender_id.is_empty());
        assert!(!msg.metadata.pubkey.is_empty());
        assert!(msg.metadata.signature.is_some());

        // Verify sender_id matches keypair's peer ID
        let expected_peer_id = keypair.public().to_peer_id().to_string();
        assert_eq!(msg.metadata.sender_id, expected_peer_id);

        // Verify pubkey matches keypair's public key
        let expected_pubkey = keypair.public().encode_protobuf();
        assert_eq!(msg.metadata.pubkey, expected_pubkey);
    }

    #[test]
    fn test_sign_verify_roundtrip() {
        let keypair = Keypair::generate_ed25519();
        let mut msg = create_test_message();

        // Sign the message
        sign_message(&keypair, &mut msg).expect("signing should succeed");

        // Verify should pass
        verify_message(&msg).expect("verification should succeed");
    }

    #[test]
    fn test_verify_tampered_message_fails() {
        let keypair = Keypair::generate_ed25519();
        let mut msg = create_test_message();

        // Sign the message
        sign_message(&keypair, &mut msg).expect("signing should succeed");

        // Tamper with the message content
        msg.doc_id = "tampered_doc".to_string();

        // Verify should fail
        let result = verify_message(&msg);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidSignature) => {}
            Err(e) => panic!("Expected InvalidSignature, got: {:?}", e),
            Ok(_) => panic!("Expected verification to fail"),
        }
    }

    #[test]
    fn test_verify_wrong_signature_fails() {
        let keypair = Keypair::generate_ed25519();
        let mut msg = create_test_message();

        // Sign the message
        sign_message(&keypair, &mut msg).expect("signing should succeed");

        // Replace signature with garbage
        msg.metadata.signature = Some(vec![0xDE, 0xAD, 0xBE, 0xEF]);

        // Verify should fail
        let result = verify_message(&msg);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidSignature) => {}
            Err(e) => panic!("Expected InvalidSignature, got: {:?}", e),
            Ok(_) => panic!("Expected verification to fail"),
        }
    }

    #[test]
    fn test_verify_pubkey_mismatch_fails() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();
        let mut msg = create_test_message();

        // Sign with keypair1
        sign_message(&keypair1, &mut msg).expect("signing should succeed");

        // Replace sender_id with keypair2's peer ID (but keep keypair1's pubkey and signature)
        msg.metadata.sender_id = keypair2.public().to_peer_id().to_string();

        // Verify should fail because pubkey doesn't match sender_id
        let result = verify_message(&msg);
        assert!(result.is_err());
        match result {
            Err(Error::PubkeyPeerIdMismatch) => {}
            Err(e) => panic!("Expected PubkeyPeerIdMismatch, got: {:?}", e),
            Ok(_) => panic!("Expected verification to fail"),
        }
    }

    #[test]
    fn test_sign_preserves_existing_message_id() {
        let keypair = Keypair::generate_ed25519();
        let mut msg = create_test_message();

        // Set a custom message ID before signing
        let custom_id = "custom-message-id-123".to_string();
        msg.metadata.message_id = custom_id.clone();

        // Sign the message
        sign_message(&keypair, &mut msg).expect("signing should succeed");

        // Message ID should be preserved
        assert_eq!(msg.metadata.message_id, custom_id);
    }

    #[test]
    fn test_verify_missing_signature_fails() {
        let keypair = Keypair::generate_ed25519();
        let mut msg = create_test_message();

        // Partially populate metadata without signature
        msg.metadata.message_id = Uuid::new_v4().to_string();
        msg.metadata.version = MESSAGE_VERSION.to_string();
        msg.metadata.sender_id = keypair.public().to_peer_id().to_string();
        msg.metadata.pubkey = keypair.public().encode_protobuf();
        // Intentionally leave signature as None

        // Verify should fail due to missing signature
        let result = verify_message(&msg);
        assert!(result.is_err());
        match result {
            Err(Error::MissingSignature) => {}
            Err(e) => panic!("Expected MissingSignature, got: {:?}", e),
            Ok(_) => panic!("Expected verification to fail"),
        }
    }

    #[test]
    fn test_sign_message_cloned() {
        let keypair = Keypair::generate_ed25519();
        let original = create_test_message();

        // Sign a cloned copy
        let signed = sign_message_cloned(&keypair, &original).expect("signing should succeed");

        // Original should be unchanged
        assert!(original.metadata.message_id.is_empty());
        assert!(original.metadata.signature.is_none());

        // Signed copy should have all fields populated
        assert!(!signed.metadata.message_id.is_empty());
        assert!(signed.metadata.signature.is_some());

        // Signed copy should verify
        verify_message(&signed).expect("verification should succeed");
    }

    #[test]
    fn test_different_keypairs_produce_different_signatures() {
        let keypair1 = Keypair::generate_ed25519();
        let keypair2 = Keypair::generate_ed25519();

        let mut msg1 = create_test_message();
        let mut msg2 = create_test_message();

        // Set same message ID for comparison
        let msg_id = "same-msg-id".to_string();
        msg1.metadata.message_id = msg_id.clone();
        msg2.metadata.message_id = msg_id;

        sign_message(&keypair1, &mut msg1).expect("signing should succeed");
        sign_message(&keypair2, &mut msg2).expect("signing should succeed");

        // Signatures should be different
        assert_ne!(msg1.metadata.signature, msg2.metadata.signature);

        // Both should verify with their own keypairs
        verify_message(&msg1).expect("msg1 should verify");
        verify_message(&msg2).expect("msg2 should verify");
    }

    #[test]
    fn test_uuid_format() {
        let keypair = Keypair::generate_ed25519();
        let mut msg = create_test_message();

        sign_message(&keypair, &mut msg).expect("signing should succeed");

        // Verify message_id is a valid UUID format
        let uuid_result = Uuid::parse_str(&msg.metadata.message_id);
        assert!(
            uuid_result.is_ok(),
            "message_id should be valid UUID format"
        );
    }
}
