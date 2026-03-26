//! Tests for message signing and verification.

use uuid::Uuid;

use p2p::error::Error;
use p2p::message::PushLogRequest;
use p2p::protocol::MESSAGE_VERSION;
use p2p::signing::{sign_message, sign_message_cloned, verify_message};
use p2p::Keypair;

fn create_test_message() -> PushLogRequest {
    PushLogRequest::new(
        "doc123".to_string(),
        bytes::Bytes::from(vec![1, 2, 3, 4]),
        "collection1".to_string(),
        "creator1".to_string(),
        bytes::Bytes::from(vec![5, 6, 7, 8]),
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
