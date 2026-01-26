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
    // Generate message ID if not already set
    if msg.message_id().is_empty() {
        msg.set_message_id(Uuid::new_v4().to_string());
    }

    // Set protocol version
    msg.set_version(MESSAGE_VERSION.to_string());

    // Set sender ID from keypair
    let peer_id = keypair.public().to_peer_id();
    msg.set_sender_id(peer_id.to_string());

    // Set public key (protobuf encoded, matching Go's libp2p)
    msg.set_pubkey(keypair.public().encode_protobuf());

    // Clear signature before serializing for signing
    msg.set_signature(None);

    // CBOR serialize the message
    let bytes = serde_cbor::to_vec(&msg).map_err(|e| Error::CborSerialization(e.to_string()))?;

    // Debug logging
    tracing::info!(
        bytes_len = bytes.len(),
        message_id = %msg.message_id(),
        sender_id = %msg.sender_id(),
        pubkey_len = msg.pubkey().len(),
        version = %msg.version(),
        "Signing message for Go interop"
    );

    // Sign the serialized bytes
    let signature = keypair
        .sign(&bytes)
        .map_err(|e| Error::SigningFailed(e.to_string()))?;

    // Set the signature
    msg.set_signature(Some(signature));

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
    // Check that signature exists
    let signature = msg.signature().ok_or(Error::MissingSignature)?;

    // Decode public key from message
    let pubkey = PublicKey::try_decode_protobuf(msg.pubkey())
        .map_err(|e| Error::PublicKeyDecode(e.to_string()))?;

    // Derive peer ID from public key
    let id_from_key = pubkey.to_peer_id();

    // Parse sender ID as peer ID
    let sender_peer_id: PeerId = msg
        .sender_id()
        .parse()
        .map_err(|e: libp2p::identity::ParseError| Error::InvalidPeerId(e.to_string()))?;

    // Verify peer ID matches
    if id_from_key != sender_peer_id {
        return Err(Error::PubkeyPeerIdMismatch);
    }

    // Clone message and clear signature for verification
    let mut msg_for_verify = msg.clone();
    msg_for_verify.set_signature(None);

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
