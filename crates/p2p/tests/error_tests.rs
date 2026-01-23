
//! Tests for P2P error types.

use std::io;

use p2p::error::{Error, Result};

#[test]
fn test_error_display() {
    // Test that all error variants have proper display messages
    let transport = Error::Transport("connection refused".to_string());
    assert!(transport.to_string().contains("transport error"));
    assert!(transport.to_string().contains("connection refused"));

    let dial = Error::Dial("no addresses".to_string());
    assert!(dial.to_string().contains("dial error"));

    let closed = Error::ConnectionClosed;
    assert_eq!(closed.to_string(), "connection closed");

    let protocol = Error::ProtocolNegotiation("mismatch".to_string());
    assert!(protocol.to_string().contains("protocol negotiation"));

    let codec = Error::Codec("invalid data".to_string());
    assert!(codec.to_string().contains("codec error"));

    let invalid_sig = Error::InvalidSignature;
    assert_eq!(invalid_sig.to_string(), "invalid signature");

    let pubkey_mismatch = Error::PubkeyPeerIdMismatch;
    assert!(pubkey_mismatch.to_string().contains("public key"));

    let msg_id_gen = Error::MessageIdGeneration;
    assert!(msg_id_gen.to_string().contains("message ID"));

    let pubkey_encode = Error::PublicKeyEncode("encoding failed".to_string());
    assert!(pubkey_encode.to_string().contains("encode public key"));

    let pubkey_decode = Error::PublicKeyDecode("decoding failed".to_string());
    assert!(pubkey_decode.to_string().contains("decode public key"));

    let signing_failed = Error::SigningFailed("key unavailable".to_string());
    assert!(signing_failed.to_string().contains("signing failed"));

    let missing_sig = Error::MissingSignature;
    assert!(missing_sig.to_string().contains("no signature"));

    let timeout = Error::ResponseTimeout;
    assert_eq!(timeout.to_string(), "response timeout");

    let unexpected = Error::UnexpectedResponseType {
        expected: "PushLogReply".to_string(),
        actual: "Unknown".to_string(),
    };
    assert!(unexpected.to_string().contains("PushLogReply"));
    assert!(unexpected.to_string().contains("Unknown"));

    let peer_not_found = Error::PeerNotFound("12D3...".to_string());
    assert!(peer_not_found.to_string().contains("peer not found"));

    let invalid_addr = Error::InvalidMultiaddr("/invalid".to_string());
    assert!(invalid_addr.to_string().contains("invalid multiaddress"));

    let swarm = Error::Swarm("behaviour error".to_string());
    assert!(swarm.to_string().contains("swarm error"));

    let io_err = Error::Io(io::Error::new(io::ErrorKind::NotFound, "not found"));
    assert!(io_err.to_string().contains("io error"));

    let cbor_ser = Error::CborSerialization("failed to serialize".to_string());
    assert!(cbor_ser.to_string().contains("cbor serialization"));

    let cbor_de = Error::CborDeserialization("failed to deserialize".to_string());
    assert!(cbor_de.to_string().contains("cbor deserialization"));

    let noise = Error::Noise("handshake failed".to_string());
    assert!(noise.to_string().contains("noise protocol"));

    let behaviour = Error::Behaviour("init failed".to_string());
    assert!(behaviour.to_string().contains("behaviour error"));

    let already_listening = Error::AlreadyListening("0.0.0.0:9000".to_string());
    assert!(already_listening.to_string().contains("already listening"));

    let not_listening = Error::NotListening;
    assert_eq!(not_listening.to_string(), "not listening");

    let invalid_peer = Error::InvalidPeerId("not-a-peer-id".to_string());
    assert!(invalid_peer.to_string().contains("invalid peer ID"));

    let channel_send = Error::ChannelSend;
    assert_eq!(channel_send.to_string(), "channel send error");

    let channel_recv = Error::ChannelReceive;
    assert_eq!(channel_recv.to_string(), "channel receive error");
}

#[test]
fn test_error_from_io() {
    let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
    let err: Error = io_err.into();

    match err {
        Error::Io(e) => assert_eq!(e.kind(), io::ErrorKind::ConnectionRefused),
        _ => panic!("Expected Io error variant"),
    }
}

#[test]
fn test_error_from_multiaddr() {
    // Create an invalid multiaddr to test the conversion
    let result: std::result::Result<libp2p::Multiaddr, _> = "/invalid/addr".parse();
    if let Err(e) = result {
        let err: Error = e.into();
        match err {
            Error::InvalidMultiaddr(msg) => {
                assert!(!msg.is_empty());
            }
            _ => panic!("Expected InvalidMultiaddr error"),
        }
    }
}

#[test]
fn test_error_debug() {
    // Test Debug implementation
    let err = Error::Transport("test".to_string());
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("Transport"));
    assert!(debug_str.contains("test"));
}

#[test]
fn test_result_type() {
    fn returns_result() -> Result<i32> {
        Ok(42)
    }

    fn returns_error() -> Result<i32> {
        Err(Error::ConnectionClosed)
    }

    assert_eq!(returns_result().unwrap(), 42);
    assert!(returns_error().is_err());
}

#[test]
fn test_gossipsub_errors() {
    let sub_err = Error::GossipSubSubscription("topic not found".to_string());
    assert!(sub_err.to_string().contains("gossipsub subscription"));
    assert!(sub_err.to_string().contains("topic not found"));

    let pub_err = Error::GossipSubPublish("no peers".to_string());
    assert!(pub_err.to_string().contains("gossipsub publish"));
    assert!(pub_err.to_string().contains("no peers"));

    let unsub_err = Error::GossipSubUnsubscribe("not subscribed".to_string());
    assert!(unsub_err.to_string().contains("gossipsub unsubscribe"));
    assert!(unsub_err.to_string().contains("not subscribed"));

    let topic_err = Error::InvalidTopic("empty topic".to_string());
    assert!(topic_err.to_string().contains("invalid topic"));
    assert!(topic_err.to_string().contains("empty topic"));
}
