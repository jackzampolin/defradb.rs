//! PushLog message types for CRDT synchronization.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::cbor::{nullable_bytes, optional_bytes};
use super::traits::Message;
use crate::protocol::MESSAGE_VERSION;

fn is_false(value: &bool) -> bool {
    !*value
}

/// PushLog request message for sending resource updates to peer nodes.
///
/// This is the primary message type for CRDT synchronization between nodes.
///
/// Note: We don't use `#[serde(flatten)]` because serde's flatten produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushLogRequest {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,

    /// Document ID being updated.
    #[serde(rename = "DocID")]
    pub doc_id: String,

    /// Content ID (CID) of the block.
    /// Uses bytes_as_cbor for CBOR byte string compatibility with Go.
    #[serde(rename = "CID", with = "super::cbor::bytes_as_cbor")]
    pub cid: Bytes,

    /// Collection ID the document belongs to.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Creator/author of the update.
    #[serde(rename = "Creator")]
    pub creator: String,

    /// The IPLD block data.
    /// Uses bytes_as_cbor for CBOR byte string compatibility with Go.
    #[serde(rename = "Block", with = "super::cbor::bytes_as_cbor")]
    pub block: Bytes,

    /// Optional authorizer-signed explicit replay capability for encrypted replay.
    #[serde(
        rename = "ExplicitReplayCapability",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub explicit_replay_capability: Option<String>,

    /// Whether the sender can receive the reply on the request's bidirectional stream.
    ///
    /// Older iroh senders omit this field and require the legacy reverse-stream response.
    /// This capability is iroh-only. Never set it on libp2p: Go signature verification
    /// re-serializes the request and would omit this unknown field.
    #[serde(
        rename = "SupportsSameStreamReply",
        skip_serializing_if = "is_false",
        default
    )]
    pub supports_same_stream_reply: bool,
}

impl PushLogRequest {
    /// Create a new PushLogRequest.
    pub fn new(
        doc_id: String,
        cid: Bytes,
        collection_id: String,
        creator: String,
        block: Bytes,
    ) -> Self {
        Self {
            version: crate::protocol::MESSAGE_VERSION.to_string(),
            message_id: String::new(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
            doc_id,
            cid,
            collection_id,
            creator,
            block,
            explicit_replay_capability: None,
            supports_same_stream_reply: false,
        }
    }
}

impl Message for PushLogRequest {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// PushLog reply message sent in response to a PushLogRequest.
///
/// Note: We don't use `#[serde(flatten)]` because serde's flatten produces
/// indefinite-length maps when flatten is used (CBOR major type 0xbf).
/// Go's fxamacker/cbor produces definite-length maps, causing signature
/// verification to fail. Instead, we duplicate the fields for wire compatibility.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PushLogReply {
    /// DefraDB message version.
    #[serde(rename = "Version")]
    pub version: String,

    /// Unique message identifier. Responses use the same ID as the request.
    #[serde(rename = "MessageID")]
    pub message_id: String,

    /// ID of the sender (PeerID when using libp2p).
    #[serde(rename = "SenderID")]
    pub sender_id: String,

    /// Public key of the node that created the message.
    #[serde(rename = "Pubkey", with = "nullable_bytes")]
    pub pubkey: Vec<u8>,

    /// Signature for message authentication.
    #[serde(
        rename = "Signature",
        skip_serializing_if = "Option::is_none",
        default,
        with = "optional_bytes"
    )]
    pub signature: Option<Vec<u8>>,

    /// Error message if something went wrong.
    #[serde(rename = "ErrMessage", skip_serializing_if = "Option::is_none")]
    pub err_message: Option<String>,
}

impl PushLogReply {
    /// Create a new successful PushLogReply.
    pub fn success(request_message_id: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: None,
        }
    }

    /// Create a new error PushLogReply.
    pub fn error(request_message_id: &str, err: &str) -> Self {
        Self {
            version: MESSAGE_VERSION.to_string(),
            message_id: request_message_id.to_string(),
            sender_id: String::new(),
            pubkey: Vec::new(),
            signature: None,
            err_message: Some(err.to_string()),
        }
    }
}

impl Message for PushLogReply {
    fn version(&self) -> &str {
        &self.version
    }

    fn set_version(&mut self, version: String) {
        self.version = version;
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }

    fn set_message_id(&mut self, id: String) {
        self.message_id = id;
    }

    fn sender_id(&self) -> &str {
        &self.sender_id
    }

    fn set_sender_id(&mut self, id: String) {
        self.sender_id = id;
    }

    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn set_pubkey(&mut self, pubkey: Vec<u8>) {
        self.pubkey = pubkey;
    }

    fn signature(&self) -> Option<&[u8]> {
        self.signature.as_deref()
    }

    fn set_signature(&mut self, signature: Option<Vec<u8>>) {
        self.signature = signature;
    }

    fn err_message(&self) -> Option<&str> {
        self.err_message.as_deref()
    }
}

/// PushLog broadcast message for GossipSub publishing.
///
/// This is a lightweight version of PushLogRequest used for pubsub broadcasts.
/// Unlike request-response messages, pubsub messages do NOT include MetaData
/// signing fields - libp2p's GossipSub handles message authentication via
/// `MessageAuthenticity::Signed`.
///
/// # Wire Compatibility
///
/// This matches Go's approach where PushLogRequest is CBOR-encoded WITHOUT
/// the MetaData signing fields for pubsub, since the sender peer ID comes
/// from the pubsub layer itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PushLogBroadcast {
    /// Document ID being updated.
    #[serde(rename = "DocID")]
    pub doc_id: String,

    /// Content ID (CID) of the block.
    /// Uses bytes_as_cbor for CBOR byte string compatibility with Go.
    #[serde(rename = "CID", with = "super::cbor::bytes_as_cbor")]
    pub cid: Bytes,

    /// Collection ID the document belongs to.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Creator/author of the update.
    #[serde(rename = "Creator")]
    pub creator: String,

    /// The IPLD block data.
    /// Uses bytes_as_cbor for CBOR byte string compatibility with Go.
    #[serde(rename = "Block", with = "super::cbor::bytes_as_cbor")]
    pub block: Bytes,
}

/// Encoding variant accepted when decoding gossip payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushLogGossipPayloadEncoding {
    CborBroadcast,
    CborRequest,
    PostcardBroadcast,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    not(any(feature = "libp2p-transport", feature = "iroh-transport")),
    allow(dead_code)
)]
pub(crate) struct PushLogGossipPayloadDebugInfo {
    pub payload_fingerprint: String,
    pub payload_shape_hint: String,
}

#[cfg_attr(
    not(any(feature = "libp2p-transport", feature = "iroh-transport")),
    allow(dead_code)
)]
const GOSSIP_TEXT_PREFIX_CHARS: usize = 48;
#[cfg_attr(
    not(any(feature = "libp2p-transport", feature = "iroh-transport")),
    allow(dead_code)
)]
const GOSSIP_PAYLOAD_FINGERPRINT_BYTES: usize = 8;

#[cfg_attr(
    not(any(feature = "libp2p-transport", feature = "iroh-transport")),
    allow(dead_code)
)]
fn truncated_text_prefix(text: &str) -> String {
    let mut prefix = String::new();
    let mut chars = text.chars();
    for _ in 0..GOSSIP_TEXT_PREFIX_CHARS {
        match chars.next() {
            Some(ch) => prefix.push(ch),
            None => return prefix,
        }
    }
    if chars.next().is_some() {
        prefix.push_str("...");
    }
    prefix
}

#[cfg_attr(
    not(any(feature = "libp2p-transport", feature = "iroh-transport")),
    allow(dead_code)
)]
fn describe_cbor_value(value: &ciborium::Value) -> String {
    match value {
        ciborium::Value::Map(entries) => {
            let keys: Vec<String> = entries
                .iter()
                .filter_map(|(key, _)| match key {
                    ciborium::Value::Text(text) => Some(text.clone()),
                    _ => None,
                })
                .take(10)
                .collect();
            if keys.is_empty() {
                format!("cbor_map(len={})", entries.len())
            } else {
                format!("cbor_map(keys=[{}])", keys.join(","))
            }
        }
        ciborium::Value::Array(items) => format!("cbor_array(len={})", items.len()),
        ciborium::Value::Bytes(bytes) => format!("cbor_bytes(len={})", bytes.len()),
        ciborium::Value::Text(text) => {
            format!("cbor_text(prefix={:?})", truncated_text_prefix(text))
        }
        ciborium::Value::Tag(tag, _) => format!("cbor_tag({tag})"),
        ciborium::Value::Bool(value) => format!("cbor_bool({value})"),
        ciborium::Value::Null => "cbor_null".to_string(),
        ciborium::Value::Integer(_) => "cbor_integer".to_string(),
        ciborium::Value::Float(_) => "cbor_float".to_string(),
        _ => "cbor_scalar".to_string(),
    }
}

#[cfg_attr(
    not(any(feature = "libp2p-transport", feature = "iroh-transport")),
    allow(dead_code)
)]
fn describe_gossip_payload_shape(payload: &[u8]) -> String {
    if payload.is_empty() {
        return "empty".to_string();
    }

    if let Ok(value) = defra_core::cbor::from_slice::<ciborium::Value>(payload) {
        return describe_cbor_value(&value);
    }

    if payload
        .iter()
        .all(|byte| byte.is_ascii_graphic() || byte.is_ascii_whitespace())
    {
        if let Ok(text) = std::str::from_utf8(payload) {
            return format!("utf8_text(prefix={:?})", truncated_text_prefix(text));
        }
    }

    format!("opaque_binary(first_byte=0x{:02x})", payload[0])
}

impl PushLogBroadcast {
    /// Create a new PushLogBroadcast.
    pub fn new(
        doc_id: String,
        cid: Bytes,
        collection_id: String,
        creator: String,
        block: Bytes,
    ) -> Self {
        Self {
            doc_id,
            cid,
            collection_id,
            creator,
            block,
        }
    }

    /// Convert from a PushLogRequest (strips metadata, O(1) Bytes clone).
    pub fn from_request(req: &PushLogRequest) -> Self {
        Self {
            doc_id: req.doc_id.clone(),
            cid: req.cid.clone(),
            collection_id: req.collection_id.clone(),
            creator: req.creator.clone(),
            block: req.block.clone(),
        }
    }

    /// Convert to a PushLogRequest (adds default metadata, O(1) Bytes clone).
    pub fn to_request(&self) -> PushLogRequest {
        PushLogRequest::new(
            self.doc_id.clone(),
            self.cid.clone(),
            self.collection_id.clone(),
            self.creator.clone(),
            self.block.clone(),
        )
    }

    /// Decode a gossip payload using the set of encodings accepted across P2P transports.
    ///
    /// CBOR is the canonical gossip encoding because it is self-describing and
    /// tolerates added fields. Request-shaped CBOR must be checked before the
    /// lighter broadcast shape because serde ignores unknown map fields.
    /// Postcard remains accepted for older Iroh peers.
    pub fn decode_gossip_payload(
        payload: &[u8],
    ) -> Result<(Self, PushLogGossipPayloadEncoding), String> {
        defra_core::cbor::from_slice::<PushLogRequest>(payload)
            .map(|request| {
                (
                    Self::from_request(&request),
                    PushLogGossipPayloadEncoding::CborRequest,
                )
            })
            .or_else(|_| {
                defra_core::cbor::from_slice::<Self>(payload)
                    .map(|broadcast| (broadcast, PushLogGossipPayloadEncoding::CborBroadcast))
            })
            .or_else(|_| {
                postcard::from_bytes::<Self>(payload)
                    .map(|broadcast| (broadcast, PushLogGossipPayloadEncoding::PostcardBroadcast))
            })
            .map_err(|error| error.to_string())
    }

    /// Encode a gossip payload using the canonical, self-describing wire format.
    pub fn encode_gossip_payload(&self) -> Result<Vec<u8>, defra_core::cbor::Error> {
        defra_core::cbor::to_vec(self)
    }

    #[cfg_attr(
        not(any(feature = "libp2p-transport", feature = "iroh-transport")),
        allow(dead_code)
    )]
    pub(crate) fn inspect_gossip_payload(payload: &[u8]) -> PushLogGossipPayloadDebugInfo {
        let digest = Sha256::digest(payload);
        PushLogGossipPayloadDebugInfo {
            payload_fingerprint: hex::encode(&digest[..GOSSIP_PAYLOAD_FINGERPRINT_BYTES]),
            payload_shape_hint: describe_gossip_payload_shape(payload),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspect_gossip_payload_identifies_cbor_request_shape() {
        let payload = defra_core::cbor::to_vec(&PushLogRequest::new(
            "doc-1".to_string(),
            Bytes::from_static(&[1, 2, 3]),
            "collection-1".to_string(),
            "creator-1".to_string(),
            Bytes::from_static(&[4, 5, 6]),
        ))
        .expect("request should encode");

        let info = PushLogBroadcast::inspect_gossip_payload(&payload);
        assert!(info.payload_shape_hint.starts_with("cbor_map("));
        assert!(info.payload_shape_hint.contains("DocID"));
        assert!(info.payload_shape_hint.contains("CollectionID"));
        assert_eq!(
            info.payload_fingerprint.len(),
            GOSSIP_PAYLOAD_FINGERPRINT_BYTES * 2
        );
    }

    #[test]
    fn inspect_gossip_payload_identifies_utf8_text_shape() {
        let info = PushLogBroadcast::inspect_gossip_payload(b"hello from some other producer");
        assert!(info.payload_shape_hint.starts_with("utf8_text("));
        assert_eq!(
            info.payload_fingerprint.len(),
            GOSSIP_PAYLOAD_FINGERPRINT_BYTES * 2
        );
    }

    #[test]
    fn inspect_gossip_payload_identifies_opaque_binary_shape() {
        let info = PushLogBroadcast::inspect_gossip_payload(&[0xff, 0x00, 0x01, 0x02]);
        assert_eq!(info.payload_shape_hint, "opaque_binary(first_byte=0xff)");
        assert_eq!(info.payload_fingerprint, "0c252d844a815f83");
    }

    #[test]
    fn postcard_round_trip_broadcast() {
        let broadcast = PushLogBroadcast::new(
            "bae-eabce396-ddf9-5a76-85ac-ade4e4205de9".to_string(),
            Bytes::from_static(&[0xaa; 38]),
            "bafyreiabcdefghijklmnopqrstuvwxyz123456789012345678".to_string(),
            "12D3KooWExamplePeerIdForTestingOnly0000000000000".to_string(),
            Bytes::from_static(&[0xbb; 128]),
        );

        let encoded = postcard::to_allocvec(&broadcast).expect("postcard encode");
        let decoded: PushLogBroadcast =
            postcard::from_bytes(&encoded).expect("postcard round trip");
        assert_eq!(decoded, broadcast);

        let (via_decode_gossip, encoding) =
            PushLogBroadcast::decode_gossip_payload(&encoded).expect("decode via gossip path");
        assert_eq!(encoding, PushLogGossipPayloadEncoding::PostcardBroadcast);
        assert_eq!(via_decode_gossip, broadcast);
    }

    #[test]
    fn canonical_gossip_payload_is_cbor_broadcast() {
        let broadcast = PushLogBroadcast::new(
            "doc-cbor".to_string(),
            Bytes::from_static(&[1, 2, 3]),
            "collection-cbor".to_string(),
            "creator-cbor".to_string(),
            Bytes::from_static(&[4, 5, 6]),
        );

        let encoded = broadcast
            .encode_gossip_payload()
            .expect("canonical gossip payload should encode");
        let (decoded, encoding) =
            PushLogBroadcast::decode_gossip_payload(&encoded).expect("decode canonical payload");

        assert_eq!(encoding, PushLogGossipPayloadEncoding::CborBroadcast);
        assert_eq!(decoded, broadcast);
    }

    #[test]
    fn cbor_gossip_payload_tolerates_unknown_future_fields() {
        #[derive(Serialize)]
        struct FutureBroadcast {
            #[serde(rename = "DocID")]
            doc_id: String,
            #[serde(rename = "CID", with = "super::super::cbor::bytes_as_cbor")]
            cid: Bytes,
            #[serde(rename = "CollectionID")]
            collection_id: String,
            #[serde(rename = "Creator")]
            creator: String,
            #[serde(rename = "Block", with = "super::super::cbor::bytes_as_cbor")]
            block: Bytes,
            #[serde(rename = "FutureField")]
            future_field: String,
        }

        let future = FutureBroadcast {
            doc_id: "doc-future".to_string(),
            cid: Bytes::from_static(&[9, 8, 7]),
            collection_id: "collection-future".to_string(),
            creator: "creator-future".to_string(),
            block: Bytes::from_static(&[6, 5, 4]),
            future_field: "ignored by older decoders".to_string(),
        };

        let encoded = defra_core::cbor::to_vec(&future).expect("future payload should encode");
        let (decoded, encoding) = PushLogBroadcast::decode_gossip_payload(&encoded)
            .expect("future payload should decode as broadcast");

        assert_eq!(encoding, PushLogGossipPayloadEncoding::CborBroadcast);
        assert_eq!(decoded.doc_id, future.doc_id);
        assert_eq!(decoded.collection_id, future.collection_id);
        assert_eq!(decoded.creator, future.creator);
        assert_eq!(decoded.cid, future.cid);
        assert_eq!(decoded.block, future.block);
    }
}
