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

#[cfg(test)]
mod push_log_fixture_tests {
    use super::*;

    // Generated by `go run ./testdata/gen_message_fixtures` using Go's
    // fxamacker/cbor encoder. Keep this contiguous so the repository's Go
    // fixture drift check can locate the exact emitted byte sequence.
    const GO_PUSH_LOG_REQUEST_HEX: &str = "aa6756657273696f6e6e2f646566726164622f302e302e31694d6573736167654944666d73672d676f6853656e646572494467706565722d676f665075626b6579420102695369676e617475726542030465446f63494468626166792d646f6363434944430171aa6c436f6c6c656374696f6e49446f626166792d636f6c6c656374696f6e6743726561746f726b6469643a6b65793a7a476f65426c6f636b44a1617801";

    #[test]
    fn push_log_request_matches_go_fxamacker_fixture() {
        let fixture = hex::decode(GO_PUSH_LOG_REQUEST_HEX).expect("valid fixture hex");
        let request: PushLogRequest =
            ciborium::from_reader(fixture.as_slice()).expect("decode Go PushLog fixture");

        assert_eq!(request.version, "/defradb/0.0.1");
        assert_eq!(request.message_id, "msg-go");
        assert_eq!(request.sender_id, "peer-go");
        assert_eq!(request.doc_id, "bafy-doc");
        assert_eq!(request.collection_id, "bafy-collection");
        assert_eq!(request.cid.as_ref(), &[0x01, 0x71, 0xaa]);
        assert_eq!(request.block.as_ref(), &[0xa1, 0x61, 0x78, 0x01]);

        let mut encoded = Vec::new();
        ciborium::into_writer(&request, &mut encoded).expect("encode Rust PushLog fixture");
        assert_eq!(encoded, fixture);
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

    /// Peer that originally published this head hint.
    ///
    /// This is an additive gossip-only provider identity. Iroh receivers trust
    /// it only when `OriginSignature` verifies with the endpoint key named
    /// here; libp2p replaces it with the native signed gossipsub author. Older
    /// CBOR readers ignore this field and older payloads decode with `None`.
    #[serde(
        rename = "SourcePeerID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub source_peer_id: Option<String>,

    /// Signature by `SourcePeerID` over the canonical broadcast with this
    /// field absent. Iroh relays preserve the envelope, allowing receivers to
    /// authenticate the original publisher independently from the connected
    /// hop selected as the durable CAR recovery provider.
    #[serde(
        rename = "OriginSignature",
        default,
        skip_serializing_if = "Option::is_none",
        with = "optional_bytes"
    )]
    pub origin_signature: Option<Vec<u8>>,

    /// Transport-authenticated recovery provider. This is ingress metadata,
    /// never serialized or accepted from the wire.
    #[serde(skip)]
    pub(crate) authenticated_source_peer_id: Option<String>,

    /// Independently authenticated publisher identity. This is ingress
    /// metadata and becomes the recovery provider only when the receiver has
    /// a live transport route to it; otherwise recovery stays on the
    /// authenticated propagation hop.
    #[serde(skip)]
    pub(crate) authenticated_origin_peer_id: Option<String>,
}

/// Pre-origin-hint postcard shape retained for rolling compatibility.
///
/// Postcard is positional and cannot default an absent trailing field.  New
/// publishers use CBOR, but receivers must continue to accept postcard frames
/// emitted by an old Iroh peer during an upgrade.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyPostcardPushLogBroadcast {
    doc_id: String,
    #[serde(with = "super::cbor::bytes_as_cbor")]
    cid: Bytes,
    collection_id: String,
    creator: String,
    #[serde(with = "super::cbor::bytes_as_cbor")]
    block: Bytes,
}

impl From<LegacyPostcardPushLogBroadcast> for PushLogBroadcast {
    fn from(value: LegacyPostcardPushLogBroadcast) -> Self {
        Self {
            doc_id: value.doc_id,
            cid: value.cid,
            collection_id: value.collection_id,
            creator: value.creator,
            block: value.block,
            source_peer_id: None,
            origin_signature: None,
            authenticated_source_peer_id: None,
            authenticated_origin_peer_id: None,
        }
    }
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
            source_peer_id: None,
            origin_signature: None,
            authenticated_source_peer_id: None,
            authenticated_origin_peer_id: None,
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
            // Go gossip uses the request-shaped CBOR envelope.  Its SenderID
            // is the only origin information available to Iroh receivers;
            // libp2p replaces this hint with the signed native author.
            source_peer_id: (!req.sender_id.is_empty()).then(|| req.sender_id.clone()),
            origin_signature: None,
            authenticated_source_peer_id: None,
            authenticated_origin_peer_id: None,
        }
    }

    /// Bytes covered by the Iroh origin signature.
    pub(crate) fn origin_signing_bytes(&self) -> Result<Vec<u8>, defra_core::cbor::Error> {
        let mut unsigned = self.clone();
        unsigned.origin_signature = None;
        unsigned.authenticated_source_peer_id = None;
        unsigned.authenticated_origin_peer_id = None;
        defra_core::cbor::to_vec(&unsigned)
    }

    /// Record provider identity authenticated by transport ingress.
    #[allow(dead_code)] // exercised by transport feature implementations
    pub(crate) fn authenticate_source_peer(&mut self, peer_id: String) {
        self.authenticated_source_peer_id = Some(peer_id);
    }

    /// Return the only identity permitted to become a durable DAG provider.
    pub(crate) fn authenticated_source_peer_id(&self) -> Option<&str> {
        self.authenticated_source_peer_id.as_deref()
    }

    /// Record the publisher identity authenticated independently from the
    /// propagation hop.
    #[allow(dead_code)] // exercised by transport feature implementations
    pub(crate) fn authenticate_origin_peer(&mut self, peer_id: String) {
        self.authenticated_origin_peer_id = Some(peer_id);
    }

    pub(crate) fn authenticated_origin_peer_id(&self) -> Option<&str> {
        self.authenticated_origin_peer_id.as_deref()
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
            .or_else(|_| {
                postcard::from_bytes::<LegacyPostcardPushLogBroadcast>(payload).map(|broadcast| {
                    (
                        broadcast.into(),
                        PushLogGossipPayloadEncoding::PostcardBroadcast,
                    )
                })
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
    fn legacy_postcard_broadcast_remains_decodable() {
        let broadcast = PushLogBroadcast::new(
            "bae-eabce396-ddf9-5a76-85ac-ade4e4205de9".to_string(),
            Bytes::from_static(&[0xaa; 38]),
            "bafyreiabcdefghijklmnopqrstuvwxyz123456789012345678".to_string(),
            "12D3KooWExamplePeerIdForTestingOnly0000000000000".to_string(),
            Bytes::from_static(&[0xbb; 128]),
        );

        let encoded = postcard::to_allocvec(&LegacyPostcardPushLogBroadcast {
            doc_id: broadcast.doc_id.clone(),
            cid: broadcast.cid.clone(),
            collection_id: broadcast.collection_id.clone(),
            creator: broadcast.creator.clone(),
            block: broadcast.block.clone(),
        })
        .expect("legacy postcard encode");

        let (via_decode_gossip, encoding) =
            PushLogBroadcast::decode_gossip_payload(&encoded).expect("decode via gossip path");
        assert_eq!(encoding, PushLogGossipPayloadEncoding::PostcardBroadcast);
        assert_eq!(via_decode_gossip, broadcast);
    }

    #[test]
    fn canonical_gossip_payload_is_cbor_broadcast() {
        let mut broadcast = PushLogBroadcast::new(
            "doc-cbor".to_string(),
            Bytes::from_static(&[1, 2, 3]),
            "collection-cbor".to_string(),
            "creator-cbor".to_string(),
            Bytes::from_static(&[4, 5, 6]),
        );
        broadcast.source_peer_id = Some("origin-peer".to_string());

        let encoded = broadcast
            .encode_gossip_payload()
            .expect("canonical gossip payload should encode");
        let (decoded, encoding) =
            PushLogBroadcast::decode_gossip_payload(&encoded).expect("decode canonical payload");

        assert_eq!(encoding, PushLogGossipPayloadEncoding::CborBroadcast);
        assert_eq!(decoded, broadcast);
    }

    #[test]
    fn origin_signature_covers_the_complete_unsigned_head_hint() {
        let mut broadcast = PushLogBroadcast::new(
            "doc-signed".to_string(),
            Bytes::from_static(&[1, 2, 3]),
            "collection-signed".to_string(),
            "creator-signed".to_string(),
            Bytes::from_static(&[4, 5, 6]),
        );
        broadcast.source_peer_id = Some("origin-peer".to_string());
        let unsigned = broadcast.origin_signing_bytes().unwrap();
        broadcast.origin_signature = Some(vec![7; 64]);

        assert_eq!(broadcast.origin_signing_bytes().unwrap(), unsigned);
        broadcast.doc_id.push_str("-tampered");
        assert_ne!(broadcast.origin_signing_bytes().unwrap(), unsigned);
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
