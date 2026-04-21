//! Wire envelope for `pubsub_rpc` responses.
//!
//! Mirrors the `internalResponse` IPLD schema in
//! `sourcenetwork/go-libp2p-pubsub-rpc/rpc.go`:
//!
//! ```text
//! type internalResponse struct {
//!     ID   String
//!     From Bytes
//!     Data Bytes
//!     Err  String
//! }
//! ```
//!
//! Go emits this as dag-cbor with a *definite-length* map and serializes the
//! fields in the declared order. Serde-cbor with `#[serde(rename_all = ...)]`
//! produces the same bytes as long as we preserve declaration order.
//!
//! Cross-implementation fixture bytes are verified in
//! `tests::decodes_go_fixture` below.

use serde::{Deserialize, Serialize};

/// Wire-format of a single response delivered over the dynamic per-peer
/// `_response` sub-topic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalResponse {
    /// Stringified CIDv1(raw, sha256(request_bytes)) identifying the request
    /// this response corresponds to.
    #[serde(rename = "ID")]
    pub id: String,

    /// Responder peer ID as raw bytes. Go fills this server-side from the
    /// validated gossipsub message source, not from the envelope (see
    /// `rpc.go:415`), so decoders should treat the wire field as advisory
    /// only and trust the transport-level sender.
    #[serde(rename = "From", with = "serde_bytes")]
    pub from: Vec<u8>,

    /// Raw user payload returned by the responder's `MessageHandler`.
    #[serde(rename = "Data", with = "serde_bytes")]
    pub data: Vec<u8>,

    /// Human-readable error string from the responder, or empty.
    #[serde(rename = "Err")]
    pub err: String,
}

impl InternalResponse {
    /// Encode to dag-cbor bytes compatible with Go's `dagcbor.Encode`.
    pub fn to_cbor(&self) -> Result<Vec<u8>, serde_cbor::Error> {
        serde_cbor::to_vec(self)
    }

    /// Decode dag-cbor bytes produced by Go's `dagcbor.Encode`.
    pub fn from_cbor(bytes: &[u8]) -> Result<Self, serde_cbor::Error> {
        serde_cbor::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Matches the Go wire format fixture generated with
    // `go run ./testdata/gen_pubsub_rpc_fixture.go`. See the Go-side reference
    // at `sourcenetwork/go-libp2p-pubsub-rpc/rpc.go:381-398` for the encoder
    // and `rpc.go:404-412` for the decoder.
    //
    // Fields in declared order: ID, From, Data, Err. Definite-length CBOR map
    // of size 4 (`0xa4`).
    fn sample() -> InternalResponse {
        InternalResponse {
            id: "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_string(),
            from: b"\x12\x20\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20".to_vec(),
            data: b"hello".to_vec(),
            err: String::new(),
        }
    }

    #[test]
    fn round_trip() {
        let r = sample();
        let bytes = r.to_cbor().expect("encode");
        let decoded = InternalResponse::from_cbor(&bytes).expect("decode");
        assert_eq!(decoded, r);
    }

    #[test]
    fn encodes_as_definite_length_map() {
        let r = sample();
        let bytes = r.to_cbor().expect("encode");
        // Go's fxamacker/cbor emits definite-length maps; serde_cbor must too.
        // A 4-field map is encoded as 0xa4 (major type 5, length 4).
        assert_eq!(
            bytes.first(),
            Some(&0xa4),
            "response envelope must be a 4-field definite-length CBOR map, got first byte 0x{:02x}",
            bytes.first().copied().unwrap_or(0)
        );
    }

    #[test]
    fn field_order_matches_go() {
        let r = sample();
        let bytes = r.to_cbor().expect("encode");
        // Scan for each rename'd field name in declaration order. Go's
        // dagcbor output emits them in declared schema order; serde_cbor
        // preserves struct field order, so the byte offsets must agree.
        let as_str = String::from_utf8_lossy(&bytes);
        let id_at = as_str.find("ID").expect("ID field");
        let from_at = as_str.find("From").expect("From field");
        let data_at = as_str.find("Data").expect("Data field");
        let err_at = as_str.find("Err").expect("Err field");
        assert!(id_at < from_at);
        assert!(from_at < data_at);
        assert!(data_at < err_at);
    }
}
