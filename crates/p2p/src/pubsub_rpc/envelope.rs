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
//! Go encodes this via `ipld.Marshal(dagcbor.Encode, ...)`, whose default
//! `MapSortMode_RFC7049` sorts keys by length then bytewise. For these field
//! names the canonical order is:
//!
//! ```text
//! ID (2), Err (3), Data (4), From (4)   // Data < From because 'D' (0x44) < 'F' (0x46)
//! ```
//!
//! This is a *different* order than the Go struct declaration. `ciborium`
//! emits fields in serde declaration order, so the Rust struct below is
//! declared in canonical wire order to produce byte-identical output.
//!
//! Byte-parity with Go is verified against the fixtures produced by
//! `testdata/gen_pubsub_rpc_fixture/main.go`.

use serde::{Deserialize, Serialize};

/// Wire-format of a single response delivered over the dynamic per-peer
/// `_response` sub-topic. Fields are declared in canonical dag-cbor
/// map-key order; do not reorder without regenerating Go fixtures.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalResponse {
    /// Stringified CIDv1(raw, sha256(request_bytes)) identifying the request
    /// this response corresponds to.
    #[serde(rename = "ID")]
    pub id: String,

    /// Human-readable error string from the responder, or empty.
    #[serde(rename = "Err")]
    pub err: String,

    /// Raw user payload returned by the responder's `MessageHandler`.
    #[serde(rename = "Data", with = "serde_bytes")]
    pub data: Vec<u8>,

    /// Responder peer ID as raw bytes. Go fills this server-side from the
    /// validated gossipsub message source, not from the envelope (see
    /// `rpc.go:415`), so decoders should treat the wire field as advisory
    /// only and trust the transport-level sender.
    #[serde(rename = "From", with = "serde_bytes")]
    pub from: Vec<u8>,
}

/// Encode/decode error for the response envelope.
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    #[error("cbor encode: {0}")]
    Encode(String),
    #[error("cbor decode: {0}")]
    Decode(String),
}

impl InternalResponse {
    /// Encode to dag-cbor bytes compatible with Go's `dagcbor.Encode`.
    pub fn to_cbor(&self) -> Result<Vec<u8>, EnvelopeError> {
        let mut out = Vec::new();
        ciborium::into_writer(self, &mut out).map_err(|e| EnvelopeError::Encode(e.to_string()))?;
        Ok(out)
    }

    /// Decode dag-cbor bytes produced by Go's `dagcbor.Encode`.
    pub fn from_cbor(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        ciborium::from_reader(bytes).map_err(|e| EnvelopeError::Decode(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fixture bytes produced by `testdata/gen_pubsub_rpc_fixture/main.go`,
    // which runs the same `ipld.Marshal(dagcbor.Encode, ...)` pipeline as
    // `sourcenetwork/go-libp2p-pubsub-rpc` (see rpc.go:389).
    //
    // To regenerate after changing the fixture values:
    //   cd testdata/gen_pubsub_rpc_fixture && go run main.go
    const GO_FIXTURE_OK_HEX: &str = "a4624944783b6261666b7265696864776463656667683464716b6a763637757a636d77376f6a6565367865647a6465746f6a757a6a657674656e78717576796b75634572726064446174614568656c6c6f6446726f6d582212200102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
    const GO_FIXTURE_ERR_HEX: &str = "a4624944783b6261666b7265696864776463656667683464716b6a763637757a636d77376f6a6565367865647a6465746f6a757a6a657674656e78717576796b75634572726b756e6b6e6f776e20646f636444617461406446726f6d40";

    fn fixture_ok() -> InternalResponse {
        InternalResponse {
            id: "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_string(),
            err: String::new(),
            data: b"hello".to_vec(),
            from: b"\x12\x20\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x20".to_vec(),
        }
    }

    fn fixture_err() -> InternalResponse {
        InternalResponse {
            id: "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku".to_string(),
            err: "unknown doc".to_string(),
            data: Vec::new(),
            from: Vec::new(),
        }
    }

    #[test]
    fn encodes_byte_identical_to_go_ok_fixture() {
        let got = fixture_ok().to_cbor().expect("encode");
        let expected = hex::decode(GO_FIXTURE_OK_HEX).expect("hex");
        assert_eq!(
            hex::encode(&got),
            hex::encode(&expected),
            "ok fixture bytes must match Go's dag-cbor output exactly"
        );
    }

    #[test]
    fn encodes_byte_identical_to_go_err_fixture() {
        let got = fixture_err().to_cbor().expect("encode");
        let expected = hex::decode(GO_FIXTURE_ERR_HEX).expect("hex");
        assert_eq!(
            hex::encode(&got),
            hex::encode(&expected),
            "err fixture bytes must match Go's dag-cbor output exactly"
        );
    }

    #[test]
    fn decodes_go_ok_fixture() {
        let bytes = hex::decode(GO_FIXTURE_OK_HEX).expect("hex");
        let decoded = InternalResponse::from_cbor(&bytes).expect("decode");
        assert_eq!(decoded, fixture_ok());
    }

    #[test]
    fn decodes_go_err_fixture() {
        let bytes = hex::decode(GO_FIXTURE_ERR_HEX).expect("hex");
        let decoded = InternalResponse::from_cbor(&bytes).expect("decode");
        assert_eq!(decoded, fixture_err());
    }

    #[test]
    fn round_trip() {
        for r in [fixture_ok(), fixture_err()] {
            let bytes = r.to_cbor().expect("encode");
            let decoded = InternalResponse::from_cbor(&bytes).expect("decode");
            assert_eq!(decoded, r);
        }
    }

    #[test]
    fn encodes_as_definite_length_map() {
        let bytes = fixture_ok().to_cbor().expect("encode");
        // A 4-field definite-length map is encoded as 0xa4 (major type 5, length 4).
        assert_eq!(bytes.first(), Some(&0xa4));
    }
}
