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
