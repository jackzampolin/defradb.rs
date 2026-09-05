//! Opaque cursor token codec for GraphQL cursor pagination.

mod errors;

pub use errors::CursorError;

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A decoded cursor token. `keys` carries indexed field values
/// (alphabetically ordered) for index-backed seeking.
///
/// Field order and serde attributes mirror Go's `cursor.CursorPayload`
/// (`d`, `k` with omitempty, `o` always present) so encoded tokens match
/// Go byte-for-byte. `direction` (`o`) is part of Go's payload but Go never
/// populates it (always `""`); we keep it for byte-exact parity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(rename = "d")]
    pub doc_id: String,

    #[serde(rename = "k", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keys: BTreeMap<String, serde_json::Value>,

    #[serde(rename = "o", default)]
    pub direction: String,
}

impl Cursor {
    /// Construct a cursor from a document ID with no key values.
    /// Used when no index is available — the planner falls back to
    /// docID-based iteration.
    ///
    /// The ID is not validated: an empty ID encodes fine but `decode`
    /// rejects it with `EmptyDocId`. Prefer `try_from_doc_id` when the
    /// ID comes from an untrusted source.
    pub fn from_doc_id(doc_id: impl Into<String>) -> Self {
        Self {
            doc_id: doc_id.into(),
            keys: BTreeMap::new(),
            direction: String::new(),
        }
    }

    /// Fallible variant of `from_doc_id` that rejects an empty document
    /// ID with `EmptyDocId` instead of producing a token `decode`
    /// would refuse.
    pub fn try_from_doc_id(doc_id: impl Into<String>) -> Result<Self, CursorError> {
        let doc_id = doc_id.into();
        if doc_id.is_empty() {
            return Err(CursorError::EmptyDocId);
        }
        Ok(Self {
            doc_id,
            keys: BTreeMap::new(),
            direction: String::new(),
        })
    }

    /// Encode to a base64url-no-pad token: `base64url(json{d, k, o})`.
    /// Matches Go's `internal/cursor.Encode` byte-for-byte.
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("Cursor serialization cannot fail");
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&json)
    }

    /// Decode a token. Returns `EmptyDocId` if `d` is empty,
    /// `InvalidBase64`/`InvalidJson` for malformed input.
    pub fn decode(token: &str) -> Result<Self, CursorError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token)?;
        let cursor: Cursor = serde_json::from_slice(&bytes)?;
        if cursor.doc_id.is_empty() {
            return Err(CursorError::EmptyDocId);
        }
        Ok(cursor)
    }
}
