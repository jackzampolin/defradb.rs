//! Document storage keys shared across the write, merge, and index layers.
//!
//! The document body and deletion-marker key layouts were previously private to
//! `crates/db` (with `db-merge` hand-rolling the `/del/` prefix), which made it
//! impossible for the index layer to ask the one question unique enforcement
//! needs: *is the document an index entry points at still alive?* (#1111/#700:
//! a stale unique entry pointing at a deleted or missing document permanently
//! blocked re-creating that value, with no repair affordance.)

/// Prefix for document body keys: `/d/{collection_id}/{doc_id}`.
pub const DOC_KEY_PREFIX: &[u8] = b"/d/";

/// Prefix for logical-deletion markers: `/del/{collection_id}/{doc_id}`.
pub const DELETED_KEY_PREFIX: &[u8] = b"/del/";

/// Storage key for a document body.
pub fn doc_key(collection_id: &str, doc_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(DOC_KEY_PREFIX.len() + collection_id.len() + 1 + doc_id.len());
    key.extend_from_slice(DOC_KEY_PREFIX);
    key.extend_from_slice(collection_id.as_bytes());
    key.push(b'/');
    key.extend_from_slice(doc_id.as_bytes());
    key
}

/// Storage key for a document's logical-deletion marker.
pub fn deleted_doc_key(collection_id: &str, doc_id: &str) -> Vec<u8> {
    let mut key =
        Vec::with_capacity(DELETED_KEY_PREFIX.len() + collection_id.len() + 1 + doc_id.len());
    key.extend_from_slice(DELETED_KEY_PREFIX);
    key.extend_from_slice(collection_id.as_bytes());
    key.push(b'/');
    key.extend_from_slice(doc_id.as_bytes());
    key
}
