//! Helper methods for extracting data from CRDT deltas.

use defra_core::block::CrdtDelta;
use storage::corekv::Store;

use super::CommitsFetcher;

impl<S: Store> CommitsFetcher<S> {
    /// Get field name from delta
    pub(super) fn get_field_name(&self, delta: &CrdtDelta) -> Option<String> {
        match delta {
            CrdtDelta::Lww(d) => Some(d.field_name.clone()),
            CrdtDelta::Counter(d) => Some(d.field_name.clone()),
            CrdtDelta::Composite(_) => Some("_C".to_string()),
            CrdtDelta::Collection(_) => None,
            CrdtDelta::CollectionSet(_) => None,
            CrdtDelta::FieldDefinition(_) => None,
            CrdtDelta::CollectionDefinition(_) => None,
            _ => None,
        }
    }

    /// Get document ID from delta
    pub(super) fn get_doc_id(&self, delta: &CrdtDelta) -> Option<String> {
        delta
            .doc_id()
            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
    }

    /// Get delta data from delta
    pub(super) fn get_delta_data(&self, delta: &CrdtDelta) -> Option<Vec<u8>> {
        match delta {
            CrdtDelta::Lww(d) => {
                if d.data.is_empty() {
                    None
                } else {
                    Some(d.data.clone())
                }
            }
            CrdtDelta::Counter(d) => {
                if d.data.is_empty() {
                    None
                } else {
                    Some(d.data.clone())
                }
            }
            CrdtDelta::Composite(_) => None,
            CrdtDelta::Collection(_) => None,
            CrdtDelta::CollectionSet(_) => None,
            CrdtDelta::FieldDefinition(_) => None,
            CrdtDelta::CollectionDefinition(_) => None,
            _ => None,
        }
    }

    /// Get schema version ID from delta
    pub(super) fn get_schema_version_id(&self, delta: &CrdtDelta) -> Option<String> {
        match delta {
            CrdtDelta::Lww(d) => Some(d.schema_version_id.clone()),
            CrdtDelta::Counter(d) => Some(d.schema_version_id.clone()),
            CrdtDelta::Composite(d) => Some(d.schema_version_id.clone()),
            CrdtDelta::Collection(d) => Some(d.schema_version_id.clone()),
            CrdtDelta::CollectionSet(_) => None,
            CrdtDelta::FieldDefinition(_) => None,
            CrdtDelta::CollectionDefinition(_) => None,
            _ => None,
        }
    }

    /// Check if a string looks like a CIDv1.
    ///
    /// Go's CID library is more lenient and parses CIDs that have valid multibase
    /// prefixes but invalid hash components. Rust's library is stricter and rejects
    /// these. This function detects strings that "look like" CIDv1 so we can return
    /// a more appropriate error message for Go compatibility.
    pub(super) fn looks_like_cidv1(s: &str) -> bool {
        if s.len() < 40 {
            return false;
        }
        s.starts_with("bafy")
            || s.starts_with("bafk")
            || s.starts_with("bafz")
            || s.starts_with("bafr")
            || s.starts_with("Qm")
    }
}
