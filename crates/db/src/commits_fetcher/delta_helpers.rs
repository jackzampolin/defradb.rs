//! Helper methods for extracting data from CRDT deltas.

use cid::Cid;
use defra_core::block::CrdtDelta;
use storage::corekv::Store;

use crate::txn::DbTxn;

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

    /// Resolve every DocID owning a block. Field blocks can be shared by
    /// multiple documents now that their payloads no longer embed identity.
    pub(super) async fn get_doc_ids(
        &self,
        txn: &mut DbTxn<S>,
        cid: &Cid,
        block: &defra_core::Block,
    ) -> crate::Result<Option<Vec<String>>> {
        let systemstore = txn.systemstore()?;
        crate::doc_id_map::resolve_block_doc_ids(&systemstore, cid, block).await
    }

    pub(super) async fn canonical_doc_id(
        &self,
        txn: &mut DbTxn<S>,
        doc_id: &str,
    ) -> crate::Result<String> {
        let systemstore = txn.systemstore()?;
        let Some(doc_ref) = crate::doc_id_map::get_doc_ref(&systemstore, doc_id).await? else {
            return Ok(doc_id.to_string());
        };
        crate::doc_id_map::get_doc_id(&systemstore, doc_ref.doc_short_id)
            .await?
            .ok_or_else(|| {
                crate::Error::InvalidDocument(format!(
                    "document short ID {} has no canonical DocID",
                    doc_ref.doc_short_id
                ))
            })
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
