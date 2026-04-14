//! PermissionFilterNode for ACP-based document filtering
//!
//! This node wraps a source node and filters out documents that the
//! identity context doesn't have permission to read.

use std::collections::HashSet;
use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity};
use async_trait::async_trait;
use identity::Did;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::planner::{Doc, PlanNode};
use crate::txn::check_doc_access_with_overlay;

/// PermissionFilterNode filters documents based on ACP permissions.
///
/// This node wraps a source node and only yields documents that the
/// identity has read permission for. Documents created without ACP
/// (public/unregistered) pass through.
pub struct PermissionFilterNode {
    /// Source node to filter
    source: Box<dyn PlanNode>,

    /// Document ACP for permission checks
    acp: Arc<dyn DocumentACP>,

    /// Identity requesting access
    identity: Identity,

    /// Policy ID from the collection
    policy_id: String,

    /// Resource name from the policy
    resource_name: String,

    /// Current document
    current_doc: Doc,

    /// Document mapping from source
    document_mapping: DocumentMapping,

    /// Doc IDs the caller explicitly requested (e.g., via
    /// `_docID: { _eq: "..." }` or `docID: "..."`). When non-empty and
    /// at least one of these is denied, `next()` returns a
    /// `permission_denied` error after the source is exhausted instead
    /// of silently returning fewer results — matching Go's behavior and
    /// closing the read-side gap reported in #551.
    requested_doc_ids: HashSet<String>,

    /// Subset of `requested_doc_ids` that were emitted by the source
    /// but failed the read-permission check.
    denied_requested_ids: Vec<String>,
}

impl PermissionFilterNode {
    /// Create a new permission filter node.
    ///
    /// # Arguments
    /// * `source` - The source node to filter
    /// * `acp` - Document ACP for permission checks
    /// * `identity` - The identity requesting access
    /// * `policy_id` - Policy ID from the collection
    /// * `resource_name` - Resource name from the policy
    pub fn new(
        source: Box<dyn PlanNode>,
        acp: Arc<dyn DocumentACP>,
        identity: Identity,
        policy_id: impl Into<String>,
        resource_name: impl Into<String>,
    ) -> Self {
        let document_mapping = source.document_map().clone();
        Self {
            source,
            acp,
            identity,
            policy_id: policy_id.into(),
            resource_name: resource_name.into(),
            current_doc: Doc::default(),
            document_mapping,
            requested_doc_ids: HashSet::new(),
            denied_requested_ids: Vec::new(),
        }
    }

    /// Mark a set of document IDs as explicitly requested by the caller.
    ///
    /// When any of these IDs are emitted by the source but denied by ACP,
    /// `next()` will return a `permission_denied` error after the source is
    /// drained instead of silently filtering them out. Browse queries
    /// (no explicit doc IDs) keep the existing filter-only semantics.
    pub fn with_requested_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        self.requested_doc_ids = doc_ids.into_iter().collect();
        self
    }

    /// Create from an optional DID for backward compatibility.
    pub fn from_optional_did(
        source: Box<dyn PlanNode>,
        acp: Arc<dyn DocumentACP>,
        did: Option<Did>,
        policy_id: impl Into<String>,
        resource_name: impl Into<String>,
    ) -> Self {
        Self::new(source, acp, Identity::from(did), policy_id, resource_name)
    }

    /// Check if the identity has read permission for a document.
    ///
    /// Checks DAC bypass (NAC admin) first, then falls through to DAC check.
    /// Fail-closed: returns false on any error to prevent security bypass.
    async fn has_read_permission(&self, doc_id: &str) -> Result<bool> {
        // Check if identity can bypass DAC (NAC admin/owner with dac-bypass permission)
        if defra_core::dac_bypass::get_dac_bypass() {
            return Ok(true);
        }

        Ok(check_doc_access_with_overlay(
            self.acp.as_ref(),
            &self.identity,
            DocumentPermission::Read,
            &self.policy_id,
            &self.resource_name,
            doc_id,
        )
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(
                doc_id = %doc_id,
                policy_id = %self.policy_id,
                resource_name = %self.resource_name,
                identity = %self.identity,
                error = %e,
                "Permission check failed, denying access to document"
            );
            false
        }))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for PermissionFilterNode {
    async fn init(&mut self) -> Result<()> {
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        loop {
            // Get next document from source
            if !self.source.next().await? {
                // Source drained. If the caller explicitly asked for
                // specific doc IDs and any of those were denied by ACP,
                // surface that as an explicit permission-denied error
                // instead of silently returning fewer results (#551).
                if !self.denied_requested_ids.is_empty() {
                    let mut ids = std::mem::take(&mut self.denied_requested_ids);
                    ids.sort();
                    ids.dedup();
                    return Err(QueryError::permission_denied(format!(
                        "read denied for doc_id(s): {}",
                        ids.join(", ")
                    )));
                }
                return Ok(false);
            }

            let doc = self.source.value();

            // Child join scan mappings may place `_docID` at a non-zero index to keep
            // schema fields aligned for FK lookups, so resolve it from the mapping
            // instead of assuming Doc field index 0.
            let doc_id = match self
                .document_mapping
                .first_index_of_name("_docID")
                .and_then(|index| doc.get(index))
                .and_then(|value| value.as_str())
            {
                Some(id) => id.to_string(),
                None => continue,
            };

            // Check read permission
            if self.has_read_permission(&doc_id).await? {
                self.current_doc = doc.deep_clone();
                return Ok(true);
            }

            // Denied. If this doc was explicitly requested by ID, remember
            // it so we can surface an error after the source is drained.
            if self.requested_doc_ids.contains(&doc_id) {
                self.denied_requested_ids.push(doc_id);
            }

            // No permission, continue to next document
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.source.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "permissionFilterNode"
    }
}
