//! PermissionFilterNode for ACP-based document filtering
//!
//! This node wraps a source node and filters out documents that the
//! identity context doesn't have permission to read.

use std::collections::{HashSet, VecDeque};
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

    /// Queue of permitted documents, populated in `start()` after the
    /// source is fully drained and all ACP checks are dispatched in
    /// parallel via `futures::future::try_join_all`. `next()` simply
    /// pops from the front (#519).
    permitted_docs: VecDeque<Doc>,
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
            permitted_docs: VecDeque::new(),
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
        self.source.start().await?;

        // Drain the source eagerly and dispatch all ACP checks in parallel
        // (#519). Previously this filter awaited each `has_read_permission`
        // call serially in `next()`, which on remote backends like SourceHub
        // (gRPC, ~50–100ms per check) made a 1000-doc result set serialize
        // into 50–100s of pure wait time.
        //
        // Why drain-and-parallelize: the upstream ScanNode already
        // materializes the full document set in `init()` via `Vec<Doc>`,
        // so the "streaming" of `PlanNode::next()` is fictional at the
        // bottom of the plan tree. Capturing full per-query parallelism
        // here is a one-file change with zero planner contract impact.
        // If/when scans ever become true streams, this should evolve to
        // a `FuturesUnordered`-based sliding window with bounded
        // concurrency (textbook async best practice for I/O parallelism).

        let docid_index = self.document_mapping.first_index_of_name("_docID");

        // Phase 1: drain the source into (doc_id, doc) pairs.
        let mut pending: Vec<(String, Doc)> = Vec::new();
        while self.source.next().await? {
            let doc = self.source.value();
            let doc_id = match docid_index
                .and_then(|index| doc.get(index))
                .and_then(|value| value.as_str())
            {
                Some(id) => id.to_string(),
                // Docs without a `_docID` mapping cannot be ACP-checked
                // and were silently skipped in the previous serial loop;
                // preserve that behavior.
                None => continue,
            };
            pending.push((doc_id, doc.deep_clone()));
        }

        if pending.is_empty() {
            return Ok(());
        }

        // Phase 2: dispatch every read-permission check concurrently.
        // `has_read_permission` borrows `&self`; the borrow checker is
        // happy because we don't mutate `self` until after the join.
        let checks = pending
            .iter()
            .map(|(doc_id, _)| self.has_read_permission(doc_id));
        let decisions = futures::future::try_join_all(checks).await?;

        // Phase 3: walk results, queue permitted docs, track denied
        // explicit-id requests for the #551 error path.
        for ((doc_id, doc), allowed) in pending.into_iter().zip(decisions) {
            if allowed {
                self.permitted_docs.push_back(doc);
            } else if self.requested_doc_ids.contains(&doc_id) {
                self.denied_requested_ids.push(doc_id);
            }
        }

        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if let Some(doc) = self.permitted_docs.pop_front() {
            self.current_doc = doc;
            return Ok(true);
        }

        // Queue drained. If the caller explicitly asked for specific
        // doc IDs and any were denied by ACP, surface a permission-denied
        // error instead of silently returning fewer results (#551).
        if !self.denied_requested_ids.is_empty() {
            let mut ids = std::mem::take(&mut self.denied_requested_ids);
            ids.sort();
            ids.dedup();
            return Err(QueryError::permission_denied(format!(
                "read denied for doc_id(s): {}",
                ids.join(", ")
            )));
        }

        Ok(false)
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
