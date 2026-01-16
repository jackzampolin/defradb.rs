//! PermissionFilterNode for ACP-based document filtering
//!
//! This node wraps a source node and filters out documents that the
//! identity context doesn't have permission to read.

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission};
use async_trait::async_trait;
use identity::Did;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

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

    /// Identity requesting access (None = anonymous)
    identity: Option<Did>,

    /// Policy ID from the collection
    policy_id: String,

    /// Resource name from the policy
    resource_name: String,

    /// Current document
    current_doc: Doc,

    /// Document mapping from source
    document_mapping: DocumentMapping,
}

impl PermissionFilterNode {
    /// Create a new permission filter node.
    ///
    /// # Arguments
    /// * `source` - The source node to filter
    /// * `acp` - Document ACP for permission checks
    /// * `identity` - The identity requesting access (None for anonymous)
    /// * `policy_id` - Policy ID from the collection
    /// * `resource_name` - Resource name from the policy
    pub fn new(
        source: Box<dyn PlanNode>,
        acp: Arc<dyn DocumentACP>,
        identity: Option<Did>,
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
        }
    }

    /// Check if the identity has read permission for a document.
    ///
    /// Fail-closed: returns false on any error to prevent security bypass.
    async fn has_read_permission(&self, doc_id: &str) -> Result<bool> {
        Ok(self
            .acp
            .check_doc_access(
                self.identity.as_ref(),
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
                    identity = ?self.identity,
                    error = %e,
                    "Permission check failed, denying access to document"
                );
                false
            }))
    }
}

#[async_trait]
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
                return Ok(false);
            }

            let doc = self.source.value();

            // Get doc ID for permission check
            let doc_id = match doc.doc_id() {
                Some(id) => id.to_string(),
                None => {
                    // No doc ID, skip this document
                    continue;
                }
            };

            // Check read permission
            if self.has_read_permission(&doc_id).await? {
                self.current_doc = doc.deep_clone();
                return Ok(true);
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

#[cfg(test)]
mod tests {
    use super::*;
    use acp::{LocalDocumentACP, MemoryAcpStore};
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use serde_json::json;

    use crate::plan::ScanNode;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
    }

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        )
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m
    }

    fn make_test_docs() -> Vec<Doc> {
        vec![
            Doc::with_fields(vec![Some(json!("doc1")), Some(json!("Alice"))]),
            Doc::with_fields(vec![Some(json!("doc2")), Some(json!("Bob"))]),
            Doc::with_fields(vec![Some(json!("doc3")), Some(json!("Charlie"))]),
        ]
    }

    fn create_scan_node(docs: Vec<Doc>) -> ScanNode {
        let collection = make_test_collection();
        let mapping = make_test_mapping();
        ScanNode::new(collection, mapping).with_docs(docs)
    }

    #[tokio::test]
    async fn test_permission_filter_public_docs_pass_through() {
        let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
        let docs = make_test_docs();
        let scan = create_scan_node(docs);

        let mut filter = PermissionFilterNode::new(
            Box::new(scan),
            acp,
            None, // Anonymous
            "policy1",
            "users",
        );

        filter.init().await.unwrap();
        filter.start().await.unwrap();

        // All docs should pass through (unregistered = public)
        let mut count = 0;
        while filter.next().await.unwrap() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_permission_filter_owner_sees_all() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let owner = test_did();

        // Register all docs with the same owner
        for doc_id in ["doc1", "doc2", "doc3"] {
            acp.register_doc_object(&owner, "policy1", "users", doc_id)
                .await
                .unwrap();
        }

        let docs = make_test_docs();
        let scan = create_scan_node(docs);

        let mut filter = PermissionFilterNode::new(
            Box::new(scan),
            acp,
            Some(owner), // Owner identity
            "policy1",
            "users",
        );

        filter.init().await.unwrap();
        filter.start().await.unwrap();

        // Owner should see all docs
        let mut count = 0;
        while filter.next().await.unwrap() {
            count += 1;
        }
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_permission_filter_non_owner_sees_nothing() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let owner = test_did();
        let stranger = test_did2();

        // Register all docs with owner
        for doc_id in ["doc1", "doc2", "doc3"] {
            acp.register_doc_object(&owner, "policy1", "users", doc_id)
                .await
                .unwrap();
        }

        let docs = make_test_docs();
        let scan = create_scan_node(docs);

        let mut filter = PermissionFilterNode::new(
            Box::new(scan),
            acp,
            Some(stranger), // Different identity
            "policy1",
            "users",
        );

        filter.init().await.unwrap();
        filter.start().await.unwrap();

        // Stranger should see nothing
        let mut count = 0;
        while filter.next().await.unwrap() {
            count += 1;
        }
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_permission_filter_partial_access() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let owner = test_did();
        let reader = test_did2();

        // Register doc1 and doc2 with owner
        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();
        acp.register_doc_object(&owner, "policy1", "users", "doc2")
            .await
            .unwrap();
        // doc3 is unregistered (public)

        // Grant reader access to doc1 only
        acp.add_actor_relationship(&owner, &reader, "users", "doc1", "reader")
            .await
            .unwrap();

        let docs = make_test_docs();
        let scan = create_scan_node(docs);

        let mut filter = PermissionFilterNode::new(
            Box::new(scan),
            acp,
            Some(reader),
            "policy1",
            "users",
        );

        filter.init().await.unwrap();
        filter.start().await.unwrap();

        // Reader should see doc1 (granted) and doc3 (public), but not doc2
        let mut seen_docs = Vec::new();
        while filter.next().await.unwrap() {
            if let Some(doc_id) = filter.value().doc_id() {
                seen_docs.push(doc_id.to_string());
            }
        }

        assert_eq!(seen_docs.len(), 2);
        assert!(seen_docs.contains(&"doc1".to_string()));
        assert!(seen_docs.contains(&"doc3".to_string()));
        assert!(!seen_docs.contains(&"doc2".to_string()));
    }

    #[tokio::test]
    async fn test_permission_filter_anonymous_only_sees_public() {
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let owner = test_did();

        // Register doc1 and doc2 with owner
        acp.register_doc_object(&owner, "policy1", "users", "doc1")
            .await
            .unwrap();
        acp.register_doc_object(&owner, "policy1", "users", "doc2")
            .await
            .unwrap();
        // doc3 is unregistered (public)

        let docs = make_test_docs();
        let scan = create_scan_node(docs);

        let mut filter = PermissionFilterNode::new(
            Box::new(scan),
            acp,
            None, // Anonymous
            "policy1",
            "users",
        );

        filter.init().await.unwrap();
        filter.start().await.unwrap();

        // Anonymous should only see doc3 (public)
        let mut seen_docs = Vec::new();
        while filter.next().await.unwrap() {
            if let Some(doc_id) = filter.value().doc_id() {
                seen_docs.push(doc_id.to_string());
            }
        }

        assert_eq!(seen_docs.len(), 1);
        assert!(seen_docs.contains(&"doc3".to_string()));
    }
}
