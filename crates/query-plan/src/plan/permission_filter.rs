//! PermissionFilterNode for ACP-based document filtering
//!
//! This node wraps a source node and filters out documents that the
//! identity context doesn't have permission to read.

use std::sync::Arc;

use acp::{DocumentACP, DocumentPermission, Identity};
use async_lock::Mutex;
use async_trait::async_trait;
use defra_core::thread_bounds::MaybeBoxFuture;
use futures::{stream::FuturesOrdered, FutureExt, StreamExt};
use identity::Did;

use crate::planner::{index_selection::CursorSeek, Doc, PlanNode};
use crate::txn::check_doc_access_with_overlay;
use query_types::document::DocumentMapping;
use query_types::error::Result;

// Keep enough remote checks moving to hide latency without letting one query
// exhaust the ACP provider's connection pool.
const MAX_IN_FLIGHT_PERMISSION_CHECKS: usize = 16;

type PermissionCheck = MaybeBoxFuture<'static, (Doc, bool)>;

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
    identity: Arc<Identity>,

    /// Policy ID from the collection
    policy_id: Arc<str>,

    /// Resource name from the policy
    resource_name: Arc<str>,

    /// Current document
    current_doc: Doc,

    /// Document mapping from source
    document_mapping: DocumentMapping,

    /// Ordered checks retain source order while allowing bounded concurrency.
    /// The mutex only supplies the `Sync` bound required by `PlanNode`; access
    /// is exclusive through `&mut self` and never locks.
    pending: Mutex<FuturesOrdered<PermissionCheck>>,

    /// Whether the wrapped source has no more documents to enqueue.
    source_exhausted: bool,
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
            identity: Arc::new(identity),
            policy_id: Arc::from(policy_id.into()),
            resource_name: Arc::from(resource_name.into()),
            current_doc: Doc::default(),
            document_mapping,
            pending: Mutex::new(FuturesOrdered::new()),
            source_exhausted: false,
        }
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

    fn permission_check(&self, doc_id: String, doc: Doc) -> PermissionCheck {
        let acp = Arc::clone(&self.acp);
        let identity = Arc::clone(&self.identity);
        let policy_id = Arc::clone(&self.policy_id);
        let resource_name = Arc::clone(&self.resource_name);

        Box::pin(async move {
            let allowed = if defra_core::dac_bypass::get_dac_bypass() {
                true
            } else {
                check_doc_access_with_overlay(
                    acp.as_ref(),
                    identity.as_ref(),
                    DocumentPermission::Read,
                    policy_id.as_ref(),
                    resource_name.as_ref(),
                    &doc_id,
                    None,
                )
                .await
                .unwrap_or_else(|error| {
                    tracing::warn!(
                        %doc_id,
                        policy_id = %policy_id,
                        resource_name = %resource_name,
                        identity = %identity,
                        %error,
                        "Permission check failed, denying access to document"
                    );
                    false
                })
            };

            (doc, allowed)
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for PermissionFilterNode {
    async fn init(&mut self) -> Result<()> {
        *self.pending.get_mut() = FuturesOrdered::new();
        self.source_exhausted = false;
        self.current_doc = Doc::default();
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        loop {
            while !self.source_exhausted
                && self.pending.get_mut().len() < MAX_IN_FLIGHT_PERMISSION_CHECKS
            {
                if !self.source.next().await? {
                    self.source_exhausted = true;
                    break;
                }

                let doc = self.source.value().deep_clone();

                // Child join mappings may place `_docID` at a non-zero index.
                let Some(doc_id) = self
                    .document_mapping
                    .first_index_of_name("_docID")
                    .and_then(|index| doc.get(index))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned)
                else {
                    continue;
                };

                let check = self.permission_check(doc_id, doc);
                self.pending.get_mut().push_back(check);

                // Start queued I/O before pulling another source row. A
                // non-blocking poll avoids racing (and potentially cancelling)
                // `source.next()` while still letting immediately available
                // decisions preserve single-row streaming latency.
                if let Some(Some((doc, allowed))) = self.pending.get_mut().next().now_or_never() {
                    if allowed {
                        self.current_doc = doc;
                        return Ok(true);
                    }
                }
            }

            match self.pending.get_mut().next().await {
                Some((doc, true)) => {
                    self.current_doc = doc;
                    return Ok(true);
                }
                Some((_, false)) => continue,
                None => return Ok(false),
            }
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        *self.pending.get_mut() = FuturesOrdered::new();
        self.source_exhausted = true;
        self.source.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.source.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
        self.source.set_cursor_seek(seek)
    }

    fn set_cursor_fetch_limit(&mut self, limit: u64) -> bool {
        self.source.set_cursor_fetch_limit(limit)
    }

    fn page_info(&self) -> Option<crate::plan::CursorPageInfo> {
        self.source.page_info()
    }

    fn kind(&self) -> &'static str {
        "permissionFilterNode"
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use async_lock::Barrier;

    use super::*;

    struct MockSource {
        docs: Vec<Doc>,
        position: usize,
        current: Doc,
        mapping: DocumentMapping,
        yielded: Arc<AtomicUsize>,
    }

    impl MockSource {
        fn new(doc_ids: &[String], yielded: Arc<AtomicUsize>) -> Self {
            let docs = doc_ids
                .iter()
                .map(|doc_id| {
                    let mut doc = Doc::new(1);
                    doc.set_doc_id(doc_id);
                    doc
                })
                .collect();
            let mut mapping = DocumentMapping::new();
            mapping.add(0, "_docID");

            Self {
                docs,
                position: 0,
                current: Doc::default(),
                mapping,
                yielded,
            }
        }
    }

    #[async_trait]
    impl PlanNode for MockSource {
        async fn init(&mut self) -> Result<()> {
            self.position = 0;
            Ok(())
        }

        async fn start(&mut self) -> Result<()> {
            Ok(())
        }

        async fn next(&mut self) -> Result<bool> {
            let Some(doc) = self.docs.get(self.position) else {
                return Ok(false);
            };
            self.position += 1;
            self.yielded.fetch_add(1, Ordering::SeqCst);
            self.current = doc.deep_clone();
            Ok(true)
        }

        fn value(&self) -> &Doc {
            &self.current
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        fn source(&self) -> Option<&dyn PlanNode> {
            None
        }

        fn document_map(&self) -> &DocumentMapping {
            &self.mapping
        }

        fn kind(&self) -> &'static str {
            "mockSource"
        }
    }

    struct MockAcp {
        barrier: Option<Arc<Barrier>>,
        delays: HashMap<String, Duration>,
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    impl MockAcp {
        fn new(barrier: Option<Arc<Barrier>>, delays: HashMap<String, Duration>) -> Self {
            Self {
                barrier,
                delays,
                active: AtomicUsize::new(0),
                max_active: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl DocumentACP for MockAcp {
        async fn register_doc_object(
            &self,
            _identity: &Did,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<()> {
            Ok(())
        }

        async fn is_doc_registered(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<bool> {
            Ok(true)
        }

        async fn check_doc_access(
            &self,
            _identity: &Identity,
            _permission: DocumentPermission,
            _policy_id: &str,
            _resource_name: &str,
            doc_id: &str,
        ) -> acp::Result<bool> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);

            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            if let Some(delay) = self.delays.get(doc_id) {
                tokio::time::sleep(*delay).await;
            }

            self.active.fetch_sub(1, Ordering::SeqCst);
            if doc_id == "error" {
                Err(acp::Error::Storage("injected failure".to_string()))
            } else {
                Ok(true)
            }
        }

        async fn add_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> acp::Result<bool> {
            Ok(true)
        }

        async fn delete_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> acp::Result<bool> {
            Ok(true)
        }

        async fn unregister_doc_object(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> acp::Result<()> {
            Ok(())
        }
    }

    fn permission_filter(
        doc_ids: &[String],
        yielded: Arc<AtomicUsize>,
        acp: Arc<MockAcp>,
    ) -> PermissionFilterNode {
        PermissionFilterNode::new(
            Box::new(MockSource::new(doc_ids, yielded)),
            acp,
            Identity::Anonymous,
            "policy",
            "User",
        )
    }

    #[tokio::test]
    async fn dispatches_a_bounded_window_concurrently() {
        let doc_ids: Vec<_> = (0..=MAX_IN_FLIGHT_PERMISSION_CHECKS)
            .map(|index| format!("doc-{index}"))
            .collect();
        let yielded = Arc::new(AtomicUsize::new(0));
        let acp = Arc::new(MockAcp::new(
            Some(Arc::new(Barrier::new(MAX_IN_FLIGHT_PERMISSION_CHECKS))),
            HashMap::new(),
        ));
        let mut node = permission_filter(&doc_ids, Arc::clone(&yielded), Arc::clone(&acp));

        node.init().await.unwrap();
        node.start().await.unwrap();
        let has_row = tokio::time::timeout(Duration::from_secs(1), node.next())
            .await
            .expect("permission checks did not run concurrently")
            .unwrap();

        assert!(has_row);
        assert_eq!(node.value().doc_id(), Some("doc-0"));
        assert_eq!(
            yielded.load(Ordering::SeqCst),
            MAX_IN_FLIGHT_PERMISSION_CHECKS
        );
        assert_eq!(
            acp.max_active.load(Ordering::SeqCst),
            MAX_IN_FLIGHT_PERMISSION_CHECKS
        );
    }

    #[tokio::test]
    async fn preserves_source_order_when_checks_finish_out_of_order() {
        let doc_ids = vec!["slow".to_string(), "fast".to_string(), "medium".to_string()];
        let delays = HashMap::from([
            ("slow".to_string(), Duration::from_millis(30)),
            ("fast".to_string(), Duration::from_millis(1)),
            ("medium".to_string(), Duration::from_millis(10)),
        ]);
        let yielded = Arc::new(AtomicUsize::new(0));
        let acp = Arc::new(MockAcp::new(None, delays));
        let mut node = permission_filter(&doc_ids, yielded, Arc::clone(&acp));

        node.init().await.unwrap();
        node.start().await.unwrap();

        let mut actual = Vec::new();
        while node.next().await.unwrap() {
            actual.push(node.value().doc_id().unwrap().to_string());
        }

        assert_eq!(actual, doc_ids);
        assert_eq!(acp.max_active.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn permission_errors_fail_closed_without_stopping_the_scan() {
        let doc_ids = vec!["error".to_string(), "allowed".to_string()];
        let acp = Arc::new(MockAcp::new(None, HashMap::new()));
        let mut node = permission_filter(&doc_ids, Arc::new(AtomicUsize::new(0)), acp);

        node.init().await.unwrap();
        node.start().await.unwrap();

        assert!(node.next().await.unwrap());
        assert_eq!(node.value().doc_id(), Some("allowed"));
        assert!(!node.next().await.unwrap());
    }
}
