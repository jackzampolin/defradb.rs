//! AllDocsNode for providing all documents as a single group
//!
//! Used to enable multiple aggregates without GROUP BY by buffering
//! all documents and making them available via `current_group_docs()`.

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, PlanNode};

/// AllDocsNode buffers all documents and yields them once as a single group.
///
/// This node enables multiple aggregates to work correctly without GROUP BY:
/// - During `start()`, it buffers all documents from the source
/// - It yields a single "group" containing all documents
/// - `current_group_docs()` returns all buffered documents
///
/// Without this node, chained aggregates would each consume the previous
/// aggregate's result instead of sharing access to the original documents.
pub struct AllDocsNode {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// All documents buffered from source
    docs: Vec<Doc>,
    /// Current representative document
    current_doc: Doc,
    /// Whether start() has been called
    started: bool,
    /// Whether we've yielded the single result
    done: bool,
}

impl AllDocsNode {
    /// Create a new AllDocsNode wrapping a source
    pub fn new(source: Box<dyn PlanNode>, document_mapping: DocumentMapping) -> Self {
        Self {
            source,
            document_mapping,
            docs: Vec::new(),
            current_doc: Doc::default(),
            started: false,
            done: false,
        }
    }
}

#[async_trait]
impl PlanNode for AllDocsNode {
    async fn init(&mut self) -> Result<()> {
        self.docs.clear();
        self.done = false;
        self.started = false;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;
        self.started = true;

        // Buffer all documents from source
        while self.source.next().await? {
            self.docs.push(self.source.value().deep_clone());
        }

        // Set representative document (first doc or empty)
        if !self.docs.is_empty() {
            self.current_doc = self.docs[0].deep_clone();
        }

        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.started {
            self.start().await?;
        }

        if self.done {
            return Ok(false);
        }

        self.done = true;
        Ok(true)
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
        "allDocsNode"
    }

    fn current_group_docs(&self) -> Option<&[Doc]> {
        Some(&self.docs)
    }
}
