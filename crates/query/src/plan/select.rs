//! SelectNode for selecting fields from documents

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::Filter;
use crate::planner::{Doc, ExecInfo, PlanNode};

/// SelectNode selects specific fields from documents.
///
/// This node wraps another plan node and applies field selection,
/// optional filtering, and prepares documents for rendering.
pub struct SelectNode {
    /// Source plan node
    source: Box<dyn PlanNode>,
    /// Document mapping for this select
    document_mapping: DocumentMapping,
    /// Optional additional filter
    filter: Option<Filter>,
    /// Current document
    current_doc: Doc,
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
    /// Count of documents that matched the filter
    filter_matches: u64,
}

impl SelectNode {
    /// Create a new select node wrapping a source
    pub fn new(source: Box<dyn PlanNode>, document_mapping: DocumentMapping) -> Self {
        Self {
            source,
            document_mapping,
            filter: None,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
            filter_matches: 0,
        }
    }

    /// Set an additional filter
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }
}

#[async_trait]
impl PlanNode for SelectNode {
    async fn init(&mut self) -> Result<()> {
        // Reset execution stats
        self.exec_info = ExecInfo::default();
        self.filter_matches = 0;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        // Track iteration (Go counts each call to next, including final false)
        self.exec_info.iterations += 1;

        loop {
            if !self.source.next().await? {
                return Ok(false);
            }

            let doc = self.source.value();

            // Apply filter if present
            if let Some(ref filter) = self.filter {
                if !filter.matches(doc.fields(), &self.document_mapping)? {
                    continue;
                }
            }

            // Track filter match
            self.filter_matches += 1;

            // Copy the document (field projection happens at render time)
            self.current_doc = doc.deep_clone();
            return Ok(true);
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
        "selectNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // Go DefraDB format: always include docID (null if not filtering by specific IDs)
        // Note: SelectNode doesn't track docIDs directly; they're handled at query parsing level
        obj.insert("docID".to_string(), serde_json::Value::Null);

        // Go DefraDB format: always include filter (null if none)
        if let Some(ref filter) = self.filter {
            obj.insert("filter".to_string(), serde_json::json!(filter.conditions()));
        } else {
            obj.insert("filter".to_string(), serde_json::Value::Null);
        }

        // Recursively explain child node - merge their wrapped structure
        let child_explain = self.source.explain();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // Go DefraDB execute format: iterations, filterMatches
        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );
        obj.insert(
            "filterMatches".to_string(),
            serde_json::json!(self.filter_matches),
        );

        // Recursively explain child node with execution info
        let child_explain = self.source.explain_execute();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }
}
