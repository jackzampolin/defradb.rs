//! LimitNode for applying limit and offset to query results

use async_trait::async_trait;

use crate::planner::{index_selection::CursorSeek, Doc, ExecInfo, PlanNode};
use query_types::document::DocumentMapping;
use query_types::error::Result;

/// LimitNode applies limit and offset to query results.
///
/// This node wraps another plan node and:
/// - Skips the first `offset` documents
/// - Returns at most `limit` documents
pub struct LimitNode {
    /// Source plan node
    source: Box<dyn PlanNode>,
    /// Maximum number of documents to return (None = unlimited)
    limit: Option<u64>,
    /// Number of documents to skip
    offset: u64,
    /// Current row index (how many docs have been processed)
    row_index: u64,
    /// Number of documents returned
    docs_returned: u64,
    /// Current document
    current_doc: Doc,
    /// Execution statistics for explain execute mode
    exec_info: ExecInfo,
}

impl LimitNode {
    /// Create a new limit node wrapping a source
    pub fn new(source: Box<dyn PlanNode>, limit: Option<u64>, offset: u64) -> Self {
        Self {
            source,
            limit,
            offset,
            row_index: 0,
            docs_returned: 0,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }

    /// Create a limit node with only a limit (no offset)
    pub fn limit_only(source: Box<dyn PlanNode>, limit: u64) -> Self {
        Self::new(source, Some(limit), 0)
    }

    /// Create a limit node with only an offset (no limit)
    pub fn offset_only(source: Box<dyn PlanNode>, offset: u64) -> Self {
        Self::new(source, None, offset)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for LimitNode {
    async fn init(&mut self) -> Result<()> {
        self.row_index = 0;
        self.docs_returned = 0;
        self.exec_info = ExecInfo::default();
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        self.exec_info.iterations += 1;

        // Check if we've already returned enough documents
        if let Some(limit) = self.limit {
            if self.docs_returned >= limit {
                return Ok(false);
            }
        }

        loop {
            // Get next document from source
            if !self.source.next().await? {
                return Ok(false);
            }

            self.row_index += 1;

            // Skip documents until we've passed the offset
            if self.row_index <= self.offset {
                continue;
            }

            // We have a document to return
            self.current_doc = self.source.value().deep_clone();
            self.docs_returned += 1;
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
        self.source.document_map()
    }

    fn kind(&self) -> &'static str {
        "limitNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        // Go always includes limit field, even when null
        obj.insert(
            "limit".to_string(),
            match self.limit {
                Some(limit) => serde_json::Value::Number(limit.into()),
                None => serde_json::Value::Null,
            },
        );

        // Go always includes offset, even when 0
        obj.insert(
            "offset".to_string(),
            serde_json::Value::Number(self.offset.into()),
        );

        // Recursively explain child node - merge their wrapped structure
        let child_explain = self.source.explain();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }

    fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
        self.source.set_cursor_seek(seek)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );

        let child_explain = self.source.explain_execute();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }
}
