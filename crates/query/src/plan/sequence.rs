//! SequenceNode chains two child plan nodes sequentially

use async_trait::async_trait;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, ExecInfo, PlanNode};

/// Chains two child plan nodes, exhausting the first then the second.
///
/// Used to concatenate orphan results with join results.
pub struct SequenceNode {
    first: Box<dyn PlanNode>,
    second: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// Whether the first child has been exhausted
    first_exhausted: bool,
    current_doc: Doc,
    exec_info: ExecInfo,
}

impl SequenceNode {
    pub fn new(
        first: Box<dyn PlanNode>,
        second: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            first,
            second,
            document_mapping,
            first_exhausted: false,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for SequenceNode {
    async fn init(&mut self) -> Result<()> {
        self.first_exhausted = false;
        self.exec_info = ExecInfo::default();
        self.first.init().await?;
        self.second.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.first.start().await?;
        self.second.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        self.exec_info.iterations += 1;

        if !self.first_exhausted {
            if self.first.next().await? {
                self.current_doc = self.first.value().deep_clone();
                return Ok(true);
            }
            self.first_exhausted = true;
        }

        if self.second.next().await? {
            self.current_doc = self.second.value().deep_clone();
            return Ok(true);
        }

        Ok(false)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.first.close().await?;
        self.second.close().await
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.first.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "sequenceNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        let first_explain = self.first.explain();
        if let Some(first_obj) = first_explain.as_object() {
            for (key, value) in first_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        let second_explain = self.second.explain();
        if let Some(second_obj) = second_explain.as_object() {
            for (key, value) in second_obj {
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

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );

        let first_explain = self.first.explain_execute();
        if let Some(first_obj) = first_explain.as_object() {
            for (key, value) in first_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        let second_explain = self.second.explain_execute();
        if let Some(second_obj) = second_explain.as_object() {
            for (key, value) in second_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }
}
