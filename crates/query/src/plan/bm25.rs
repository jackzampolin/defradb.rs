//! BM25Node for injecting pre-computed full-text search relevance scores.
//!
//! Scores are computed from the inverted index at the storage layer and
//! passed to this node at construction time. The node simply looks up each
//! document's score by doc_id and injects it into the output.

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, ExecInfo, PlanNode};

/// BM25Node injects pre-computed BM25 scores into documents.
///
/// During `next()`, extracts the doc_id from each source document, looks up
/// its pre-computed score, and injects it at `score_index`.
pub struct BM25Node {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    score_index: usize,
    query: String,
    precomputed_scores: HashMap<String, f64>,
    current_doc: Doc,
    exec_info: ExecInfo,
}

impl BM25Node {
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        score_index: usize,
        query: String,
        precomputed_scores: HashMap<String, f64>,
    ) -> Self {
        Self {
            source,
            document_mapping,
            score_index,
            query,
            precomputed_scores,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for BM25Node {
    async fn init(&mut self) -> Result<()> {
        self.exec_info = ExecInfo::default();
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        self.exec_info.iterations += 1;

        if !self.source.next().await? {
            return Ok(false);
        }

        let mut doc = self.source.value().deep_clone();

        let score = doc
            .doc_id()
            .and_then(|id| self.precomputed_scores.get(id))
            .copied()
            .unwrap_or(0.0);

        let json_score = serde_json::Number::from_f64(score)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null);
        doc.set(self.score_index, json_score);

        self.current_doc = doc;
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
        "bm25Node"
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );
        obj.insert("query".to_string(), serde_json::json!(self.query));

        let child_explain = self.source.explain_execute();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        JsonValue::Object(obj)
    }
}
