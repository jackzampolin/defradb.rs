//! BM25Node for computing full-text search relevance scores
//!
//! Buffers all source documents, computes corpus-wide statistics (total docs,
//! per-term document frequency, average field length), then serves scored
//! documents with proper BM25 including IDF.

use async_trait::async_trait;
use bm25::{DefaultTokenizer, Tokenizer};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use storage::index::parse_language;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{Doc, ExecInfo, PlanNode};

/// BM25Node computes BM25 relevance scores for documents based on a text query.
///
/// During `start()`, buffers all source documents and computes corpus-wide
/// statistics. During `next()`, serves documents with proper BM25 scores
/// that include IDF weighting.
pub struct BM25Node {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    field_indices: Vec<usize>,
    score_index: usize,
    query: String,
    query_tokens: Vec<String>,
    tokenizer: DefaultTokenizer,
    k1: f64,
    b: f64,
    /// Buffered documents with their extracted text tokens
    buffered_docs: Vec<Doc>,
    /// Per-document token frequency maps (parallel to buffered_docs)
    doc_token_freqs: Vec<HashMap<String, u32>>,
    /// Per-document field lengths in tokens (parallel to buffered_docs)
    doc_field_lens: Vec<u32>,
    /// Total number of buffered documents (computed once in start())
    total_docs: f64,
    /// Average field length across all docs (computed once in start())
    avgdl: f64,
    /// Current position in the buffered docs
    cursor: usize,
    current_doc: Doc,
    exec_info: ExecInfo,
}

impl BM25Node {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        field_indices: Vec<usize>,
        score_index: usize,
        query: String,
        k1: f64,
        b: f64,
        language: &str,
    ) -> Self {
        let lang = parse_language(language);
        let tokenizer = DefaultTokenizer::new(lang);
        let query_tokens = tokenizer.tokenize(&query);
        Self {
            source,
            document_mapping,
            field_indices,
            score_index,
            query,
            query_tokens,
            tokenizer,
            k1,
            b,
            buffered_docs: Vec::new(),
            doc_token_freqs: Vec::new(),
            doc_field_lens: Vec::new(),
            total_docs: 0.0,
            avgdl: 0.0,
            cursor: 0,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }

    fn extract_text(&self, doc: &Doc) -> String {
        let mut combined = String::new();
        for &idx in &self.field_indices {
            if let Some(field_value) = doc.get(idx) {
                if let Some(text) = field_value.as_str() {
                    if !combined.is_empty() {
                        combined.push(' ');
                    }
                    combined.push_str(text);
                }
            }
        }
        combined
    }

    fn tokenize_with_freqs(&self, text: &str) -> (HashMap<String, u32>, u32) {
        let tokens = self.tokenizer.tokenize(text);
        let len = tokens.len() as u32;
        let mut freqs = HashMap::new();
        for token in tokens {
            *freqs.entry(token).or_insert(0u32) += 1;
        }
        (freqs, len)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for BM25Node {
    async fn init(&mut self) -> Result<()> {
        self.exec_info = ExecInfo::default();
        self.buffered_docs.clear();
        self.doc_token_freqs.clear();
        self.doc_field_lens.clear();
        self.total_docs = 0.0;
        self.avgdl = 0.0;
        self.cursor = 0;
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await?;

        // Buffer all source documents and tokenize their text
        while self.source.next().await? {
            let doc = self.source.value().deep_clone();
            let text = self.extract_text(&doc);
            let (freqs, field_len) = if text.is_empty() {
                (HashMap::new(), 0)
            } else {
                self.tokenize_with_freqs(&text)
            };
            self.buffered_docs.push(doc);
            self.doc_token_freqs.push(freqs);
            self.doc_field_lens.push(field_len);
        }

        self.total_docs = self.buffered_docs.len() as f64;
        self.avgdl = if self.buffered_docs.is_empty() {
            0.0
        } else {
            self.doc_field_lens.iter().map(|&l| l as f64).sum::<f64>() / self.total_docs
        };

        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        self.exec_info.iterations += 1;

        if self.cursor >= self.buffered_docs.len() {
            return Ok(false);
        }

        let idx = self.cursor;
        self.cursor += 1;

        let mut doc = self.buffered_docs[idx].deep_clone();
        let freqs = &self.doc_token_freqs[idx];
        let dl = self.doc_field_lens[idx] as f64;
        let n = self.total_docs;
        let avgdl = self.avgdl;

        let mut score = 0.0f64;
        for query_term in &self.query_tokens {
            let tf = *freqs.get(query_term.as_str()).unwrap_or(&0) as f64;
            if tf > 0.0 {
                // Document frequency: how many docs contain this term
                let df = self
                    .doc_token_freqs
                    .iter()
                    .filter(|f| f.contains_key(query_term.as_str()))
                    .count() as f64;

                // IDF: ln((N - df + 0.5) / (df + 0.5) + 1)
                let idf = ((n - df + 0.5) / (df + 0.5) + 1.0).ln();

                // BM25 TF component: (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl))
                let denom = if avgdl > 0.0 {
                    tf + self.k1 * (1.0 - self.b + self.b * dl / avgdl)
                } else {
                    tf + self.k1
                };
                let tf_norm = (tf * (self.k1 + 1.0)) / denom;

                score += idf * tf_norm;
            }
        }

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
