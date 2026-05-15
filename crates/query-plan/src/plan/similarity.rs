//! SimilarityNode for computing dot product similarity between document vectors and query vectors

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::planner::{index_selection::CursorSeek, Doc, ExecInfo, PlanNode};
use query_types::document::DocumentMapping;
use query_types::error::{QueryError, Result};

/// SimilarityNode computes the dot product between a document's vector field
/// and a query vector, storing the result as a Float in the document.
///
/// For each document from the source, it:
/// 1. Reads the target field (a numeric array like `[Int!]` or `[Float64!]`)
/// 2. Computes the dot product with the query vector
/// 3. Stores the result at the designated similarity index
///
/// Errors if vectors have different lengths.
pub struct SimilarityNode {
    source: Box<dyn PlanNode>,
    document_mapping: DocumentMapping,
    /// Index of the target field containing the document's vector
    field_index: usize,
    /// Index where the similarity result should be stored
    similarity_index: usize,
    /// The query vector to compute dot product against
    vector: Vec<f64>,
    /// Current document with similarity result
    current_doc: Doc,
    /// Execution statistics
    exec_info: ExecInfo,
}

impl SimilarityNode {
    pub fn new(
        source: Box<dyn PlanNode>,
        document_mapping: DocumentMapping,
        field_index: usize,
        similarity_index: usize,
        vector: Vec<f64>,
    ) -> Self {
        Self {
            source,
            document_mapping,
            field_index,
            similarity_index,
            vector,
            current_doc: Doc::default(),
            exec_info: ExecInfo::default(),
        }
    }

    /// Compute dot product of two f64 slices.
    fn dot_product(a: &[f64], b: &[f64]) -> f64 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    /// Extract a numeric vector from a JSON array value.
    fn extract_vector(value: &JsonValue) -> Option<Vec<f64>> {
        match value {
            JsonValue::Array(items) => {
                let mut vec = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        JsonValue::Number(n) => {
                            vec.push(n.as_f64()?);
                        }
                        _ => return None,
                    }
                }
                Some(vec)
            }
            _ => None,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for SimilarityNode {
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

        let doc = self.source.value();
        let mut new_doc = doc.deep_clone();

        // Get the document's vector field
        if let Some(field_value) = doc.get(self.field_index) {
            if let Some(doc_vector) = Self::extract_vector(field_value) {
                // Check vector length match
                if doc_vector.len() != self.vector.len() {
                    return Err(QueryError::execution(format!(
                        "source and vector must be of the same length. Source: {}, Vector: {}",
                        doc_vector.len(),
                        self.vector.len()
                    )));
                }

                // Compute dot product
                let result = Self::dot_product(&doc_vector, &self.vector);

                // Store as Float (f64)
                let json_result = serde_json::Number::from_f64(result)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null);

                new_doc.set(self.similarity_index, json_result);
            } else {
                // Field value is not a numeric array
                new_doc.set(self.similarity_index, JsonValue::Null);
            }
        } else {
            // Field not present in document
            new_doc.set(self.similarity_index, JsonValue::Null);
        }

        self.current_doc = new_doc;
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

    fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
        self.source.set_cursor_seek(seek)
    }

    fn kind(&self) -> &'static str {
        "similarityNode"
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

        let child_explain = self.source.explain_execute();
        if let Some(child_obj) = child_explain.as_object() {
            for (key, value) in child_obj {
                obj.insert(key.clone(), value.clone());
            }
        }

        serde_json::Value::Object(obj)
    }
}
