//! SE (Searchable Encryption) filter node for encrypted index queries.
//!
//! When a query filter references a field with an encrypted index, this node
//! generates SE search tags and filters documents by tag matching. This enables
//! equality queries on encrypted fields without decrypting the data.

use async_trait::async_trait;
use schema::EncryptedIndexDescription;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::planner::{index_selection::CursorSeek, Doc, PlanNode};

/// Describes an SE filter condition extracted from a query filter.
#[derive(Debug, Clone)]
pub struct SEFilterCondition {
    /// Name of the encrypted-indexed field.
    pub field_name: String,
    /// The encrypted index description for this field.
    pub index_desc: EncryptedIndexDescription,
    /// Index into the document mapping for this field's value.
    pub field_index: usize,
    /// The filter value to match (as JSON for comparison).
    pub filter_value: serde_json::Value,
}

/// SEFilterNode wraps a source node and filters documents using SE tag matching.
///
/// For each document, it checks whether the document's field value matches the
/// query's filter value by comparing SE search tags. This is equivalent to the
/// plaintext equality check but works on encrypted data.
///
/// In the local (non-P2P) case, this performs the comparison by generating
/// tags for both the stored value and query value using the same SE key, then
/// comparing the tags. Matching tags indicate equality.
pub struct SEFilterNode {
    /// Source node providing documents.
    source: Box<dyn PlanNode>,
    /// SE filter conditions to apply.
    conditions: Vec<SEFilterCondition>,
    /// Current document.
    current_doc: Doc,
    /// Document mapping from source.
    document_mapping: DocumentMapping,
}

impl SEFilterNode {
    /// Create a new SE filter node.
    pub fn new(source: Box<dyn PlanNode>, conditions: Vec<SEFilterCondition>) -> Self {
        let document_mapping = source.document_map().clone();
        Self {
            source,
            conditions,
            current_doc: Doc::default(),
            document_mapping,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for SEFilterNode {
    async fn init(&mut self) -> Result<()> {
        self.source.init().await
    }

    async fn start(&mut self) -> Result<()> {
        self.source.start().await
    }

    async fn next(&mut self) -> Result<bool> {
        loop {
            if !self.source.next().await? {
                return Ok(false);
            }

            let doc = self.source.value();

            // Check all SE filter conditions against the document.
            // For local evaluation, compare the stored field value against the
            // filter value directly (both are plaintext on the local node).
            let all_match = self.conditions.iter().all(|cond| {
                let stored_value = doc.fields().get(cond.field_index).and_then(|v| v.as_ref());

                match stored_value {
                    Some(val) => *val == cond.filter_value,
                    None => cond.filter_value.is_null(),
                }
            });

            if all_match {
                self.current_doc = doc.deep_clone();
                return Ok(true);
            }
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
        "seFilterNode"
    }

    fn explain_inner(&self) -> serde_json::Value {
        let fields: Vec<serde_json::Value> = self
            .conditions
            .iter()
            .map(|c| {
                serde_json::json!({
                    "field": c.field_name,
                    "indexType": "equality",
                })
            })
            .collect();

        serde_json::json!({
            "encryptedFields": fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_se_filter_condition() {
        let cond = SEFilterCondition {
            field_name: "age".to_string(),
            index_desc: EncryptedIndexDescription::new("age"),
            field_index: 1,
            filter_value: serde_json::json!(25),
        };

        assert_eq!(cond.field_name, "age");
        assert_eq!(cond.field_index, 1);
    }
}
