//! Core planner traits implementing the Volcano Iterator Model

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use storage::corekv::MaybeSendSync;

use crate::document::DocumentMapping;
use crate::error::Result;

/// Document status (active or deleted)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DocStatus {
    #[default]
    Active,
    Deleted,
}

/// Document fields as a vector of optional JSON values
pub type DocFields = Vec<Option<JsonValue>>;

/// A document yielded by plan nodes
#[derive(Debug, Clone, Default)]
pub struct Doc {
    /// Whether this doc should be hidden from final output
    pub hidden: bool,
    /// Field values indexed by position
    fields: DocFields,
    /// Document status
    pub status: DocStatus,
    /// Schema version ID (for migrations)
    pub schema_version_id: Option<String>,
    /// Number of stored fields from the original document (for fieldFetches metrics)
    pub stored_field_count: usize,
}

impl Doc {
    /// Create a new document with the given number of fields
    pub fn new(num_fields: usize) -> Self {
        Self {
            hidden: false,
            fields: vec![None; num_fields],
            status: DocStatus::Active,
            schema_version_id: None,
            stored_field_count: 0,
        }
    }

    /// Create a new document from existing fields
    pub fn with_fields(fields: DocFields) -> Self {
        Self {
            hidden: false,
            fields,
            status: DocStatus::Active,
            schema_version_id: None,
            stored_field_count: 0,
        }
    }

    /// Get the document ID (field at index 0)
    pub fn doc_id(&self) -> Option<&str> {
        self.fields
            .first()
            .and_then(|f| f.as_ref())
            .and_then(|v| v.as_str())
    }

    /// Set the document ID (field at index 0)
    pub fn set_doc_id(&mut self, doc_id: impl Into<String>) {
        if self.fields.is_empty() {
            self.fields.push(None);
        }
        self.fields[0] = Some(JsonValue::String(doc_id.into()));
    }

    /// Get a field value by index
    pub fn get(&self, index: usize) -> Option<&JsonValue> {
        self.fields.get(index).and_then(|f| f.as_ref())
    }

    /// Set a field value by index
    pub fn set(&mut self, index: usize, value: JsonValue) {
        if index >= self.fields.len() {
            self.fields.resize(index + 1, None);
        }
        self.fields[index] = Some(value);
    }

    /// Get all fields as a slice
    pub fn fields(&self) -> &[Option<JsonValue>] {
        &self.fields
    }

    /// Get the number of fields
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Clone the document
    pub fn deep_clone(&self) -> Self {
        Self {
            hidden: self.hidden,
            fields: self.fields.clone(),
            status: self.status,
            schema_version_id: self.schema_version_id.clone(),
            stored_field_count: self.stored_field_count,
        }
    }

    /// Mark as deleted
    pub fn mark_deleted(&mut self) {
        self.status = DocStatus::Deleted;
    }

    /// Check if deleted
    pub fn is_deleted(&self) -> bool {
        self.status == DocStatus::Deleted
    }
}

/// Volcano Iterator Model plan node trait.
///
/// All query plan nodes implement this async trait following the lifecycle:
/// `init() -> start() -> next()* -> close()`
///
/// The iterator pattern allows lazy evaluation and pipelining of results.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait PlanNode: MaybeSendSync {
    /// Initialize or reinitialize the node.
    ///
    /// Called before start() to set up internal state.
    async fn init(&mut self) -> Result<()>;

    /// Start internal processes.
    ///
    /// Called after init() to begin actual data scanning/processing.
    async fn start(&mut self) -> Result<()>;

    /// Get the next document.
    ///
    /// Returns `Ok(true)` if a document is available (retrieve with `value()`),
    /// `Ok(false)` when iteration is complete.
    async fn next(&mut self) -> Result<bool>;

    /// Get the current document value.
    ///
    /// Only valid after `next()` returns `true`.
    fn value(&self) -> &Doc;

    /// Close and release resources.
    ///
    /// Called when iteration is complete or on error.
    async fn close(&mut self) -> Result<()>;

    /// Get the source/child plan node (for plan tree traversal).
    fn source(&self) -> Option<&dyn PlanNode>;

    /// Get the document mapping for this node.
    fn document_map(&self) -> &DocumentMapping;

    /// Get the node type name (for debugging/explain).
    fn kind(&self) -> &'static str;

    /// Get the documents in the current group (for GROUP BY aggregation).
    ///
    /// Returns Some(&[Doc]) if this node is a GroupByNode and is positioned on a group,
    /// None otherwise. Default implementation returns None.
    fn current_group_docs(&self) -> Option<&[Doc]> {
        None
    }

    /// Whether this node (or an ancestor) is a GroupBy source.
    ///
    /// Used by aggregate nodes to detect that they're in a GroupBy context,
    /// even when the GroupByNode has no groups (empty collection).
    /// Without this, aggregates would fall through to non-grouped mode
    /// and yield a synthetic result instead of returning empty.
    fn is_grouped_source(&self) -> bool {
        false
    }

    /// Generate an explanation of this node for EXPLAIN queries.
    ///
    /// Returns a JSON object in Go DefraDB format where the node kind is the key:
    /// `{ "scanNode": { "collectionName": "..." } }`
    ///
    /// Child nodes are nested under their kind name, creating a tree structure:
    /// `{ "selectNode": { "filter": ..., "scanNode": { ... } } }`
    fn explain(&self) -> JsonValue {
        let node_kind = self.kind().to_string();
        let inner = self.explain_inner();

        let mut wrapper = serde_json::Map::new();
        wrapper.insert(node_kind, inner);
        JsonValue::Object(wrapper)
    }

    /// Generate the inner explanation content for this node.
    ///
    /// Override this in specific nodes to add node-specific attributes.
    /// Child nodes are automatically added by the default implementation.
    fn explain_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();

        // Recursively explain child nodes - merge their wrapped structure
        if let Some(source) = self.source() {
            let child_explain = source.explain();
            // Child explain is { "childKind": { ... } }, merge it into our object
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    obj.insert(key.clone(), value.clone());
                }
            }
        }

        JsonValue::Object(obj)
    }

    /// Generate a debug explanation showing all nodes including internal ones.
    ///
    /// Uses Go DefraDB format with node kind as key.
    fn explain_debug(&self) -> JsonValue {
        let node_kind = self.kind().to_string();
        let inner = self.explain_debug_inner();

        let mut wrapper = serde_json::Map::new();
        wrapper.insert(node_kind, inner);
        JsonValue::Object(wrapper)
    }

    /// Generate the inner debug explanation content for this node.
    fn explain_debug_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();

        // Recursively explain all child nodes
        if let Some(source) = self.source() {
            let child_explain = source.explain_debug();
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    obj.insert(key.clone(), value.clone());
                }
            }
        }

        JsonValue::Object(obj)
    }

    /// Generate an explanation with execution metrics for EXPLAIN (type: execute).
    ///
    /// Returns a JSON object in Go DefraDB format with execution statistics:
    /// - scanNode: iterations, docFetches, fieldFetches, indexFetches
    /// - selectNode: iterations, filterMatches
    /// - etc.
    fn explain_execute(&self) -> JsonValue {
        let node_kind = self.kind().to_string();
        let inner = self.explain_execute_inner();

        let mut wrapper = serde_json::Map::new();
        wrapper.insert(node_kind, inner);
        JsonValue::Object(wrapper)
    }

    /// Generate the inner execution explanation content for this node.
    ///
    /// Override this in specific nodes to add execution-specific metrics.
    /// Default implementation includes child nodes.
    fn explain_execute_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();

        // Recursively explain child nodes with execution info
        if let Some(source) = self.source() {
            let child_explain = source.explain_execute();
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    obj.insert(key.clone(), value.clone());
                }
            }
        }

        JsonValue::Object(obj)
    }

    /// Get execution info for this node (for Execute explain mode).
    ///
    /// Returns the execution statistics collected during query execution.
    /// Default implementation returns empty ExecInfo.
    fn exec_info(&self) -> ExecInfo {
        ExecInfo::default()
    }
}

/// Execution statistics for plan nodes
#[derive(Debug, Clone, Default)]
pub struct ExecInfo {
    /// Number of documents fetched
    pub docs_fetched: u64,
    /// Number of fields fetched
    pub fields_fetched: u64,
    /// Number of index lookups
    pub indexes_fetched: u64,
    /// Number of iterations
    pub iterations: u64,
}

impl ExecInfo {
    pub fn new() -> Self {
        Self::default()
    }

    /// Merge another ExecInfo into this one
    pub fn merge(&mut self, other: &ExecInfo) {
        self.docs_fetched += other.docs_fetched;
        self.fields_fetched += other.fields_fetched;
        self.indexes_fetched += other.indexes_fetched;
        self.iterations += other.iterations;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_doc_new() {
        let doc = Doc::new(3);
        assert_eq!(doc.num_fields(), 3);
        assert!(doc.fields().iter().all(|f| f.is_none()));
    }

    #[test]
    fn test_doc_id() {
        let mut doc = Doc::new(3);
        doc.set_doc_id("doc123");
        assert_eq!(doc.doc_id(), Some("doc123"));
    }

    #[test]
    fn test_doc_set_get() {
        let mut doc = Doc::new(3);
        doc.set(1, json!("Alice"));
        doc.set(2, json!(30));

        assert_eq!(doc.get(1), Some(&json!("Alice")));
        assert_eq!(doc.get(2), Some(&json!(30)));
        assert_eq!(doc.get(0), None);
    }

    #[test]
    fn test_doc_auto_resize() {
        let mut doc = Doc::new(2);
        doc.set(5, json!("value"));

        assert_eq!(doc.num_fields(), 6);
        assert_eq!(doc.get(5), Some(&json!("value")));
    }

    #[test]
    fn test_doc_status() {
        let mut doc = Doc::new(1);
        assert!(!doc.is_deleted());

        doc.mark_deleted();
        assert!(doc.is_deleted());
        assert_eq!(doc.status, DocStatus::Deleted);
    }

    #[test]
    fn test_exec_info_merge() {
        let mut info1 = ExecInfo {
            docs_fetched: 10,
            fields_fetched: 30,
            indexes_fetched: 2,
            iterations: 10,
        };

        let info2 = ExecInfo {
            docs_fetched: 5,
            fields_fetched: 15,
            indexes_fetched: 1,
            iterations: 5,
        };

        info1.merge(&info2);

        assert_eq!(info1.docs_fetched, 15);
        assert_eq!(info1.fields_fetched, 45);
        assert_eq!(info1.indexes_fetched, 3);
        assert_eq!(info1.iterations, 15);
    }
}
