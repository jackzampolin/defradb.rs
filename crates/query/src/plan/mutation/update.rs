//! UpdateNode for updating existing documents
//!
//! This node updates documents in storage during query execution, following
//! the Go DefraDB pattern where persistence happens within the plan node.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Utc};
use document::{DocID, Document, NormalValue};
use schema::{CType, CollectionVersion};
use serde_json::Value as JsonValue;
use tracing;

use crate::document::{document_to_plan_doc, DocumentMapping};
use crate::error::{QueryError, Result};
use crate::fetcher::DocFetcher;
use crate::mapper::Filter;
use crate::mutator::{DocMutator, UpdateResult};
use crate::planner::{Doc, PlanNode};

use super::create::{json_to_normal_value_with_kind, normal_value_to_json};

/// Input for an update mutation - field values to patch on existing documents.
#[derive(Debug, Clone)]
pub struct UpdateInput {
    /// Field values to update, keyed by field name
    pub fields: std::collections::HashMap<String, JsonValue>,
}

impl UpdateInput {
    /// Create a new empty input.
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    /// Add a field value to update.
    pub fn with_field(mut self, name: impl Into<String>, value: JsonValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    /// Apply this update to a document, using schema-aware type coercion when available.
    ///
    /// For counter CRDT fields (PCounter/PNCounter), the input value is treated as an
    /// increment rather than a replacement, matching Go DefraDB behavior.
    ///
    /// The `utc_now` parameter is used for `UTC_NOW` values to ensure consistent timestamps.
    pub fn apply_to(
        &self,
        doc: &mut Document,
        collection: Option<&CollectionVersion>,
        utc_now: DateTime<FixedOffset>,
    ) -> Result<usize> {
        let mut modified_count = 0;

        for (field_name, value) in &self.fields {
            let field_def =
                collection.and_then(|c| c.fields.iter().find(|f| f.name == *field_name));
            let field_kind = field_def.map(|f| &f.kind);
            let normal_value = json_to_normal_value_with_kind(value, field_kind, utc_now)?;

            // Counter CRDT fields use increment semantics
            if let Some(fd) = field_def {
                if fd.crdt_type.is_counter() {
                    // PCounter rejects negative increments
                    if fd.crdt_type == CType::PCounter {
                        validate_pcounter_increment(&normal_value)?;
                    }
                    let current = doc.get(field_name);
                    let new_value = increment_value(current, &normal_value)?;
                    doc.set(field_name.clone(), new_value);
                    modified_count += 1;
                    continue;
                }
            }

            doc.set(field_name.clone(), normal_value);
            modified_count += 1;
        }

        Ok(modified_count)
    }
}

impl Default for UpdateInput {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate that a PCounter increment is non-negative.
fn validate_pcounter_increment(value: &NormalValue) -> Result<()> {
    let is_negative = match value {
        NormalValue::Int(v) => *v < 0,
        NormalValue::Float64(v) => *v < 0.0,
        NormalValue::Float32(v) => *v < 0.0,
        _ => false,
    };
    if is_negative {
        return Err(QueryError::execution("value cannot be negative"));
    }
    Ok(())
}

/// Increment a counter value by an increment amount.
/// For counter CRDTs, updates add to the current value rather than replacing it.
/// Uses wrapping arithmetic to match Go DefraDB overflow behavior.
fn increment_value(current: Option<&NormalValue>, increment: &NormalValue) -> Result<NormalValue> {
    match increment {
        NormalValue::Int(inc) => {
            let cur = match current {
                Some(NormalValue::Int(v)) => *v,
                None | Some(NormalValue::Null) => 0,
                _ => 0,
            };
            Ok(NormalValue::Int(cur.wrapping_add(*inc)))
        }
        NormalValue::Float64(inc) => {
            let cur = match current {
                Some(NormalValue::Float64(v)) => *v,
                Some(NormalValue::Float32(v)) => *v as f64,
                None | Some(NormalValue::Null) => 0.0,
                _ => 0.0,
            };
            Ok(NormalValue::Float64(cur + inc))
        }
        NormalValue::Float32(inc) => {
            let cur = match current {
                Some(NormalValue::Float32(v)) => *v,
                Some(NormalValue::Float64(v)) => *v as f32,
                None | Some(NormalValue::Null) => 0.0,
                _ => 0.0,
            };
            Ok(NormalValue::Float32(cur + inc))
        }
        _ => Err(QueryError::execution(format!(
            "Counter fields only support numeric increments, got: {:?}",
            increment
        ))),
    }
}

/// UpdateNode updates existing documents in a collection.
///
/// This node implements the Volcano iterator model. On the first call to `next()`,
/// it finds all matching documents (by docIDs or filter), applies the update patch,
/// and persists them via the `DocMutator`. Subsequent calls iterate through results.
///
/// # Example
///
/// ```ignore
/// let input = UpdateInput::new()
///     .with_field("email", json!("newemail@example.com"));
///
/// let mut node = UpdateNode::new("Users", mutator, fetcher, mapping)
///     .with_doc_ids(vec!["bae-123".to_string()])
///     .with_input(input);
///
/// node.init().await?;
/// node.start().await?;
///
/// while node.next().await? {
///     let updated_doc = node.value();
///     println!("Updated: {:?}", updated_doc.doc_id());
/// }
/// ```
pub struct UpdateNode {
    /// Collection name to update documents in
    collection_name: String,
    /// Document mutator for storage operations
    mutator: Arc<dyn DocMutator>,
    /// Document fetcher for resolving filters and getting all documents
    fetcher: Arc<dyn DocFetcher>,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Collection schema for schema-aware type coercion (e.g., DateTime parsing)
    collection: Option<Arc<CollectionVersion>>,
    /// Document IDs to update (mutually exclusive with filter)
    doc_ids: Option<Vec<String>>,
    /// Filter to find documents to update (mutually exclusive with doc_ids)
    filter: Option<Filter>,
    /// Update input (fields to patch)
    input: UpdateInput,
    /// Updated documents (populated after first next())
    updated_docs: Vec<Doc>,
    /// Document IDs that were requested but not found
    not_found_ids: Vec<String>,
    /// Current position in updated_docs
    position: usize,
    /// Current document being yielded
    current_doc: Doc,
    /// Whether updates have been performed yet
    did_update: bool,
    /// Whether the node has been initialized
    initialized: bool,
}

impl UpdateNode {
    /// Create a new update node for a collection.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection to update documents in
    /// * `mutator` - Document mutator for storage operations
    /// * `fetcher` - Document fetcher for resolving filters and getting all documents
    /// * `document_mapping` - Field mapping for result documents
    pub fn new(
        collection_name: impl Into<String>,
        mutator: Arc<dyn DocMutator>,
        fetcher: Arc<dyn DocFetcher>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            collection_name: collection_name.into(),
            mutator,
            fetcher,
            document_mapping,
            collection: None,
            doc_ids: None,
            filter: None,
            input: UpdateInput::new(),
            updated_docs: Vec::new(),
            not_found_ids: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            did_update: false,
            initialized: false,
        }
    }

    /// Set specific document IDs to update.
    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        self.doc_ids = Some(doc_ids);
        self
    }

    /// Set a filter to find documents to update.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set the update input (fields to patch).
    pub fn with_input(mut self, input: UpdateInput) -> Self {
        self.input = input;
        self
    }

    /// Set the collection schema for schema-aware type coercion.
    pub fn with_collection(mut self, collection: Arc<CollectionVersion>) -> Self {
        self.collection = Some(collection);
        self
    }

    /// Get the number of documents that were updated.
    pub fn updated_count(&self) -> usize {
        self.updated_docs.len()
    }

    /// Get the document IDs that were requested but not found.
    ///
    /// This allows callers to detect when update operations silently
    /// skipped documents due to them not existing in the collection.
    pub fn not_found_ids(&self) -> &[String] {
        &self.not_found_ids
    }

    /// Convert an UpdateResult to a plan Doc using our document mapping.
    fn update_result_to_doc(&self, result: &UpdateResult) -> Result<Doc> {
        let num_fields = self.document_mapping.next_index();
        let mut doc = Doc::new(num_fields);

        // Set document ID at index 0
        if let Some(doc_id) = result.document.id() {
            doc.set_doc_id(doc_id.to_string());
        }

        // Map each field from the updated document
        for (field_name, field_value) in result.document.values() {
            if let Some(index) = self.document_mapping.first_index_of_name(field_name) {
                let json_value = normal_value_to_json(field_value.value());
                doc.set(index, json_value);
            }
        }

        Ok(doc)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for UpdateNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.updated_docs.clear();
        self.not_found_ids.clear();
        self.did_update = false;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "UpdateNode.next() called before init()",
            ));
        }

        // On first call, perform all updates
        if !self.did_update {
            // Get document IDs to update
            let doc_ids_to_update = if let Some(ref ids) = self.doc_ids {
                // Explicit doc_ids provided
                ids.clone()
            } else {
                // No doc_ids - fetch documents and optionally filter
                let all_docs = self.fetcher.get_all(&self.collection_name).await?;
                let mut matching_ids = Vec::new();

                for doc in all_docs {
                    // If filter exists, check if document matches
                    if let Some(ref filter) = self.filter {
                        let plan_doc = document_to_plan_doc(&doc, &self.document_mapping)?;
                        if !filter.matches(plan_doc.fields(), &self.document_mapping)? {
                            continue;
                        }
                    }
                    // Include this document
                    if let Some(id) = doc.id() {
                        matching_ids.push(id.to_string());
                    }
                }
                matching_ids
            };

            // Capture a single timestamp for all UTC_NOW values in this update
            let utc_offset = FixedOffset::east_opt(0).unwrap();
            let utc_now = Utc::now().with_timezone(&utc_offset);

            // Update each document
            for doc_id_str in doc_ids_to_update {
                let doc_id = match DocID::from_string(&doc_id_str) {
                    Ok(id) => id,
                    Err(_) => {
                        // Invalid DocID format - treat as not found (Go compatibility)
                        tracing::warn!(
                            collection = %self.collection_name,
                            doc_id = %doc_id_str,
                            "Invalid DocID format - skipping"
                        );
                        self.not_found_ids.push(doc_id_str.clone());
                        continue;
                    }
                };

                // Fetch document for update
                let doc_opt = self
                    .mutator
                    .get_for_update(&self.collection_name, &doc_id)
                    .await?;

                if let Some(mut doc) = doc_opt {
                    // Apply update input with schema-aware coercion
                    self.input.apply_to(&mut doc, self.collection.as_deref(), utc_now)?;

                    // Collect the modified field names for block creation
                    let modified_fields: std::collections::HashSet<String> =
                        self.input.fields.keys().cloned().collect();

                    // Persist update with modified field tracking
                    let result = self
                        .mutator
                        .update(&self.collection_name, doc, modified_fields)
                        .await?;

                    // Convert to plan Doc
                    let plan_doc = self.update_result_to_doc(&result)?;

                    // Re-filter: if a filter was used, the updated document must still
                    // match the filter to be included in results (Go compatibility).
                    // Use the full document with a collection-based mapping since the
                    // filter may reference fields not in the mutation result mapping.
                    if let Some(ref filter) = self.filter {
                        if let Some(ref col) = self.collection {
                            let mut filter_mapping = DocumentMapping::new();
                            for field in &col.fields {
                                let idx = filter_mapping.next_index();
                                filter_mapping.add(idx, &field.name);
                            }
                            let full_doc = document_to_plan_doc(&result.document, &filter_mapping)?;
                            if !filter.matches(full_doc.fields(), &filter_mapping)? {
                                continue;
                            }
                        }
                    }

                    self.updated_docs.push(plan_doc);
                } else {
                    // Track and log missing documents instead of silently skipping
                    tracing::warn!(
                        collection = %self.collection_name,
                        doc_id = %doc_id_str,
                        "Document not found for update - skipping"
                    );
                    self.not_found_ids.push(doc_id_str.clone());
                }
            }

            self.did_update = true;
        }

        // Iterate through updated documents
        if self.position >= self.updated_docs.len() {
            return Ok(false);
        }

        self.current_doc = self.updated_docs[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.updated_docs.clear();
        self.not_found_ids.clear();
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // UpdateNode is a leaf node
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "updateNode"
    }
}
