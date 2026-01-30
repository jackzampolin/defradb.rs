//! UpsertNode for conditional create-or-update operations
//!
//! This node implements upsert semantics following Go DefraDB's behavior:
//! - If filter matches 0 documents: CREATE with `create` fields
//! - If filter matches 1 document: UPDATE with `update` fields
//! - If filter matches >1 document: Return error

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Utc};
use document::{DocID, Document};
use schema::{CType, CollectionVersion};
use serde_json::Value as JsonValue;
use tracing;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mutator::DocMutator;
use crate::planner::{Doc, PlanNode};

use super::create::{json_to_normal_value_with_kind_and_time, normal_value_to_json, CreateInput};

/// Input for an upsert mutation - field values for create or update.
#[derive(Debug, Clone)]
pub struct UpsertInput {
    /// Field values keyed by field name (used for both create and update operations)
    pub fields: std::collections::HashMap<String, JsonValue>,
}

impl UpsertInput {
    /// Create a new empty input.
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    /// Add a field value.
    pub fn with_field(mut self, name: impl Into<String>, value: JsonValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    /// Convert to a CreateInput for new documents.
    pub fn to_create_input(&self) -> CreateInput {
        let mut input = CreateInput::new();
        for (name, value) in &self.fields {
            input = input.with_field(name.clone(), value.clone());
        }
        input
    }

    /// Apply as update to an existing document, using schema-aware type coercion when available.
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
            let field_kind = collection.and_then(|c| {
                c.fields
                    .iter()
                    .find(|f| f.name == *field_name)
                    .map(|f| &f.kind)
            });
            let normal_value =
                json_to_normal_value_with_kind_and_time(value, field_kind, Some(utc_now))?;
            doc.set(field_name.clone(), normal_value);
            modified_count += 1;
        }

        Ok(modified_count)
    }
}

impl Default for UpsertInput {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of an upsert operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertAction {
    /// A new document was created
    Created,
    /// An existing document was updated
    Updated,
}

/// UpsertNode performs conditional create-or-update operations.
///
/// Following Go DefraDB's semantics:
/// - If filter matches 0 documents: CREATE with `create_input` fields
/// - If filter matches 1 document: UPDATE with `update_input` fields
/// - If filter matches >1 document: Return error
///
/// # Example
///
/// ```ignore
/// let create_input = UpsertInput::new()
///     .with_field("name", json!("Alice"))
///     .with_field("age", json!(30));
///
/// let update_input = UpsertInput::new()
///     .with_field("age", json!(31));
///
/// let mut node = UpsertNode::new("Users", mutator, mapping)
///     .with_create_input(create_input)
///     .with_update_input(update_input)
///     .with_doc_ids(vec!["bae-123".to_string()]);
///
/// node.init().await?;
/// node.start().await?;
///
/// while node.next().await? {
///     let doc = node.value();
///     println!("Upserted: {:?}", doc.doc_id());
/// }
/// ```
pub struct UpsertNode {
    /// Collection name
    collection_name: String,
    /// Document mutator for storage operations
    mutator: Arc<dyn DocMutator>,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Collection schema for schema-aware type coercion (e.g., DateTime/UTC_NOW)
    collection: Option<Arc<CollectionVersion>>,
    /// Pre-computed request time for UTC_NOW resolution (ensures consistency within a request)
    request_time: Option<DateTime<FixedOffset>>,
    /// Input documents to upsert (for batch operations without docIDs)
    inputs: Vec<UpsertInput>,
    /// Document IDs to upsert (from resolved filter)
    doc_ids: Option<Vec<String>>,
    /// Input for creating new document (Go's 'create' argument)
    create_input: Option<UpsertInput>,
    /// Input for updating existing document (Go's 'update' argument)
    update_input: Option<UpsertInput>,
    /// Single input to apply to all doc_ids (same fields for create and update)
    single_input: Option<UpsertInput>,
    /// Upserted documents (populated after first next())
    upserted_docs: Vec<Doc>,
    /// Actions taken for each document
    actions: Vec<UpsertAction>,
    /// Current position in upserted_docs
    position: usize,
    /// Current document being yielded
    current_doc: Doc,
    /// Whether upserts have been performed yet
    did_upsert: bool,
    /// Whether the node has been initialized
    initialized: bool,
}

impl UpsertNode {
    /// Create a new upsert node for a collection.
    pub fn new(
        collection_name: impl Into<String>,
        mutator: Arc<dyn DocMutator>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            collection_name: collection_name.into(),
            mutator,
            document_mapping,
            collection: None,
            request_time: None,
            inputs: Vec::new(),
            doc_ids: None,
            create_input: None,
            update_input: None,
            single_input: None,
            upserted_docs: Vec::new(),
            actions: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            did_upsert: false,
            initialized: false,
        }
    }

    /// Set the collection schema for schema-aware type coercion.
    pub fn with_collection(mut self, collection: Arc<CollectionVersion>) -> Self {
        self.collection = Some(collection);
        self
    }

    /// Set the pre-computed request time for UTC_NOW resolution.
    pub fn with_request_time(mut self, request_time: DateTime<FixedOffset>) -> Self {
        self.request_time = Some(request_time);
        self
    }

    /// Set the create input (used when no matching document found).
    /// This follows Go DefraDB's 'create' argument semantics.
    pub fn with_create_input(mut self, input: UpsertInput) -> Self {
        self.create_input = Some(input);
        self
    }

    /// Set the update input (used when matching document found).
    /// This follows Go DefraDB's 'update' argument semantics.
    pub fn with_update_input(mut self, input: UpsertInput) -> Self {
        self.update_input = Some(input);
        self
    }

    /// Add a single input to apply to all operations (same fields for create and update).
    /// For Go DefraDB's separate create/update semantics, use with_create_input/with_update_input.
    pub fn with_input(mut self, input: UpsertInput) -> Self {
        self.single_input = Some(input);
        self
    }

    /// Add multiple input documents (each creates new document).
    pub fn with_inputs(mut self, inputs: Vec<UpsertInput>) -> Self {
        self.inputs = inputs;
        self
    }

    /// Set document IDs to upsert (typically from resolved filter).
    pub fn with_doc_ids(mut self, doc_ids: Vec<String>) -> Self {
        self.doc_ids = Some(doc_ids);
        self
    }

    /// Get the number of documents that were upserted.
    pub fn upserted_count(&self) -> usize {
        self.upserted_docs.len()
    }

    /// Get the count of created documents.
    pub fn created_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| **a == UpsertAction::Created)
            .count()
    }

    /// Get the count of updated documents.
    pub fn updated_count(&self) -> usize {
        self.actions
            .iter()
            .filter(|a| **a == UpsertAction::Updated)
            .count()
    }

    /// Upsert a single document by ID with the given input.
    async fn upsert_by_id(
        &mut self,
        doc_id_str: &str,
        input: &UpsertInput,
        utc_now: DateTime<FixedOffset>,
    ) -> Result<()> {
        let doc_id = DocID::from_string(doc_id_str)
            .map_err(|e| QueryError::execution(format!("Invalid DocID '{}': {}", doc_id_str, e)))?;

        // Check if document exists
        let existing = self
            .mutator
            .get_for_update(&self.collection_name, &doc_id)
            .await?;

        if let Some(mut doc) = existing {
            // Document exists - update it
            tracing::debug!(
                collection = %self.collection_name,
                doc_id = %doc_id_str,
                "Upsert: updating existing document"
            );

            let collection_ref = self.collection.as_deref();
            input.apply_to(&mut doc, collection_ref, utc_now)?;

            // Collect the modified field names for block creation
            let modified_fields: std::collections::HashSet<String> =
                input.fields.keys().cloned().collect();

            let result = self
                .mutator
                .update(&self.collection_name, doc, modified_fields)
                .await?;

            let plan_doc = self.result_to_doc(&result.document)?;
            self.upserted_docs.push(plan_doc);
            self.actions.push(UpsertAction::Updated);
        } else {
            // Document does not exist - create it with the provided ID
            tracing::debug!(
                collection = %self.collection_name,
                doc_id = %doc_id_str,
                "Upsert: creating new document with specified ID"
            );

            // Create document with the specified ID, using schema-aware type coercion
            let mut doc = Document::with_id(doc_id);
            if let Some(ref collection) = self.collection {
                doc.set_collection((**collection).clone());
            }
            for (field_name, value) in &input.fields {
                let field_kind = self.collection.as_ref().and_then(|c| {
                    c.fields
                        .iter()
                        .find(|f| f.name == *field_name)
                        .map(|f| &f.kind)
                });
                let crdt_type = self.collection.as_ref().and_then(|c| {
                    c.fields
                        .iter()
                        .find(|f| f.name == *field_name)
                        .map(|f| f.crdt_type)
                }).unwrap_or(CType::LwwRegister);
                let normal_value = json_to_normal_value_with_kind_and_time(value, field_kind, Some(utc_now))?;
                doc.set_with_crdt(field_name.clone(), crdt_type, normal_value)
                    .map_err(|e| {
                        QueryError::execution(format!(
                            "Failed to set field '{}' with CRDT type {:?}: {}",
                            field_name, crdt_type, e
                        ))
                    })?;
            }

            let result = self.mutator.create(&self.collection_name, doc).await?;

            let plan_doc = self.result_to_doc(&result.document)?;
            self.upserted_docs.push(plan_doc);
            self.actions.push(UpsertAction::Created);
        }

        Ok(())
    }

    /// Create a new document (no ID specified - generates new ID).
    async fn create_new(&mut self, input: &UpsertInput) -> Result<()> {
        let create_input = input.to_create_input();
        let utc_now = self.request_time.unwrap_or_else(|| {
            let utc_offset = FixedOffset::east_opt(0).unwrap();
            Utc::now().with_timezone(&utc_offset)
        });
        let doc = if let Some(ref collection) = self.collection {
            create_input.to_document_with_schema_and_time(collection, Some(utc_now))?
        } else {
            create_input.to_document()?
        };

        let result = self.mutator.create(&self.collection_name, doc).await?;

        let plan_doc = self.result_to_doc(&result.document)?;
        self.upserted_docs.push(plan_doc);
        self.actions.push(UpsertAction::Created);

        Ok(())
    }

    /// Convert a Document to a plan Doc using our document mapping.
    fn result_to_doc(&self, document: &Document) -> Result<Doc> {
        let num_fields = self.document_mapping.next_index();
        let mut doc = Doc::new(num_fields);

        // Set document ID at index 0
        if let Some(doc_id) = document.id() {
            doc.set_doc_id(doc_id.to_string());
        }

        // Map each field from the document
        for (field_name, field_value) in document.values() {
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
impl PlanNode for UpsertNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.upserted_docs.clear();
        self.actions.clear();
        self.did_upsert = false;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "UpsertNode.next() called before init()",
            ));
        }

        // On first call, perform all upserts
        if !self.did_upsert {
            // Use pre-computed request time for UTC_NOW consistency, or compute now
            let utc_now = self.request_time.unwrap_or_else(|| {
                let utc_offset = FixedOffset::east_opt(0).unwrap();
                Utc::now().with_timezone(&utc_offset)
            });

            // Go DefraDB upsert semantics (with create_input and update_input)
            if self.create_input.is_some() || self.update_input.is_some() {
                // Clone doc_ids to avoid borrow conflict
                let doc_ids_clone = self.doc_ids.clone();
                match doc_ids_clone {
                    Some(ref doc_ids) if doc_ids.len() > 1 => {
                        // Go returns error when filter matches multiple documents
                        return Err(QueryError::execution(
                            "cannot upsert multiple matching documents",
                        ));
                    }
                    Some(ref doc_ids) if doc_ids.len() == 1 => {
                        // Exactly one match - UPDATE with update_input
                        let update_input = self.update_input.clone().ok_or_else(|| {
                            QueryError::execution(
                                "upsert matched existing document but no 'update' input was provided",
                            )
                        })?;
                        let doc_id = doc_ids[0].clone();
                        self.upsert_by_id(&doc_id, &update_input, utc_now).await?;
                    }
                    _ => {
                        // No matches - CREATE with create_input
                        let create_input = self.create_input.clone().ok_or_else(|| {
                            QueryError::execution(
                                "upsert filter matched no documents but no 'create' input was provided",
                            )
                        })?;
                        self.create_new(&create_input).await?;
                    }
                }
            }
            // Legacy behavior (single_input for both create and update)
            else if let Some(ref doc_ids) = self.doc_ids {
                // Upsert by document IDs
                let input = self.single_input.clone().unwrap_or_else(|| {
                    tracing::warn!(
                        collection = %self.collection_name,
                        doc_id_count = doc_ids.len(),
                        "Upsert called with doc_ids but no input fields - documents will be created/updated with empty data"
                    );
                    UpsertInput::default()
                });
                for doc_id_str in doc_ids.clone() {
                    self.upsert_by_id(&doc_id_str, &input, utc_now).await?;
                }
            } else if !self.inputs.is_empty() {
                // Create new documents from inputs
                for input in self.inputs.clone() {
                    self.create_new(&input).await?;
                }
            } else if let Some(ref input) = self.single_input.clone() {
                // Single input without docID - create new
                self.create_new(input).await?;
            } else {
                return Err(QueryError::execution(
                    "UpsertNode requires either doc_ids with input, or inputs",
                ));
            }

            self.did_upsert = true;
        }

        // Iterate through upserted documents
        if self.position >= self.upserted_docs.len() {
            return Ok(false);
        }

        self.current_doc = self.upserted_docs[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.upserted_docs.clear();
        self.actions.clear();
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // UpsertNode is a leaf node
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "upsertNode"
    }
}
