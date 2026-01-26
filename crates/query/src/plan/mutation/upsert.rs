//! UpsertNode for conditional create-or-update operations
//!
//! This node implements upsert semantics following Go DefraDB's behavior:
//! - If filter matches 0 documents: CREATE with `create` fields
//! - If filter matches 1 document: UPDATE with `update` fields
//! - If filter matches >1 document: Return error

use std::sync::Arc;

use async_trait::async_trait;
use document::{DocID, Document};
use serde_json::Value as JsonValue;
use tracing;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mutator::DocMutator;
use crate::planner::{Doc, PlanNode};

use super::create::{json_to_normal_value, normal_value_to_json, CreateInput};

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

    /// Apply as update to an existing document.
    pub fn apply_to(&self, doc: &mut Document) -> Result<usize> {
        let mut modified_count = 0;

        for (field_name, value) in &self.fields {
            let normal_value = json_to_normal_value(value)?;
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
    async fn upsert_by_id(&mut self, doc_id_str: &str, input: &UpsertInput) -> Result<()> {
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

            input.apply_to(&mut doc)?;
            let result = self.mutator.update(&self.collection_name, doc).await?;

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

            // Create document with the specified ID
            let mut doc = Document::with_id(doc_id);
            for (field_name, value) in &input.fields {
                let normal_value = json_to_normal_value(value)?;
                doc.set(field_name.clone(), normal_value);
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
        let doc = create_input.to_document()?;

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

#[async_trait]
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
                        self.upsert_by_id(&doc_id, &update_input).await?;
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
                    self.upsert_by_id(&doc_id_str, &input).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutator::{CreateResult, DeleteResult, UpdateResult};
    use serde_json::json;
    use std::sync::Mutex;

    struct MockMutator {
        docs: Mutex<std::collections::HashMap<String, Document>>,
    }

    impl MockMutator {
        fn new() -> Self {
            Self {
                docs: Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn add_doc(&self, doc: Document) {
            if let Some(id) = doc.id() {
                self.docs.lock().unwrap().insert(id.to_string(), doc);
            }
        }

        fn doc_count(&self) -> usize {
            self.docs.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl DocMutator for MockMutator {
        async fn create(&self, _collection_name: &str, mut doc: Document) -> Result<CreateResult> {
            if doc.id().is_none() {
                doc.generate_and_set_doc_id().map_err(|e| {
                    QueryError::execution(format!("Failed to generate DocID: {}", e))
                })?;
            }

            let doc_id = doc
                .id()
                .cloned()
                .ok_or_else(|| QueryError::execution("Document should have ID"))?;

            self.docs
                .lock()
                .unwrap()
                .insert(doc_id.to_string(), doc.clone());

            Ok(CreateResult::new(doc_id, doc))
        }

        async fn update(&self, _collection_name: &str, doc: Document) -> Result<UpdateResult> {
            let doc_id = doc
                .id()
                .ok_or_else(|| QueryError::execution("Document must have ID for update"))?;

            let modified = doc.values().len();

            self.docs
                .lock()
                .unwrap()
                .insert(doc_id.to_string(), doc.clone());

            Ok(UpdateResult::new(doc, modified))
        }

        async fn delete(&self, _collection_name: &str, doc_id: &DocID) -> Result<DeleteResult> {
            let existed = self
                .docs
                .lock()
                .unwrap()
                .remove(&doc_id.to_string())
                .is_some();
            Ok(DeleteResult::new(doc_id.clone(), existed))
        }

        async fn exists(&self, _collection_name: &str, doc_id: &DocID) -> Result<bool> {
            Ok(self.docs.lock().unwrap().contains_key(&doc_id.to_string()))
        }

        async fn get_for_update(
            &self,
            _collection_name: &str,
            doc_id: &DocID,
        ) -> Result<Option<Document>> {
            Ok(self.docs.lock().unwrap().get(&doc_id.to_string()).cloned())
        }
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "email");
        m
    }

    fn create_test_doc(name: &str, email: &str) -> Document {
        let mut doc = Document::new();
        doc.set("name", name.to_string());
        doc.set("email", email.to_string());
        doc.generate_and_set_doc_id().unwrap();
        doc
    }

    #[tokio::test]
    async fn test_upsert_creates_when_not_exists() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        // Create a valid DocID that doesn't exist
        let mut template = Document::new();
        template.set("name", "Test".to_string());
        template.generate_and_set_doc_id().unwrap();
        let new_doc_id = template.id().unwrap().to_string();

        let input = UpsertInput::new()
            .with_field("name", json!("Alice"))
            .with_field("email", json!("alice@example.com"));

        let mut node = UpsertNode::new("Users", mutator.clone(), mapping)
            .with_doc_ids(vec![new_doc_id])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        assert!(node.next().await.unwrap());
        assert!(!node.next().await.unwrap());

        assert_eq!(node.upserted_count(), 1);
        assert_eq!(node.created_count(), 1);
        assert_eq!(node.updated_count(), 0);
        assert_eq!(mutator.doc_count(), 1);
    }

    #[tokio::test]
    async fn test_upsert_updates_when_exists() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        // Create existing document
        let existing_doc = create_test_doc("Alice", "alice@old.com");
        let doc_id = existing_doc.id().unwrap().to_string();
        mutator.add_doc(existing_doc);

        let input = UpsertInput::new().with_field("email", json!("alice@new.com"));

        let mut node = UpsertNode::new("Users", mutator.clone(), mapping)
            .with_doc_ids(vec![doc_id.clone()])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        assert!(node.next().await.unwrap());
        let doc = node.value();
        assert_eq!(doc.get(2), Some(&json!("alice@new.com")));

        assert!(!node.next().await.unwrap());

        assert_eq!(node.upserted_count(), 1);
        assert_eq!(node.created_count(), 0);
        assert_eq!(node.updated_count(), 1);
    }

    #[tokio::test]
    async fn test_upsert_mixed_create_and_update() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        // Create one existing document
        let existing_doc = create_test_doc("Alice", "alice@old.com");
        let existing_id = existing_doc.id().unwrap().to_string();
        mutator.add_doc(existing_doc);

        // Create a new DocID that doesn't exist
        let mut template = Document::new();
        template.set("name", "New".to_string());
        template.generate_and_set_doc_id().unwrap();
        let new_id = template.id().unwrap().to_string();

        let input = UpsertInput::new()
            .with_field("name", json!("Updated"))
            .with_field("email", json!("updated@example.com"));

        let mut node = UpsertNode::new("Users", mutator.clone(), mapping)
            .with_doc_ids(vec![existing_id, new_id])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let mut count = 0;
        while node.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 2);
        assert_eq!(node.upserted_count(), 2);
        assert_eq!(node.created_count(), 1);
        assert_eq!(node.updated_count(), 1);
        assert_eq!(mutator.doc_count(), 2);
    }

    #[tokio::test]
    async fn test_upsert_create_without_doc_id() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let input = UpsertInput::new()
            .with_field("name", json!("NewUser"))
            .with_field("email", json!("new@example.com"));

        let mut node = UpsertNode::new("Users", mutator.clone(), mapping).with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        assert!(node.next().await.unwrap());
        assert!(!node.next().await.unwrap());

        assert_eq!(node.created_count(), 1);
        assert_eq!(mutator.doc_count(), 1);
    }

    #[tokio::test]
    async fn test_upsert_next_before_init_errors() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let mut node = UpsertNode::new("Users", mutator, mapping);

        let result = node.next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_upsert_invalid_doc_id_returns_error() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let input = UpsertInput::new().with_field("name", json!("Alice"));

        // Use an invalid DocID format
        let mut node = UpsertNode::new("Users", mutator, mapping)
            .with_doc_ids(vec!["not-a-valid-docid".to_string()])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let result = node.next().await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid DocID") || err_msg.contains("invalid"));
    }

    #[tokio::test]
    async fn test_upsert_with_batch_inputs() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let inputs = vec![
            UpsertInput::new()
                .with_field("name", json!("User1"))
                .with_field("email", json!("user1@example.com")),
            UpsertInput::new()
                .with_field("name", json!("User2"))
                .with_field("email", json!("user2@example.com")),
            UpsertInput::new()
                .with_field("name", json!("User3"))
                .with_field("email", json!("user3@example.com")),
        ];

        let mut node = UpsertNode::new("Users", mutator.clone(), mapping).with_inputs(inputs);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let mut count = 0;
        while node.next().await.unwrap() {
            count += 1;
        }

        assert_eq!(count, 3);
        assert_eq!(node.created_count(), 3);
        assert_eq!(mutator.doc_count(), 3);
    }

    #[tokio::test]
    async fn test_upsert_empty_doc_ids_does_nothing() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let input = UpsertInput::new().with_field("name", json!("Alice"));

        // Empty doc_ids list
        let mut node = UpsertNode::new("Users", mutator.clone(), mapping)
            .with_doc_ids(vec![])
            .with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        // No documents should be created
        assert!(!node.next().await.unwrap());
        assert_eq!(node.upserted_count(), 0);
        assert_eq!(mutator.doc_count(), 0);
    }

    #[tokio::test]
    async fn test_upsert_reinit_clears_state() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let input = UpsertInput::new().with_field("name", json!("Alice"));

        let mut node = UpsertNode::new("Users", mutator.clone(), mapping).with_input(input);

        // First run
        node.init().await.unwrap();
        node.start().await.unwrap();
        assert!(node.next().await.unwrap());
        assert!(!node.next().await.unwrap());
        assert_eq!(node.created_count(), 1);

        // Close and reinit
        node.close().await.unwrap();
        node.init().await.unwrap();
        node.start().await.unwrap();

        // Should be able to run again (created_count resets after init)
        assert!(node.next().await.unwrap());
        assert!(!node.next().await.unwrap());
        assert_eq!(node.created_count(), 1); // Second batch count

        // Note: Since same input creates same DocID (deterministic),
        // the second create overwrites the first in storage
        // Total unique docs is 1, not 2
        assert_eq!(mutator.doc_count(), 1);
    }
}
