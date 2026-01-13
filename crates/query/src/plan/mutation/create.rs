//! CreateNode for creating new documents

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::planner::{Doc, PlanNode};

/// Input for a create mutation
#[derive(Debug, Clone)]
pub struct CreateInput {
    /// Field values keyed by field name
    pub fields: std::collections::HashMap<String, JsonValue>,
}

impl CreateInput {
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    pub fn with_field(mut self, name: impl Into<String>, value: JsonValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }
}

impl Default for CreateInput {
    fn default() -> Self {
        Self::new()
    }
}

/// CreateNode creates new documents in a collection.
///
/// This node takes input values and creates a new document with:
/// - A generated document ID
/// - Field values from the input
/// - CRDT initialization for each field
pub struct CreateNode {
    /// Collection schema
    collection: CollectionVersion,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Input documents to create
    inputs: Vec<CreateInput>,
    /// Current position in inputs
    position: usize,
    /// Current document (the created document)
    current_doc: Doc,
    /// Whether the node has been initialized
    initialized: bool,
    /// Generated document IDs (for testing/inspection)
    generated_doc_ids: Vec<String>,
}

impl CreateNode {
    /// Create a new create node for a collection
    pub fn new(collection: CollectionVersion, document_mapping: DocumentMapping) -> Self {
        Self {
            collection,
            document_mapping,
            inputs: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            initialized: false,
            generated_doc_ids: Vec::new(),
        }
    }

    /// Add an input to create
    pub fn with_input(mut self, input: CreateInput) -> Self {
        self.inputs.push(input);
        self
    }

    /// Add multiple inputs
    pub fn with_inputs(mut self, inputs: Vec<CreateInput>) -> Self {
        self.inputs = inputs;
        self
    }

    /// Get the generated document IDs after execution
    pub fn generated_doc_ids(&self) -> &[String] {
        &self.generated_doc_ids
    }

    /// Generate a deterministic document ID based on collection and index
    fn generate_doc_id(&self, index: usize) -> String {
        format!("bae-{}-{}", self.collection.collection_id, index)
    }

    /// Create a document from input
    fn create_document(&self, input: &CreateInput, doc_id: &str) -> Result<Doc> {
        let num_fields = self.document_mapping.next_index();
        let mut doc = Doc::new(num_fields);

        // Set document ID
        doc.set_doc_id(doc_id);

        // Set field values from input
        for (field_name, value) in &input.fields {
            if let Some(index) = self.document_mapping.first_index_of_name(field_name) {
                doc.set(index, value.clone());
            } else {
                return Err(QueryError::unknown_field(field_name.clone()));
            }
        }

        Ok(doc)
    }
}

#[async_trait]
impl PlanNode for CreateNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.generated_doc_ids.clear();
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if self.position >= self.inputs.len() {
            return Ok(false);
        }

        let input = &self.inputs[self.position];
        let doc_id = self.generate_doc_id(self.position);

        // Create the document
        self.current_doc = self.create_document(input, &doc_id)?;

        self.generated_doc_ids.push(doc_id);
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // CreateNode is a leaf node (generates data)
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "createNode"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{FieldDescription, FieldKind};
    use serde_json::json;

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
    }

    fn make_test_mapping() -> DocumentMapping {
        let mut m = DocumentMapping::new();
        m.add(0, "_docID");
        m.add(1, "name");
        m.add(2, "age");
        m
    }

    #[tokio::test]
    async fn test_create_single_document() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let input = CreateInput::new()
            .with_field("name", json!("Alice"))
            .with_field("age", json!(30));

        let mut create = CreateNode::new(collection, mapping).with_input(input);

        create.init().await.unwrap();
        create.start().await.unwrap();

        assert!(create.next().await.unwrap());

        let doc = create.value();
        assert!(doc.doc_id().unwrap().starts_with("bae-"));
        assert_eq!(doc.get(1), Some(&json!("Alice")));
        assert_eq!(doc.get(2), Some(&json!(30)));

        assert!(!create.next().await.unwrap()); // No more documents
    }

    #[tokio::test]
    async fn test_create_multiple_documents() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let inputs = vec![
            CreateInput::new()
                .with_field("name", json!("Alice"))
                .with_field("age", json!(30)),
            CreateInput::new()
                .with_field("name", json!("Bob"))
                .with_field("age", json!(25)),
        ];

        let mut create = CreateNode::new(collection, mapping).with_inputs(inputs);

        create.init().await.unwrap();
        create.start().await.unwrap();

        let mut results = Vec::new();
        while create.next().await.unwrap() {
            results.push((
                create.value().doc_id().map(String::from),
                create.value().get(1).cloned(),
            ));
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, Some(json!("Alice")));
        assert_eq!(results[1].1, Some(json!("Bob")));

        // Check that document IDs were generated
        assert_eq!(create.generated_doc_ids().len(), 2);
    }

    #[tokio::test]
    async fn test_create_unknown_field_error() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let input = CreateInput::new().with_field("unknown_field", json!("value"));

        let mut create = CreateNode::new(collection, mapping).with_input(input);

        create.init().await.unwrap();
        create.start().await.unwrap();

        let result = create.next().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::UnknownField(_)));
    }

    #[tokio::test]
    async fn test_create_with_no_inputs() {
        let collection = make_test_collection();
        let mapping = make_test_mapping();

        let mut create = CreateNode::new(collection, mapping);
        // No inputs added

        create.init().await.unwrap();
        create.start().await.unwrap();

        // Should return false immediately with no inputs
        assert!(!create.next().await.unwrap());
        assert!(create.generated_doc_ids().is_empty());
    }
}
