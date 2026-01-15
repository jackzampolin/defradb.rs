//! CreateNode for creating new documents
//!
//! This node creates documents in storage during query execution, following
//! the Go DefraDB pattern where persistence happens within the plan node.

use std::sync::Arc;

use async_trait::async_trait;
use document::Document;
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mutator::{CreateResult, DocMutator};
use crate::planner::{Doc, PlanNode};

/// Input for a create mutation - field values to set on the new document.
#[derive(Debug, Clone)]
pub struct CreateInput {
    /// Field values keyed by field name
    pub fields: std::collections::HashMap<String, JsonValue>,
}

impl CreateInput {
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

    /// Convert to a Document for storage.
    pub fn to_document(&self) -> Result<Document> {
        let mut doc = Document::new();

        for (field_name, value) in &self.fields {
            // Convert JsonValue to appropriate NormalValue
            let normal_value = json_to_normal_value(value)?;
            doc.set(field_name.clone(), normal_value);
        }

        Ok(doc)
    }
}

impl Default for CreateInput {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a JSON value to a document NormalValue.
pub fn json_to_normal_value(value: &JsonValue) -> Result<document::NormalValue> {
    use document::NormalValue;

    match value {
        JsonValue::Null => Ok(NormalValue::Null),
        JsonValue::Bool(b) => Ok(NormalValue::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(NormalValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(NormalValue::Float64(f))
            } else {
                Err(QueryError::execution("Invalid number value"))
            }
        }
        JsonValue::String(s) => Ok(NormalValue::String(s.clone())),
        JsonValue::Array(arr) => {
            // Determine array type from first non-null element
            let first_non_null = arr.iter().find(|v| !v.is_null());

            match first_non_null {
                Some(JsonValue::Bool(_)) => {
                    let bools: Vec<bool> = arr
                        .iter()
                        .map(|v| v.as_bool().unwrap_or(false))
                        .collect();
                    Ok(NormalValue::BoolArray(bools))
                }
                Some(JsonValue::Number(n)) if n.is_i64() => {
                    let ints: Vec<i64> = arr.iter().map(|v| v.as_i64().unwrap_or(0)).collect();
                    Ok(NormalValue::IntArray(ints))
                }
                Some(JsonValue::Number(_)) => {
                    let floats: Vec<f64> = arr.iter().map(|v| v.as_f64().unwrap_or(0.0)).collect();
                    Ok(NormalValue::Float64Array(floats))
                }
                Some(JsonValue::String(_)) => {
                    let strings: Vec<String> = arr
                        .iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect();
                    Ok(NormalValue::StringArray(strings))
                }
                _ => {
                    // Empty array or mixed types - default to string array
                    let strings: Vec<String> = arr
                        .iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect();
                    Ok(NormalValue::StringArray(strings))
                }
            }
        }
        JsonValue::Object(_) => {
            // Nested objects could be sub-documents - for now, store as JSON
            Ok(NormalValue::Json(value.clone()))
        }
    }
}

/// CreateNode creates new documents in a collection.
///
/// This node implements the Volcano iterator model, yielding created documents
/// one at a time. On the first call to `next()`, all documents are created in
/// storage via the `DocMutator`. Subsequent calls iterate through the results.
///
/// # Example
///
/// ```ignore
/// let input = CreateInput::new()
///     .with_field("name", json!("Alice"))
///     .with_field("age", json!(30));
///
/// let mut node = CreateNode::new("Users", mutator, mapping)
///     .with_input(input);
///
/// node.init().await?;
/// node.start().await?;
///
/// while node.next().await? {
///     let created_doc = node.value();
///     println!("Created: {:?}", created_doc.doc_id());
/// }
/// ```
pub struct CreateNode {
    /// Collection name to create documents in
    collection_name: String,
    /// Document mutator for storage operations
    mutator: Arc<dyn DocMutator>,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Input documents to create
    inputs: Vec<CreateInput>,
    /// Created documents (populated after first next())
    created_docs: Vec<Doc>,
    /// Current position in created_docs
    position: usize,
    /// Current document being yielded
    current_doc: Doc,
    /// Whether documents have been created yet
    did_create: bool,
    /// Whether the node has been initialized
    initialized: bool,
}

impl CreateNode {
    /// Create a new create node for a collection.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection to create documents in
    /// * `mutator` - Document mutator for storage operations
    /// * `document_mapping` - Field mapping for result documents
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
            created_docs: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            did_create: false,
            initialized: false,
        }
    }

    /// Add an input document to create.
    pub fn with_input(mut self, input: CreateInput) -> Self {
        self.inputs.push(input);
        self
    }

    /// Add multiple input documents.
    pub fn with_inputs(mut self, inputs: Vec<CreateInput>) -> Self {
        self.inputs = inputs;
        self
    }

    /// Get the number of documents that were created.
    pub fn created_count(&self) -> usize {
        self.created_docs.len()
    }

    /// Convert a CreateResult to a plan Doc using our document mapping.
    fn create_result_to_doc(&self, result: &CreateResult) -> Result<Doc> {
        let num_fields = self.document_mapping.next_index();
        let mut doc = Doc::new(num_fields);

        // Set document ID at index 0
        doc.set_doc_id(&result.doc_id.to_string());

        // Map each field from the created document
        for (field_name, field_value) in result.document.values() {
            if let Some(index) = self.document_mapping.first_index_of_name(field_name) {
                // Convert NormalValue back to JsonValue for the plan Doc
                let json_value = normal_value_to_json(field_value.value());
                doc.set(index, json_value);
            }
        }

        Ok(doc)
    }
}

/// Convert a NormalValue to JsonValue for plan Doc storage.
pub fn normal_value_to_json(value: &document::NormalValue) -> JsonValue {
    use document::NormalValue;

    match value {
        NormalValue::Null => JsonValue::Null,
        NormalValue::Bool(b) => JsonValue::Bool(*b),
        NormalValue::Int(i) => JsonValue::Number((*i).into()),
        NormalValue::Float64(f) => {
            serde_json::Number::from_f64(*f)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        NormalValue::Float32(f) => {
            serde_json::Number::from_f64(*f as f64)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null)
        }
        NormalValue::String(s) => JsonValue::String(s.clone()),
        NormalValue::Bytes(b) => {
            // Store bytes as JSON array of numbers
            JsonValue::Array(b.iter().map(|byte| JsonValue::Number((*byte).into())).collect())
        }
        NormalValue::Json(j) => j.clone(),
        // Arrays
        NormalValue::BoolArray(arr) => {
            JsonValue::Array(arr.iter().map(|b| JsonValue::Bool(*b)).collect())
        }
        NormalValue::IntArray(arr) => {
            JsonValue::Array(arr.iter().map(|i| JsonValue::Number((*i).into())).collect())
        }
        NormalValue::Float64Array(arr) => JsonValue::Array(
            arr.iter()
                .filter_map(|f| serde_json::Number::from_f64(*f).map(JsonValue::Number))
                .collect(),
        ),
        NormalValue::StringArray(arr) => {
            JsonValue::Array(arr.iter().map(|s| JsonValue::String(s.clone())).collect())
        }
        // For other complex types, convert to JSON representation
        _ => JsonValue::Null,
    }
}

#[async_trait]
impl PlanNode for CreateNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.created_docs.clear();
        self.did_create = false;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "CreateNode.next() called before init()",
            ));
        }

        // On first call, create all documents
        if !self.did_create {
            for input in &self.inputs {
                // Convert input to Document
                let doc = input.to_document()?;

                // Create in storage (generates DocID)
                let result = self.mutator.create(&self.collection_name, doc).await?;

                // Convert result to plan Doc
                let plan_doc = self.create_result_to_doc(&result)?;
                self.created_docs.push(plan_doc);
            }
            self.did_create = true;
        }

        // Iterate through created documents
        if self.position >= self.created_docs.len() {
            return Ok(false);
        }

        self.current_doc = self.created_docs[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.created_docs.clear();
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
    use serde_json::json;
    use std::sync::Mutex;

    /// Mock mutator for testing
    struct MockMutator {
        created: Mutex<Vec<(String, Document)>>,
        next_doc_id: Mutex<u32>,
    }

    impl MockMutator {
        fn new() -> Self {
            Self {
                created: Mutex::new(Vec::new()),
                next_doc_id: Mutex::new(0),
            }
        }

        fn created_docs(&self) -> Vec<(String, Document)> {
            self.created.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl DocMutator for MockMutator {
        async fn create(&self, collection_name: &str, mut doc: Document) -> Result<CreateResult> {
            // Generate a mock DocID
            let mut id = self.next_doc_id.lock().unwrap();
            *id += 1;

            // Create a deterministic DocID by generating and setting it
            doc.generate_and_set_doc_id()
                .map_err(|e| QueryError::execution(format!("Failed to generate DocID: {}", e)))?;

            let doc_id = doc.id().cloned().ok_or_else(|| {
                QueryError::execution("Document should have ID after generation")
            })?;

            // Store for verification
            self.created
                .lock()
                .unwrap()
                .push((collection_name.to_string(), doc.clone()));

            Ok(CreateResult::new(doc_id, doc))
        }

        async fn update(
            &self,
            _collection_name: &str,
            _doc: Document,
        ) -> Result<crate::mutator::UpdateResult> {
            unimplemented!("Not needed for CreateNode tests")
        }

        async fn delete(
            &self,
            _collection_name: &str,
            _doc_id: &document::DocID,
        ) -> Result<crate::mutator::DeleteResult> {
            unimplemented!("Not needed for CreateNode tests")
        }

        async fn exists(
            &self,
            _collection_name: &str,
            _doc_id: &document::DocID,
        ) -> Result<bool> {
            Ok(false)
        }

        async fn get_for_update(
            &self,
            _collection_name: &str,
            _doc_id: &document::DocID,
        ) -> Result<Option<Document>> {
            Ok(None)
        }
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
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let input = CreateInput::new()
            .with_field("name", json!("Alice"))
            .with_field("age", json!(30));

        let mut node =
            CreateNode::new("Users", mutator.clone(), mapping).with_input(input);

        node.init().await.unwrap();
        node.start().await.unwrap();

        assert!(node.next().await.unwrap());

        let doc = node.value();
        assert!(doc.doc_id().is_some());
        assert_eq!(doc.get(1), Some(&json!("Alice")));
        assert_eq!(doc.get(2), Some(&json!(30)));

        assert!(!node.next().await.unwrap()); // No more documents

        // Verify the document was created in storage
        let created = mutator.created_docs();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].0, "Users");
    }

    #[tokio::test]
    async fn test_create_multiple_documents() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let inputs = vec![
            CreateInput::new()
                .with_field("name", json!("Alice"))
                .with_field("age", json!(30)),
            CreateInput::new()
                .with_field("name", json!("Bob"))
                .with_field("age", json!(25)),
        ];

        let mut node =
            CreateNode::new("Users", mutator.clone(), mapping).with_inputs(inputs);

        node.init().await.unwrap();
        node.start().await.unwrap();

        let mut results = Vec::new();
        while node.next().await.unwrap() {
            results.push((
                node.value().doc_id().map(String::from),
                node.value().get(1).cloned(),
            ));
        }

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, Some(json!("Alice")));
        assert_eq!(results[1].1, Some(json!("Bob")));

        // All should have unique DocIDs
        assert!(results[0].0.is_some());
        assert!(results[1].0.is_some());
        assert_ne!(results[0].0, results[1].0);

        // Verify storage
        let created = mutator.created_docs();
        assert_eq!(created.len(), 2);
    }

    #[tokio::test]
    async fn test_create_with_no_inputs() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let mut node = CreateNode::new("Users", mutator.clone(), mapping);

        node.init().await.unwrap();
        node.start().await.unwrap();

        // Should return false immediately with no inputs
        assert!(!node.next().await.unwrap());
        assert_eq!(node.created_count(), 0);

        // Nothing should have been created
        assert!(mutator.created_docs().is_empty());
    }

    #[tokio::test]
    async fn test_create_next_before_init_errors() {
        let mutator = Arc::new(MockMutator::new());
        let mapping = make_test_mapping();

        let mut node = CreateNode::new("Users", mutator, mapping);

        let result = node.next().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_input_to_document() {
        let input = CreateInput::new()
            .with_field("name", json!("Alice"))
            .with_field("age", json!(30))
            .with_field("active", json!(true));

        let doc = input.to_document().unwrap();

        assert_eq!(doc.get("name").unwrap().as_str(), Some("Alice"));
        assert_eq!(doc.get("age").unwrap().as_int(), Some(30));
        assert_eq!(doc.get("active").unwrap().as_bool(), Some(true));
    }

    #[tokio::test]
    async fn test_create_input_with_arrays() {
        let input = CreateInput::new()
            .with_field("tags", json!(["rust", "database"]))
            .with_field("scores", json!([85, 90, 95]));

        let doc = input.to_document().unwrap();

        // Tags should be a string array
        let tags = doc.get("tags").unwrap();
        assert!(matches!(tags, document::NormalValue::StringArray(_)));

        // Scores should be an int array
        let scores = doc.get("scores").unwrap();
        assert!(matches!(scores, document::NormalValue::IntArray(_)));
    }
}
