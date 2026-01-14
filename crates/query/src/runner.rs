//! Query runner - executes queries against storage
//!
//! This module provides the QueryRunner which bridges the query planner
//! with the storage layer, executing queries and returning JSON results.

use async_trait::async_trait;
use document::Document;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::executor::{QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
use crate::json_convert::normal_value_to_json;
use crate::mapper::{Requestable, Select};
use crate::plan::{LimitNode, ScanNode, SelectNode};
use crate::planner::{Doc, PlanNode};
use crate::query_parse::parse_query;

/// Storage abstraction for fetching documents.
#[async_trait]
pub trait DocFetcher: Send + Sync {
    /// Get all documents from a collection.
    async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>>;

    /// Get documents by their IDs.
    async fn get_by_ids(&self, collection_name: &str, doc_ids: &[String]) -> Result<Vec<Document>>;
}

/// Query runner that executes GraphQL queries against storage.
pub struct QueryRunner<F: DocFetcher> {
    /// Document fetcher for storage access
    fetcher: F,
    /// Collection schemas by name
    collections: HashMap<String, Arc<CollectionVersion>>,
}

impl<F: DocFetcher> QueryRunner<F> {
    /// Create a new query runner with the given fetcher and collections.
    pub fn new(fetcher: F, collections: Vec<CollectionVersion>) -> Self {
        let collections_map = collections
            .iter()
            .map(|c| (c.name.clone(), Arc::new(c.clone())))
            .collect();
        Self {
            fetcher,
            collections: collections_map,
        }
    }

    /// Execute a GraphQL query and return JSON results.
    pub async fn execute_query(&self, query: &str) -> Result<JsonValue> {
        let selects = parse_query(query)?;

        let mut results = Map::new();

        for select in selects {
            let result = self.execute_select(&select).await?;
            let key = select.field.output_name();
            results.insert(key.to_string(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute a single Select operation.
    async fn execute_select(&self, select: &Select) -> Result<JsonValue> {
        // Get collection schema
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Validate unsupported features
        self.validate_select(select)?;

        // Fetch documents from storage
        let docs = if let Some(ref doc_ids) = select.doc_ids {
            self.fetcher
                .get_by_ids(&select.collection_name, doc_ids)
                .await?
        } else {
            self.fetcher.get_all(&select.collection_name).await?
        };

        // Build document mapping
        let mapping = self.build_mapping(select, collection)?;

        // Convert storage documents to plan docs
        let plan_docs = self.convert_documents(&docs, &mapping)?;

        // Build and execute the plan
        let mut plan = self.build_plan(select, plan_docs, mapping.clone())?;

        // Execute the plan and collect results
        plan.init().await?;
        plan.start().await?;

        let mut results = Vec::new();

        while plan.next().await? {
            let doc = plan.value();
            let json = self.doc_to_json(doc, &mapping)?;
            results.push(json);
        }

        plan.close().await?;

        Ok(JsonValue::Array(results))
    }

    /// Validate that the select doesn't use unsupported features.
    fn validate_select(&self, select: &Select) -> Result<()> {
        if select.order_by.is_some() {
            return Err(QueryError::execution(
                "ordering is not yet implemented; remove the 'order' argument",
            ));
        }
        if select.group_by.is_some() {
            return Err(QueryError::execution(
                "grouping is not yet implemented; remove the 'groupBy' argument",
            ));
        }
        if select.cid.is_some() {
            return Err(QueryError::execution(
                "CID-based queries are not yet implemented; remove the 'cid' argument",
            ));
        }

        // Check for nested selections (relations)
        for field in &select.fields {
            if let Requestable::Select(nested) = field {
                return Err(QueryError::execution(format!(
                    "nested selections (relations) are not yet implemented; \
                     remove the nested '{}' selection",
                    nested.collection_name
                )));
            }
        }

        Ok(())
    }

    /// Build the document mapping for a select operation.
    fn build_mapping(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Add requested fields
        for field in select.requested_fields() {
            let index = mapping.next_index();
            mapping.add(index, &field.name);
            mapping.add_render_key(index, field.output_name());
        }

        // If no fields specified, add all from collection
        if mapping.next_index() == 0 {
            for (i, field) in collection.fields.iter().enumerate() {
                mapping.add(i, &field.name);
                mapping.add_render_key(i, &field.name);
            }
        }

        Ok(mapping)
    }

    /// Convert storage Documents to plan Docs.
    fn convert_documents(&self, docs: &[Document], mapping: &DocumentMapping) -> Result<Vec<Doc>> {
        let mut result = Vec::with_capacity(docs.len());

        for doc in docs {
            let plan_doc = self.document_to_plan_doc(doc, mapping)?;
            result.push(plan_doc);
        }

        Ok(result)
    }

    /// Convert a single storage Document to a plan Doc.
    fn document_to_plan_doc(&self, doc: &Document, mapping: &DocumentMapping) -> Result<Doc> {
        let num_fields = mapping.next_index();
        let mut fields: Vec<Option<JsonValue>> = vec![None; num_fields];

        // Set _docID if present in mapping
        if let Some(index) = mapping.first_index_of_name("_docID") {
            if let Some(doc_id) = doc.id() {
                fields[index] = Some(JsonValue::String(doc_id.to_string()));
            }
        }

        // Set other fields
        for field_name in doc.field_names() {
            if let Some(index) = mapping.first_index_of_name(field_name) {
                if let Some(value) = doc.get(field_name) {
                    let json = normal_value_to_json(value)?;
                    fields[index] = Some(json);
                }
            }
        }

        Ok(Doc::with_fields(fields))
    }

    /// Build a plan tree from a Select operation and documents.
    fn build_plan(
        &self,
        select: &Select,
        docs: Vec<Doc>,
        mapping: DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Create ScanNode with preloaded documents
        let scan = ScanNode::new((**collection).clone(), mapping.clone())
            .with_docs(docs)
            .with_show_deleted(select.show_deleted);

        let mut plan: Box<dyn PlanNode> = Box::new(scan);

        // Add SelectNode for filtering
        if let Some(ref filter) = select.filter {
            let select_node = SelectNode::new(plan, mapping.clone()).with_filter(filter.clone());
            plan = Box::new(select_node);
        }

        // Add LimitNode if needed
        if let Some(ref limit) = select.limit {
            plan = Box::new(LimitNode::new(plan, limit.limit, limit.offset));
        }

        Ok(plan)
    }

    /// Convert a plan Doc to JSON for output.
    fn doc_to_json(&self, doc: &Doc, mapping: &DocumentMapping) -> Result<JsonValue> {
        let mut obj = Map::new();

        for render_key in &mapping.render_keys {
            let value = doc
                .fields()
                .get(render_key.index)
                .cloned()
                .flatten()
                .unwrap_or(JsonValue::Null);
            obj.insert(render_key.key.clone(), value);
        }

        Ok(JsonValue::Object(obj))
    }
}

#[async_trait]
impl<F: DocFetcher> QueryExecutor for QueryRunner<F> {
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        match self.execute_query(&request.query).await {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
            },
            Err(e) => QueryResponse {
                data: None,
                errors: vec![QueryResponseError {
                    message: e.to_string(),
                    path: None,
                    locations: None,
                }],
            },
        }
    }

    async fn execute_in_txn(&self, _request: QueryRequest, txn_id: &str) -> QueryResponse {
        QueryResponse::error(format!(
            "execute_in_txn is not yet implemented: transaction '{}' context cannot be used",
            txn_id
        ))
    }

    async fn schema(&self) -> Result<String> {
        let mut schema_str = String::new();
        for collection in self.collections.values() {
            schema_str.push_str(&format!("type {} {{\n", collection.name));
            for field in &collection.fields {
                let gql_type = field.kind.graphql_type_name();
                schema_str.push_str(&format!("  {}: {}\n", field.name, gql_type));
            }
            schema_str.push_str("}\n\n");
        }
        Ok(schema_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{FieldDescription, FieldKind};
    use std::sync::Mutex;

    /// Mock fetcher for testing
    struct MockFetcher {
        docs: Mutex<HashMap<String, Vec<Document>>>,
    }

    impl MockFetcher {
        fn new() -> Self {
            Self {
                docs: Mutex::new(HashMap::new()),
            }
        }

        fn add_doc(&self, collection: &str, doc: Document) {
            let mut docs = self.docs.lock().unwrap();
            docs.entry(collection.to_string()).or_default().push(doc);
        }
    }

    #[async_trait]
    impl DocFetcher for MockFetcher {
        async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>> {
            let docs = self.docs.lock().unwrap();
            Ok(docs.get(collection_name).cloned().unwrap_or_default())
        }

        async fn get_by_ids(
            &self,
            collection_name: &str,
            doc_ids: &[String],
        ) -> Result<Vec<Document>> {
            let docs = self.docs.lock().unwrap();
            let all = docs.get(collection_name).cloned().unwrap_or_default();
            let filtered: Vec<_> = all
                .into_iter()
                .filter(|d| {
                    d.id()
                        .map(|id| doc_ids.contains(&id.to_string()))
                        .unwrap_or(false)
                })
                .collect();
            Ok(filtered)
        }
    }

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "Users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
    }

    #[tokio::test]
    async fn test_execute_simple_query() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { name age } }")
            .await
            .unwrap();

        assert!(result.is_object());
        let users = result.get("Users").unwrap();
        assert!(users.is_array());
        let arr = users.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].get("name").unwrap(), "Alice");
        assert_eq!(arr[0].get("age").unwrap(), 30);
    }

    #[tokio::test]
    async fn test_execute_query_with_docid() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Bob");
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { _docID name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert!(users[0].get("_docID").unwrap().is_string());
        assert_eq!(users[0].get("name").unwrap(), "Bob");
    }

    #[tokio::test]
    async fn test_execute_empty_collection() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Users { name } }").await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users.is_empty());
    }

    #[tokio::test]
    async fn test_execute_unknown_collection() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Posts { title } }").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_with_limit() {
        let fetcher = MockFetcher::new();

        for i in 0..5 {
            let mut doc = Document::new();
            doc.set("name", format!("User{}", i));
            doc.set("age", i as i64);
            doc.generate_and_set_doc_id().unwrap();
            fetcher.add_doc("Users", doc);
        }

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(limit: 2) { name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);
    }

    #[tokio::test]
    async fn test_query_executor_trait() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let request = QueryRequest {
            query: "{ Users { name } }".to_string(),
            operation_name: None,
            variables: None,
        };

        let response = runner.execute(request).await;

        assert!(response.errors.is_empty());
        assert!(response.data.is_some());
    }

    /// Mock fetcher that returns errors
    struct FailingFetcher;

    #[async_trait]
    impl DocFetcher for FailingFetcher {
        async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
            Err(QueryError::execution("storage failure"))
        }

        async fn get_by_ids(
            &self,
            _collection_name: &str,
            _doc_ids: &[String],
        ) -> Result<Vec<Document>> {
            Err(QueryError::execution("storage failure"))
        }
    }

    #[tokio::test]
    async fn test_fetcher_error_propagates() {
        let runner = QueryRunner::new(FailingFetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Users { name } }").await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("storage failure"));
    }

    #[tokio::test]
    async fn test_query_executor_error_response_format() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let request = QueryRequest {
            query: "{ InvalidCollection { name } }".to_string(),
            operation_name: None,
            variables: None,
        };

        let response = runner.execute(request).await;

        assert!(response.data.is_none());
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0].message.contains("collection not found"));
    }

    #[tokio::test]
    async fn test_execute_with_offset() {
        let fetcher = MockFetcher::new();

        for i in 0..5 {
            let mut doc = Document::new();
            doc.set("name", format!("User{}", i));
            doc.set("age", i as i64);
            doc.generate_and_set_doc_id().unwrap();
            fetcher.add_doc("Users", doc);
        }

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(offset: 2) { name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);
    }

    #[tokio::test]
    async fn test_execute_with_limit_and_offset() {
        let fetcher = MockFetcher::new();

        for i in 0..10 {
            let mut doc = Document::new();
            doc.set("name", format!("User{}", i));
            doc.generate_and_set_doc_id().unwrap();
            fetcher.add_doc("Users", doc);
        }

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(limit: 3, offset: 2) { name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);
    }

    #[tokio::test]
    async fn test_execute_query_with_doc_ids() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let query = format!(r#"{{ Users(docIDs: ["{}"]) {{ name }} }}"#, doc1_id);
        let result = runner.execute_query(&query).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn test_unknown_collection_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Posts { title } }").await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("collection not found: Posts"));
    }

    #[tokio::test]
    async fn test_order_by_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(order: {name: ASC}) { name } }")
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("ordering is not yet implemented"));
    }

    #[tokio::test]
    async fn test_group_by_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(groupBy: [name]) { name } }")
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("grouping is not yet implemented"));
    }

    #[tokio::test]
    async fn test_nested_selection_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { name posts { title } } }")
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("nested selections (relations) are not yet implemented"));
    }

    #[tokio::test]
    async fn test_execute_in_txn_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let request = QueryRequest::new("{ Users { name } }");
        let response = runner.execute_in_txn(request, "txn-123").await;

        assert!(response.has_errors());
        assert!(response.errors[0]
            .message
            .contains("execute_in_txn is not yet implemented"));
        assert!(response.errors[0].message.contains("txn-123"));
    }

    #[tokio::test]
    async fn test_execute_query_with_filter() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(r#"{ Users(filter: {age: {_gte: 30}}) { name age } }"#)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        let names: Vec<&str> = users
            .iter()
            .map(|u| u.get("name").unwrap().as_str().unwrap())
            .collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Charlie"));
        assert!(!names.contains(&"Bob"));
    }

    #[tokio::test]
    async fn test_field_alias_in_output() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { userName: name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert!(users[0].get("userName").is_some());
        assert!(users[0].get("name").is_none());
        assert_eq!(users[0].get("userName").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn test_collection_alias_in_output() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ allUsers: Users { name } }")
            .await
            .unwrap();

        assert!(result.get("allUsers").is_some());
        assert!(result.get("Users").is_none());
        let users = result.get("allUsers").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
    }

    #[tokio::test]
    async fn test_schema_generation() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let schema = runner.schema().await.unwrap();

        assert!(schema.contains("type Users"));
        assert!(schema.contains("_docID: ID"));
        assert!(schema.contains("name: String"));
        assert!(schema.contains("age: Int"));
    }
}
