//! Query runner - executes queries against storage
//!
//! This module provides the QueryRunner which bridges the query planner
//! with the storage layer, executing queries and returning JSON results.
//!
//! # Transaction Support
//!
//! The QueryRunner supports executing queries within transaction contexts via
//! a `TransactionRegistry`. The registry manages transaction lifecycle and provides
//! transaction-scoped document fetchers for query execution.

use async_trait::async_trait;
use document::Document;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result, TransactionError};
use crate::executor::{QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
use crate::json_convert::normal_value_to_json;
use crate::mapper::{Requestable, Select};
use crate::plan::{LimitNode, ScanNode, SelectNode};
use crate::planner::{Doc, PlanNode};
use crate::query_parse::parse_query;
use crate::txn::{
    GetTransactionResult, NoOpTransactionRegistry, TransactionHandle, TransactionRegistry,
};

// Re-export for backwards compatibility
pub use crate::fetcher::{DocFetcher, FetchByIdsResult};

/// Query runner that executes GraphQL queries against storage.
pub struct QueryRunner<F: DocFetcher, R: TransactionRegistry = NoOpTransactionRegistry> {
    /// Document fetcher for storage access (used for non-transactional queries)
    fetcher: Arc<F>,
    /// Collection schemas by name
    collections: HashMap<String, Arc<CollectionVersion>>,
    /// Transaction registry for transaction lifecycle management
    registry: Arc<R>,
}

impl<F: DocFetcher> QueryRunner<F, NoOpTransactionRegistry> {
    /// Create a new query runner with the given fetcher and collections.
    ///
    /// This creates a runner without transaction support. Use `with_registry`
    /// to enable transaction support.
    pub fn new(fetcher: F, collections: Vec<CollectionVersion>) -> Self {
        let collections_map = collections
            .iter()
            .map(|c| (c.name.clone(), Arc::new(c.clone())))
            .collect();
        Self {
            fetcher: Arc::new(fetcher),
            collections: collections_map,
            registry: Arc::new(NoOpTransactionRegistry),
        }
    }
}

impl<F: DocFetcher, R: TransactionRegistry> QueryRunner<F, R> {
    /// Create a new query runner with transaction support.
    pub fn with_registry(fetcher: F, collections: Vec<CollectionVersion>, registry: R) -> Self {
        let collections_map = collections
            .iter()
            .map(|c| (c.name.clone(), Arc::new(c.clone())))
            .collect();
        Self {
            fetcher: Arc::new(fetcher),
            collections: collections_map,
            registry: Arc::new(registry),
        }
    }

    /// Execute a GraphQL query and return JSON results.
    pub async fn execute_query(&self, query: &str) -> Result<JsonValue> {
        self.execute_query_with_fetcher(query, self.fetcher.as_ref())
            .await
    }

    /// Execute a GraphQL query with a specific fetcher.
    ///
    /// This is used internally for both regular queries (using the default fetcher)
    /// and transactional queries (using a transaction-scoped fetcher).
    async fn execute_query_with_fetcher(
        &self,
        query: &str,
        fetcher: &dyn DocFetcher,
    ) -> Result<JsonValue> {
        let selects = parse_query(query)?;

        let mut results = Map::new();

        for select in selects {
            let result = self.execute_select_with_fetcher(&select, fetcher).await?;
            let key = select.field.output_name();
            results.insert(key.to_string(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute a single Select operation with a specific fetcher.
    async fn execute_select_with_fetcher(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
    ) -> Result<JsonValue> {
        // Get collection schema
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Validate unsupported features
        self.validate_select(select)?;

        // Fetch documents from storage
        let docs = if let Some(ref doc_ids) = select.doc_ids {
            let result = fetcher.get_by_ids(&select.collection_name, doc_ids).await?;
            result.into_docs()
        } else {
            fetcher.get_all(&select.collection_name).await?
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
impl<F: DocFetcher, R: TransactionRegistry> QueryExecutor for QueryRunner<F, R> {
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        match self.execute_query(&request.query).await {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
            },
            Err(e) => {
                tracing::error!(
                    query = %request.query,
                    error = %e,
                    "Query execution failed"
                );
                QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                }
            }
        }
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> QueryResponse {
        // Look up the transaction in the registry
        let txn_ctx = match self.registry.get(handle) {
            GetTransactionResult::Found(ctx) => ctx,
            GetTransactionResult::NotFound => {
                return QueryResponse::error(format!(
                    "transaction '{}' not found or has been committed/rolled back",
                    handle
                ));
            }
            GetTransactionResult::LockPoisoned => {
                return QueryResponse::error(format!(
                    "transaction registry lock poisoned - system may be in corrupted state (transaction '{}')",
                    handle
                ));
            }
        };

        // Get the transaction-scoped fetcher and execute
        let fetcher = txn_ctx.doc_fetcher();
        match self
            .execute_query_with_fetcher(&request.query, fetcher.as_ref())
            .await
        {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
            },
            Err(e) => {
                tracing::error!(
                    query = %request.query,
                    txn_id = %handle,
                    error = %e,
                    "Query execution failed in transaction"
                );
                QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                }
            }
        }
    }

    async fn begin_txn(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.registry.begin(readonly).await
    }

    async fn commit_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.registry.commit(handle).await
    }

    async fn rollback_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.registry.rollback(handle).await
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
        ) -> Result<FetchByIdsResult> {
            let docs = self.docs.lock().unwrap();
            let all = docs.get(collection_name).cloned().unwrap_or_default();

            let mut found = Vec::new();
            let mut missing = Vec::new();

            for id in doc_ids {
                if let Some(doc) = all.iter().find(|d| {
                    d.id()
                        .map(|doc_id| doc_id.to_string() == *id)
                        .unwrap_or(false)
                }) {
                    found.push(doc.clone());
                } else {
                    missing.push(id.clone());
                }
            }

            Ok(FetchByIdsResult::partial(found, missing))
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
        ) -> Result<FetchByIdsResult> {
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
    async fn test_execute_in_txn_without_registry_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let request = QueryRequest::new("{ Users { name } }");
        let handle: TransactionHandle = "txn-123".parse().unwrap();
        let response = runner.execute_in_txn(request, &handle).await;

        // Without a proper registry, transactions are not found
        assert!(response.has_errors());
        assert!(response.errors[0].message.contains("txn-123"));
        assert!(response.errors[0].message.contains("not found"));
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

    // Transaction support tests

    use crate::txn::{GetTransactionResult, TransactionContext, TransactionRegistry};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Mock transaction context for testing
    struct MockTxnContext {
        id: String,
        readonly: bool,
        fetcher: Arc<dyn DocFetcher>,
    }

    #[async_trait]
    impl TransactionContext for MockTxnContext {
        fn id(&self) -> &str {
            &self.id
        }

        fn is_readonly(&self) -> bool {
            self.readonly
        }

        fn doc_fetcher(&self) -> Arc<dyn DocFetcher> {
            self.fetcher.clone()
        }
    }

    /// Mock transaction registry for testing
    struct MockTxnRegistry {
        counter: AtomicU64,
        transactions: Mutex<HashMap<String, Arc<dyn TransactionContext>>>,
        fetcher: Arc<MockFetcher>,
    }

    impl MockTxnRegistry {
        fn new(fetcher: MockFetcher) -> Self {
            Self {
                counter: AtomicU64::new(0),
                transactions: Mutex::new(HashMap::new()),
                fetcher: Arc::new(fetcher),
            }
        }
    }

    #[async_trait]
    impl TransactionRegistry for MockTxnRegistry {
        async fn begin(
            &self,
            readonly: bool,
        ) -> std::result::Result<TransactionHandle, TransactionError> {
            let id = self.counter.fetch_add(1, Ordering::SeqCst);
            let txn_id = format!("txn-{}", id);

            let ctx = Arc::new(MockTxnContext {
                id: txn_id.clone(),
                readonly,
                fetcher: self.fetcher.clone(),
            });

            self.transactions
                .lock()
                .unwrap()
                .insert(txn_id.clone(), ctx);
            Ok(TransactionHandle::new(txn_id))
        }

        fn get(&self, handle: &TransactionHandle) -> GetTransactionResult {
            match self
                .transactions
                .lock()
                .unwrap()
                .get(handle.as_str())
                .cloned()
            {
                Some(ctx) => GetTransactionResult::Found(ctx),
                None => GetTransactionResult::NotFound,
            }
        }

        async fn commit(
            &self,
            handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            match self.transactions.lock().unwrap().remove(handle.as_str()) {
                Some(_) => Ok(()),
                None => Err(TransactionError::not_found(format!(
                    "transaction '{}' not found",
                    handle
                ))),
            }
        }

        async fn rollback(
            &self,
            handle: &TransactionHandle,
        ) -> std::result::Result<(), TransactionError> {
            match self.transactions.lock().unwrap().remove(handle.as_str()) {
                Some(_) => Ok(()),
                None => Err(TransactionError::not_found(format!(
                    "transaction '{}' not found",
                    handle
                ))),
            }
        }
    }

    #[tokio::test]
    async fn test_begin_txn() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();
        assert!(txn_id.starts_with("txn-"));
    }

    #[tokio::test]
    async fn test_begin_and_commit_txn() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();
        let result = runner.commit_txn(&txn_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_begin_and_rollback_txn() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();
        let result = runner.rollback_txn(&txn_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_commit_nonexistent_txn_returns_error() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let nonexistent_handle: TransactionHandle = "nonexistent-txn".parse().unwrap();
        let result = runner.commit_txn(&nonexistent_handle).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_in_txn_success() {
        let fetcher = MockFetcher::new();

        // Set up data in the registry's fetcher
        let registry_fetcher = MockFetcher::new();
        let mut doc = Document::new();
        doc.set("name", "TxnAlice");
        doc.set("age", 40i64);
        doc.generate_and_set_doc_id().unwrap();
        registry_fetcher.add_doc("Users", doc);

        let registry = MockTxnRegistry::new(registry_fetcher);
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        // Begin transaction
        let txn_id = runner.begin_txn(false).await.unwrap();

        // Execute query in transaction
        let request = QueryRequest::new("{ Users { name age } }");
        let response = runner.execute_in_txn(request, &txn_id).await;

        assert!(!response.has_errors());
        let data = response.data.unwrap();
        let users = data.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "TxnAlice");
        assert_eq!(users[0].get("age").unwrap(), 40);

        // Commit
        runner.commit_txn(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_execute_in_txn_after_commit_fails() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();
        runner.commit_txn(&txn_id).await.unwrap();

        // Try to execute after commit
        let request = QueryRequest::new("{ Users { name } }");
        let response = runner.execute_in_txn(request, &txn_id).await;

        assert!(response.has_errors());
        assert!(response.errors[0].message.contains("not found"));
    }

    #[tokio::test]
    async fn test_multiple_queries_in_same_transaction() {
        let registry_fetcher = MockFetcher::new();
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        registry_fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        registry_fetcher.add_doc("Users", doc2);

        let registry = MockTxnRegistry::new(registry_fetcher);
        let runner =
            QueryRunner::with_registry(MockFetcher::new(), vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();

        // First query
        let request1 = QueryRequest::new("{ Users { name } }");
        let response1 = runner.execute_in_txn(request1, &txn_id).await;
        assert!(!response1.has_errors());

        // Second query in same transaction
        let request2 = QueryRequest::new("{ Users { age } }");
        let response2 = runner.execute_in_txn(request2, &txn_id).await;
        assert!(!response2.has_errors());

        // Both should see the same data
        let users1 = response1
            .data
            .unwrap()
            .get("Users")
            .unwrap()
            .as_array()
            .unwrap()
            .len();
        let users2 = response2
            .data
            .unwrap()
            .get("Users")
            .unwrap()
            .as_array()
            .unwrap()
            .len();
        assert_eq!(users1, users2);
        assert_eq!(users1, 2);

        runner.commit_txn(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_begin_txn_without_registry_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.begin_txn(false).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not supported"));
    }

    #[tokio::test]
    async fn test_query_error_does_not_invalidate_transaction() {
        let registry_fetcher = MockFetcher::new();
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        registry_fetcher.add_doc("Users", doc);

        let registry = MockTxnRegistry::new(registry_fetcher);
        let runner =
            QueryRunner::with_registry(MockFetcher::new(), vec![make_test_collection()], registry);

        // Begin transaction
        let txn_id = runner.begin_txn(false).await.unwrap();

        // Execute an invalid query (unknown collection) - should return error response
        let bad_request = QueryRequest::new("{ NonExistentCollection { name } }");
        let bad_response = runner.execute_in_txn(bad_request, &txn_id).await;
        assert!(
            bad_response.has_errors(),
            "Query for unknown collection should fail"
        );
        assert!(
            bad_response.errors[0]
                .message
                .contains("collection not found"),
            "Error should mention collection not found"
        );

        // The transaction should still be valid - execute a good query
        let good_request = QueryRequest::new("{ Users { name } }");
        let good_response = runner.execute_in_txn(good_request, &txn_id).await;
        assert!(
            !good_response.has_errors(),
            "Valid query should succeed after failed query"
        );
        let data = good_response.data.unwrap();
        let users = data.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "Alice");

        // Commit should succeed - transaction was not invalidated
        let commit_result = runner.commit_txn(&txn_id).await;
        assert!(
            commit_result.is_ok(),
            "Commit should succeed after query error"
        );
    }
}
