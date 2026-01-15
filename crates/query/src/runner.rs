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
use crate::mapper::{Mutation, MutationType, Requestable, Select};
use crate::mutator::DocMutator;
use crate::plan::{
    CreateInput, CreateNode, DeleteNode, LimitNode, ScanNode, SelectNode, UpdateInput, UpdateNode,
    UpsertInput, UpsertNode,
};
use crate::planner::{Doc, PlanNode};
use crate::query_parse::{parse_mutations, parse_query};
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
    /// Document mutator for mutation operations (optional)
    mutator: Option<Arc<dyn DocMutator>>,
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
            mutator: None,
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
            mutator: None,
        }
    }

    /// Set the document mutator for mutation operations.
    ///
    /// This enables support for CREATE, UPDATE, and DELETE mutations.
    pub fn with_mutator(mut self, mutator: Arc<dyn DocMutator>) -> Self {
        self.mutator = Some(mutator);
        self
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

    /// Execute a GraphQL mutation and return JSON results.
    ///
    /// Requires a mutator to be configured via `with_mutator()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let runner = QueryRunner::new(fetcher, collections)
    ///     .with_mutator(mutator);
    ///
    /// let result = runner.execute_mutation(r#"
    ///     mutation {
    ///         create_Users(input: [{name: "Alice", age: 30}]) {
    ///             _docID
    ///             name
    ///         }
    ///     }
    /// "#).await?;
    /// ```
    pub async fn execute_mutation(&self, mutation_str: &str) -> Result<JsonValue> {
        let mutator = self.mutator.as_ref().ok_or_else(|| {
            QueryError::execution("mutations require a mutator; call with_mutator() first")
        })?;

        self.execute_mutation_with_mutator(mutation_str, mutator.clone())
            .await
    }

    /// Execute a GraphQL mutation with a specific mutator.
    async fn execute_mutation_with_mutator(
        &self,
        mutation_str: &str,
        mutator: Arc<dyn DocMutator>,
    ) -> Result<JsonValue> {
        let mutations = parse_mutations(mutation_str)?;

        let mut results = Map::new();

        for mutation in mutations {
            let result = self
                .execute_single_mutation(&mutation, mutator.clone())
                .await?;
            // Use collection name as key (Go behavior)
            results.insert(mutation.collection_name.clone(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute a single mutation operation.
    async fn execute_single_mutation(
        &self,
        mutation: &Mutation,
        mutator: Arc<dyn DocMutator>,
    ) -> Result<JsonValue> {
        // Validate collection exists
        let _collection = self
            .collections
            .get(&mutation.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&mutation.collection_name))?;

        // Build document mapping from requested fields
        let mapping = self.build_mutation_mapping(mutation)?;

        // Resolve filter to doc_ids if filter is provided without doc_ids
        let resolved_doc_ids = self.resolve_filter_to_doc_ids(mutation).await?;

        // Build and execute the appropriate mutation plan
        let mut plan: Box<dyn PlanNode> = match mutation.mutation_type {
            MutationType::Create => {
                let inputs = self.build_create_inputs(mutation)?;
                Box::new(
                    CreateNode::new(&mutation.collection_name, mutator, mapping.clone())
                        .with_inputs(inputs),
                )
            }
            MutationType::Update => {
                let input = self.build_update_input(mutation)?;
                let mut node = UpdateNode::new(&mutation.collection_name, mutator, mapping.clone())
                    .with_input(input);

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                Box::new(node)
            }
            MutationType::Delete => {
                let mut node = DeleteNode::new(&mutation.collection_name, mutator, mapping.clone());

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                Box::new(node)
            }
            MutationType::Upsert => {
                let input = self.build_upsert_input(mutation)?;
                let mut node = UpsertNode::new(&mutation.collection_name, mutator, mapping.clone())
                    .with_input(input);

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                Box::new(node)
            }
        };

        // Execute the plan
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

    /// Resolve a filter to document IDs by querying the collection.
    ///
    /// This is used for filter-based mutations where we need to first
    /// find matching documents, then perform the mutation on them.
    async fn resolve_filter_to_doc_ids(&self, mutation: &Mutation) -> Result<Option<Vec<String>>> {
        // Only resolve if there's a filter but no explicit doc_ids
        let filter = match (&mutation.filter, &mutation.doc_ids) {
            (Some(filter), None) => filter,
            _ => return Ok(None),
        };

        // Get the collection schema to build a mapping
        let collection = self
            .collections
            .get(&mutation.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&mutation.collection_name))?;

        // Build mapping from collection schema
        let mut mapping = DocumentMapping::new();
        for (i, field) in collection.fields.iter().enumerate() {
            mapping.add(i, &field.name);
        }

        // Get all documents from the collection
        let all_docs = self.fetcher.get_all(&mutation.collection_name).await?;

        // Apply filter to find matching documents
        let mut matching_ids = Vec::new();
        for doc in &all_docs {
            // Convert Document to fields array for filter matching
            let plan_doc = self.document_to_plan_doc(doc, &mapping)?;
            let fields = plan_doc.fields();

            if filter.matches(fields, &mapping)? {
                if let Some(id) = doc.id() {
                    matching_ids.push(id.to_string());
                }
            }
        }

        Ok(Some(matching_ids))
    }

    /// Build document mapping for mutation result fields.
    fn build_mutation_mapping(&self, mutation: &Mutation) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Add requested fields
        for field in mutation.requested_fields() {
            let index = mapping.next_index();
            mapping.add(index, &field.name);
            mapping.add_render_key(index, field.output_name());
        }

        // If no fields specified, at minimum return _docID
        if mapping.next_index() == 0 {
            mapping.add(0, "_docID");
            mapping.add_render_key(0, "_docID");
        }

        Ok(mapping)
    }

    /// Build CreateInput objects from mutation input.
    fn build_create_inputs(&self, mutation: &Mutation) -> Result<Vec<CreateInput>> {
        let mut inputs = Vec::new();

        for doc_input in &mutation.create_input {
            let mut create_input = CreateInput::new();
            for (field_name, value) in doc_input {
                create_input = create_input.with_field(field_name.clone(), value.clone());
            }
            inputs.push(create_input);
        }

        Ok(inputs)
    }

    /// Build UpdateInput from mutation input.
    fn build_update_input(&self, mutation: &Mutation) -> Result<UpdateInput> {
        let mut update_input = UpdateInput::new();

        for (field_name, value) in &mutation.update_input {
            update_input = update_input.with_field(field_name.clone(), value.clone());
        }

        Ok(update_input)
    }

    /// Build UpsertInput from mutation input.
    fn build_upsert_input(&self, mutation: &Mutation) -> Result<UpsertInput> {
        let mut upsert_input = UpsertInput::new();

        // Upsert uses update_input for the field values
        for (field_name, value) in &mutation.update_input {
            upsert_input = upsert_input.with_field(field_name.clone(), value.clone());
        }

        Ok(upsert_input)
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
    use crate::test_utils::{MockFetcher, MockTxnRegistry};
    use schema::{FieldDescription, FieldKind};

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

    // Mutation tests

    /// Mock mutator for testing
    struct MockMutator {
        docs: std::sync::Mutex<Vec<(String, Document)>>,
    }

    impl MockMutator {
        fn new() -> Self {
            Self {
                docs: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn created_docs(&self) -> Vec<(String, Document)> {
            self.docs.lock().unwrap().clone()
        }

        fn add_doc(&self, collection: &str, doc: Document) {
            self.docs
                .lock()
                .unwrap()
                .push((collection.to_string(), doc));
        }
    }

    #[async_trait]
    impl crate::mutator::DocMutator for MockMutator {
        async fn create(
            &self,
            collection_name: &str,
            mut doc: Document,
        ) -> Result<crate::mutator::CreateResult> {
            doc.generate_and_set_doc_id()
                .map_err(|e| QueryError::execution(format!("Failed to generate DocID: {}", e)))?;

            let doc_id = doc
                .id()
                .cloned()
                .ok_or_else(|| QueryError::execution("Document should have ID after generation"))?;

            self.docs
                .lock()
                .unwrap()
                .push((collection_name.to_string(), doc.clone()));

            Ok(crate::mutator::CreateResult::new(doc_id, doc))
        }

        async fn update(
            &self,
            _collection_name: &str,
            doc: Document,
        ) -> Result<crate::mutator::UpdateResult> {
            let modified = doc.values().len();
            Ok(crate::mutator::UpdateResult::new(doc, modified))
        }

        async fn delete(
            &self,
            _collection_name: &str,
            doc_id: &document::DocID,
        ) -> Result<crate::mutator::DeleteResult> {
            // Check if doc exists and remove it
            let mut docs = self.docs.lock().unwrap();
            let existed = docs
                .iter()
                .position(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string()))
                .map(|i| docs.remove(i))
                .is_some();
            Ok(crate::mutator::DeleteResult::new(doc_id.clone(), existed))
        }

        async fn exists(&self, _collection_name: &str, doc_id: &document::DocID) -> Result<bool> {
            let docs = self.docs.lock().unwrap();
            Ok(docs
                .iter()
                .any(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string())))
        }

        async fn get_for_update(
            &self,
            _collection_name: &str,
            doc_id: &document::DocID,
        ) -> Result<Option<Document>> {
            let docs = self.docs.lock().unwrap();
            Ok(docs
                .iter()
                .find(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string()))
                .map(|(_, d)| d.clone()))
        }
    }

    #[tokio::test]
    async fn test_execute_mutation_without_mutator_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_mutation(r#"mutation { create_Users(input: [{name: "Alice"}]) { _docID } }"#)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("mutations require a mutator"));
    }

    #[tokio::test]
    async fn test_execute_create_mutation() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let result = runner
            .execute_mutation(
                r#"mutation { create_Users(input: [{name: "Alice", age: 30}]) { _docID name } }"#,
            )
            .await
            .unwrap();

        // Check response structure
        assert!(result.is_object());
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert!(users[0].get("_docID").is_some());
        assert_eq!(users[0].get("name").unwrap(), "Alice");

        // Verify document was created via mutator
        let created = mutator.created_docs();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].0, "Users");
    }

    #[tokio::test]
    async fn test_execute_create_multiple_documents() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let result = runner
            .execute_mutation(
                r#"mutation {
                    create_Users(input: [
                        {name: "Alice", age: 30},
                        {name: "Bob", age: 25}
                    ]) { _docID name }
                }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        let names: Vec<&str> = users
            .iter()
            .map(|u| u.get("name").unwrap().as_str().unwrap())
            .collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));

        // Verify both documents were created
        let created = mutator.created_docs();
        assert_eq!(created.len(), 2);
    }

    #[tokio::test]
    async fn test_execute_mutation_unknown_collection_returns_error() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator);

        let result = runner
            .execute_mutation(
                r#"mutation { create_NonExistent(input: [{name: "Alice"}]) { _docID } }"#,
            )
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("collection not found"));
    }

    #[tokio::test]
    async fn test_execute_delete_mutation() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}"]) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc_id);

        // Verify document was deleted
        assert!(mutator.created_docs().is_empty());
    }

    // ==========================================================================
    // Update mutation tests
    // ==========================================================================

    #[tokio::test]
    async fn test_execute_update_mutation() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 25i64);
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], input: {{name: "Alice Updated", age: 30}}) {{ _docID name age }} }}"#,
            doc_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc_id);
        assert_eq!(users[0].get("name").unwrap(), "Alice Updated");
        assert_eq!(users[0].get("age").unwrap(), 30);
    }

    #[tokio::test]
    async fn test_execute_update_multiple_documents() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with multiple documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 25i64);
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 30i64);
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}", "{}"], input: {{age: 99}}) {{ _docID age }} }}"#,
            doc1_id, doc2_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        // Both should have updated age
        for user in users {
            assert_eq!(user.get("age").unwrap(), 99);
        }
    }

    #[tokio::test]
    async fn test_execute_update_nonexistent_document_skipped() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with one document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        let existing_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Generate a non-existent ID
        let mut template = Document::new();
        template.set("name", "NonExistent");
        template.generate_and_set_doc_id().unwrap();
        let nonexistent_id = template.id().unwrap().to_string();

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Try to update both existing and non-existent
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}", "{}"], input: {{name: "Updated"}}) {{ _docID name }} }}"#,
            existing_id, nonexistent_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only the existing document should be returned
        assert_eq!(users.len(), 1);
        assert_eq!(
            users[0].get("_docID").unwrap().as_str().unwrap(),
            existing_id
        );
    }

    #[tokio::test]
    async fn test_execute_delete_multiple_documents() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with multiple documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}", "{}"]) {{ _docID }} }}"#,
            doc1_id, doc2_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        let deleted_ids: Vec<&str> = users
            .iter()
            .map(|u| u.get("_docID").unwrap().as_str().unwrap())
            .collect();
        assert!(deleted_ids.contains(&doc1_id.as_str()));
        assert!(deleted_ids.contains(&doc2_id.as_str()));
    }

    #[tokio::test]
    async fn test_execute_delete_nonexistent_document_skipped() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with one document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        let existing_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Generate a non-existent ID
        let mut template = Document::new();
        template.set("name", "NonExistent");
        template.generate_and_set_doc_id().unwrap();
        let nonexistent_id = template.id().unwrap().to_string();

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Try to delete both existing and non-existent
        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}", "{}"]) {{ _docID }} }}"#,
            existing_id, nonexistent_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only the existing document should be returned as deleted
        assert_eq!(users.len(), 1);
        assert_eq!(
            users[0].get("_docID").unwrap().as_str().unwrap(),
            existing_id
        );
    }

    // ==========================================================================
    // Upsert mutation tests
    // ==========================================================================

    #[tokio::test]
    async fn test_execute_upsert_creates_when_not_exists() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Generate a valid docID that doesn't exist in the store
        let mut template = Document::new();
        template.set("name", "Template");
        template.generate_and_set_doc_id().unwrap();
        let new_doc_id = template.id().unwrap().to_string();

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ upsert_Users(docIDs: ["{}"], input: {{name: "Alice", age: 30}}) {{ _docID name age }} }}"#,
            new_doc_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "Alice");
        assert_eq!(users[0].get("age").unwrap(), 30);

        // Verify document was created
        assert_eq!(mutator.created_docs().len(), 1);
    }

    #[tokio::test]
    async fn test_execute_upsert_updates_when_exists() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut existing_doc = Document::new();
        existing_doc.set("name", "Alice");
        existing_doc.set("age", 25i64);
        existing_doc.generate_and_set_doc_id().unwrap();
        let existing_id = existing_doc.id().unwrap().to_string();
        mutator.add_doc("Users", existing_doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ upsert_Users(docIDs: ["{}"], input: {{age: 30}}) {{ _docID name age }} }}"#,
            existing_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        // Name should be preserved from existing doc
        assert_eq!(users[0].get("name").unwrap(), "Alice");
        // Age should be updated
        assert_eq!(users[0].get("age").unwrap(), 30);
    }

    #[tokio::test]
    async fn test_execute_upsert_mixed_create_and_update() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with one document
        let mut existing_doc = Document::new();
        existing_doc.set("name", "Alice");
        existing_doc.set("age", 25i64);
        existing_doc.generate_and_set_doc_id().unwrap();
        let existing_id = existing_doc.id().unwrap().to_string();
        mutator.add_doc("Users", existing_doc);

        // Generate a new ID that doesn't exist
        let mut template = Document::new();
        template.set("name", "Template");
        template.generate_and_set_doc_id().unwrap();
        let new_id = template.id().unwrap().to_string();

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ upsert_Users(docIDs: ["{}", "{}"], input: {{name: "Updated", age: 99}}) {{ _docID name age }} }}"#,
            existing_id, new_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        // Both should have the upserted values
        for user in users {
            assert_eq!(user.get("name").unwrap(), "Updated");
            assert_eq!(user.get("age").unwrap(), 99);
        }
    }

    #[tokio::test]
    async fn test_execute_upsert_create_without_doc_id() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Upsert without docIDs creates a new document
        let result = runner
            .execute_mutation(
                r#"mutation { upsert_Users(input: {name: "NewUser", age: 42}) { _docID name age } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert!(users[0].get("_docID").is_some());
        assert_eq!(users[0].get("name").unwrap(), "NewUser");
        assert_eq!(users[0].get("age").unwrap(), 42);

        // Verify document was created
        assert_eq!(mutator.created_docs().len(), 1);
    }

    // ==========================================================================
    // Filter-based mutation tests
    // ==========================================================================

    #[tokio::test]
    async fn test_execute_update_with_filter() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 20i64);
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        fetcher.add_doc("Users", doc1.clone());
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 30i64);
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        fetcher.add_doc("Users", doc2.clone());
        mutator.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 40i64);
        doc3.generate_and_set_doc_id().unwrap();
        let doc3_id = doc3.id().unwrap().to_string();
        fetcher.add_doc("Users", doc3.clone());
        mutator.add_doc("Users", doc3);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Update only users with age >= 30
        let result = runner
            .execute_mutation(
                r#"mutation { update_Users(filter: {age: {_gte: 30}}, input: {name: "Updated"}) { _docID name } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only Bob (30) and Charlie (40) should be updated
        assert_eq!(users.len(), 2);

        let updated_ids: Vec<&str> = users
            .iter()
            .map(|u| u.get("_docID").unwrap().as_str().unwrap())
            .collect();
        assert!(!updated_ids.contains(&doc1_id.as_str())); // Alice (20) not updated
        assert!(updated_ids.contains(&doc2_id.as_str())); // Bob (30) updated
        assert!(updated_ids.contains(&doc3_id.as_str())); // Charlie (40) updated
    }

    #[tokio::test]
    async fn test_execute_delete_with_filter() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 25i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1.clone());
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 35i64);
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        fetcher.add_doc("Users", doc2.clone());
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Delete only users with age > 30
        let result = runner
            .execute_mutation(
                r#"mutation { delete_Users(filter: {age: {_gt: 30}}) { _docID } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only Bob should be deleted
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc2_id);
    }

    #[tokio::test]
    async fn test_execute_upsert_with_filter() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 25i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1.clone());
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 35i64);
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        fetcher.add_doc("Users", doc2.clone());
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Upsert users with age > 30 (should update Bob)
        let result = runner
            .execute_mutation(
                r#"mutation { upsert_Users(filter: {age: {_gt: 30}}, input: {name: "Updated"}) { _docID name } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc2_id);
        assert_eq!(users[0].get("name").unwrap(), "Updated");
    }

    #[tokio::test]
    async fn test_filter_mutation_no_matches_returns_empty() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 25i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc.clone());
        mutator.add_doc("Users", doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Filter matches nothing (no users with age > 100)
        let result = runner
            .execute_mutation(
                r#"mutation { update_Users(filter: {age: {_gt: 100}}, input: {name: "Updated"}) { _docID } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Should return empty array, not an error
        assert!(users.is_empty());
    }

    #[tokio::test]
    async fn test_filter_delete_no_matches_returns_empty() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 25i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc.clone());
        mutator.add_doc("Users", doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Filter matches nothing
        let result = runner
            .execute_mutation(
                r#"mutation { delete_Users(filter: {name: {_eq: "NonExistent"}}) { _docID } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users.is_empty());

        // Original document should still exist
        assert_eq!(mutator.created_docs().len(), 1);
    }

    #[tokio::test]
    async fn test_doc_ids_takes_priority_over_filter() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 50i64);
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        fetcher.add_doc("Users", doc1.clone());
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 60i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2.clone());
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Provide both docIDs and filter - docIDs should take priority
        // Filter would match both, but docIDs only specifies doc1
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], filter: {{age: {{_gte: 50}}}}, input: {{name: "Updated"}}) {{ _docID name }} }}"#,
            doc1_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only doc1 should be updated (docIDs takes priority)
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc1_id);
    }
}
