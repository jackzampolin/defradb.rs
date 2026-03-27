//! Production implementation of REST operations using QueryRunner.

use std::sync::Arc;

use async_trait::async_trait;
use identity::Did;
use serde_json::Value as JsonValue;

use crate::fetcher::DocFetcher;
use crate::runner::QueryRunner;
use crate::txn::TransactionRegistry;

use super::error::{RestError, RestResult};
use super::trait_def::RestOperations;

/// Production implementation of REST operations using QueryRunner.
///
/// This wraps a QueryRunner and translates REST operations into GraphQL queries/mutations.
pub struct RestOperationsImpl<F: DocFetcher, R: TransactionRegistry> {
    runner: Arc<QueryRunner<F, R>>,
}

impl<F: DocFetcher + 'static, R: TransactionRegistry> RestOperationsImpl<F, R> {
    /// Create a new REST operations implementation.
    pub fn new(runner: Arc<QueryRunner<F, R>>) -> Self {
        Self { runner }
    }

    /// Convert a JSON value to GraphQL input syntax.
    fn json_to_graphql_input(value: &JsonValue) -> String {
        match value {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => {
                let escaped = s
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                    .replace('\r', "\\r")
                    .replace('\t', "\\t");
                format!("\"{}\"", escaped)
            }
            JsonValue::Array(arr) => {
                let items: Vec<String> = arr.iter().map(Self::json_to_graphql_input).collect();
                format!("[{}]", items.join(", "))
            }
            JsonValue::Object(obj) => {
                let fields: Vec<String> = obj
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k, Self::json_to_graphql_input(v)))
                    .collect();
                format!("{{{}}}", fields.join(", "))
            }
        }
    }

    fn build_list_ids_query(&self, collection: &str) -> String {
        format!(
            r#"{{ {collection} {{ _docID }} }}"#,
            collection = collection
        )
    }

    fn build_create_mutation(&self, collection: &str, data: &JsonValue) -> String {
        let graphql_data = Self::json_to_graphql_input(data);
        format!(
            r#"mutation {{ add_{collection}(input: [{graphql_data}]) {{ _docID }} }}"#,
            collection = collection,
            graphql_data = graphql_data
        )
    }

    fn build_create_many_mutation(&self, collection: &str, docs: &[JsonValue]) -> String {
        let inputs: Vec<String> = docs.iter().map(Self::json_to_graphql_input).collect();
        format!(
            r#"mutation {{ add_{collection}(input: [{inputs}]) {{ _docID }} }}"#,
            collection = collection,
            inputs = inputs.join(", ")
        )
    }

    fn build_update_mutation(&self, collection: &str, doc_id: &str, patch: &JsonValue) -> String {
        let graphql_patch = Self::json_to_graphql_input(patch);
        format!(
            r#"mutation {{ update_{collection}(docIDs: ["{doc_id}"], input: {graphql_patch}) {{ _docID }} }}"#,
            collection = collection,
            doc_id = doc_id,
            graphql_patch = graphql_patch
        )
    }

    fn build_delete_mutation(&self, collection: &str, doc_id: &str) -> String {
        format!(
            r#"mutation {{ delete_{collection}(docIDs: ["{doc_id}"]) {{ _docID }} }}"#,
            collection = collection,
            doc_id = doc_id
        )
    }

    fn extract_doc_ids(&self, result: &JsonValue, collection: &str) -> RestResult<Vec<String>> {
        let docs = result.get(collection).ok_or_else(|| {
            tracing::warn!(
                collection = %collection,
                result = ?result,
                "Query result missing collection key"
            );
            RestError::internal(format!("query result missing key '{}'", collection))
        })?;

        let arr = docs.as_array().ok_or_else(|| {
            tracing::warn!(
                collection = %collection,
                result = ?result,
                "Query result collection is not an array"
            );
            RestError::internal(format!(
                "expected array for collection '{}', got {}",
                collection,
                match docs {
                    JsonValue::Null => "null",
                    JsonValue::Bool(_) => "boolean",
                    JsonValue::Number(_) => "number",
                    JsonValue::String(_) => "string",
                    JsonValue::Object(_) => "object",
                    JsonValue::Array(_) => "array",
                }
            ))
        })?;

        Ok(arr
            .iter()
            .filter_map(|doc| doc.get("_docID").and_then(|id| id.as_str()))
            .map(String::from)
            .collect())
    }

    async fn fetch_full_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>> {
        let coll = self
            .runner
            .get_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?;
        let fields: Vec<&str> = std::iter::once("_docID")
            .chain(coll.fields.iter().filter_map(|f| {
                if f.name == "_docID" || !f.kind.is_scalar() {
                    None
                } else {
                    Some(f.name.as_str())
                }
            }))
            .collect();
        let selection = fields.join(" ");

        let query = format!(
            r#"{{ {collection}(docID: "{doc_id}") {{ {selection} }} }}"#,
            collection = collection,
            doc_id = doc_id,
            selection = selection
        );

        let result = self
            .runner
            .execute_query_with_identity(&query, identity.cloned())
            .await?;

        let doc = result
            .get(collection)
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .cloned();

        Ok(doc)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<F: DocFetcher + 'static, R: TransactionRegistry> RestOperations for RestOperationsImpl<F, R> {
    async fn list_collections(&self) -> RestResult<Vec<String>> {
        self.runner
            .collection_names()
            .await
            .map_err(|e| RestError::internal(e.to_string()))
    }

    async fn get_collection_doc_ids(
        &self,
        collection: &str,
        identity: Option<&Did>,
    ) -> RestResult<Vec<String>> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let query = self.build_list_ids_query(collection);
        let result = self
            .runner
            .execute_query_with_identity(&query, identity.cloned())
            .await?;
        self.extract_doc_ids(&result, collection)
    }

    async fn get_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<Option<JsonValue>> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        self.fetch_full_document(collection, doc_id, identity).await
    }

    async fn create_document(
        &self,
        collection: &str,
        data: JsonValue,
        identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let mutation = self.build_create_mutation(collection, &data);
        let result = self
            .runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        let doc = result
            .get(format!("add_{}", collection))
            .or_else(|| result.get(collection))
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .cloned()
            .unwrap_or_default();
        Ok(doc)
    }

    async fn create_documents(
        &self,
        collection: &str,
        data: Vec<JsonValue>,
        identity: Option<&Did>,
    ) -> RestResult<Vec<JsonValue>> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let mutation = self.build_create_many_mutation(collection, &data);
        let result = self
            .runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        let docs = result
            .get(format!("add_{}", collection))
            .or_else(|| result.get(collection))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(docs)
    }

    async fn update_document(
        &self,
        collection: &str,
        doc_id: &str,
        patch: JsonValue,
        identity: Option<&Did>,
    ) -> RestResult<JsonValue> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let existing = self
            .fetch_full_document(collection, doc_id, identity)
            .await?;
        if existing.is_none() {
            return Err(RestError::document_not_found(doc_id));
        }

        let mutation = self.build_update_mutation(collection, doc_id, &patch);
        self.runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        self.fetch_full_document(collection, doc_id, identity)
            .await?
            .ok_or_else(|| RestError::internal("updated document not found"))
    }

    async fn delete_document(
        &self,
        collection: &str,
        doc_id: &str,
        identity: Option<&Did>,
    ) -> RestResult<bool> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let existing = self
            .fetch_full_document(collection, doc_id, identity)
            .await?;
        if existing.is_none() {
            return Ok(false);
        }

        let mutation = self.build_delete_mutation(collection, doc_id);
        let result = self
            .runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        let delete_key = format!("delete_{}", collection);
        let delete_result = result.get(&delete_key).or_else(|| result.get(collection));

        let deleted = match delete_result {
            Some(v) => match v.as_array() {
                Some(arr) => !arr.is_empty(),
                None => {
                    tracing::warn!(
                        collection = %collection,
                        doc_id = %doc_id,
                        result = ?result,
                        "Delete mutation result is not an array"
                    );
                    return Err(RestError::internal(
                        "unexpected delete mutation result format: expected array",
                    ));
                }
            },
            None => {
                tracing::warn!(
                    collection = %collection,
                    doc_id = %doc_id,
                    result = ?result,
                    "Delete mutation returned unexpected result structure"
                );
                return Err(RestError::internal(
                    "unexpected delete mutation result: missing expected key",
                ));
            }
        };

        Ok(deleted)
    }
}
