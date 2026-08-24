//! Production implementation of REST operations using QueryRunner.

use std::sync::Arc;

use async_trait::async_trait;
use identity::Did;
use serde_json::Value as JsonValue;

use crate::fetcher::DocFetcher;
use crate::runner::QueryRunner;
use crate::txn::TransactionRegistry;

use super::error::{RestError, RestResult};
use super::gql;
use super::trait_def::{CollectionDocIdsPage, CollectionDocIdsPagination, RestOperations};

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

    /// Pull the `_docID` list out of a `<op>_<Collection>` mutation result.
    ///
    /// One extractor rather than one per call site: the single-document and
    /// filtered paths have to agree on the result shape, and two copies of
    /// this would drift.
    fn mutation_doc_ids(
        &self,
        result: &JsonValue,
        collection: &str,
        op: &str,
    ) -> RestResult<Vec<String>> {
        let key = format!("{op}_{collection}");
        let docs = result
            .get(&key)
            .or_else(|| result.get(collection))
            .ok_or_else(|| {
                tracing::warn!(
                    collection = %collection,
                    op = %op,
                    result = ?result,
                    "Mutation returned unexpected result structure"
                );
                RestError::internal(format!(
                    "unexpected {op} mutation result: missing key '{key}'"
                ))
            })?;

        let docs = docs.as_array().ok_or_else(|| {
            tracing::warn!(
                collection = %collection,
                op = %op,
                result = ?result,
                "Mutation result is not an array"
            );
            RestError::internal(format!(
                "unexpected {op} mutation result format: expected array"
            ))
        })?;

        Ok(docs
            .iter()
            .filter_map(|doc| doc.get("_docID").and_then(|id| id.as_str()))
            .map(String::from)
            .collect())
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

    fn extract_doc_id_total(&self, result: &JsonValue, collection: &str) -> RestResult<usize> {
        let total = result
            .get("total")
            .and_then(|value| value.as_u64())
            .ok_or_else(|| {
                tracing::warn!(
                    collection = %collection,
                    result = ?result,
                    "Count query result missing numeric total"
                );
                RestError::internal(format!(
                    "expected numeric total for collection '{}'",
                    collection
                ))
            })?;

        usize::try_from(total).map_err(|_| {
            RestError::internal(format!(
                "document count for collection '{}' exceeds platform limits",
                collection
            ))
        })
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

        let query = gql::build_list_ids_query(collection, None);
        let result = self
            .runner
            .execute_query_with_identity(&query, identity.cloned())
            .await?;
        self.extract_doc_ids(&result, collection)
    }

    async fn get_collection_doc_ids_page(
        &self,
        collection: &str,
        pagination: CollectionDocIdsPagination,
        identity: Option<&Did>,
    ) -> RestResult<CollectionDocIdsPage> {
        if !self
            .runner
            .has_collection(collection)
            .await
            .map_err(|e| RestError::internal(e.to_string()))?
        {
            return Err(RestError::collection_not_found(collection));
        }

        let count_query = gql::build_count_query(collection);
        let count_result = self
            .runner
            .execute_query_with_identity(&count_query, identity.cloned())
            .await?;
        let total = self.extract_doc_id_total(&count_result, collection)?;

        if pagination.offset >= total {
            return Ok(CollectionDocIdsPage {
                doc_ids: Vec::new(),
                total,
                limit: pagination.limit,
                offset: pagination.offset,
            });
        }

        let query = gql::build_list_ids_query(collection, Some(pagination));
        let result = self
            .runner
            .execute_query_with_identity(&query, identity.cloned())
            .await?;
        let doc_ids = self.extract_doc_ids(&result, collection)?;

        Ok(CollectionDocIdsPage {
            doc_ids,
            total,
            limit: pagination.limit,
            offset: pagination.offset,
        })
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

        let mutation = gql::build_create_mutation(collection, &data)?;
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

        let mutation = gql::build_create_many_mutation(collection, &data)?;
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

        let mutation = gql::build_update_mutation(collection, doc_id, &patch)?;
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

        let mutation = gql::build_delete_mutation(collection, doc_id);
        let result = self
            .runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        let deleted = !self
            .mutation_doc_ids(&result, collection, "delete")?
            .is_empty();

        Ok(deleted)
    }

    async fn delete_documents_with_filter(
        &self,
        collection: &str,
        filter: &JsonValue,
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

        let mutation = gql::build_filtered_delete_mutation(collection, filter)?;
        let result = self
            .runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        self.mutation_doc_ids(&result, collection, "delete")
    }

    async fn update_documents_with_filter(
        &self,
        collection: &str,
        filter: &JsonValue,
        updater: &JsonValue,
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

        let mutation = gql::build_filtered_update_mutation(collection, filter, updater)?;
        let result = self
            .runner
            .execute_mutation_with_identity(&mutation, identity.cloned())
            .await?;

        self.mutation_doc_ids(&result, collection, "update")
    }
}
