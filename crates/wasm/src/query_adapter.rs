//! Query adapter for WASM client.
//!
//! This module provides adapters to bridge the WASM storage layer
//! with the query crate's execution engine.

use std::sync::Arc;

use async_trait::async_trait;
use document::{Document, NormalValue};
use query::fetcher::{DocFetcher, FetchByIdsResult};
use query::QueryRunner;
use schema::CollectionVersion;

use crate::storage::WasmStore;

/// Document fetcher implementation for WASM storage.
///
/// This adapter bridges the `WasmStore` to the `DocFetcher` trait
/// required by the query engine.
pub struct WasmDocFetcher {
    store: Arc<WasmStore>,
}

impl WasmDocFetcher {
    /// Create a new fetcher wrapping a WASM store.
    pub fn new(store: Arc<WasmStore>) -> Self {
        Self { store }
    }

    /// Load a single document from storage by collection and ID.
    async fn load_document(
        &self,
        collection_name: &str,
        doc_id: &str,
    ) -> query::error::Result<Option<Document>> {
        let key = format!("docs/{}/{}", collection_name, doc_id);
        let key_bytes = key.as_bytes();

        match self.store.get(key_bytes).await {
            Ok(Some(bytes)) => {
                let json: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
                let doc = json_to_document(&json, doc_id)?;
                Ok(Some(doc))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(query::error::QueryError::execution(e.to_string())),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocFetcher for WasmDocFetcher {
    /// Get all documents from a collection.
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let prefix = format!("docs/{}/", collection_name);
        let keys = self
            .store
            .keys_with_prefix(prefix.as_bytes())
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        let mut documents = Vec::new();
        for key in keys {
            let key_str = String::from_utf8_lossy(&key);
            // Extract doc_id from key: "docs/{collection}/{doc_id}"
            let doc_id = key_str
                .strip_prefix(&prefix)
                .unwrap_or(&key_str)
                .to_string();

            if let Some(bytes) = self
                .store
                .get(&key)
                .await
                .map_err(|e| query::error::QueryError::execution(e.to_string()))?
            {
                let json: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|e| query::error::QueryError::execution(e.to_string()))?;
                let doc = json_to_document(&json, &doc_id)?;
                documents.push(doc);
            }
        }

        Ok(documents)
    }

    /// Get documents by their IDs.
    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        let mut found = Vec::new();
        let mut missing = Vec::new();

        for doc_id in doc_ids {
            match self.load_document(collection_name, doc_id).await? {
                Some(doc) => found.push(doc),
                None => missing.push(doc_id.clone()),
            }
        }

        if missing.is_empty() {
            Ok(FetchByIdsResult::all_found(found))
        } else {
            Ok(FetchByIdsResult::partial(found, missing))
        }
    }

    /// Get documents by a field value (for FK lookups).
    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        // For now, do a full scan and filter.
        // In the future, this could use an index.
        let all_docs = self.get_all(collection_name).await?;

        let matching: Vec<Document> = all_docs
            .into_iter()
            .filter(|doc| {
                if let Some(field_value) = doc.get(field_name) {
                    // Compare as strings for simplicity
                    match field_value {
                        NormalValue::String(s) => s == value,
                        NormalValue::Int(i) => i.to_string() == value,
                        NormalValue::Bool(b) => b.to_string() == value,
                        _ => false,
                    }
                } else {
                    false
                }
            })
            .collect();

        Ok(matching)
    }
}

/// Convert a JSON value to a Document.
fn json_to_document(json: &serde_json::Value, doc_id: &str) -> query::error::Result<Document> {
    let obj = json
        .as_object()
        .ok_or_else(|| query::error::QueryError::execution("Document is not an object".to_string()))?;

    // Create a document with the given ID
    let mut doc = Document::new();

    // Set the document ID
    if let Ok(id) = document::DocID::from_string(doc_id) {
        doc.set_id(id);
    }

    // Copy all fields from the JSON object
    for (key, value) in obj {
        // Skip internal fields
        if key.starts_with('_') {
            continue;
        }

        // Convert JSON value to NormalValue and set
        if let Some(normal_value) = json_value_to_normal_value(value) {
            doc.set(key.clone(), normal_value);
        }
    }

    Ok(doc)
}

/// Convert a serde_json::Value to a document::NormalValue.
fn json_value_to_normal_value(value: &serde_json::Value) -> Option<NormalValue> {
    match value {
        serde_json::Value::Null => Some(NormalValue::Null),
        serde_json::Value::Bool(b) => Some(NormalValue::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(NormalValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Some(NormalValue::Float64(f))
            } else {
                None
            }
        }
        serde_json::Value::String(s) => Some(NormalValue::String(s.clone())),
        serde_json::Value::Array(arr) => {
            // Arrays of primitives - try to determine the type from first element
            if arr.is_empty() {
                // Empty array - default to string array
                Some(NormalValue::StringArray(vec![]))
            } else {
                match &arr[0] {
                    serde_json::Value::String(_) => {
                        let strings: Vec<String> = arr
                            .iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect();
                        Some(NormalValue::StringArray(strings))
                    }
                    serde_json::Value::Number(n) if n.is_i64() => {
                        let ints: Vec<i64> = arr.iter().filter_map(|v| v.as_i64()).collect();
                        Some(NormalValue::IntArray(ints))
                    }
                    serde_json::Value::Number(_) => {
                        let floats: Vec<f64> = arr.iter().filter_map(|v| v.as_f64()).collect();
                        Some(NormalValue::Float64Array(floats))
                    }
                    serde_json::Value::Bool(_) => {
                        let bools: Vec<bool> = arr.iter().filter_map(|v| v.as_bool()).collect();
                        Some(NormalValue::BoolArray(bools))
                    }
                    _ => None, // Nested arrays or objects not supported in arrays
                }
            }
        }
        serde_json::Value::Object(_) => {
            // Store as JSON for schemaless nested objects
            Some(NormalValue::Json(value.clone()))
        }
    }
}

/// Create a QueryRunner for the WASM client.
pub fn create_query_runner(
    store: Arc<WasmStore>,
    collections: Vec<CollectionVersion>,
) -> QueryRunner<WasmDocFetcher> {
    let fetcher = WasmDocFetcher::new(store);
    QueryRunner::new(fetcher, collections)
}
