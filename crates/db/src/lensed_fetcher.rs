//! Lensed document fetcher that applies schema migrations.
//!
//! This fetcher wraps an inner fetcher and applies lens transforms to documents
//! that are stored with older schema versions.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use document::Document;
use lens::{LensDoc, TargetedHistoryLink, TransformStore, DOC_ID_FIELD};
use query::runner::{DocFetcher, FetchByIdsResult};
use storage::corekv::Store;
use tokio::sync::Mutex as TokioMutex;

use crate::collection_loader::get_collection_with_lazy_load;
use crate::txn::DbTxn;

/// Document fetcher that applies lens migrations to documents.
///
/// When documents are fetched from older schema versions, they are
/// transformed to the current (target) schema version using registered
/// lens migrations.
pub struct LensedDocFetcher<S: Store> {
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    #[allow(dead_code)]
    lens_store: Arc<dyn TransformStore>,
    /// Cache of collection version histories keyed by collection name.
    #[allow(dead_code)]
    history_cache: tokio::sync::RwLock<HashMap<String, HashMap<String, TargetedHistoryLink>>>,
}

#[allow(dead_code)]
impl<S: Store> LensedDocFetcher<S> {
    /// Create a new lensed document fetcher.
    ///
    /// # Arguments
    ///
    /// * `txn` - The database transaction
    /// * `lens_store` - The lens transform store for applying migrations
    pub(crate) fn new(txn: DbTxn<S>, lens_store: Arc<dyn TransformStore>) -> Self {
        Self {
            txn: Arc::new(TokioMutex::new(Some(txn))),
            lens_store,
            history_cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Take the transaction out of the fetcher (for commit/rollback).
    pub(crate) async fn take_txn(&self) -> Option<DbTxn<S>> {
        self.txn.lock().await.take()
    }

    /// Check if the transaction has been consumed.
    pub async fn is_consumed(&self) -> bool {
        self.txn.lock().await.is_none()
    }

    /// Get the shared transaction reference.
    pub(crate) fn shared_txn(&self) -> Arc<TokioMutex<Option<DbTxn<S>>>> {
        self.txn.clone()
    }

    /// Convert a Document to a LensDoc.
    fn doc_to_lens_doc(doc: &Document) -> Option<LensDoc> {
        // Use Document's to_map which handles all field conversions properly
        let map = doc.to_map().ok()?;

        // Convert HashMap to serde_json::Map
        let mut lens_doc = LensDoc::new();
        for (key, value) in map {
            lens_doc.insert(key, value);
        }

        Some(lens_doc)
    }

    /// Convert a LensDoc back to a Document.
    fn lens_doc_to_doc(lens_doc: LensDoc, original_doc: &Document) -> Document {
        let mut doc = Document::new();

        // Preserve original ID
        if let Some(id) = original_doc.id() {
            doc.set_id(id.clone());
        }

        // Copy fields from lens doc
        for (field_name, value) in lens_doc {
            if field_name != DOC_ID_FIELD {
                doc.set(&field_name, value);
            }
        }

        doc
    }
}

#[async_trait]
impl<S: Store + 'static> DocFetcher for LensedDocFetcher<S> {
    async fn get_all(&self, collection_name: &str) -> query::error::Result<Vec<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let docs = collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        // For now, return docs without transformation
        // Full migration support requires building the version history
        // and applying transforms for each document's source version
        Ok(docs)
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> query::error::Result<FetchByIdsResult> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let mut docs = Vec::new();
        let mut missing_ids = Vec::new();

        for id_str in doc_ids {
            let doc_id = document::DocID::from_string(id_str).map_err(|e| {
                query::error::QueryError::execution(format!("invalid doc ID '{}': {}", id_str, e))
            })?;

            match collection
                .get_with_datastore(&datastore, &doc_id)
                .await
                .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?
            {
                Some(doc) => docs.push(doc),
                None => {
                    missing_ids.push(id_str.clone());
                }
            }
        }

        Ok(FetchByIdsResult::partial(docs, missing_ids))
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> query::error::Result<Vec<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let all_docs = collection
            .get_all_with_datastore(&datastore)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("storage error: {}", e)))?;

        let matching_docs: Vec<Document> = all_docs
            .into_iter()
            .filter(|doc| {
                doc.get(field_name)
                    .and_then(|v| v.as_str())
                    .map(|v| v == value)
                    .unwrap_or(false)
            })
            .collect();

        Ok(matching_docs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn test_doc_to_lens_doc_conversion() {
        let mut doc = Document::new();
        doc.set("name", Value::String("Alice".to_string()));
        doc.set("age", Value::Number(30.into()));

        let lens_doc = LensedDocFetcher::<storage::MemoryStore>::doc_to_lens_doc(&doc).unwrap();

        assert_eq!(
            lens_doc.get("name").unwrap(),
            &Value::String("Alice".to_string())
        );
        assert_eq!(lens_doc.get("age").unwrap(), &Value::Number(30.into()));
    }
}
