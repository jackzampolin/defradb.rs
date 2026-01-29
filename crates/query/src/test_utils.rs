//! Test utilities for the query crate.
//!
//! This module provides mock implementations for testing query execution.

use async_trait::async_trait;
use document::Document;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{Result, TransactionError};
use crate::fetcher::{DocFetcher, FetchByIdsResult};
use crate::txn::{
    GetTransactionResult, TransactionContext, TransactionHandle, TransactionRegistry,
};

/// Mock fetcher for testing that stores documents in memory.
pub struct MockFetcher {
    docs: Mutex<HashMap<String, Vec<Document>>>,
}

impl MockFetcher {
    /// Create a new empty mock fetcher.
    pub fn new() -> Self {
        Self {
            docs: Mutex::new(HashMap::new()),
        }
    }

    /// Add a document to a collection.
    pub fn add_doc(&self, collection: &str, doc: Document) {
        let mut docs = self.docs.lock().unwrap();
        docs.entry(collection.to_string()).or_default().push(doc);
    }
}

impl Default for MockFetcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
            let doc = all.iter().find(|d| {
                d.id()
                    .map(|doc_id| doc_id.to_string() == *id)
                    .unwrap_or(false)
            });
            match doc {
                Some(d) => found.push(d.clone()),
                None => missing.push(id.clone()),
            }
        }

        Ok(FetchByIdsResult::partial(found, missing))
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> Result<Vec<Document>> {
        let docs = self.docs.lock().unwrap();
        let all = docs.get(collection_name).cloned().unwrap_or_default();

        let matching: Vec<Document> = all
            .into_iter()
            .filter(|doc| {
                doc.get(field_name)
                    .and_then(|v| v.as_str())
                    .map(|v| v == value)
                    .unwrap_or(false)
            })
            .collect();

        Ok(matching)
    }
}

/// Mock transaction context for testing.
pub struct MockTxnContext {
    id: String,
    readonly: bool,
    fetcher: Arc<dyn DocFetcher>,
}

impl MockTxnContext {
    /// Create a new mock transaction context.
    pub fn new(id: String, readonly: bool, fetcher: Arc<dyn DocFetcher>) -> Self {
        Self {
            id,
            readonly,
            fetcher,
        }
    }
}

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

/// Mock transaction registry for testing.
pub struct MockTxnRegistry {
    counter: AtomicU64,
    transactions: Mutex<HashMap<String, Arc<dyn TransactionContext>>>,
    fetcher: Arc<MockFetcher>,
}

impl MockTxnRegistry {
    /// Create a new mock registry with the given fetcher for transaction-scoped access.
    pub fn new(fetcher: MockFetcher) -> Self {
        Self {
            counter: AtomicU64::new(0),
            transactions: Mutex::new(HashMap::new()),
            fetcher: Arc::new(fetcher),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl TransactionRegistry for MockTxnRegistry {
    async fn begin(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        let id = self.counter.fetch_add(1, Ordering::SeqCst);
        let txn_id = format!("txn-{}", id);

        let ctx = Arc::new(MockTxnContext::new(
            txn_id.clone(),
            readonly,
            self.fetcher.clone(),
        ));

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
