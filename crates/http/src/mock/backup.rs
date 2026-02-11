//! Mock backup operations for testing backup handlers.

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::router::{BackupOperations, ImportResult};

/// Mock backup operations for testing backup handlers.
#[derive(Debug)]
pub struct MockBackupOperations {
    data: Arc<RwLock<String>>,
}

impl Clone for MockBackupOperations {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
        }
    }
}

impl Default for MockBackupOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBackupOperations {
    /// Create a new mock backup operations instance.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(
                r#"{"Users": [{"_docID": "bae-123", "name": "Alice"}]}"#.to_string(),
            )),
        }
    }

    /// Create with custom backup data.
    pub fn with_data(data: &str) -> Self {
        Self {
            data: Arc::new(RwLock::new(data.to_string())),
        }
    }
}

#[async_trait]
impl BackupOperations for MockBackupOperations {
    async fn export(
        &self,
        _collections: Option<Vec<String>>,
        pretty: bool,
    ) -> Result<String, String> {
        let data = self.data.read().unwrap().clone();
        if pretty {
            // Format the JSON nicely
            let parsed: serde_json::Value =
                serde_json::from_str(&data).map_err(|e| format!("invalid JSON: {}", e))?;
            serde_json::to_string_pretty(&parsed).map_err(|e| format!("failed to serialize: {}", e))
        } else {
            Ok(data)
        }
    }

    async fn import(&self, data: &str) -> Result<ImportResult, String> {
        // Parse the incoming data to validate it
        let parsed: serde_json::Value =
            serde_json::from_str(data).map_err(|e| format!("invalid JSON: {}", e))?;

        // Count documents and collections
        let mut documents_imported = 0u64;
        let mut collections_affected = Vec::new();

        if let Some(obj) = parsed.as_object() {
            for (collection, docs) in obj {
                collections_affected.push(collection.clone());
                if let Some(arr) = docs.as_array() {
                    documents_imported += arr.len() as u64;
                }
            }
        }

        // Store the new data
        *self.data.write().unwrap() = data.to_string();

        Ok(ImportResult {
            documents_imported,
            documents_skipped: 0,
            collections_affected,
            errors: vec![],
        })
    }
}

/// Mock backup operations that always fails with a configurable error.
#[derive(Debug, Clone)]
pub struct FailingMockBackupOperations {
    error: String,
}

impl FailingMockBackupOperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl BackupOperations for FailingMockBackupOperations {
    async fn export(
        &self,
        _collections: Option<Vec<String>>,
        _pretty: bool,
    ) -> Result<String, String> {
        Err(self.error.clone())
    }

    async fn import(&self, _data: &str) -> Result<ImportResult, String> {
        Err(self.error.clone())
    }
}
