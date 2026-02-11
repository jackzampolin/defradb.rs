//! Mock index operations for testing index handlers.

use async_trait::async_trait;
use std::sync::{Arc, RwLock};

use crate::router::{IndexFieldInfo, IndexInfo, IndexOperations};

/// Mock index operations for testing index handlers.
#[derive(Debug)]
pub struct MockIndexOperations {
    indexes: Arc<RwLock<Vec<IndexInfo>>>,
    next_id: Arc<RwLock<u64>>,
}

impl Clone for MockIndexOperations {
    fn clone(&self) -> Self {
        Self {
            indexes: Arc::clone(&self.indexes),
            next_id: Arc::clone(&self.next_id),
        }
    }
}

impl Default for MockIndexOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockIndexOperations {
    /// Create a new mock index operations instance.
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(vec![])),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// Create with a pre-existing index.
    pub fn with_index(self, collection: &str, name: &str, fields: Vec<&str>, unique: bool) -> Self {
        self.indexes.write().unwrap().push(IndexInfo {
            name: name.to_string(),
            collection: collection.to_string(),
            fields: fields
                .into_iter()
                .map(|f| IndexFieldInfo {
                    name: f.to_string(),
                    direction: Some("ASC".to_string()),
                })
                .collect(),
            unique,
        });
        self
    }
}

#[async_trait]
impl IndexOperations for MockIndexOperations {
    async fn create_index(
        &self,
        collection: &str,
        fields: Vec<String>,
        name: Option<&str>,
        unique: bool,
    ) -> Result<IndexInfo, String> {
        let index_name = match name {
            Some(n) => n.to_string(),
            None => {
                let mut id = self.next_id.write().unwrap();
                let name = format!("idx_{}_{}", collection.to_lowercase(), *id);
                *id += 1;
                name
            }
        };

        let index = IndexInfo {
            name: index_name,
            collection: collection.to_string(),
            fields: fields
                .into_iter()
                .map(|f| IndexFieldInfo {
                    name: f,
                    direction: Some("ASC".to_string()),
                })
                .collect(),
            unique,
        };

        self.indexes.write().unwrap().push(index.clone());
        Ok(index)
    }

    async fn list_indexes(&self, collection: Option<&str>) -> Result<Vec<IndexInfo>, String> {
        let indexes = self.indexes.read().unwrap();
        match collection {
            Some(col) => Ok(indexes
                .iter()
                .filter(|i| i.collection == col)
                .cloned()
                .collect()),
            None => Ok(indexes.clone()),
        }
    }

    async fn drop_index(&self, collection: &str, name: &str) -> Result<(), String> {
        let mut indexes = self.indexes.write().unwrap();
        let initial_len = indexes.len();
        indexes.retain(|i| !(i.collection == collection && i.name == name));
        if indexes.len() < initial_len {
            Ok(())
        } else {
            Err(format!(
                "index '{}' not found in collection '{}'",
                name, collection
            ))
        }
    }
}

/// Mock index operations that always fails with a configurable error.
#[derive(Debug, Clone)]
pub struct FailingMockIndexOperations {
    error: String,
}

impl FailingMockIndexOperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl IndexOperations for FailingMockIndexOperations {
    async fn create_index(
        &self,
        _collection: &str,
        _fields: Vec<String>,
        _name: Option<&str>,
        _unique: bool,
    ) -> Result<IndexInfo, String> {
        Err(self.error.clone())
    }

    async fn list_indexes(&self, _collection: Option<&str>) -> Result<Vec<IndexInfo>, String> {
        Err(self.error.clone())
    }

    async fn drop_index(&self, _collection: &str, _name: &str) -> Result<(), String> {
        Err(self.error.clone())
    }
}
