//! CollectionProvider implementation for the database.
//!
//! This module implements the `CollectionProvider` trait from the query crate,
//! enabling on-demand collection resolution from the database at query time.

use async_trait::async_trait;
use query::error::{QueryError, Result as QueryResult};
use query::fetcher::CollectionProvider;
use schema::CollectionVersion;
use std::sync::Arc;
use storage::corekv::Store;

use crate::database::DB;

/// Wrapper around Arc<DB> that implements CollectionProvider.
///
/// This enables the QueryRunner to resolve collections from the database
/// at query time, ensuring newly added schemas are immediately available.
pub struct DbCollectionProvider<S: Store + 'static> {
    db: Arc<DB<S>>,
}

impl<S: Store + 'static> DbCollectionProvider<S> {
    /// Create a new database collection provider.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }

    /// Create a new provider wrapped in an Arc.
    pub fn new_arc(db: Arc<DB<S>>) -> Arc<Self> {
        Arc::new(Self::new(db))
    }
}

#[async_trait]
impl<S: Store + 'static> CollectionProvider for DbCollectionProvider<S> {
    async fn get_collection(&self, name: &str) -> QueryResult<Option<Arc<CollectionVersion>>> {
        match self.db.get_collection(name) {
            Ok(Some(coll)) => Ok(Some(Arc::new(coll.schema().clone()))),
            Ok(None) => Ok(None),
            Err(e) => Err(QueryError::execution(e.to_string())),
        }
    }

    async fn list_collections(&self) -> QueryResult<Vec<String>> {
        self.db
            .list_collections()
            .map_err(|e| QueryError::execution(e.to_string()))
    }
}
