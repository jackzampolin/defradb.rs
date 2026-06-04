//! Collection lookup for the database.

use std::sync::Arc;

use crate::p2p_adapter::CollectionLookup;

/// Implementation of CollectionLookup for the database.
///
/// Retained for backward compatibility. Prefer `DbDocPusher` for new code.
pub struct DbCollectionLookup<S: storage::corekv::Store> {
    db: Arc<db::DB<S>>,
}

impl<S: storage::corekv::Store + 'static> DbCollectionLookup<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self { db }
    }

    pub fn new_arc(db: Arc<db::DB<S>>) -> Arc<dyn CollectionLookup> {
        Arc::new(Self::new(db))
    }
}

impl<S: storage::corekv::Store + 'static> CollectionLookup for DbCollectionLookup<S> {
    fn get_collection_id(&self, name: &str) -> Option<String> {
        match self.db.get_collection(name) {
            Ok(Some(collection)) => Some(collection.collection_id().to_string()),
            Ok(None) => {
                tracing::debug!(collection_name = %name, "Collection not found for P2P lookup");
                None
            }
            Err(e) => {
                tracing::warn!(
                    collection_name = %name,
                    error = %e,
                    "Error looking up collection for P2P"
                );
                None
            }
        }
    }
}
