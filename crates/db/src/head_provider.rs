//! Document head provider implementation for P2P DocSync.
//!
//! This module provides a database-backed implementation of the `DocumentHeadProvider`
//! trait, allowing the P2P layer to query document heads for DocSync responses.

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use storage::corekv::{IterOptions, Store};
use storage::keys::headstore::HeadstoreDocKey;

use p2p::sync::DocumentHeadProvider;

use crate::database::DB;

/// Database-backed document head provider.
///
/// This implementation queries the headstore for composite head CIDs
/// to respond to DocSync requests from peers.
pub struct DbHeadProvider<S: Store> {
    db: Arc<DB<S>>,
}

impl<S: Store> DbHeadProvider<S> {
    /// Create a new DbHeadProvider with the given database.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl<S: Store + 'static> DocumentHeadProvider for DbHeadProvider<S> {
    async fn get_document_heads(&self, doc_id: &str) -> Result<Vec<Cid>, String> {
        // Create a read-only transaction to access the headstore
        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        let headstore = txn
            .headstore()
            .map_err(|e| format!("failed to get headstore: {}", e))?;

        // Query composite heads with prefix /d/{doc_id}/C/
        let prefix = HeadstoreDocKey::field_prefix(doc_id, "C");
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = headstore
            .iterator(opts)
            .await
            .map_err(|e| format!("failed to iterate headstore: {}", e))?;

        let mut cids = Vec::new();

        while let Some(pair) = iter
            .next()
            .await
            .map_err(|e| format!("headstore iteration error: {}", e))?
        {
            // Parse CID from key: /d/{doc_id}/C/{cid}
            let key_str = String::from_utf8_lossy(&pair.key);
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() < 5 {
                continue;
            }

            if let Ok(cid) = Cid::from_str(parts[4]) {
                cids.push(cid);
            }
        }

        iter.close()
            .await
            .map_err(|e| format!("headstore close error: {}", e))?;

        Ok(cids)
    }
}
