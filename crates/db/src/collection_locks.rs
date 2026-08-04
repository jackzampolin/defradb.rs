use std::sync::Arc;

use async_lock::{Mutex, RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use storage::corekv::Store;

use crate::database::DB;
use crate::error::{Error, Result};
use crate::txn::DbTxn;

impl<S: Store> DB<S> {
    fn collection_lock(&self, collection_id: &str) -> Result<Arc<RwLock<()>>> {
        let mut locks = self
            .collection_locks
            .lock()
            .map_err(|_| Error::LockPoisoned("collection lock map poisoned".into()))?;
        Ok(locks
            .entry(collection_id.to_string())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone())
    }

    pub(crate) async fn collection_read_guard(
        &self,
        collection_id: &str,
    ) -> Result<RwLockReadGuardArc<()>> {
        Ok(self.collection_lock(collection_id)?.read_arc().await)
    }

    pub(crate) async fn acquire_collection_read_lock(
        &self,
        shared_txn: &Arc<Mutex<Option<DbTxn<S>>>>,
        collection_id: &str,
    ) -> Result<()> {
        let mut txn = shared_txn.lock().await;
        let txn = txn.as_mut().ok_or(Error::TxnNotActive)?;
        if txn.has_collection_guard(collection_id) {
            return Ok(());
        }

        let guard = self.collection_lock(collection_id)?.read_arc().await;
        txn.insert_collection_read_guard(collection_id.to_string(), guard);
        Ok(())
    }

    pub(crate) async fn acquire_collection_write_locks_for_txn(
        &self,
        txn: &mut DbTxn<S>,
        collection_ids: impl IntoIterator<Item = String>,
    ) -> Result<()> {
        let mut collection_ids: Vec<_> = collection_ids.into_iter().collect();
        collection_ids.sort();
        collection_ids.dedup();

        for collection_id in &collection_ids {
            if !txn.has_collection_write_guard(collection_id) {
                txn.remove_collection_guard(collection_id);
            }
        }

        for collection_id in collection_ids {
            if txn.has_collection_write_guard(&collection_id) {
                continue;
            }
            let guard = self.collection_lock(&collection_id)?.write_arc().await;
            txn.insert_collection_write_guard(collection_id, guard);
        }
        Ok(())
    }

    pub(crate) async fn collection_write_guards(
        &self,
        collection_ids: impl IntoIterator<Item = String>,
    ) -> Result<Vec<RwLockWriteGuardArc<()>>> {
        let mut collection_ids: Vec<_> = collection_ids.into_iter().collect();
        collection_ids.sort();
        collection_ids.dedup();

        let mut guards = Vec::with_capacity(collection_ids.len());
        for collection_id in collection_ids {
            guards.push(self.collection_lock(&collection_id)?.write_arc().await);
        }
        Ok(guards)
    }
}
