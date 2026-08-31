//! Rebuild secondary indexes whose on-disk key encoding changed.
//!
//! DateTime index keys were one varint of nanoseconds and are now a marker plus
//! separate seconds and nanos varints, so entries written by an older build are
//! unreadable by this one. Nothing rebuilds them on its own: the existing
//! reindex path is driven by schema migration, not by a storage format change.
//!
//! Left alone, an upgraded store answers a range query from bounds in the new
//! format against entries in the old one, so rows go missing, deletes orphan
//! their entries, and a unique index stops rejecting duplicates. A stamped
//! version and a rebuild on open is the only thing that keeps those queries
//! honest.

use storage::corekv::Store;
use tracing::{info, warn};

use crate::error::{Error, Result};
use crate::DB;

/// Bumped whenever an index key encoding changes. Version 1 is the original
/// single-varint DateTime encoding; version 2 splits seconds from nanos.
pub const CURRENT_INDEX_FORMAT: u32 = 2;

/// Systemstore key holding the format the indexes on disk were written with.
pub const INDEX_FORMAT_KEY: &[u8] = b"/system/index_format";

impl<S: Store> DB<S> {
    /// Rebuild secondary indexes when the store predates the current encoding.
    ///
    /// A store with no stamp is either brand new, where the rebuild is a no-op
    /// because there are no documents, or written before the stamp existed, in
    /// which case it needs the rebuild. Both are handled by rebuilding and then
    /// stamping.
    pub(crate) async fn migrate_index_format(&self) -> Result<()>
    where
        S: 'static,
    {
        let stored = self.stored_index_format().await?;
        if stored == Some(CURRENT_INDEX_FORMAT) {
            return Ok(());
        }

        let stale: Vec<String> = self
            .get_all_active_collections_internal()?
            .into_iter()
            .filter(|collection| !collection.indexes.is_empty())
            .map(|collection| collection.name)
            .collect();

        if !stale.is_empty() {
            info!(
                from = ?stored,
                to = CURRENT_INDEX_FORMAT,
                collections = stale.len(),
                "rebuilding secondary indexes for the new key encoding"
            );
        }

        for name in stale {
            if let Err(error) = self.reindex_collection_with_migrations(&name).await {
                // Leaving the stamp unwritten means the next open retries, which
                // is the honest outcome: a half-rebuilt store must not report
                // itself as current.
                warn!(collection = %name, error = %error, "index rebuild failed");
                return Err(error);
            }
        }

        self.stamp_index_format().await
    }

    /// The format stamped on disk, or `None` on a store that predates it.
    pub async fn stored_index_format(&self) -> Result<Option<u32>> {
        let txn = self.new_txn(true).await?;
        let systemstore = txn.systemstore()?;
        let raw = systemstore
            .get(INDEX_FORMAT_KEY)
            .await
            .map_err(Error::Storage)?;
        let _ = txn.discard();

        Ok(raw
            .and_then(|bytes| <[u8; 4]>::try_from(bytes.as_ref()).ok())
            .map(u32::from_be_bytes))
    }

    async fn stamp_index_format(&self) -> Result<()> {
        let txn = self.new_txn(false).await?;
        txn.systemstore()?
            .set(INDEX_FORMAT_KEY, &CURRENT_INDEX_FORMAT.to_be_bytes())
            .await
            .map_err(Error::Storage)?;
        txn.commit().await
    }
}
