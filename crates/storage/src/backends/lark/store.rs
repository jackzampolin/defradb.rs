use async_trait::async_trait;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use super::config::LarkStoreOptions;
use super::transaction::LarkTxn;
use crate::backends::shared::{ConflictTracker, DurabilityMode};
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// Pure Rust LSM-tree key-value store backed by lark-kv.
pub struct LarkStore {
    db: Arc<lark_kv::Db>,
    closed: AtomicBool,
    conflict_tracker: Arc<ConflictTracker>,
    db_path: std::path::PathBuf,
    active_txn_count: Arc<AtomicUsize>,
    close_timeout: std::time::Duration,
    durability: DurabilityMode,
}

impl LarkStore {
    /// Open a Lark database at the specified path with default options.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, LarkStoreOptions::default())
    }

    /// Open a Lark database at the specified path with custom options.
    pub fn open_with_options<P: AsRef<Path>>(path: P, opts: LarkStoreOptions) -> Result<Self> {
        let path = path.as_ref();
        let db_path = if path.extension().is_some() {
            path.parent().unwrap_or(path).join("data.lark")
        } else {
            path.join("data.lark")
        };

        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                Error::Backend(format!(
                    "failed to create directory '{}': {}",
                    parent.display(),
                    e
                ))
            })?;
        }

        let lark_opts = opts.to_lark_options();
        let db = lark_kv::Db::open(&db_path, lark_opts).map_err(|e| {
            tracing::error!(
                db_path = %db_path.display(),
                error = %e,
                "Failed to open Lark database"
            );
            Error::Backend(format!("failed to open lark db: {}", e))
        })?;

        Ok(Self {
            db: Arc::new(db),
            closed: AtomicBool::new(false),
            conflict_tracker: Arc::new(ConflictTracker::new()),
            db_path,
            active_txn_count: Arc::new(AtomicUsize::new(0)),
            close_timeout: opts.close_timeout(),
            durability: opts.durability(),
        })
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl crate::corekv::private::Sealed for LarkStore {}

#[async_trait]
impl Store for LarkStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::DBClosed);
        }
        self.active_txn_count.fetch_add(1, Ordering::AcqRel);
        if self.closed.load(Ordering::Acquire) {
            self.active_txn_count.fetch_sub(1, Ordering::AcqRel);
            return Err(Error::DBClosed);
        }

        Ok(Box::new(LarkTxn::new(
            Arc::clone(&self.db),
            Arc::clone(&self.conflict_tracker),
            Arc::clone(&self.active_txn_count),
            readonly,
            self.durability,
        )))
    }

    async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::Release) {
            return Ok(());
        }

        let active = self.active_txn_count.load(Ordering::Acquire);
        if active > 0 {
            tracing::info!(
                active_transactions = active,
                db_path = %self.db_path.display(),
                "Store closing with active transactions - waiting for completion"
            );

            let start = std::time::Instant::now();
            let timeout = self.close_timeout;
            while self.active_txn_count.load(Ordering::Acquire) > 0 {
                if start.elapsed() > timeout {
                    let remaining = self.active_txn_count.load(Ordering::Acquire);
                    tracing::error!(
                        remaining_transactions = remaining,
                        timeout_secs = timeout.as_secs(),
                        db_path = %self.db_path.display(),
                        "Failed to close store - transactions still active after timeout"
                    );
                    return Err(Error::Other(format!(
                        "Close timeout: {} transaction(s) still active after {}s (db: {})",
                        remaining,
                        timeout.as_secs(),
                        self.db_path.display()
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        self.db
            .close()
            .map_err(|e| Error::Backend(format!("failed to close lark: {}", e)))?;

        Ok(())
    }
}

#[async_trait]
impl Dropable for LarkStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        self.db
            .drop_all()
            .map_err(|e| Error::Backend(format!("failed to drop all: {}", e)))?;

        Ok(())
    }
}
