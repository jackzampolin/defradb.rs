//! The regolith-backed store.
//!
//! regolith validates its own transactions, so there is no conflict
//! tracker, no commit gate and no read-set bookkeeping here. A
//! transaction is begun, used, and committed; the engine decides whether
//! it may land.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use regolith::{OptimisticTransactionDb, StreamOptions};

use super::config::RegolithStoreOptions;
use super::transaction::RegolithTxn;
use crate::backends::shared::TransactionStatsHandle;
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// Key-value store backed by regolith.
///
/// Cloning hands back another handle on the same database, not another
/// database: the close flag and the in-flight count are shared, so
/// closing through one handle closes it for all of them.
#[derive(Clone)]
pub struct RegolithStore {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    db: Arc<OptimisticTransactionDb>,
    options: RegolithStoreOptions,
    closed: AtomicBool,
    active_txns: Arc<AtomicUsize>,
    stats: TransactionStatsHandle,
    path: PathBuf,
    /// The mounted OPFS environment, kept so `persist` can reach it.
    ///
    /// Where the browser refuses synchronous access handles the database is
    /// resident in linear memory, and only `OpfsEnv::persist` writes it back.
    /// Dropping the handle at mount would leave no way to ask.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    opfs: Option<Arc<regolith::env::opfs::OpfsEnv>>,
}

impl RegolithStore {
    /// Open at `path` with the profile for this target.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_options(path, RegolithStoreOptions::default())
    }

    /// Open at `path` with explicit options.
    pub fn open_with_options<P: AsRef<Path>>(
        path: P,
        options: RegolithStoreOptions,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    Error::Backend(format!(
                        "failed to create directory '{}': {error}",
                        parent.display()
                    ))
                })?;
            }
        }
        let db = OptimisticTransactionDb::open(&path, options.engine.clone())
            .map_err(|error| Error::Backend(format!("failed to open regolith: {error}")))?
            .with_isolation(options.isolation);
        Ok(Self {
            inner: Arc::new(StoreInner {
                db: Arc::new(db),
                options,
                closed: AtomicBool::new(false),
                active_txns: Arc::new(AtomicUsize::new(0)),
                stats: TransactionStatsHandle::for_backend("regolith"),
                path,
                #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
                opfs: None,
            }),
        })
    }

    /// An in-memory database. Nothing reaches a filesystem, so this is
    /// the store for a test or a deliberately ephemeral node.
    pub fn in_memory() -> Result<Self> {
        Self::open_with_options("regolith-memory", RegolithStoreOptions::memory())
    }

    /// Open a store persisted in the browser's origin-private filesystem.
    ///
    /// `wasm32-unknown-unknown` has no filesystem of its own, so the engine
    /// takes one: this mounts OPFS on `db_name` and installs it before
    /// opening. The mount is asynchronous because acquiring the OPFS root is,
    /// which is why this cannot be a plain `open`.
    ///
    /// The mount probes for synchronous access handles and falls back to a
    /// resident mirror when the browser refuses them, which it does outside a
    /// Worker. The fallback keeps the database in linear memory, so it is
    /// bounded rather than unlimited.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub async fn open_opfs(db_name: &str) -> Result<Self> {
        use std::sync::Arc;

        let env = Arc::new(
            regolith::env::opfs::OpfsEnv::mount(
                db_name,
                regolith::env::opfs::OpfsOptions::default(),
            )
            .await
            .map_err(|error| Error::Backend(format!("failed to mount OPFS: {error}")))?,
        );

        let mut options = RegolithStoreOptions::wasm();
        options.engine.env = env.as_env();
        let store = Self::open_with_options(db_name, options)?;
        // Safe to reach in: nothing else holds this store yet.
        let mut inner = Arc::try_unwrap(store.inner)
            .map_err(|_| Error::Backend("store handle escaped during open".to_string()))?;
        inner.opfs = Some(env);
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Write everything buffered in linear memory back to OPFS.
    ///
    /// A no-op when the browser granted synchronous access handles, because
    /// those write through. In the mirror fallback this is what makes the
    /// database survive the tab.
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    pub async fn persist(&self) -> Result<()> {
        let Some(env) = self.inner.opfs.as_ref() else {
            return Ok(());
        };
        self.inner
            .db
            .db()
            .flush()
            .map_err(|error| Error::Backend(format!("flush before persist failed: {error}")))?;
        env.persist()
            .await
            .map_err(|error| Error::Backend(format!("OPFS persist failed: {error}")))
    }

    /// Where the database lives.
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// A writer that bounds its own memory rather than the caller's
    /// input, for a bulk load whose size the caller does not control.
    ///
    /// Each flush is atomic; the stream as a whole is not. Work that must
    /// land all-or-nothing belongs in a transaction.
    pub fn streaming_writer(&self, opts: StreamOptions) -> regolith::StreamingWriter<'_> {
        self.inner.db.db().streaming_writer(opts)
    }

    /// Wait for in-flight transactions to finish, up to the configured
    /// timeout.
    ///
    /// On wasm there is nothing to wait for: the runtime is
    /// single-threaded, so any transaction still counted here belongs to
    /// this task's own call stack and spinning would deadlock rather than
    /// let it finish. Report it instead.
    #[cfg(not(target_arch = "wasm32"))]
    async fn await_quiescence(&self) -> Result<()> {
        let deadline = web_time::Instant::now() + self.inner.options.close_timeout;
        let mut backoff = std::time::Duration::from_millis(1);
        while self.inner.active_txns.load(Ordering::Acquire) > 0 {
            if web_time::Instant::now() >= deadline {
                return Err(self.in_flight_error());
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_millis(50));
        }
        Ok(())
    }

    #[cfg(target_arch = "wasm32")]
    async fn await_quiescence(&self) -> Result<()> {
        if self.inner.active_txns.load(Ordering::Acquire) > 0 {
            return Err(self.in_flight_error());
        }
        Ok(())
    }

    fn in_flight_error(&self) -> Error {
        Error::Backend(format!(
            "closed with {} transaction(s) still in flight",
            self.inner.active_txns.load(Ordering::Acquire)
        ))
    }

    fn ensure_open(&self) -> Result<()> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(Error::DBClosed);
        }
        Ok(())
    }
}

impl crate::corekv::private::Sealed for RegolithStore {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Store for RegolithStore {
    fn transaction_stats_handle(&self) -> Option<TransactionStatsHandle> {
        Some(self.inner.stats.clone())
    }

    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        self.ensure_open()?;
        self.inner.active_txns.fetch_add(1, Ordering::AcqRel);
        // Closing between the check and the count would leak the count,
        // so re-check and hand it back rather than leaving `close`
        // waiting on a transaction that was never handed out.
        if self.inner.closed.load(Ordering::Acquire) {
            self.inner.active_txns.fetch_sub(1, Ordering::AcqRel);
            return Err(Error::DBClosed);
        }
        Ok(Box::new(RegolithTxn::new(
            &self.inner.db,
            readonly,
            self.inner.options.isolation,
            Arc::clone(&self.inner.active_txns),
            self.inner.stats.clone(),
        )))
    }

    async fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.await_quiescence().await?;
        self.inner
            .db
            .db()
            .close()
            .map_err(|error| Error::Backend(format!("failed to close regolith: {error}")))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Dropable for RegolithStore {
    async fn drop_all(&self) -> Result<()> {
        self.ensure_open()?;
        self.inner
            .db
            .db()
            .drop_all()
            .map_err(|error| Error::Backend(format!("drop_all failed: {error}")))
    }
}
