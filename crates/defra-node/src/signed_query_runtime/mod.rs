use std::sync::Arc;
use std::time::Duration;

use defra_core::signing::SigningConfig;
use query::{QueryExecutor, QueryRequest, QueryResponse, TransactionHandle};

#[cfg(test)]
mod tests;

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn execute_with_signing_context(
    executor: Arc<dyn QueryExecutor>,
    request: QueryRequest,
    txn_handle: Option<TransactionHandle>,
    signing_config: SigningConfig,
    node_did: String,
    runtime_handle: tokio::runtime::Handle,
    signed_query_permit: SignedQueryPermit,
) -> QueryResponse {
    let spawn_handle = runtime_handle.clone();
    let batch_session_key = Some(signing_config.public_key_hex.clone());

    let run_query = move || {
        let _signed_query_permit = signed_query_permit;
        let _signing_guard = ThreadSigningContextGuard::install(signing_config, batch_session_key);
        let _id_guard = defra_core::current_identity::scoped_current_identity(Some(node_did));
        runtime_handle.block_on(async {
            match txn_handle {
                Some(txn_handle) => executor.execute_in_txn(request, &txn_handle).await,
                None => executor.execute(request).await,
            }
        })
    };
    let result = spawn_handle.spawn_blocking(run_query).await;

    match result {
        Ok(response) => response,
        Err(join_error) => {
            QueryResponse::error(format!("query execution task failed: {join_error}"))
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn unavailable_node_signer_response(node_did: &str) -> QueryResponse {
    QueryResponse::error(format!(
        "configured node signing identity {node_did} is unavailable"
    ))
}

struct ThreadSigningContextGuard {
    previous_signing_config: Option<SigningConfig>,
    previous_batch_session_key: Option<String>,
}

impl ThreadSigningContextGuard {
    fn install(signing_config: SigningConfig, batch_session_key: Option<String>) -> Self {
        let previous_signing_config = defra_core::signing::get_signing_config();
        let previous_batch_session_key = defra_core::batch_signing::get_batch_session_key();
        defra_core::signing::set_signing_config(Some(signing_config));
        defra_core::batch_signing::set_batch_session_key(batch_session_key);
        Self {
            previous_signing_config,
            previous_batch_session_key,
        }
    }
}

impl Drop for ThreadSigningContextGuard {
    fn drop(&mut self) {
        defra_core::signing::set_signing_config(self.previous_signing_config.take());
        defra_core::batch_signing::set_batch_session_key(self.previous_batch_session_key.take());
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) struct SignedQueryRuntime {
    handle: tokio::runtime::Handle,
    state: Arc<SignedQueryRuntimeState>,
    shutdown_tx: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    owner_thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[cfg(not(target_arch = "wasm32"))]
struct SignedQueryRuntimeState {
    closing: std::sync::atomic::AtomicBool,
    active_queries: std::sync::atomic::AtomicUsize,
    active_queries_drained: tokio::sync::Notify,
    active_queries_mutex: std::sync::Mutex<()>,
    active_queries_changed: std::sync::Condvar,
    closed: std::sync::atomic::AtomicBool,
    closed_notify: tokio::sync::Notify,
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) const SIGNED_QUERY_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(not(target_arch = "wasm32"))]
const SIGNED_QUERY_DROP_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(not(target_arch = "wasm32"))]
const SIGNED_QUERY_RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(not(target_arch = "wasm32"))]
pub(super) struct SignedQueryPermit {
    state: Arc<SignedQueryRuntimeState>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SignedQueryPermit {
    fn drop(&mut self) {
        let _active_queries_guard = self
            .state
            .active_queries_mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self
            .state
            .active_queries
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
            == 1
        {
            self.state.active_queries_drained.notify_waiters();
        }
        self.state.active_queries_changed.notify_all();
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct SignedQueryRuntimeClosedGuard {
    state: Arc<SignedQueryRuntimeState>,
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SignedQueryRuntimeClosedGuard {
    fn drop(&mut self) {
        self.state
            .closed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.state.closed_notify.notify_waiters();
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl SignedQueryRuntime {
    pub(super) fn new() -> Result<Self, String> {
        let state = Arc::new(SignedQueryRuntimeState {
            closing: std::sync::atomic::AtomicBool::new(false),
            active_queries: std::sync::atomic::AtomicUsize::new(0),
            active_queries_drained: tokio::sync::Notify::new(),
            active_queries_mutex: std::sync::Mutex::new(()),
            active_queries_changed: std::sync::Condvar::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
            closed_notify: tokio::sync::Notify::new(),
        });
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let (startup_tx, startup_rx) = std::sync::mpsc::sync_channel(1);
        let owner_state = state.clone();
        let owner_thread = std::thread::Builder::new()
            .name("defra-signed-query-owner".to_string())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .thread_name("defra-signed-query")
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = startup_tx.send(Err(format!(
                            "failed to create signed query runtime: {error}"
                        )));
                        return;
                    }
                };
                if startup_tx.send(Ok(runtime.handle().clone())).is_err() {
                    runtime.shutdown_background();
                    return;
                }
                let _closed_guard = SignedQueryRuntimeClosedGuard {
                    state: owner_state.clone(),
                };
                let _ = shutdown_rx.recv();
                wait_for_active_signed_queries(&owner_state, SIGNED_QUERY_DROP_DRAIN_TIMEOUT);
                runtime.shutdown_timeout(SIGNED_QUERY_RUNTIME_SHUTDOWN_TIMEOUT);
            })
            .map_err(|error| format!("failed to start signed query runtime owner: {error}"))?;
        let handle = match startup_rx.recv() {
            Ok(Ok(handle)) => handle,
            Ok(Err(error)) => {
                let _ = owner_thread.join();
                return Err(error);
            }
            Err(error) => {
                let _ = owner_thread.join();
                return Err(format!(
                    "signed query runtime owner exited during startup: {error}"
                ));
            }
        };
        Ok(Self {
            handle,
            state,
            shutdown_tx: std::sync::Mutex::new(Some(shutdown_tx)),
            owner_thread: std::sync::Mutex::new(Some(owner_thread)),
        })
    }

    pub(super) fn handle(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }

    pub(super) fn admit(&self) -> Option<SignedQueryPermit> {
        if self.state.closing.load(std::sync::atomic::Ordering::SeqCst) {
            return None;
        }
        self.state
            .active_queries
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.state.closing.load(std::sync::atomic::Ordering::SeqCst) {
            drop(SignedQueryPermit {
                state: self.state.clone(),
            });
            return None;
        }
        Some(SignedQueryPermit {
            state: self.state.clone(),
        })
    }

    pub(super) fn active_queries(&self) -> usize {
        self.state
            .active_queries
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn close_admission(&self) {
        self.state
            .closing
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    async fn wait_for_active_queries(&self) {
        loop {
            let notified = self.state.active_queries_drained.notified();
            if self.active_queries() == 0 {
                return;
            }
            notified.await;
        }
    }

    pub(super) async fn close_admission_and_wait_for(&self, timeout: Duration) -> bool {
        self.close_admission();
        tokio::time::timeout(timeout, self.wait_for_active_queries())
            .await
            .is_ok()
    }

    pub(super) async fn shutdown(&self) {
        self.signal_shutdown();
        let owner_thread = self
            .owner_thread
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(owner_thread) = owner_thread {
            let _ = tokio::task::spawn_blocking(move || owner_thread.join()).await;
        } else {
            loop {
                let notified = self.state.closed_notify.notified();
                if self.state.closed.load(std::sync::atomic::Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
        }
    }

    fn signal_shutdown(&self) {
        self.close_admission();
        let shutdown_tx = self
            .shutdown_tx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        if let Some(shutdown_tx) = shutdown_tx {
            let _ = shutdown_tx.send(());
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for SignedQueryRuntime {
    fn drop(&mut self) {
        self.signal_shutdown();
        self.owner_thread
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn wait_for_active_signed_queries(state: &SignedQueryRuntimeState, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    let mut guard = state
        .active_queries_mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    loop {
        if state
            .active_queries
            .load(std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            return true;
        }
        let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
            return false;
        };
        let (next_guard, wait_result) = state
            .active_queries_changed
            .wait_timeout(guard, remaining)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard = next_guard;
        if wait_result.timed_out() {
            return state
                .active_queries
                .load(std::sync::atomic::Ordering::SeqCst)
                == 0;
        }
    }
}
