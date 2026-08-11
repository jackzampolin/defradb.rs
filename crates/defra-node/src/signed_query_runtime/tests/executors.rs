use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use defra_core::signing::{SigningConfig, SigningKeyType};
use query::{QueryExecutor, QueryRequest, TransactionError, TransactionHandle};

enum TestExecution {
    Slow {
        started: Arc<AtomicBool>,
        completed: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    },
    Spawning {
        completed: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    },
    ObserveContext {
        expected_did: String,
        expected_public_key_hex: String,
    },
}

pub(super) fn slow_signing_executor(
    started: Arc<AtomicBool>,
    completed: std::sync::mpsc::Sender<()>,
) -> Arc<dyn QueryExecutor> {
    Arc::new(TestExecution::Slow {
        started,
        completed: std::sync::Mutex::new(Some(completed)),
    })
}

pub(super) fn spawning_signing_executor(
    completed: std::sync::mpsc::Sender<()>,
) -> Arc<dyn QueryExecutor> {
    Arc::new(TestExecution::Spawning {
        completed: std::sync::Mutex::new(Some(completed)),
    })
}

pub(super) fn context_observing_executor(
    expected_did: String,
    expected_public_key_hex: String,
) -> Arc<dyn QueryExecutor> {
    Arc::new(TestExecution::ObserveContext {
        expected_did,
        expected_public_key_hex,
    })
}

pub(super) fn test_signing_config() -> SigningConfig {
    SigningConfig {
        key_type: SigningKeyType::Secp256r1,
        private_key_bytes: vec![1],
        public_key_bytes: vec![2],
        public_key_hex: "02".to_string(),
        remote_signer: None,
        signing_authorization: None,
    }
}

#[async_trait::async_trait]
impl QueryExecutor for TestExecution {
    async fn execute(&self, _request: QueryRequest) -> query::QueryResponse {
        match self {
            Self::Slow { started, completed } => {
                started.store(true, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                completed
                    .lock()
                    .expect("completion sender poisoned")
                    .take()
                    .expect("completion sender missing")
                    .send(())
                    .expect("completion receiver dropped");
            }
            Self::Spawning { completed } => {
                let completed = completed
                    .lock()
                    .expect("completion sender poisoned")
                    .take()
                    .expect("completion sender missing");
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    completed.send(()).expect("completion receiver dropped");
                });
            }
            Self::ObserveContext {
                expected_did,
                expected_public_key_hex,
            } => {
                for _ in 0..3 {
                    let signing_config = defra_core::signing::get_signing_config();
                    let current_identity = defra_core::current_identity::get_current_identity();
                    let batch_session_key = defra_core::batch_signing::get_batch_session_key();
                    if signing_config
                        .as_ref()
                        .map(|config| config.public_key_hex.as_str())
                        != Some(expected_public_key_hex.as_str())
                        || current_identity.as_deref() != Some(expected_did.as_str())
                        || batch_session_key.as_deref() != Some(expected_public_key_hex.as_str())
                    {
                        return query::QueryResponse::error(
                            "signed query context did not survive an await boundary",
                        );
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
        query::QueryResponse::success(serde_json::json!({"ok": true}))
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        _handle: &TransactionHandle,
    ) -> query::QueryResponse {
        self.execute(request).await
    }

    async fn begin_txn(
        &self,
        _readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        Err(TransactionError::not_supported("test executor"))
    }

    async fn commit_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported("test executor"))
    }

    async fn rollback_txn(
        &self,
        _handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        Err(TransactionError::not_supported("test executor"))
    }

    async fn schema(&self) -> query::Result<String> {
        Ok(String::new())
    }
}
