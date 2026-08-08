use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use defra_core::signing::{SigningConfig, SigningKeyType};
use query::{QueryRequest, TransactionError, TransactionHandle};

use super::tests::{TestRemoteSigner, SIGNING_STORE_GUARD};
use super::{EmbeddedNode, ExecuteRetryPolicy, QueryExecutor};

struct IdentityRecordingExecutor {
    identities: std::sync::Mutex<Vec<Option<String>>>,
    remaining_conflicts: AtomicUsize,
}

impl IdentityRecordingExecutor {
    fn new(remaining_conflicts: usize) -> Self {
        Self {
            identities: std::sync::Mutex::new(Vec::new()),
            remaining_conflicts: AtomicUsize::new(remaining_conflicts),
        }
    }

    fn identities(&self) -> Vec<Option<String>> {
        self.identities
            .lock()
            .expect("recorded identities poisoned")
            .clone()
    }

    fn record(&self, request: &QueryRequest) {
        self.identities
            .lock()
            .expect("recorded identities poisoned")
            .push(request.identity.as_ref().map(ToString::to_string));
    }
}

#[async_trait::async_trait]
impl QueryExecutor for IdentityRecordingExecutor {
    async fn execute(&self, request: QueryRequest) -> query::QueryResponse {
        self.record(&request);
        if self.remaining_conflicts.load(Ordering::SeqCst) > 0 {
            self.remaining_conflicts.fetch_sub(1, Ordering::SeqCst);
            query::QueryResponse::transaction_conflict("test transaction conflict")
        } else {
            query::QueryResponse::success(serde_json::json!({"ok": true}))
        }
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        _handle: &TransactionHandle,
    ) -> query::QueryResponse {
        self.record(&request);
        query::QueryResponse::success(serde_json::json!({"ok": true}))
    }

    async fn begin_txn(&self, _readonly: bool) -> Result<TransactionHandle, TransactionError> {
        Ok(TransactionHandle::new("identity-test-txn".to_string()))
    }

    async fn commit_txn(&self, _handle: &TransactionHandle) -> Result<(), TransactionError> {
        Ok(())
    }

    async fn rollback_txn(&self, _handle: &TransactionHandle) -> Result<(), TransactionError> {
        Ok(())
    }

    async fn schema(&self) -> query::Result<String> {
        Ok(String::new())
    }
}

fn register_test_remote_node_identity(did: &str) {
    defra_core::signing::store_identity(
        did,
        SigningConfig {
            key_type: SigningKeyType::Secp256r1,
            private_key_bytes: Vec::new(),
            public_key_bytes: vec![2, 3, 4],
            public_key_hex: "020304".to_string(),
            remote_signer: Some(Arc::new(TestRemoteSigner)),
            signing_authorization: None,
        },
    );
}

async fn signed_node_with_executor(
    did: &str,
    executor: Arc<IdentityRecordingExecutor>,
) -> EmbeddedNode {
    register_test_remote_node_identity(did);
    let mut node = EmbeddedNode::builder()
        .with_node_identity_did(did)
        .build()
        .await
        .expect("build signed identity-observing node");
    node.runner = executor;
    node
}

fn no_retry() -> ExecuteRetryPolicy {
    ExecuteRetryPolicy::new(0, std::time::Duration::ZERO, std::time::Duration::ZERO)
}

#[tokio::test]
async fn builder_rejects_invalid_node_identity_did() {
    let _serial = SIGNING_STORE_GUARD.lock().await;
    defra_core::signing::clear_identity_store();
    let invalid_did = "invalid:key:zNode";
    register_test_remote_node_identity(invalid_did);

    let result = EmbeddedNode::builder()
        .with_node_identity_did(invalid_did)
        .build()
        .await;
    let error = match result {
        Ok(node) => {
            node.shutdown().await;
            panic!("invalid node identity DID must be rejected")
        }
        Err(error) => error,
    };

    assert!(
        error.to_string().contains("invalid node identity DID"),
        "{error:#}"
    );
    defra_core::signing::clear_identity_store();
}

#[tokio::test]
async fn embedded_execute_defaults_node_identity_and_preserves_explicit_actor() {
    let _serial = SIGNING_STORE_GUARD.lock().await;
    defra_core::signing::clear_identity_store();
    let node_did = "did:key:zEmbeddedNodeActor";
    let explicit_did = identity::Did::new("did:key:zExplicitActor").unwrap();
    let executor = Arc::new(IdentityRecordingExecutor::new(0));
    let node = signed_node_with_executor(node_did, executor.clone()).await;

    let defaulted = node.execute("query { defaulted }").await;
    assert!(!defaulted.has_errors(), "defaulted query failed");
    let explicit = node
        .execute_request_with_retry(
            QueryRequest::new("query { explicit }").with_identity(Some(explicit_did.clone())),
            no_retry(),
        )
        .await;
    assert!(!explicit.has_errors(), "explicit-actor query failed");
    assert_eq!(
        executor.identities(),
        vec![Some(node_did.to_string()), Some(explicit_did.to_string())]
    );

    node.shutdown().await;
    defra_core::signing::clear_identity_store();
}

#[tokio::test]
async fn embedded_retry_keeps_node_identity_on_every_attempt() {
    let _serial = SIGNING_STORE_GUARD.lock().await;
    defra_core::signing::clear_identity_store();
    let node_did = "did:key:zEmbeddedRetryActor";
    let executor = Arc::new(IdentityRecordingExecutor::new(2));
    let node = signed_node_with_executor(node_did, executor.clone()).await;

    let response = node
        .execute_with_retry(
            "mutation { retry }",
            ExecuteRetryPolicy::new(2, std::time::Duration::ZERO, std::time::Duration::ZERO),
        )
        .await;
    assert!(!response.has_errors(), "retry query failed");
    assert_eq!(
        executor.identities(),
        vec![
            Some(node_did.to_string()),
            Some(node_did.to_string()),
            Some(node_did.to_string()),
        ]
    );

    node.shutdown().await;
    defra_core::signing::clear_identity_store();
}

#[tokio::test]
async fn embedded_transaction_defaults_node_identity_and_preserves_explicit_actor() {
    let _serial = SIGNING_STORE_GUARD.lock().await;
    defra_core::signing::clear_identity_store();
    let node_did = "did:key:zEmbeddedTransactionActor";
    let explicit_did = identity::Did::new("did:key:zExplicitTransactionActor").unwrap();
    let executor = Arc::new(IdentityRecordingExecutor::new(0));
    let node = signed_node_with_executor(node_did, executor.clone()).await;
    let handle = TransactionHandle::new("identity-test-txn".to_string());

    let defaulted = node
        .execute_request_in_txn(QueryRequest::new("query { defaulted }"), &handle)
        .await;
    assert!(
        !defaulted.has_errors(),
        "defaulted transaction query failed"
    );
    let explicit = node
        .execute_request_in_txn(
            QueryRequest::new("query { explicit }").with_identity(Some(explicit_did.clone())),
            &handle,
        )
        .await;
    assert!(!explicit.has_errors(), "explicit transaction query failed");
    assert_eq!(
        executor.identities(),
        vec![Some(node_did.to_string()), Some(explicit_did.to_string())]
    );

    node.shutdown().await;
    defra_core::signing::clear_identity_store();
}

#[tokio::test]
async fn unsigned_embedded_execution_remains_anonymous() {
    let executor = Arc::new(IdentityRecordingExecutor::new(0));
    let mut node = EmbeddedNode::builder()
        .build()
        .await
        .expect("build unsigned identity-observing node");
    node.runner = executor.clone();
    let handle = TransactionHandle::new("anonymous-test-txn".to_string());

    let direct = node.execute("query { direct }").await;
    let retry = node.execute_with_retry("query { retry }", no_retry()).await;
    let transactional = node
        .execute_request_in_txn(QueryRequest::new("query { transaction }"), &handle)
        .await;

    assert!(!direct.has_errors());
    assert!(!retry.has_errors());
    assert!(!transactional.has_errors());
    assert_eq!(executor.identities(), vec![None, None, None]);

    node.shutdown().await;
}
