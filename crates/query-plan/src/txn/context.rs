//! Transaction context trait.

use acp::error::{Error as AcpError, Result as AcpResult};
use acp::DocumentACP;
use acp::DocumentPermission;
use acp::Identity;
use defra_core::thread_bounds::MaybeBoxFuture;
use futures::FutureExt;
use identity::Did;
use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use crate::fetcher::CollectionProvider;
use crate::fetcher::DocFetcher;
use crate::mutator::DocMutator;

#[cfg(not(target_arch = "wasm32"))]
tokio::task_local! {
    static TXN_DEFERRED_ACP_MUTATIONS: Arc<DeferredAcpMutations>;
}

#[cfg(not(target_arch = "wasm32"))]
type DeferredAcpHook = Box<dyn FnOnce() -> MaybeBoxFuture<'static, Result<(), String>> + Send>;
#[cfg(target_arch = "wasm32")]
type DeferredAcpHook = Box<dyn FnOnce() -> MaybeBoxFuture<'static, Result<(), String>>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum ProjectedDocRegistration {
    Registered { owner: Did },
    Unregistered,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct DocRegistrationKey {
    policy_id: String,
    resource_name: String,
    doc_id: String,
}

struct DeferredAcpHookEntry {
    description: &'static str,
    callback: DeferredAcpHook,
}

#[derive(Default)]
struct DeferredAcpState {
    projected_registrations: HashMap<DocRegistrationKey, ProjectedDocRegistration>,
    hooks: Vec<DeferredAcpHookEntry>,
}

/// Deferred ACP mutations and their transaction-local registration projection.
///
/// Explicit database transactions use this to:
/// - buffer ACP writes until commit succeeds
/// - expose projected registration state to permission checks within the txn
#[derive(Default)]
pub struct DeferredAcpMutations {
    state: std::sync::Mutex<DeferredAcpState>,
}

impl DeferredAcpMutations {
    /// Create an empty deferred ACP state container.
    pub fn new() -> Self {
        Self::default()
    }

    fn doc_key(policy_id: &str, resource_name: &str, doc_id: &str) -> DocRegistrationKey {
        DocRegistrationKey {
            policy_id: policy_id.to_string(),
            resource_name: resource_name.to_string(),
            doc_id: doc_id.to_string(),
        }
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, DeferredAcpState>, String> {
        self.state
            .lock()
            .map_err(|_| "deferred ACP mutation state lock poisoned".to_string())
    }

    fn push_hook(
        &self,
        description: &'static str,
        callback: DeferredAcpHook,
    ) -> Result<(), String> {
        let mut state = self.lock_state()?;
        state.hooks.push(DeferredAcpHookEntry {
            description,
            callback,
        });
        Ok(())
    }

    fn with_request_bearer_token(
        did: &Did,
        token: Option<String>,
    ) -> Option<RequestBearerTokenGuard> {
        let token = token?;
        Some(RequestBearerTokenGuard::new(did.as_str(), token))
    }

    /// Return the projected registration state for a document within the current transaction.
    fn projected_registration(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> AcpResult<Option<ProjectedDocRegistration>> {
        let state = self.lock_state().map_err(AcpError::Storage)?;
        Ok(state
            .projected_registrations
            .get(&Self::doc_key(policy_id, resource_name, doc_id))
            .cloned())
    }

    /// Schedule a document registration to run after the storage transaction commits.
    pub fn schedule_register_doc_object(
        &self,
        acp: Arc<dyn DocumentACP>,
        identity: Did,
        policy_id: String,
        resource_name: String,
        doc_id: String,
        request_bearer_token: Option<String>,
    ) -> Result<(), String> {
        {
            let mut state = self.lock_state()?;
            state.projected_registrations.insert(
                Self::doc_key(&policy_id, &resource_name, &doc_id),
                ProjectedDocRegistration::Registered {
                    owner: identity.clone(),
                },
            );
        }

        let doc_id_for_log = doc_id.clone();
        let policy_id_for_log = policy_id.clone();
        self.push_hook(
            "register_doc_object",
            Box::new(move || {
                Box::pin(async move {
                    let _request_token_guard =
                        Self::with_request_bearer_token(&identity, request_bearer_token);
                    acp.register_doc_object(&identity, &policy_id, &resource_name, &doc_id)
                        .await
                        .map_err(|e| {
                            format!(
                                "failed to register ACP document '{}' for policy '{}': {}",
                                doc_id_for_log, policy_id_for_log, e
                            )
                        })
                })
            }),
        )
    }

    /// Schedule a document unregistration to run after the storage transaction commits.
    pub fn schedule_unregister_doc_object(
        &self,
        acp: Arc<dyn DocumentACP>,
        policy_id: String,
        resource_name: String,
        doc_id: String,
        caller_identity: Option<Did>,
        request_bearer_token: Option<String>,
    ) -> Result<(), String> {
        {
            let mut state = self.lock_state()?;
            state.projected_registrations.insert(
                Self::doc_key(&policy_id, &resource_name, &doc_id),
                ProjectedDocRegistration::Unregistered,
            );
        }

        let doc_id_for_log = doc_id.clone();
        let policy_id_for_log = policy_id.clone();
        self.push_hook(
            "unregister_doc_object",
            Box::new(move || {
                Box::pin(async move {
                    let _request_token_guard = caller_identity
                        .as_ref()
                        .and_then(|did| Self::with_request_bearer_token(did, request_bearer_token));

                    if let Err(err) = acp
                        .unregister_doc_object(&policy_id, &resource_name, &doc_id)
                        .await
                    {
                        tracing::warn!(
                            doc_id = %doc_id_for_log,
                            policy_id = %policy_id_for_log,
                            error = %err,
                            "Deferred ACP unregister failed after commit"
                        );
                    }

                    Ok(())
                })
            }),
        )
    }

    /// Run all deferred ACP hooks in registration order.
    pub async fn run_all_logged(&self) {
        let hooks = match self.lock_state() {
            Ok(mut state) => std::mem::take(&mut state.hooks),
            Err(err) => {
                tracing::error!(error = %err, "Failed to drain deferred ACP hooks");
                return;
            }
        };

        for hook in hooks {
            let description = hook.description;
            let result = AssertUnwindSafe((hook.callback)()).catch_unwind().await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::error!(
                        hook = description,
                        error = %err,
                        "Deferred ACP hook failed after transaction commit"
                    );
                }
                Err(panic) => {
                    tracing::error!(
                        hook = description,
                        panic = ?panic,
                        "Deferred ACP hook panicked after transaction commit"
                    );
                }
            }
        }
    }
}

struct RequestBearerTokenGuard {
    did: String,
    previous_token: Option<String>,
}

impl RequestBearerTokenGuard {
    fn new(did: &str, new_token: String) -> Self {
        let previous_token = defra_core::signing::get_request_bearer_token(did);
        defra_core::signing::set_request_bearer_token(did, new_token);
        Self {
            did: did.to_string(),
            previous_token,
        }
    }
}

impl Drop for RequestBearerTokenGuard {
    fn drop(&mut self) {
        if let Some(previous_token) = self.previous_token.take() {
            defra_core::signing::set_request_bearer_token(&self.did, previous_token);
        } else {
            defra_core::signing::clear_request_bearer_token(&self.did);
        }
    }
}

/// Execute a future with the given deferred ACP mutation projection in scope.
#[cfg(not(target_arch = "wasm32"))]
pub async fn scope_deferred_acp_mutations<Fut, T>(
    mutations: Arc<DeferredAcpMutations>,
    future: Fut,
) -> T
where
    Fut: Future<Output = T>,
{
    TXN_DEFERRED_ACP_MUTATIONS.scope(mutations, future).await
}

/// Execute a future with the given deferred ACP mutation projection in scope.
#[cfg(target_arch = "wasm32")]
pub async fn scope_deferred_acp_mutations<Fut, T>(
    _mutations: Arc<DeferredAcpMutations>,
    future: Fut,
) -> T
where
    Fut: Future<Output = T>,
{
    future.await
}

/// Return the current transaction's deferred ACP projection, if any.
pub fn current_deferred_acp_mutations() -> Option<Arc<DeferredAcpMutations>> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        TXN_DEFERRED_ACP_MUTATIONS
            .try_with(|mutations| mutations.clone())
            .ok()
    }

    #[cfg(target_arch = "wasm32")]
    {
        None
    }
}

/// Check document registration while honoring any ACP mutations buffered in the current txn.
pub async fn is_doc_registered_with_overlay(
    acp: &dyn DocumentACP,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
) -> AcpResult<bool> {
    if let Some(mutations) = current_deferred_acp_mutations() {
        if let Some(projected) =
            mutations.projected_registration(policy_id, resource_name, doc_id)?
        {
            return Ok(matches!(
                projected,
                ProjectedDocRegistration::Registered { .. }
            ));
        }
    }

    acp.is_doc_registered(policy_id, resource_name, doc_id)
        .await
}

/// Check document access while honoring any ACP mutations buffered in the current txn.
pub async fn check_doc_access_with_overlay(
    acp: &dyn DocumentACP,
    identity: &Identity,
    permission: DocumentPermission,
    policy_id: &str,
    resource_name: &str,
    doc_id: &str,
    node_did: Option<&Did>,
) -> AcpResult<bool> {
    if let (Some(node), Identity::Authenticated(requester)) = (node_did, identity) {
        if node == requester {
            return Ok(true);
        }
    }

    if let Some(mutations) = current_deferred_acp_mutations() {
        if let Some(projected) =
            mutations.projected_registration(policy_id, resource_name, doc_id)?
        {
            return Ok(match projected {
                ProjectedDocRegistration::Unregistered => true,
                ProjectedDocRegistration::Registered { owner } => {
                    matches!(identity, Identity::Authenticated(did) if did == &owner)
                }
            });
        }
    }

    acp.check_doc_access(identity, permission, policy_id, resource_name, doc_id)
        .await
}

/// Transaction context that provides storage access within a transaction.
///
/// This is implemented by the database layer to provide transaction-scoped
/// document fetching and mutation.
pub trait TransactionContext: MaybeSendSync {
    /// Get the transaction ID.
    fn id(&self) -> &str;

    /// Check if this is a read-only transaction.
    fn is_readonly(&self) -> bool;

    /// Get a document fetcher scoped to this transaction.
    fn doc_fetcher(&self) -> Arc<dyn DocFetcher>;

    /// Get a document mutator scoped to this transaction.
    ///
    /// Returns `None` if this is a read-only transaction or if mutators
    /// are not supported by this context implementation.
    ///
    /// The mutator shares the same underlying transaction as the fetcher,
    /// so all read and write operations are within the same transaction context.
    fn doc_mutator(&self) -> Option<Arc<dyn DocMutator>> {
        None
    }

    /// Get a collection provider scoped to this transaction.
    ///
    /// Returns `None` by default. Implementations that support transaction-scoped
    /// schema resolution should override this to return a provider that reads
    /// from the transaction's uncommitted state (falling back to the process-wide cache).
    fn collection_provider(&self) -> Option<Arc<dyn CollectionProvider>> {
        None
    }

    /// Get deferred ACP mutations for this transaction.
    ///
    /// Implementations with transactional ACP buffering should override this to
    /// return a shared state object that can be used both for commit-time hooks
    /// and for transaction-local ACP projections.
    fn deferred_acp_mutations(&self) -> Option<Arc<DeferredAcpMutations>> {
        None
    }

    /// Get the mutex that serializes top-level actions on this transaction.
    ///
    /// Implementations that share one underlying storage transaction across
    /// multiple public calls should override this so callers can prevent
    /// concurrent use of that handle.
    fn action_lock(&self) -> Option<Arc<async_lock::Mutex<()>>> {
        None
    }

    /// Check if the transaction is still active (not yet committed or rolled back).
    ///
    /// Returns `true` if the transaction can still be used for queries.
    /// Returns `false` if the transaction has been consumed via commit/rollback.
    ///
    /// # Implementation Note
    ///
    /// Concrete implementations SHOULD override this method if they track
    /// consumption state. The default returns `true`, which is appropriate
    /// for implementations that don't track state or where checking state
    /// synchronously isn't feasible (e.g., when state is behind an async mutex).
    fn is_active(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn projected_registration_reports_registered_and_owner_only_access() {
        let mutations = Arc::new(DeferredAcpMutations::new());
        let owner: Did = "did:key:z6MksQ6mYqY1YxR4KQ2NW6y6x8vJrNs2LEd8w5pYxHw3rroA"
            .parse()
            .expect("owner did");
        let stranger: Did = "did:key:z6MkwQm1zZ1p9raM2AfU4vX9y1Aq5PEYB9H9n1tFoZT3zh7T"
            .parse()
            .expect("stranger did");

        mutations
            .lock_state()
            .expect("state lock")
            .projected_registrations
            .insert(
                DeferredAcpMutations::doc_key("policy", "User", "doc-1"),
                ProjectedDocRegistration::Registered {
                    owner: owner.clone(),
                },
            );

        let access = scope_deferred_acp_mutations(
            mutations.clone(),
            check_doc_access_with_overlay(
                &NoopDocumentAcp,
                &Identity::Authenticated(owner),
                DocumentPermission::Read,
                "policy",
                "User",
                "doc-1",
                None,
            ),
        )
        .await
        .expect("owner access");
        assert!(access);

        let access = scope_deferred_acp_mutations(
            mutations,
            check_doc_access_with_overlay(
                &NoopDocumentAcp,
                &Identity::Authenticated(stranger),
                DocumentPermission::Read,
                "policy",
                "User",
                "doc-1",
                None,
            ),
        )
        .await
        .expect("stranger access");
        assert!(!access);
    }

    #[tokio::test]
    async fn projected_unregistration_reports_public_access() {
        let mutations = Arc::new(DeferredAcpMutations::new());
        mutations
            .lock_state()
            .expect("state lock")
            .projected_registrations
            .insert(
                DeferredAcpMutations::doc_key("policy", "User", "doc-1"),
                ProjectedDocRegistration::Unregistered,
            );

        let access = scope_deferred_acp_mutations(
            mutations.clone(),
            check_doc_access_with_overlay(
                &NoopDocumentAcp,
                &Identity::Anonymous,
                DocumentPermission::Read,
                "policy",
                "User",
                "doc-1",
                None,
            ),
        )
        .await
        .expect("anonymous access");
        assert!(access);

        let registered = scope_deferred_acp_mutations(
            mutations,
            is_doc_registered_with_overlay(&NoopDocumentAcp, "policy", "User", "doc-1"),
        )
        .await
        .expect("registration");
        assert!(!registered);
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl DocumentACP for NoopDocumentAcp {
        async fn register_doc_object(
            &self,
            _identity: &Did,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<()> {
            Ok(())
        }

        async fn is_doc_registered(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<bool> {
            Ok(false)
        }

        async fn get_doc_owner(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<Option<Did>> {
            Ok(None)
        }

        async fn check_doc_access(
            &self,
            _identity: &Identity,
            _permission: DocumentPermission,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<bool> {
            Ok(false)
        }

        async fn add_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> AcpResult<bool> {
            Ok(false)
        }

        async fn delete_actor_relationship(
            &self,
            _requestor: &Did,
            _target: &Did,
            _policy_id: &str,
            _collection_id: &str,
            _doc_id: &str,
            _relation: &str,
            _managing_relations: &[String],
        ) -> AcpResult<bool> {
            Ok(false)
        }

        async fn unregister_doc_object(
            &self,
            _policy_id: &str,
            _resource_name: &str,
            _doc_id: &str,
        ) -> AcpResult<()> {
            Ok(())
        }
    }

    struct NoopDocumentAcp;
}
