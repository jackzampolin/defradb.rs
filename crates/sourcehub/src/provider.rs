use async_trait::async_trait;
use defra_core::thread_bounds::MaybeSendSync;

#[non_exhaustive]
pub enum SubjectRef {
    Actor(String),
    AllActors,
}

pub struct ProviderPolicyInfo {
    pub id: String,
    pub name: String,
    pub raw_policy: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AcpLightClientStatus {
    pub height: u64,
    pub module_state_root: String,
    pub cache_entries: usize,
    pub last_invalidation_height: u64,
    pub connected: bool,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ProviderError {
    #[error("query error: {0}")]
    Query(String),

    #[error("transaction error: {0}")]
    Transaction(String),

    #[error("config error: {0}")]
    Config(String),

    /// SourceHub is temporarily unreachable (circuit breaker open or timeout).
    /// All access decisions must fail-closed when this is returned.
    #[error("SourceHub unavailable: {0}")]
    Unavailable(String),

    /// The provider backend does not support the requested operation.
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait SourceHubProvider: MaybeSendSync {
    fn authorized_account(&self) -> String;

    async fn create_bearer_token(&self, did: &str) -> Result<String, ProviderError>;

    fn self_did(&self) -> Option<String>;

    async fn create_policy(&self, policy_yaml: &str) -> Result<String, ProviderError>;

    async fn register_object(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(), ProviderError>;

    async fn archive_object(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(), ProviderError>;

    async fn set_relationship(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &SubjectRef,
    ) -> Result<bool, ProviderError>;

    async fn delete_relationship(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &SubjectRef,
    ) -> Result<bool, ProviderError>;

    /// Emit a structured cross-object (object-edge) or userset subject to the
    /// backend, routed by `kind` per the frozen cross-object wire contract
    /// (`kind` 2 = object edge, `kind` 3 = userset). The `subject_*` fields are
    /// the codec output of [`zanzibar::encode_subject`].
    ///
    /// Backends that route through SourceHub policy commands may use the bearer
    /// token; typed-precompile backends may ignore it. The default rejects with
    /// [`ProviderError::Unsupported`].
    #[allow(clippy::too_many_arguments)]
    async fn set_relationship_subject(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        kind: u8,
        subject_resource: &str,
        subject_object_id: &str,
        subject_relation: &str,
    ) -> Result<bool, ProviderError> {
        let _ = (
            bearer_token,
            policy_id,
            resource,
            object_id,
            relation,
            kind,
            subject_resource,
            subject_object_id,
            subject_relation,
        );
        Err(ProviderError::Unsupported(
            "structured cross-object/userset subjects are not supported by this provider".into(),
        ))
    }

    /// Remove a structured cross-object or userset subject. Revoke mirror of
    /// [`set_relationship_subject`](Self::set_relationship_subject); the default
    /// rejects with [`ProviderError::Unsupported`].
    #[allow(clippy::too_many_arguments)]
    async fn delete_relationship_subject(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        kind: u8,
        subject_resource: &str,
        subject_object_id: &str,
        subject_relation: &str,
    ) -> Result<bool, ProviderError> {
        let _ = (
            bearer_token,
            policy_id,
            resource,
            object_id,
            relation,
            kind,
            subject_resource,
            subject_object_id,
            subject_relation,
        );
        Err(ProviderError::Unsupported(
            "structured cross-object/userset subjects are not supported by this provider".into(),
        ))
    }

    async fn query_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<ProviderPolicyInfo>, ProviderError>;

    async fn query_object_owner(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(bool, String), ProviderError>;

    async fn verify_access(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<bool, ProviderError>;

    async fn create_access_decision(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<Option<String>, ProviderError> {
        let _ = (policy_id, resource, object_id, permission, actor_did);
        Ok(None)
    }

    fn acp_light_client_status(&self) -> Result<AcpLightClientStatus, ProviderError> {
        Err(ProviderError::Unavailable(
            "ACP light client status is not available for this provider".into(),
        ))
    }
}
