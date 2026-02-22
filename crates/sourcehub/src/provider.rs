use async_trait::async_trait;
use defra_core::thread_bounds::MaybeSendSync;

pub enum SubjectRef {
    Actor(String),
    AllActors,
}

pub struct ProviderPolicyInfo {
    pub id: String,
    pub name: String,
}

#[derive(Debug, thiserror::Error)]
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
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait SourceHubProvider: MaybeSendSync {
    fn authorized_account(&self) -> String;

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
}
