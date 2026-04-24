mod access_cache;
mod circuit_breaker;
pub mod cosmos;
pub mod hub_rs;
mod policy_cache;
mod provider;
mod tuning;

pub use cosmos::{CosmosProvider, SourceHubDocumentACP};
pub use hub_rs::HubRsProvider;
pub use provider::{
    AcpLightClientStatus, ProviderError, ProviderPolicyInfo, SourceHubProvider, SubjectRef,
};
pub use tuning::AcpTuning;

#[cfg(test)]
pub(crate) fn signing_state_test_guard() -> std::sync::MutexGuard<'static, ()> {
    use std::sync::{Mutex, OnceLock};

    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}
