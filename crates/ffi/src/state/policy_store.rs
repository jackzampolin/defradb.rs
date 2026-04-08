use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use crate::policy_yaml::ParsedPolicy;

/// In-memory policy store for DAC policies.
///
/// Generates Go-compatible policy IDs using parsed-field hashing
/// combined with a monotonic sequence counter.
pub struct PolicyStore {
    policies: RwLock<HashMap<String, String>>,
    counter: AtomicU64,
}

impl Default for PolicyStore {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyStore {
    /// Create a new empty policy store.
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
            counter: AtomicU64::new(1), // Go's counter starts at 1 (GetNext returns currID + 1)
        }
    }

    /// Add a policy and return its Go-compatible ID.
    pub fn add_policy(&self, policy: &str, parsed: &ParsedPolicy) -> String {
        let policy_id = self.next_policy_id(parsed);
        self.policies
            .write()
            .insert(policy_id.clone(), policy.to_string());

        policy_id
    }

    /// Generate the next Go-compatible policy ID without storing the policy body.
    pub fn next_policy_id(&self, parsed: &ParsedPolicy) -> String {
        let counter_val = self.counter.fetch_add(1, Ordering::SeqCst);
        acp::policy_yaml::generate_policy_id(parsed, counter_val)
    }

    /// Store a policy with a known ID (used for SourceHub-created policies).
    pub fn store_policy(&self, id: &str, policy: &str) {
        self.policies
            .write()
            .insert(id.to_string(), policy.to_string());
    }

    /// Remove a policy from the cache.
    pub fn remove_policy(&self, id: &str) {
        self.policies.write().remove(id);
    }

    /// Get a policy by ID.
    pub fn get_policy(&self, id: &str) -> Option<String> {
        self.policies.read().get(id).cloned()
    }

    /// List all policy IDs.
    pub fn list_policies(&self) -> Vec<String> {
        self.policies.read().keys().cloned().collect()
    }
}
