use std::collections::HashMap;

use parking_lot::RwLock;

/// In-memory cache of DAC policy documents, keyed by policy ID.
pub struct PolicyStore {
    policies: RwLock<HashMap<String, String>>,
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
        }
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
