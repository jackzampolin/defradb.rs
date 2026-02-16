mod parse;
mod validate;

pub use parse::{check_duplicate_yaml_keys, parse_policy_yaml};
pub use validate::validate_policy_expressions;

use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Generate a Go-compatible policy ID from parsed policy fields.
///
/// Matches Go's `acp_core` `IdTransformer.Transform` + `hashPol`:
/// 1. Inner hash: SHA256(name + sorted resources/relations/permissions)
/// 2. Outer hash: SHA256(inner_hash_bytes + counter_as_string)
pub fn generate_policy_id(parsed: &ParsedPolicy, counter: u64) -> String {
    let inner_hash = hash_policy_fields(parsed);

    let mut outer_hasher = Sha256::new();
    outer_hasher.update(&inner_hash);
    outer_hasher.update(format!("{}", counter).as_bytes());

    let hash = outer_hasher.finalize();
    hash.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hash_policy_fields(policy: &ParsedPolicy) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(policy.name.as_bytes());

    let mut resources: Vec<_> = policy.resources.iter().collect();
    resources.sort_by(|a, b| a.name.cmp(&b.name));

    for resource in resources {
        hasher.update(resource.name.as_bytes());

        let mut relations: Vec<_> = resource.relations.iter().collect();
        relations.sort_by(|a, b| a.name.cmp(&b.name));
        for relation in relations {
            hasher.update(relation.name.as_bytes());
        }

        let mut permissions: Vec<_> = resource.permissions.iter().collect();
        permissions.sort_by(|a, b| a.name.cmp(&b.name));
        for permission in permissions {
            hasher.update(permission.name.as_bytes());
            hasher.update(permission.expr.as_bytes());
        }
    }

    hasher.finalize().to_vec()
}

#[derive(Deserialize)]
pub struct ParsedPolicy {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub resources: Vec<PolicyResource>,
}

#[derive(Deserialize)]
pub struct PolicyResource {
    pub name: String,
    #[serde(default)]
    pub permissions: Vec<PolicyPermission>,
    #[serde(default)]
    pub relations: Vec<PolicyRelation>,
}

#[derive(Deserialize)]
pub struct PolicyPermission {
    pub name: String,
    #[serde(default)]
    pub expr: String,
}

#[derive(Deserialize)]
pub struct PolicyRelation {
    pub name: String,
    #[serde(default)]
    pub manages: Vec<String>,
}

impl ParsedPolicy {
    pub fn find_resource(&self, name: &str) -> Option<&PolicyResource> {
        self.resources.iter().find(|r| r.name == name)
    }
}

impl PolicyResource {
    pub fn has_permission(&self, name: &str) -> bool {
        self.permissions.iter().any(|p| p.name == name)
    }

    pub fn has_relation(&self, name: &str) -> bool {
        self.relations.iter().any(|r| r.name == name)
    }

    /// Get relation names that manage the given relation.
    ///
    /// For example, if "admin" has `manages: [reader]`, then
    /// `get_managers_for_relation("reader")` returns `["admin"]`.
    pub fn get_managers_for_relation(&self, relation: &str) -> Vec<&str> {
        self.relations
            .iter()
            .filter(|r| r.manages.iter().any(|m| m == relation))
            .map(|r| r.name.as_str())
            .collect()
    }
}
