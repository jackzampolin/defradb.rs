mod parse;
mod validate;

pub use parse::{check_duplicate_yaml_keys, parse_policy_yaml};
pub use validate::validate_policy_expressions;

use serde::Deserialize;

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
