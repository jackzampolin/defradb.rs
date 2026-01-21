//! Policy lookup table for O(1) expression lookup.
//!
//! Provides fast lookup of relation expressions by (policy_id, resource, relation).

use std::collections::HashMap;

use super::expression::RelationExpression;
use super::types::Policy;
use crate::error::{Error, Result};

/// Lookup table for fast access to relation expressions.
///
/// Built from policies, provides O(1) lookup of relation expressions
/// by (policy_id, resource, relation) tuple.
#[derive(Debug, Default)]
pub struct PolicyLookupTable {
    /// Map: policy_id -> (resource -> (relation -> expression))
    policies: HashMap<String, HashMap<String, HashMap<String, RelationExpression>>>,
}

impl PolicyLookupTable {
    /// Create a new empty lookup table.
    pub fn new() -> Self {
        Self {
            policies: HashMap::new(),
        }
    }

    /// Add a policy to the lookup table.
    pub fn add_policy(&mut self, policy: &Policy) {
        let mut resources = HashMap::new();

        for resource in &policy.resources {
            let mut relations = HashMap::new();

            for relation in &resource.relations {
                relations.insert(relation.name.clone(), relation.expression.clone());
            }

            resources.insert(resource.name.clone(), relations);
        }

        self.policies.insert(policy.id.clone(), resources);
    }

    /// Remove a policy from the lookup table.
    pub fn remove_policy(&mut self, policy_id: &str) {
        self.policies.remove(policy_id);
    }

    /// Update (reload) a policy in the lookup table.
    /// Removes the old version and adds the new one.
    pub fn update_policy(&mut self, policy: &Policy) {
        self.remove_policy(&policy.id);
        self.add_policy(policy);
    }

    /// Clear all policies from the lookup table.
    pub fn clear(&mut self) {
        self.policies.clear();
    }

    /// Look up a relation expression.
    ///
    /// Returns the expression for the given (policy_id, resource, relation) tuple.
    pub fn get_expression(
        &self,
        policy_id: &str,
        resource: &str,
        relation: &str,
    ) -> Result<&RelationExpression> {
        self.policies
            .get(policy_id)
            .ok_or_else(|| Error::PolicyNotFound(policy_id.to_string()))?
            .get(resource)
            .ok_or_else(|| Error::ResourceNotFound(resource.to_string()))?
            .get(relation)
            .ok_or_else(|| Error::RelationNotFound {
                resource: resource.to_string(),
                relation: relation.to_string(),
            })
    }

    /// Check if a policy exists in the lookup table.
    pub fn has_policy(&self, policy_id: &str) -> bool {
        self.policies.contains_key(policy_id)
    }

    /// Check if a resource exists in a policy.
    pub fn has_resource(&self, policy_id: &str, resource: &str) -> bool {
        self.policies
            .get(policy_id)
            .map(|r| r.contains_key(resource))
            .unwrap_or(false)
    }

    /// Check if a relation exists in a resource.
    pub fn has_relation(&self, policy_id: &str, resource: &str, relation: &str) -> bool {
        self.policies
            .get(policy_id)
            .and_then(|r| r.get(resource))
            .map(|rel| rel.contains_key(relation))
            .unwrap_or(false)
    }

    /// Get all relation names for a resource.
    pub fn get_relations(&self, policy_id: &str, resource: &str) -> Option<Vec<String>> {
        self.policies
            .get(policy_id)
            .and_then(|r| r.get(resource).map(|rel| rel.keys().cloned().collect()))
    }

    /// Get all resource names for a policy.
    pub fn get_resources(&self, policy_id: &str) -> Option<Vec<String>> {
        self.policies
            .get(policy_id)
            .map(|r| r.keys().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zanzibar::types::{Relation, Resource};

    fn test_policy() -> Policy {
        Policy::new("policy1", "Test Policy")
            .with_resource(
                Resource::new("document")
                    .with_relation(Relation::direct("owner"))
                    .with_relation(Relation::computed(
                        "reader",
                        RelationExpression::union(vec![
                            RelationExpression::this(),
                            RelationExpression::computed_userset("owner"),
                        ]),
                    )),
            )
            .with_resource(Resource::new("folder").with_relation(Relation::direct("owner")))
    }

    #[test]
    fn test_add_policy() {
        let mut table = PolicyLookupTable::new();
        let policy = test_policy();

        table.add_policy(&policy);

        assert!(table.has_policy("policy1"));
        assert!(table.has_resource("policy1", "document"));
        assert!(table.has_resource("policy1", "folder"));
        assert!(table.has_relation("policy1", "document", "owner"));
        assert!(table.has_relation("policy1", "document", "reader"));
    }

    #[test]
    fn test_get_expression() {
        let mut table = PolicyLookupTable::new();
        let policy = test_policy();

        table.add_policy(&policy);

        // Direct relation
        let expr = table
            .get_expression("policy1", "document", "owner")
            .unwrap();
        assert!(expr.is_this());

        // Computed relation
        let expr = table
            .get_expression("policy1", "document", "reader")
            .unwrap();
        match expr {
            RelationExpression::Union(_) => {}
            _ => panic!("expected Union expression"),
        }
    }

    #[test]
    fn test_get_expression_not_found() {
        let mut table = PolicyLookupTable::new();
        let policy = test_policy();

        table.add_policy(&policy);

        // Missing policy
        let result = table.get_expression("missing", "document", "owner");
        assert!(matches!(result, Err(Error::PolicyNotFound(_))));

        // Missing resource
        let result = table.get_expression("policy1", "missing", "owner");
        assert!(matches!(result, Err(Error::ResourceNotFound(_))));

        // Missing relation
        let result = table.get_expression("policy1", "document", "missing");
        assert!(matches!(result, Err(Error::RelationNotFound { .. })));
    }

    #[test]
    fn test_remove_policy() {
        let mut table = PolicyLookupTable::new();
        let policy = test_policy();

        table.add_policy(&policy);
        assert!(table.has_policy("policy1"));

        table.remove_policy("policy1");
        assert!(!table.has_policy("policy1"));
    }

    #[test]
    fn test_get_relations() {
        let mut table = PolicyLookupTable::new();
        let policy = test_policy();

        table.add_policy(&policy);

        let relations = table.get_relations("policy1", "document").unwrap();
        assert_eq!(relations.len(), 2);
        assert!(relations.contains(&"owner".to_string()));
        assert!(relations.contains(&"reader".to_string()));
    }

    #[test]
    fn test_get_resources() {
        let mut table = PolicyLookupTable::new();
        let policy = test_policy();

        table.add_policy(&policy);

        let resources = table.get_resources("policy1").unwrap();
        assert_eq!(resources.len(), 2);
        assert!(resources.contains(&"document".to_string()));
        assert!(resources.contains(&"folder".to_string()));
    }
}
