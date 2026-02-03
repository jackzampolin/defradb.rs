//! Built-in NAC policy definition.
//!
//! The NAC policy uses the Zanzibar permission model with:
//! - `owner` relation: the node identity that enabled NAC
//! - `admin` relation: identities with admin access (can manage other relations)
//! - 34 permission relations: one for each NodePermission
//!
//! All permissions have expression: `owner + admin` meaning either the owner
//! or any admin can perform any operation.

use super::permission::NodePermission;
use crate::zanzibar::{Policy, Relation, RelationExpression, Resource};

/// The fixed policy ID for the NAC policy.
pub const NODE_POLICY_ID: &str = "defra-nac-policy";

/// The fixed resource name for node operations.
pub const NODE_RESOURCE_NAME: &str = "node";

/// Relation names for NAC.
pub const OWNER_RELATION: &str = "owner";
pub const ADMIN_RELATION: &str = "admin";

/// Create the built-in NAC policy.
///
/// This policy defines:
/// - `owner`: Direct relation for the node identity
/// - `admin`: Computed relation (owner + direct admin), can manage all non-owner relations
/// - All 34 permissions with expression `owner + admin`
///
/// The admin relation has `manages` set to all permission relation names,
/// allowing admins to grant/revoke any permission (except owner).
pub fn create_node_policy() -> Policy {
    let mut resource = Resource::new(NODE_RESOURCE_NAME)
        // Owner is a direct relation (the node identity)
        .with_relation(Relation::direct(OWNER_RELATION));

    // Collect all permission names for the admin's manages list
    let permission_names: Vec<String> = NodePermission::all()
        .iter()
        .map(|p| p.as_str().to_string())
        .collect();

    // Admin relation: owner + direct admins
    // Admin can manage all permission relations
    let admin_relation = Relation::computed(
        ADMIN_RELATION,
        RelationExpression::union(vec![
            RelationExpression::this(),
            RelationExpression::computed_userset(OWNER_RELATION),
        ]),
    )
    .with_manages(permission_names);

    resource = resource.with_relation(admin_relation);

    // Add a relation for each of the 34 permissions
    // Each permission has expression: owner + admin
    for perm in NodePermission::all() {
        let perm_relation = Relation::computed(
            perm.as_str(),
            RelationExpression::union(vec![
                RelationExpression::computed_userset(OWNER_RELATION),
                RelationExpression::computed_userset(ADMIN_RELATION),
            ]),
        );
        resource = resource.with_relation(perm_relation);
    }

    Policy::new(NODE_POLICY_ID, "Node Access Control Policy").with_resource(resource)
}

/// Validate the NAC policy is correctly configured.
///
/// This is a sanity check to ensure the policy has all expected relations.
pub fn validate_node_policy(policy: &Policy) -> Result<(), String> {
    let resource = policy
        .get_resource(NODE_RESOURCE_NAME)
        .ok_or_else(|| format!("missing resource '{}'", NODE_RESOURCE_NAME))?;

    // Check owner relation
    if resource.get_relation(OWNER_RELATION).is_none() {
        return Err(format!("missing '{}' relation", OWNER_RELATION));
    }

    // Check admin relation
    if resource.get_relation(ADMIN_RELATION).is_none() {
        return Err(format!("missing '{}' relation", ADMIN_RELATION));
    }

    // Check all permission relations
    for perm in NodePermission::all() {
        if resource.get_relation(perm.as_str()).is_none() {
            return Err(format!("missing permission relation '{}'", perm.as_str()));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_node_policy() {
        let policy = create_node_policy();

        assert_eq!(policy.id, NODE_POLICY_ID);
        assert_eq!(policy.name, "Node Access Control Policy");

        let resource = policy.get_resource(NODE_RESOURCE_NAME).unwrap();

        // Should have owner, admin, and 34 permission relations = 36 total
        assert_eq!(resource.relations.len(), 36);
    }

    #[test]
    fn test_validate_node_policy() {
        let policy = create_node_policy();
        assert!(validate_node_policy(&policy).is_ok());
    }

    #[test]
    fn test_node_policy_has_owner() {
        let policy = create_node_policy();
        let resource = policy.get_resource(NODE_RESOURCE_NAME).unwrap();
        assert!(resource.get_relation(OWNER_RELATION).is_some());
    }

    #[test]
    fn test_node_policy_has_admin() {
        let policy = create_node_policy();
        let resource = policy.get_resource(NODE_RESOURCE_NAME).unwrap();
        let admin = resource.get_relation(ADMIN_RELATION).unwrap();

        // Admin should manage all 34 permissions
        assert_eq!(admin.manages.len(), 33);
    }

    #[test]
    fn test_node_policy_has_all_permissions() {
        let policy = create_node_policy();
        let resource = policy.get_resource(NODE_RESOURCE_NAME).unwrap();

        for perm in NodePermission::all() {
            assert!(
                resource.get_relation(perm.as_str()).is_some(),
                "missing relation for {}",
                perm.as_str()
            );
        }
    }

    #[test]
    fn test_permission_expressions_include_owner() {
        let policy = create_node_policy();
        let resource = policy.get_resource(NODE_RESOURCE_NAME).unwrap();

        for perm in NodePermission::all() {
            let relation = resource.get_relation(perm.as_str()).unwrap();

            // Each permission should be a union including owner
            match &relation.expression {
                RelationExpression::Union(exprs) => {
                    let has_owner = exprs.iter().any(|e| {
                        matches!(e, RelationExpression::ComputedUserset { relation } if relation == OWNER_RELATION)
                    });
                    assert!(has_owner, "permission {} missing owner", perm.as_str());
                }
                _ => panic!("permission {} should be union expression", perm.as_str()),
            }
        }
    }
}
