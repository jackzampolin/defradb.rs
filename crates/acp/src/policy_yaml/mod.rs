mod parse;
mod validate;

pub use parse::{check_duplicate_yaml_keys, parse_policy_yaml};
pub use validate::validate_policy_expressions;

use std::collections::HashMap;

use serde::Deserialize;
use sha2::{Digest, Sha256};
use zanzibar::{Policy, Relation, RelationExpression, Resource, SubjectRestriction};

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
    pub types: Vec<String>,
    #[serde(default)]
    pub manages: Vec<String>,
}

/// Map a relation's declared `types:` to an enforced [`SubjectRestriction`].
///
/// Each type names an allowed subject shape:
/// - `actor`               → an actor DID or the all-actors wildcard `*`
/// - `resource`            → a cross-object edge to an object of `resource`
/// - `resource->relation`  → a userset `resource:obj#relation`
///
/// The userset separator is acp_core's tuple-to-userset operator `->`, not `#`
/// (which is the tuple-subject grammar handled by `parse_target_subject`).
///
/// Multiple types combine as a union. An empty list yields `None` (no
/// restriction), preserving the behaviour of relations that omit `types:`.
fn build_subject_restriction(types: &[String]) -> crate::error::Result<Option<SubjectRestriction>> {
    let mut restrictions = Vec::with_capacity(types.len());
    for ty in types {
        let ty = ty.trim();
        let restriction = if ty == "actor" {
            SubjectRestriction::Actor
        } else if let Some((resource, relation)) = ty.split_once("->") {
            if resource.is_empty() || relation.is_empty() {
                return Err(crate::error::Error::InvalidRelation(format!(
                    "invalid relation type '{}': expected 'actor', 'resource', or 'resource->relation'",
                    ty
                )));
            }
            SubjectRestriction::EntitySet {
                resource: resource.to_string(),
                relation: relation.to_string(),
            }
        } else if ty.is_empty() {
            return Err(crate::error::Error::InvalidRelation(
                "empty relation type".to_string(),
            ));
        } else {
            // A bare resource name authorises a cross-object edge: an EntitySet
            // subject of that resource carrying no relation.
            SubjectRestriction::EntitySet {
                resource: ty.to_string(),
                relation: String::new(),
            }
        };
        restrictions.push(restriction);
    }

    Ok(match restrictions.len() {
        0 => None,
        1 => Some(restrictions.into_iter().next().unwrap()),
        _ => Some(SubjectRestriction::AnyOf(restrictions)),
    })
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

/// Build a Zanzibar Policy from an already-parsed YAML policy.
///
/// The `counter` parameter is a monotonic sequence number used together with
/// the parsed policy fields to generate Go-compatible policy IDs.
pub fn build_policy(parsed: &ParsedPolicy, counter: u64) -> crate::error::Result<Policy> {
    let id = generate_policy_id(parsed, counter);

    let mut attributes = HashMap::new();
    if !parsed.description.is_empty() {
        attributes.insert("description".to_string(), parsed.description.clone());
    }

    let mut resources = Vec::new();
    for res in &parsed.resources {
        let mut relations = Vec::new();

        // Auto-inject the reserved 'owner' relation (matches Go DefraDB behavior)
        relations.push(Relation::direct("owner"));

        for rel in &res.relations {
            let mut relation = Relation::direct(&rel.name);
            if !rel.manages.is_empty() {
                relation = relation.with_manages(rel.manages.clone());
            }
            if let Some(restriction) = build_subject_restriction(&rel.types)? {
                relation = relation.with_restriction(restriction);
            }
            relations.push(relation);
        }

        for perm in &res.permissions {
            let expression = if perm.expr.is_empty() {
                // A permission with no explicit expression is still valid and
                // defaults to owner-only access in Go.
                RelationExpression::computed_userset("owner")
            } else {
                let user_expr = RelationExpression::parse(&perm.expr)?;
                // DPI: every permission must include 'owner' in its expression
                RelationExpression::Union(vec![
                    RelationExpression::computed_userset("owner"),
                    user_expr,
                ])
            };
            relations.push(Relation::computed(&perm.name, expression));
        }

        resources.push(Resource {
            name: res.name.clone(),
            relations,
        });
    }

    Ok(Policy {
        id,
        name: parsed.name.clone(),
        resources,
        attributes,
    })
}
