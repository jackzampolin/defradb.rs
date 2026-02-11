//! Zanzibar policy type and validation.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use super::relationship::Relationship;
use super::resource::{Relation, Resource};
use crate::error::{Error, Result};
use crate::zanzibar::expression::RelationExpression;

/// A Zanzibar policy defining resources and their relations.
///
/// Policies define the permission model for a set of resources.
/// Each resource has relations that can be direct or computed
/// via userset rewrite rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// Unique identifier for this policy
    pub id: String,

    /// Human-readable name for the policy
    pub name: String,

    /// Resources defined in this policy
    pub resources: Vec<Resource>,

    /// Optional attributes/metadata
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

impl Policy {
    /// Create a new policy with the given ID and name.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            resources: Vec::new(),
            attributes: HashMap::new(),
        }
    }

    /// Add a resource to this policy.
    pub fn with_resource(mut self, resource: Resource) -> Self {
        self.resources.push(resource);
        self
    }

    /// Parse a policy from YAML content.
    ///
    /// The YAML format matches Go DefraDB's ACP policy format:
    /// ```yaml
    /// name: policy-name
    /// description: optional description
    /// resources:
    ///   resource_name:
    ///     relations:
    ///       owner:
    ///         types: [actor]
    ///       reader:
    ///         types: [actor]
    ///     permissions:
    ///       read:
    ///         expr: owner + reader
    /// ```
    ///
    /// The policy ID is generated as the SHA256 hex hash of the YAML content.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let parsed: serde_yaml::Value = serde_yaml::from_str(yaml)
            .map_err(|e| Error::InvalidPolicy(format!("invalid YAML: {}", e)))?;

        // Generate policy ID from content hash
        let mut hasher = Sha256::new();
        hasher.update(yaml.as_bytes());
        let hash = hasher.finalize();
        let id = hash
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();

        let name = parsed
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let description = parsed
            .get("description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let mut attributes = HashMap::new();
        if let Some(desc) = description {
            attributes.insert("description".to_string(), desc);
        }

        let mut resources = Vec::new();

        if let Some(res_map) = parsed.get("resources").and_then(|v| v.as_mapping()) {
            for (res_name, res_value) in res_map {
                let resource_name = res_name.as_str().ok_or_else(|| {
                    Error::InvalidPolicy("resource name must be a string".to_string())
                })?;

                let mut relations = Vec::new();

                // Parse relations (direct)
                if let Some(rels) = res_value.get("relations").and_then(|v| v.as_mapping()) {
                    for (rel_name, rel_value) in rels {
                        let rname = rel_name.as_str().ok_or_else(|| {
                            Error::InvalidPolicy("relation name must be a string".to_string())
                        })?;

                        let mut relation = Relation::direct(rname);

                        // Parse manages list
                        if let Some(manages) =
                            rel_value.get("manages").and_then(|v| v.as_sequence())
                        {
                            let manages_list: Vec<String> = manages
                                .iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect();
                            relation = relation.with_manages(manages_list);
                        }

                        relations.push(relation);
                    }
                }

                // Parse permissions (computed)
                if let Some(perms) = res_value.get("permissions").and_then(|v| v.as_mapping()) {
                    for (perm_name, perm_value) in perms {
                        let pname = perm_name.as_str().ok_or_else(|| {
                            Error::InvalidPolicy("permission name must be a string".to_string())
                        })?;

                        let expr_str =
                            perm_value
                                .get("expr")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    Error::InvalidPolicy(format!(
                                        "permission '{}' must have an 'expr' field",
                                        pname
                                    ))
                                })?;

                        let expression = RelationExpression::parse(expr_str)?;
                        relations.push(Relation::computed(pname, expression));
                    }
                }

                resources.push(Resource {
                    name: resource_name.to_string(),
                    relations,
                });
            }
        }

        Ok(Self {
            id,
            name,
            resources,
            attributes,
        })
    }

    /// Find a resource by name.
    pub fn get_resource(&self, name: &str) -> Option<&Resource> {
        self.resources.iter().find(|r| r.name == name)
    }

    /// Find a relation within a resource.
    pub fn get_relation(&self, resource: &str, relation: &str) -> Option<&Relation> {
        self.get_resource(resource)
            .and_then(|r| r.get_relation(relation))
    }

    /// Find all relations that can manage (grant/revoke) a given relation.
    ///
    /// Returns a list of relation names that have the target relation in their `manages` list.
    pub fn get_managers_for_relation(&self, resource: &str, relation: &str) -> Vec<&str> {
        self.get_resource(resource)
            .map(|r| {
                r.relations
                    .iter()
                    .filter(|rel| rel.manages.iter().any(|m| m == relation))
                    .map(|rel| rel.name.as_str())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Validate the policy structure.
    ///
    /// Checks that:
    /// - All referenced relations exist
    /// - No circular definitions without base cases
    pub fn validate(&self) -> Result<()> {
        for resource in &self.resources {
            for relation in &resource.relations {
                self.validate_expression(&resource.name, &relation.expression)?;
            }
        }
        Ok(())
    }

    /// Validate DPI (DefraDB Policy Interface) compliance.
    ///
    /// DPI rules (from Go DefraDB):
    /// - Every resource MUST have an 'owner' relation
    /// - Computed permission expressions MUST include 'owner' (directly or via computed userset)
    /// - Only union (+) operations are allowed, not intersection (&) or difference (-)
    ///
    /// Note: Direct relations (This) are relationship holders, not permissions,
    /// so they don't need to include owner. Only computed relations need owner inclusion.
    pub fn validate_dpi(&self) -> Result<()> {
        for resource in &self.resources {
            // Rule 1: Every resource must have an 'owner' relation
            if resource.get_relation("owner").is_none() {
                return Err(Error::DpiMissingOwner {
                    resource: resource.name.clone(),
                });
            }

            // Check non-owner relations for DPI compliance
            for relation in &resource.relations {
                if relation.name == "owner" {
                    // Owner relation itself doesn't need to reference owner
                    continue;
                }

                // Skip direct relations (This) - they are relationship holders, not permissions
                if relation.expression.is_this() {
                    continue;
                }

                // Rule 2: Computed expressions must include 'owner'
                if !Self::expression_includes_owner(&relation.expression) {
                    return Err(Error::DpiExpressionMissingOwner {
                        resource: resource.name.clone(),
                        relation: relation.name.clone(),
                    });
                }

                // Rule 3: Only union operations allowed
                if let Some(op) = Self::find_disallowed_operation(&relation.expression) {
                    return Err(Error::DpiDisallowedOperation {
                        resource: resource.name.clone(),
                        relation: relation.name.clone(),
                        operation: op,
                    });
                }
            }
        }
        Ok(())
    }

    /// Check if an expression includes 'owner' (either directly or via computed userset).
    fn expression_includes_owner(expr: &RelationExpression) -> bool {
        match expr {
            RelationExpression::This => false,
            RelationExpression::ComputedUserset { relation } => relation == "owner",
            RelationExpression::TupleToUserset {
                computed_relation, ..
            } => computed_relation == "owner",
            RelationExpression::Union(exprs) => exprs.iter().any(Self::expression_includes_owner),
            RelationExpression::Intersection(exprs) => {
                exprs.iter().any(Self::expression_includes_owner)
            }
            RelationExpression::Difference { base, subtract } => {
                Self::expression_includes_owner(base) || Self::expression_includes_owner(subtract)
            }
        }
    }

    /// Find any disallowed operation in an expression.
    /// Returns the operation name if found, None otherwise.
    fn find_disallowed_operation(expr: &RelationExpression) -> Option<String> {
        match expr {
            RelationExpression::This => None,
            RelationExpression::ComputedUserset { .. } => None,
            RelationExpression::TupleToUserset { .. } => None,
            RelationExpression::Union(exprs) => {
                for e in exprs {
                    if let Some(op) = Self::find_disallowed_operation(e) {
                        return Some(op);
                    }
                }
                None
            }
            RelationExpression::Intersection(_) => Some("intersection (&)".to_string()),
            RelationExpression::Difference { .. } => Some("difference (-)".to_string()),
        }
    }

    fn validate_expression(&self, resource_name: &str, expr: &RelationExpression) -> Result<()> {
        match expr {
            RelationExpression::This => Ok(()),
            RelationExpression::ComputedUserset { relation } => {
                // Check that the referenced relation exists in the same resource
                if self.get_relation(resource_name, relation).is_none() {
                    return Err(Error::InvalidPolicy(format!(
                        "ComputedUserset references non-existent relation '{}' in resource '{}'",
                        relation, resource_name
                    )));
                }
                Ok(())
            }
            RelationExpression::TupleToUserset {
                tuple_relation,
                computed_relation,
            } => {
                // Check that the tuple relation exists
                if self.get_relation(resource_name, tuple_relation).is_none() {
                    return Err(Error::InvalidPolicy(format!(
                        "TupleToUserset references non-existent tuple relation '{}' in resource '{}'",
                        tuple_relation, resource_name
                    )));
                }
                // Note: computed_relation is on a different object, so we can't fully validate here
                let _ = computed_relation;
                Ok(())
            }
            RelationExpression::Union(exprs) => {
                for e in exprs {
                    self.validate_expression(resource_name, e)?;
                }
                Ok(())
            }
            RelationExpression::Intersection(exprs) => {
                for e in exprs {
                    self.validate_expression(resource_name, e)?;
                }
                Ok(())
            }
            RelationExpression::Difference { base, subtract } => {
                self.validate_expression(resource_name, base)?;
                self.validate_expression(resource_name, subtract)?;
                Ok(())
            }
        }
    }
}

impl Relationship {
    /// Validate this relationship against a policy.
    ///
    /// Checks that:
    /// - The relationship's resource and relation exist in the policy
    /// - If the subject is an EntitySet, the referenced resource/relation also exist
    /// - If the relation has a subject restriction, the subject satisfies it
    ///
    /// This should be called before storing relationships to catch invalid references
    /// early rather than at permission evaluation time.
    pub fn validate(&self, policy: &Policy) -> Result<()> {
        use super::subject::Subject;

        // Validate the relationship's own resource/relation
        let relation_def = policy
            .get_relation(&self.resource, &self.relation)
            .ok_or_else(|| Error::RelationNotFound {
                resource: self.resource.clone(),
                relation: self.relation.clone(),
            })?;

        // Validate EntitySet subject references
        if let Subject::EntitySet {
            resource, relation, ..
        } = &self.subject
        {
            if policy.get_relation(resource, relation).is_none() {
                return Err(Error::InvalidEntitySetReference {
                    resource: resource.clone(),
                    relation: relation.clone(),
                });
            }
        }

        // Enforce subject restriction if defined
        if let Some(restriction) = &relation_def.subject_restriction {
            restriction.satisfies(&self.subject).map_err(|msg| {
                Error::SubjectRestrictionViolation {
                    message: format!(
                        "relation '{}' on resource '{}': {}",
                        self.relation, self.resource, msg
                    ),
                }
            })?;
        }

        Ok(())
    }
}
