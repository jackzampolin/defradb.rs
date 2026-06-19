use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::relationship::Relationship;
use super::resource::{Relation, Resource};
use crate::error::{Error, Result};
use crate::expression::RelationExpression;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub id: String,
    pub name: String,
    pub resources: Vec<Resource>,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

impl Policy {
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            resources: Vec::new(),
            attributes: HashMap::new(),
        }
    }

    pub fn with_resource(mut self, resource: Resource) -> Self {
        self.resources.push(resource);
        self
    }

    pub fn get_resource(&self, name: &str) -> Option<&Resource> {
        self.resources.iter().find(|r| r.name == name)
    }

    pub fn get_relation(&self, resource: &str, relation: &str) -> Option<&Relation> {
        self.get_resource(resource)
            .and_then(|r| r.get_relation(relation))
    }

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

    pub fn validate(&self) -> Result<()> {
        for resource in &self.resources {
            for relation in &resource.relations {
                self.validate_expression(&resource.name, &relation.expression)?;
            }
        }
        Ok(())
    }

    pub fn validate_dpi(&self) -> Result<()> {
        for resource in &self.resources {
            if resource.get_relation("owner").is_none() {
                return Err(Error::DpiMissingOwner {
                    resource: resource.name.clone(),
                });
            }

            for relation in &resource.relations {
                if relation.name == "owner" {
                    continue;
                }

                if relation.expression.is_this() {
                    continue;
                }

                if !Self::expression_includes_owner(&relation.expression) {
                    return Err(Error::DpiExpressionMissingOwner {
                        resource: resource.name.clone(),
                        relation: relation.name.clone(),
                    });
                }

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

    fn expression_includes_owner(expr: &RelationExpression) -> bool {
        match expr {
            RelationExpression::This => false,
            RelationExpression::ComputedUserset { relation } => relation == "owner",
            RelationExpression::TupleToUserset {
                computed_relation, ..
            } => computed_relation == "owner",
            RelationExpression::Union(exprs) => exprs.iter().any(Self::expression_includes_owner),
            // Intersection requires `owner` in EVERY branch. An actor only gains
            // access through an intersection if they satisfy all branches, so the
            // "owner always has access" guarantee holds only when each branch grants
            // the owner (e.g. `(owner + a) & (owner + b)`). Using `.any()` here would
            // let `owner & reader` pass DPI while leaving a pure owner without access.
            RelationExpression::Intersection(exprs) => {
                exprs.iter().all(Self::expression_includes_owner)
            }
            RelationExpression::Difference { base, subtract } => {
                Self::expression_includes_owner(base) || Self::expression_includes_owner(subtract)
            }
        }
    }

    fn find_disallowed_operation(expr: &RelationExpression) -> Option<String> {
        match expr {
            RelationExpression::This => None,
            RelationExpression::ComputedUserset { .. } => None,
            RelationExpression::TupleToUserset { .. } => None,
            RelationExpression::Union(exprs) => {
                exprs.iter().find_map(Self::find_disallowed_operation)
            }
            RelationExpression::Intersection(exprs) => {
                exprs.iter().find_map(Self::find_disallowed_operation)
            }
            RelationExpression::Difference { base, subtract } => {
                Self::find_disallowed_operation(base)
                    .or_else(|| Self::find_disallowed_operation(subtract))
            }
        }
    }

    fn validate_expression(&self, resource_name: &str, expr: &RelationExpression) -> Result<()> {
        match expr {
            RelationExpression::This => Ok(()),
            RelationExpression::ComputedUserset { relation } => {
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
                if self.get_relation(resource_name, tuple_relation).is_none() {
                    return Err(Error::InvalidPolicy(format!(
                        "TupleToUserset references non-existent tuple relation '{}' in resource '{}'",
                        tuple_relation, resource_name
                    )));
                }
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
    pub fn validate(&self, policy: &Policy) -> Result<()> {
        use super::subject::Subject;

        let relation_def = policy
            .get_relation(&self.resource, &self.relation)
            .ok_or_else(|| Error::RelationNotFound {
                resource: self.resource.clone(),
                relation: self.relation.clone(),
            })?;

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
