//! Resource and Relation types.

use serde::{Deserialize, Serialize};

use super::subject::SubjectRestriction;
use crate::zanzibar::expression::RelationExpression;

/// A resource type within a policy.
///
/// Resources represent object types (e.g., "document", "folder")
/// and define the relations that can exist on instances of that type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    /// Name of this resource type (e.g., "document", "folder")
    pub name: String,

    /// Relations defined on this resource
    pub relations: Vec<Relation>,
}

impl Resource {
    /// Create a new resource with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            relations: Vec::new(),
        }
    }

    /// Add a relation to this resource.
    pub fn with_relation(mut self, relation: Relation) -> Self {
        self.relations.push(relation);
        self
    }

    /// Find a relation by name.
    pub fn get_relation(&self, name: &str) -> Option<&Relation> {
        self.relations.iter().find(|r| r.name == name)
    }
}

/// A relation definition within a resource.
///
/// Relations can be:
/// - Direct (stored tuples) via `This`
/// - Computed via userset rewrite rules
/// - Combined via set operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    /// Name of this relation (e.g., "owner", "reader", "viewer")
    pub name: String,

    /// Expression defining how to compute this relation
    pub expression: RelationExpression,

    /// Optional restriction on valid subject types
    #[serde(default)]
    pub subject_restriction: Option<SubjectRestriction>,

    /// List of relations this relation can manage (grant/revoke).
    ///
    /// For example, an "admin" relation with `manages: ["reader", "updater", "deleter"]`
    /// allows actors with the "admin" relation to grant or revoke those relations
    /// on behalf of the owner.
    ///
    /// This implements the DefraDB delegation pattern where non-owners can manage
    /// certain relationships if they have the appropriate managing relation.
    #[serde(default)]
    pub manages: Vec<String>,
}

impl Relation {
    /// Create a new direct relation (This expression).
    pub fn direct(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expression: RelationExpression::This,
            subject_restriction: None,
            manages: Vec::new(),
        }
    }

    /// Create a new computed relation.
    pub fn computed(name: impl Into<String>, expression: RelationExpression) -> Self {
        Self {
            name: name.into(),
            expression,
            subject_restriction: None,
            manages: Vec::new(),
        }
    }

    /// Add a subject restriction.
    pub fn with_restriction(mut self, restriction: SubjectRestriction) -> Self {
        self.subject_restriction = Some(restriction);
        self
    }

    /// Add relations that this relation can manage.
    ///
    /// For example, an "admin" relation that manages ["reader", "updater", "deleter"]
    /// allows actors with "admin" to grant/revoke those relations.
    pub fn with_manages(mut self, manages: Vec<impl Into<String>>) -> Self {
        self.manages = manages.into_iter().map(|s| s.into()).collect();
        self
    }
}
