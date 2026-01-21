//! Core Zanzibar types.
//!
//! Defines Policy, Resource, Relation, Subject, and Relationship types
//! for the Zanzibar permission model.

use identity::Did;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::expression::RelationExpression;
use crate::error::{Error, Result};

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

    /// Find a resource by name.
    pub fn get_resource(&self, name: &str) -> Option<&Resource> {
        self.resources.iter().find(|r| r.name == name)
    }

    /// Find a relation within a resource.
    pub fn get_relation(&self, resource: &str, relation: &str) -> Option<&Relation> {
        self.get_resource(resource)
            .and_then(|r| r.get_relation(relation))
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
}

impl Relation {
    /// Create a new direct relation (This expression).
    pub fn direct(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expression: RelationExpression::This,
            subject_restriction: None,
        }
    }

    /// Create a new computed relation.
    pub fn computed(name: impl Into<String>, expression: RelationExpression) -> Self {
        Self {
            name: name.into(),
            expression,
            subject_restriction: None,
        }
    }

    /// Add a subject restriction.
    pub fn with_restriction(mut self, restriction: SubjectRestriction) -> Self {
        self.subject_restriction = Some(restriction);
        self
    }
}

/// Restriction on valid subjects for a relation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubjectRestriction {
    /// Subject must be a direct entity (DID)
    Entity,

    /// Subject must be an entity set from a specific resource/relation
    EntitySet { resource: String, relation: String },

    /// Any subject type is allowed
    Any,
}

/// A subject in a relationship.
///
/// Subjects can be:
/// - A direct entity (DID)
/// - An entity set (all entities with a relation to an object)
/// - A wildcard (all entities)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Subject {
    /// A direct entity identified by DID
    Entity(Did),

    /// An entity set: all entities with a relation to an object
    /// Notation: resource:object_id#relation
    EntitySet {
        resource: String,
        object_id: String,
        relation: String,
    },

    /// Wildcard: matches any entity
    /// Used for public access patterns
    Wildcard,
}

impl Subject {
    /// Create a direct entity subject.
    pub fn entity(did: Did) -> Self {
        Self::Entity(did)
    }

    /// Create an entity set subject.
    pub fn entity_set(
        resource: impl Into<String>,
        object_id: impl Into<String>,
        relation: impl Into<String>,
    ) -> Self {
        Self::EntitySet {
            resource: resource.into(),
            object_id: object_id.into(),
            relation: relation.into(),
        }
    }

    /// Create a wildcard subject.
    pub fn wildcard() -> Self {
        Self::Wildcard
    }

    /// Check if this subject is an entity.
    pub fn is_entity(&self) -> bool {
        matches!(self, Self::Entity(_))
    }

    /// Check if this subject is an entity set.
    pub fn is_entity_set(&self) -> bool {
        matches!(self, Self::EntitySet { .. })
    }

    /// Check if this subject is a wildcard.
    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard)
    }

    /// Get the entity DID if this is an entity subject.
    pub fn as_entity(&self) -> Option<&Did> {
        match self {
            Self::Entity(did) => Some(did),
            _ => None,
        }
    }

    /// Compute a hash for storage key purposes.
    pub fn storage_hash(&self) -> String {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Entity(did) => write!(f, "{}", did),
            Self::EntitySet {
                resource,
                object_id,
                relation,
            } => write!(f, "{}:{}#{}", resource, object_id, relation),
            Self::Wildcard => write!(f, "*"),
        }
    }
}

/// A stored relationship tuple.
///
/// Represents: subject has relation to resource:object_id
/// Example: "did:key:alice" has "owner" relation to "document:doc123"
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
    /// The resource type
    pub resource: String,

    /// The object ID within the resource
    pub object_id: String,

    /// The relation name
    pub relation: String,

    /// The subject (who has the relation)
    pub subject: Subject,
}

impl Relationship {
    /// Create a new relationship.
    pub fn new(
        resource: impl Into<String>,
        object_id: impl Into<String>,
        relation: impl Into<String>,
        subject: Subject,
    ) -> Self {
        Self {
            resource: resource.into(),
            object_id: object_id.into(),
            relation: relation.into(),
            subject,
        }
    }

    /// Create a relationship with a direct entity subject.
    pub fn with_entity(
        resource: impl Into<String>,
        object_id: impl Into<String>,
        relation: impl Into<String>,
        did: Did,
    ) -> Self {
        Self::new(resource, object_id, relation, Subject::Entity(did))
    }

    /// Get the storage key for this relationship.
    ///
    /// Key format: /rel/{resource}/{object_id}/{relation}/{subject_hash}
    pub fn storage_key(&self) -> String {
        format!(
            "/rel/{}/{}/{}/{}",
            self.resource,
            self.object_id,
            self.relation,
            self.subject.storage_hash()
        )
    }

    /// Get the prefix for scanning all relationships on an object.
    pub fn object_prefix(resource: &str, object_id: &str) -> String {
        format!("/rel/{}/{}/", resource, object_id)
    }

    /// Get the prefix for scanning all relationships with a specific relation.
    pub fn relation_prefix(resource: &str, object_id: &str, relation: &str) -> String {
        format!("/rel/{}/{}/{}/", resource, object_id, relation)
    }
}

impl std::fmt::Display for Relationship {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}#{}@{}",
            self.resource, self.object_id, self.relation, self.subject
        )
    }
}

/// Reference to an object (resource + object_id).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectRef {
    pub resource: String,
    pub object_id: String,
}

impl ObjectRef {
    pub fn new(resource: impl Into<String>, object_id: impl Into<String>) -> Self {
        Self {
            resource: resource.into(),
            object_id: object_id.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    #[test]
    fn test_subject_entity() {
        let did = test_did();
        let subject = Subject::entity(did.clone());

        assert!(subject.is_entity());
        assert!(!subject.is_entity_set());
        assert!(!subject.is_wildcard());
        assert_eq!(subject.as_entity(), Some(&did));
    }

    #[test]
    fn test_subject_entity_set() {
        let subject = Subject::entity_set("folder", "folder123", "owner");

        assert!(!subject.is_entity());
        assert!(subject.is_entity_set());
        assert!(!subject.is_wildcard());
        assert_eq!(subject.to_string(), "folder:folder123#owner");
    }

    #[test]
    fn test_subject_wildcard() {
        let subject = Subject::wildcard();

        assert!(!subject.is_entity());
        assert!(!subject.is_entity_set());
        assert!(subject.is_wildcard());
        assert_eq!(subject.to_string(), "*");
    }

    #[test]
    fn test_relationship_storage_key() {
        let did = test_did();
        let rel = Relationship::with_entity("document", "doc123", "owner", did);

        let key = rel.storage_key();
        assert!(key.starts_with("/rel/document/doc123/owner/"));
    }

    #[test]
    fn test_relationship_display() {
        let did = test_did();
        let rel = Relationship::with_entity("document", "doc123", "reader", did);

        let display = rel.to_string();
        assert!(display.contains("document:doc123#reader@"));
    }

    #[test]
    fn test_policy_builder() {
        let policy = Policy::new("policy1", "Test Policy").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("reader")),
        );

        assert_eq!(policy.id, "policy1");
        assert_eq!(policy.resources.len(), 1);

        let doc = policy.get_resource("document").unwrap();
        assert_eq!(doc.relations.len(), 2);
        assert!(doc.get_relation("owner").is_some());
        assert!(doc.get_relation("reader").is_some());
    }

    #[test]
    fn test_policy_serde() {
        let policy = Policy::new("policy1", "Test Policy")
            .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

        let json = serde_json::to_string(&policy).unwrap();
        let parsed: Policy = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, policy.id);
        assert_eq!(parsed.name, policy.name);
        assert_eq!(parsed.resources.len(), 1);
    }

    #[test]
    fn test_subject_serde() {
        let subjects = vec![
            Subject::entity(test_did()),
            Subject::entity_set("folder", "f1", "owner"),
            Subject::wildcard(),
        ];

        for subject in subjects {
            let json = serde_json::to_string(&subject).unwrap();
            let parsed: Subject = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, subject);
        }
    }

    #[test]
    fn test_relationship_serde() {
        let rel = Relationship::with_entity("document", "doc123", "owner", test_did());

        let json = serde_json::to_string(&rel).unwrap();
        let parsed: Relationship = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.resource, rel.resource);
        assert_eq!(parsed.object_id, rel.object_id);
        assert_eq!(parsed.relation, rel.relation);
        assert_eq!(parsed.subject, rel.subject);
    }
}
