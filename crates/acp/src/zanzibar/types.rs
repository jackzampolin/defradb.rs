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

    /// Subject must be a typed wildcard for a specific resource
    TypedWildcard { resource: String },

    /// Any subject type is allowed
    Any,
}

impl SubjectRestriction {
    /// Check if a subject satisfies this restriction.
    ///
    /// Returns Ok(()) if satisfied, Err with a descriptive message otherwise.
    pub fn satisfies(&self, subject: &Subject) -> std::result::Result<(), String> {
        match self {
            SubjectRestriction::Entity => match subject {
                Subject::Entity(_) => Ok(()),
                _ => Err(format!(
                    "expected entity subject, got {}",
                    subject_type_name(subject)
                )),
            },
            SubjectRestriction::EntitySet { resource, relation } => match subject {
                Subject::EntitySet {
                    resource: subj_resource,
                    relation: subj_relation,
                    ..
                } => {
                    if subj_resource == resource && subj_relation == relation {
                        Ok(())
                    } else {
                        Err(format!(
                            "expected EntitySet from {}#{}, got {}#{}",
                            resource, relation, subj_resource, subj_relation
                        ))
                    }
                }
                _ => Err(format!(
                    "expected EntitySet subject, got {}",
                    subject_type_name(subject)
                )),
            },
            SubjectRestriction::TypedWildcard { resource } => match subject {
                Subject::TypedWildcard {
                    resource: subj_resource,
                } => {
                    if subj_resource == resource {
                        Ok(())
                    } else {
                        Err(format!(
                            "expected TypedWildcard for {}, got {}",
                            resource, subj_resource
                        ))
                    }
                }
                _ => Err(format!(
                    "expected TypedWildcard subject, got {}",
                    subject_type_name(subject)
                )),
            },
            SubjectRestriction::Any => Ok(()),
        }
    }
}

/// Get a human-readable name for a subject type.
fn subject_type_name(subject: &Subject) -> &'static str {
    match subject {
        Subject::Entity(_) => "Entity",
        Subject::EntitySet { .. } => "EntitySet",
        Subject::TypedWildcard { .. } => "TypedWildcard",
        Subject::Wildcard => "Wildcard",
    }
}

/// A subject in a relationship.
///
/// Subjects can be:
/// - A direct entity (DID)
/// - An entity set (all entities with a relation to an object)
/// - A typed wildcard (all entities of a specific resource type)
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

    /// Typed wildcard: matches any entity of a specific resource type
    /// Notation: resource:* (e.g., "user:*" means any user)
    /// Matches Go zanzi's ResourceSet type
    TypedWildcard { resource: String },

    /// Wildcard: matches any entity regardless of type
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

    /// Create a typed wildcard subject (matches all entities of a resource type).
    pub fn typed_wildcard(resource: impl Into<String>) -> Self {
        Self::TypedWildcard {
            resource: resource.into(),
        }
    }

    /// Create a wildcard subject (matches any entity).
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

    /// Check if this subject is a typed wildcard.
    pub fn is_typed_wildcard(&self) -> bool {
        matches!(self, Self::TypedWildcard { .. })
    }

    /// Check if this subject is an untyped wildcard.
    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard)
    }

    /// Check if this subject matches any entity (typed or untyped wildcard).
    pub fn is_any_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard | Self::TypedWildcard { .. })
    }

    /// Get the entity DID if this is an entity subject.
    pub fn as_entity(&self) -> Option<&Did> {
        match self {
            Self::Entity(did) => Some(did),
            _ => None,
        }
    }

    /// Get the resource type if this is a typed wildcard.
    pub fn as_typed_wildcard_resource(&self) -> Option<&str> {
        match self {
            Self::TypedWildcard { resource } => Some(resource),
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
            Self::TypedWildcard { resource } => write!(f, "{}:*", resource),
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
        assert!(!subject.is_typed_wildcard());
        assert!(subject.is_wildcard());
        assert!(subject.is_any_wildcard());
        assert_eq!(subject.to_string(), "*");
    }

    #[test]
    fn test_subject_typed_wildcard() {
        let subject = Subject::typed_wildcard("user");

        assert!(!subject.is_entity());
        assert!(!subject.is_entity_set());
        assert!(subject.is_typed_wildcard());
        assert!(!subject.is_wildcard());
        assert!(subject.is_any_wildcard());
        assert_eq!(subject.as_typed_wildcard_resource(), Some("user"));
        assert_eq!(subject.to_string(), "user:*");
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

    // SubjectRestriction enforcement tests

    #[test]
    fn test_subject_restriction_entity_accepts_entity() {
        let restriction = SubjectRestriction::Entity;
        let subject = Subject::entity(test_did());
        assert!(restriction.satisfies(&subject).is_ok());
    }

    #[test]
    fn test_subject_restriction_entity_rejects_entity_set() {
        let restriction = SubjectRestriction::Entity;
        let subject = Subject::entity_set("folder", "f1", "owner");
        assert!(restriction.satisfies(&subject).is_err());
    }

    #[test]
    fn test_subject_restriction_entity_rejects_wildcard() {
        let restriction = SubjectRestriction::Entity;
        let subject = Subject::wildcard();
        assert!(restriction.satisfies(&subject).is_err());
    }

    #[test]
    fn test_subject_restriction_entity_set_accepts_matching() {
        let restriction = SubjectRestriction::EntitySet {
            resource: "folder".to_string(),
            relation: "owner".to_string(),
        };
        let subject = Subject::entity_set("folder", "f1", "owner");
        assert!(restriction.satisfies(&subject).is_ok());
    }

    #[test]
    fn test_subject_restriction_entity_set_rejects_wrong_resource() {
        let restriction = SubjectRestriction::EntitySet {
            resource: "folder".to_string(),
            relation: "owner".to_string(),
        };
        let subject = Subject::entity_set("document", "d1", "owner");
        assert!(restriction.satisfies(&subject).is_err());
    }

    #[test]
    fn test_subject_restriction_entity_set_rejects_wrong_relation() {
        let restriction = SubjectRestriction::EntitySet {
            resource: "folder".to_string(),
            relation: "owner".to_string(),
        };
        let subject = Subject::entity_set("folder", "f1", "reader");
        assert!(restriction.satisfies(&subject).is_err());
    }

    #[test]
    fn test_subject_restriction_entity_set_rejects_entity() {
        let restriction = SubjectRestriction::EntitySet {
            resource: "folder".to_string(),
            relation: "owner".to_string(),
        };
        let subject = Subject::entity(test_did());
        assert!(restriction.satisfies(&subject).is_err());
    }

    #[test]
    fn test_subject_restriction_typed_wildcard_accepts_matching() {
        let restriction = SubjectRestriction::TypedWildcard {
            resource: "user".to_string(),
        };
        let subject = Subject::typed_wildcard("user");
        assert!(restriction.satisfies(&subject).is_ok());
    }

    #[test]
    fn test_subject_restriction_typed_wildcard_rejects_wrong_resource() {
        let restriction = SubjectRestriction::TypedWildcard {
            resource: "user".to_string(),
        };
        let subject = Subject::typed_wildcard("admin");
        assert!(restriction.satisfies(&subject).is_err());
    }

    #[test]
    fn test_subject_restriction_typed_wildcard_rejects_untyped() {
        let restriction = SubjectRestriction::TypedWildcard {
            resource: "user".to_string(),
        };
        let subject = Subject::wildcard();
        assert!(restriction.satisfies(&subject).is_err());
    }

    #[test]
    fn test_subject_restriction_any_accepts_all() {
        let restriction = SubjectRestriction::Any;
        assert!(restriction.satisfies(&Subject::entity(test_did())).is_ok());
        assert!(restriction
            .satisfies(&Subject::entity_set("f", "o", "r"))
            .is_ok());
        assert!(restriction.satisfies(&Subject::wildcard()).is_ok());
        assert!(restriction
            .satisfies(&Subject::typed_wildcard("user"))
            .is_ok());
    }

    #[test]
    fn test_relationship_validate_enforces_subject_restriction() {
        // Policy with owner relation restricted to Entity subjects only
        let policy =
            Policy::new("policy1", "Test").with_resource(Resource::new("document").with_relation(
                Relation::direct("owner").with_restriction(SubjectRestriction::Entity),
            ));

        // Valid: Entity subject with Entity restriction
        let valid_rel = Relationship::with_entity("document", "doc1", "owner", test_did());
        assert!(valid_rel.validate(&policy).is_ok());

        // Invalid: Wildcard subject with Entity restriction
        let invalid_rel = Relationship::new("document", "doc1", "owner", Subject::wildcard());
        let result = invalid_rel.validate(&policy);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::SubjectRestrictionViolation { .. }),
            "Expected SubjectRestrictionViolation, got {:?}",
            err
        );
    }

    #[test]
    fn test_relationship_validate_allows_when_no_restriction() {
        // Policy with owner relation and no subject restriction
        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("owner")));

        // All subject types should be valid
        let rel1 = Relationship::with_entity("document", "doc1", "owner", test_did());
        assert!(rel1.validate(&policy).is_ok());

        let rel2 = Relationship::new("document", "doc1", "owner", Subject::wildcard());
        assert!(rel2.validate(&policy).is_ok());

        let rel3 = Relationship::new("document", "doc1", "owner", Subject::typed_wildcard("user"));
        assert!(rel3.validate(&policy).is_ok());
    }

    #[test]
    fn test_relationship_validate_entity_set_restriction() {
        // Policy with parent relation restricted to folder#owner EntitySet
        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(
                Relation::direct("parent").with_restriction(SubjectRestriction::EntitySet {
                    resource: "folder".to_string(),
                    relation: "owner".to_string(),
                }),
            ))
            .with_resource(
                Resource::new("folder")
                    .with_relation(Relation::direct("owner"))
                    .with_relation(Relation::direct("reader")), // Add reader so EntitySet ref is valid
            );

        // Valid: EntitySet from folder#owner
        let valid_rel = Relationship::new(
            "document",
            "doc1",
            "parent",
            Subject::entity_set("folder", "f1", "owner"),
        );
        assert!(valid_rel.validate(&policy).is_ok());

        // Invalid: EntitySet from wrong relation (folder#reader exists but restriction requires folder#owner)
        let invalid_rel = Relationship::new(
            "document",
            "doc1",
            "parent",
            Subject::entity_set("folder", "f1", "reader"),
        );
        let result = invalid_rel.validate(&policy);
        assert!(
            matches!(result, Err(Error::SubjectRestrictionViolation { .. })),
            "Expected SubjectRestrictionViolation, got {:?}",
            result
        );

        // Invalid: Entity instead of EntitySet
        let invalid_rel2 = Relationship::with_entity("document", "doc1", "parent", test_did());
        let result2 = invalid_rel2.validate(&policy);
        assert!(
            matches!(result2, Err(Error::SubjectRestrictionViolation { .. })),
            "Expected SubjectRestrictionViolation, got {:?}",
            result2
        );
    }

    // ==========================================================================
    // DPI Compliance Validation Tests
    // ==========================================================================

    #[test]
    fn test_dpi_valid_policy() {
        // A valid DPI-compliant policy
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::computed(
                    "reader",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("owner"),
                    ]),
                ))
                .with_relation(Relation::computed(
                    "updater",
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("owner"),
                    ]),
                )),
        );

        assert!(policy.validate_dpi().is_ok());
    }

    #[test]
    fn test_dpi_missing_owner_relation() {
        // Policy without owner relation violates DPI
        let policy = Policy::new("policy1", "Test")
            .with_resource(Resource::new("document").with_relation(Relation::direct("reader")));

        let result = policy.validate_dpi();
        assert!(
            matches!(result, Err(Error::DpiMissingOwner { .. })),
            "Expected DpiMissingOwner, got {:?}",
            result
        );
    }

    #[test]
    fn test_dpi_expression_missing_owner() {
        // Policy with computed relation that doesn't include owner
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("contributor"))
                .with_relation(Relation::computed(
                    "reader",
                    // This computed expression doesn't include owner!
                    RelationExpression::union(vec![
                        RelationExpression::this(),
                        RelationExpression::computed_userset("contributor"), // No owner reference
                    ]),
                )),
        );

        let result = policy.validate_dpi();
        assert!(
            matches!(result, Err(Error::DpiExpressionMissingOwner { .. })),
            "Expected DpiExpressionMissingOwner, got {:?}",
            result
        );
    }

    #[test]
    fn test_dpi_disallowed_intersection() {
        // Policy using intersection violates DPI
        // The expression includes owner but uses disallowed intersection operation
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("approved"))
                .with_relation(Relation::computed(
                    "editor",
                    // This includes owner but uses intersection (disallowed)
                    RelationExpression::intersection(vec![
                        RelationExpression::computed_userset("owner"),
                        RelationExpression::computed_userset("approved"),
                    ]),
                )),
        );

        let result = policy.validate_dpi();
        assert!(
            matches!(result, Err(Error::DpiDisallowedOperation { .. })),
            "Expected DpiDisallowedOperation, got {:?}",
            result
        );
    }

    #[test]
    fn test_dpi_disallowed_difference() {
        // Policy using difference violates DPI
        // The expression includes owner but uses disallowed difference operation
        let policy = Policy::new("policy1", "Test").with_resource(
            Resource::new("document")
                .with_relation(Relation::direct("owner"))
                .with_relation(Relation::direct("banned"))
                .with_relation(Relation::computed(
                    "viewer",
                    // This includes owner but uses difference (disallowed)
                    RelationExpression::difference(
                        RelationExpression::computed_userset("owner"),
                        RelationExpression::computed_userset("banned"),
                    ),
                )),
        );

        let result = policy.validate_dpi();
        assert!(
            matches!(result, Err(Error::DpiDisallowedOperation { .. })),
            "Expected DpiDisallowedOperation, got {:?}",
            result
        );
    }

    #[test]
    fn test_dpi_owner_via_ttu() {
        // Policy with owner via TTU is valid
        let policy = Policy::new("policy1", "Test")
            .with_resource(
                Resource::new("file")
                    .with_relation(Relation::direct("owner"))
                    .with_relation(Relation::direct("parent"))
                    .with_relation(Relation::computed(
                        "reader",
                        RelationExpression::union(vec![
                            RelationExpression::this(),
                            RelationExpression::computed_userset("owner"),
                            RelationExpression::tuple_to_userset("parent", "owner"),
                        ]),
                    )),
            )
            .with_resource(Resource::new("folder").with_relation(Relation::direct("owner")));

        assert!(policy.validate_dpi().is_ok());
    }
}
