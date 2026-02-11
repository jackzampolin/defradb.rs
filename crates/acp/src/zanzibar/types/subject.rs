//! Subject types and restrictions.

use identity::Did;
use serde::{Deserialize, Serialize};

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
