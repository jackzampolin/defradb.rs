use serde::{Deserialize, Serialize};

use crate::did::Did;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SubjectRestriction {
    Entity,
    EntitySet {
        resource: String,
        relation: String,
    },
    TypedWildcard {
        resource: String,
    },
    /// An actor: a single entity (DID) or the all-actors wildcard (`*`). This is
    /// what `types: [actor]` maps to — unlike [`Entity`](Self::Entity), it also
    /// admits [`Subject::Wildcard`] so existing all-actors grants stay valid.
    Actor,
    /// Satisfied if the subject satisfies any inner restriction — how a relation
    /// declaring multiple `types:` is enforced.
    AnyOf(Vec<SubjectRestriction>),
    Any,
}

impl SubjectRestriction {
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
            SubjectRestriction::Actor => match subject {
                Subject::Entity(_) | Subject::Wildcard => Ok(()),
                _ => Err(format!(
                    "expected an actor (entity or '*'), got {}",
                    subject_type_name(subject)
                )),
            },
            SubjectRestriction::AnyOf(restrictions) => {
                if restrictions.iter().any(|r| r.satisfies(subject).is_ok()) {
                    Ok(())
                } else {
                    Err(format!(
                        "subject {} satisfies none of the relation's declared types",
                        subject_type_name(subject)
                    ))
                }
            }
            SubjectRestriction::Any => Ok(()),
        }
    }
}

fn subject_type_name(subject: &Subject) -> &'static str {
    match subject {
        Subject::Entity(_) => "Entity",
        Subject::EntitySet { .. } => "EntitySet",
        Subject::TypedWildcard { .. } => "TypedWildcard",
        Subject::Wildcard => "Wildcard",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Subject {
    Entity(Did),

    EntitySet {
        resource: String,
        object_id: String,
        relation: String,
    },

    TypedWildcard {
        resource: String,
    },

    Wildcard,
}

impl Subject {
    pub fn entity(did: Did) -> Self {
        Self::Entity(did)
    }

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

    pub fn typed_wildcard(resource: impl Into<String>) -> Self {
        Self::TypedWildcard {
            resource: resource.into(),
        }
    }

    pub fn wildcard() -> Self {
        Self::Wildcard
    }

    pub fn is_entity(&self) -> bool {
        matches!(self, Self::Entity(_))
    }

    pub fn is_entity_set(&self) -> bool {
        matches!(self, Self::EntitySet { .. })
    }

    pub fn is_typed_wildcard(&self) -> bool {
        matches!(self, Self::TypedWildcard { .. })
    }

    pub fn is_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard)
    }

    pub fn is_any_wildcard(&self) -> bool {
        matches!(self, Self::Wildcard | Self::TypedWildcard { .. })
    }

    pub fn as_entity(&self) -> Option<&Did> {
        match self {
            Self::Entity(did) => Some(did),
            _ => None,
        }
    }

    pub fn as_typed_wildcard_resource(&self) -> Option<&str> {
        match self {
            Self::TypedWildcard { resource } => Some(resource),
            _ => None,
        }
    }

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
