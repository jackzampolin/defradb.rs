use serde::{Deserialize, Serialize};

use crate::did::Did;
use crate::error::Error;

use super::subject::Subject;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
    pub resource: String,
    pub object_id: String,
    pub relation: String,
    pub subject: Subject,
}

/// Characters forbidden in relationship fields because they are used as
/// path separators in storage keys.
const FORBIDDEN_CHARS: &[char] = &['/', '\\'];

fn validate_field(field_name: &str, value: &str) -> Result<(), Error> {
    if value.is_empty() {
        return Err(Error::InvalidRelationshipField {
            field: field_name.to_string(),
            reason: "must not be empty".to_string(),
        });
    }
    if let Some(ch) = value.chars().find(|c| FORBIDDEN_CHARS.contains(c)) {
        return Err(Error::InvalidRelationshipField {
            field: field_name.to_string(),
            reason: format!("contains forbidden character '{}'", ch),
        });
    }
    Ok(())
}

impl Relationship {
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

    /// Create a new Relationship with field validation.
    ///
    /// Validates that resource, object_id, and relation fields are non-empty
    /// and do not contain path separator characters (`/`, `\`) which would
    /// corrupt storage keys.
    pub fn try_new(
        resource: impl Into<String>,
        object_id: impl Into<String>,
        relation: impl Into<String>,
        subject: Subject,
    ) -> Result<Self, Error> {
        let resource = resource.into();
        let object_id = object_id.into();
        let relation = relation.into();
        validate_field("resource", &resource)?;
        validate_field("object_id", &object_id)?;
        validate_field("relation", &relation)?;
        Ok(Self {
            resource,
            object_id,
            relation,
            subject,
        })
    }

    pub fn with_entity(
        resource: impl Into<String>,
        object_id: impl Into<String>,
        relation: impl Into<String>,
        did: Did,
    ) -> Self {
        Self::new(resource, object_id, relation, Subject::Entity(did))
    }

    pub fn storage_key(&self) -> String {
        format!(
            "/rel/{}/{}/{}/{}",
            self.resource,
            self.object_id,
            self.relation,
            self.subject.storage_hash()
        )
    }

    pub fn object_prefix(resource: &str, object_id: &str) -> String {
        format!("/rel/{}/{}/", resource, object_id)
    }

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
