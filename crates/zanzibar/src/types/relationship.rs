use serde::{Deserialize, Serialize};

use crate::did::Did;

use super::subject::Subject;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
    pub resource: String,
    pub object_id: String,
    pub relation: String,
    pub subject: Subject,
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
