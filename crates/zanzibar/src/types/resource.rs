use serde::{Deserialize, Serialize};

use super::subject::SubjectRestriction;
use crate::expression::RelationExpression;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub name: String,
    pub relations: Vec<Relation>,
}

impl Resource {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            relations: Vec::new(),
        }
    }

    pub fn with_relation(mut self, relation: Relation) -> Self {
        self.relations.push(relation);
        self
    }

    pub fn get_relation(&self, name: &str) -> Option<&Relation> {
        self.relations.iter().find(|r| r.name == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relation {
    pub name: String,
    pub expression: RelationExpression,
    #[serde(default)]
    pub subject_restriction: Option<SubjectRestriction>,
    #[serde(default)]
    pub manages: Vec<String>,
}

impl Relation {
    pub fn direct(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            expression: RelationExpression::This,
            subject_restriction: None,
            manages: Vec::new(),
        }
    }

    pub fn computed(name: impl Into<String>, expression: RelationExpression) -> Self {
        Self {
            name: name.into(),
            expression,
            subject_restriction: None,
            manages: Vec::new(),
        }
    }

    pub fn with_restriction(mut self, restriction: SubjectRestriction) -> Self {
        self.subject_restriction = Some(restriction);
        self
    }

    pub fn with_manages(mut self, manages: Vec<impl Into<String>>) -> Self {
        self.manages = manages.into_iter().map(|s| s.into()).collect();
        self
    }
}
