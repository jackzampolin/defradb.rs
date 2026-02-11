//! Relation expression types and parsing.

mod parser;

use serde::{Deserialize, Serialize};

/// A relation expression defining how to compute membership.
///
/// These expressions implement Zanzibar's userset rewrite rules:
/// - `This`: Direct lookup of stored tuples
/// - `ComputedUserset`: Check a different relation on the same object
/// - `TupleToUserset`: Follow a relation, then check another relation
/// - `Union`: OR of multiple expressions (short-circuit)
/// - `Intersection`: AND of multiple expressions
/// - `Difference`: Left AND NOT right
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationExpression {
    /// Direct lookup: subject has this exact relation to the object.
    This,

    /// Computed userset: check a different relation on the same object.
    ComputedUserset { relation: String },

    /// Tuple-to-userset: follow a relation, then check another relation.
    TupleToUserset {
        tuple_relation: String,
        computed_relation: String,
    },

    /// Union of expressions (OR with short-circuit).
    Union(Vec<RelationExpression>),

    /// Intersection of expressions (AND).
    Intersection(Vec<RelationExpression>),

    /// Difference: base AND NOT subtract.
    Difference {
        base: Box<RelationExpression>,
        subtract: Box<RelationExpression>,
    },
}

impl RelationExpression {
    pub fn this() -> Self {
        Self::This
    }

    pub fn computed_userset(relation: impl Into<String>) -> Self {
        Self::ComputedUserset {
            relation: relation.into(),
        }
    }

    pub fn tuple_to_userset(
        tuple_relation: impl Into<String>,
        computed_relation: impl Into<String>,
    ) -> Self {
        Self::TupleToUserset {
            tuple_relation: tuple_relation.into(),
            computed_relation: computed_relation.into(),
        }
    }

    pub fn union(exprs: Vec<RelationExpression>) -> Self {
        Self::Union(exprs)
    }

    pub fn intersection(exprs: Vec<RelationExpression>) -> Self {
        Self::Intersection(exprs)
    }

    pub fn difference(base: RelationExpression, subtract: RelationExpression) -> Self {
        Self::Difference {
            base: Box::new(base),
            subtract: Box::new(subtract),
        }
    }

    pub fn is_this(&self) -> bool {
        matches!(self, Self::This)
    }
}

impl std::fmt::Display for RelationExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::This => write!(f, "_this"),
            Self::ComputedUserset { relation } => write!(f, "{}", relation),
            Self::TupleToUserset {
                tuple_relation,
                computed_relation,
            } => write!(f, "{}->{}", tuple_relation, computed_relation),
            Self::Union(exprs) => {
                let parts: Vec<_> = exprs.iter().map(|e| e.to_string()).collect();
                write!(f, "({})", parts.join(" + "))
            }
            Self::Intersection(exprs) => {
                let parts: Vec<_> = exprs.iter().map(|e| e.to_string()).collect();
                write!(f, "({})", parts.join(" & "))
            }
            Self::Difference { base, subtract } => {
                write!(f, "({} - {})", base, subtract)
            }
        }
    }
}
