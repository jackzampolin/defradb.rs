//! Relation expression types and parsing.
//!
//! Defines the RelationExpression enum for userset rewrite rules
//! and provides parsing from string format.

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

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
    /// Create a This expression (direct lookup).
    pub fn this() -> Self {
        Self::This
    }

    /// Create a ComputedUserset expression.
    pub fn computed_userset(relation: impl Into<String>) -> Self {
        Self::ComputedUserset {
            relation: relation.into(),
        }
    }

    /// Create a TupleToUserset expression.
    pub fn tuple_to_userset(
        tuple_relation: impl Into<String>,
        computed_relation: impl Into<String>,
    ) -> Self {
        Self::TupleToUserset {
            tuple_relation: tuple_relation.into(),
            computed_relation: computed_relation.into(),
        }
    }

    /// Create a Union expression.
    pub fn union(exprs: Vec<RelationExpression>) -> Self {
        Self::Union(exprs)
    }

    /// Create an Intersection expression.
    pub fn intersection(exprs: Vec<RelationExpression>) -> Self {
        Self::Intersection(exprs)
    }

    /// Create a Difference expression.
    pub fn difference(base: RelationExpression, subtract: RelationExpression) -> Self {
        Self::Difference {
            base: Box::new(base),
            subtract: Box::new(subtract),
        }
    }

    /// Check if this is a This expression.
    pub fn is_this(&self) -> bool {
        matches!(self, Self::This)
    }

    /// Parse an expression from string format.
    ///
    /// Grammar:
    /// - `_this` -> This
    /// - `relation_name` -> ComputedUserset
    /// - `relation->computed` -> TupleToUserset
    /// - `expr + expr` -> Union
    /// - `expr & expr` -> Intersection
    /// - `expr - expr` -> Difference (note: "->" is NOT a difference operator)
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            return Err(Error::InvalidExpression("empty expression".into()));
        }

        // Check for union ('+')
        if let Some(pos) = find_operator(input, '+') {
            let left = Self::parse(&input[..pos])?;
            let right = Self::parse(&input[pos + 1..])?;
            return Ok(merge_union(left, right));
        }

        // Check for difference ('-') but NOT '->'
        if let Some(pos) = find_difference_operator(input) {
            let left = Self::parse(&input[..pos])?;
            let right = Self::parse(&input[pos + 1..])?;
            return Ok(Self::Difference {
                base: Box::new(left),
                subtract: Box::new(right),
            });
        }

        // Check for intersection ('&')
        if let Some(pos) = find_operator(input, '&') {
            let left = Self::parse(&input[..pos])?;
            let right = Self::parse(&input[pos + 1..])?;
            return Ok(merge_intersection(left, right));
        }

        // No operators, parse as single term
        parse_term(input)
    }
}

/// Find the position of an operator, respecting parentheses.
fn find_operator(input: &str, op: char) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in input.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ if c == op && depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Find a difference operator '-', but NOT '->'.
fn find_difference_operator(input: &str) -> Option<usize> {
    let mut depth = 0;
    let chars: Vec<char> = input.chars().collect();
    for i in 0..chars.len() {
        match chars[i] {
            '(' => depth += 1,
            ')' => depth -= 1,
            '-' if depth == 0 => {
                // Check if this is part of '->'
                if i + 1 < chars.len() && chars[i + 1] == '>' {
                    // This is '->', skip
                    continue;
                }
                return Some(i);
            }
            _ => {}
        }
    }
    None
}

/// Parse a single term (no operators).
fn parse_term(input: &str) -> Result<RelationExpression> {
    let input = input.trim();

    // Check for _this
    if input == "_this" {
        return Ok(RelationExpression::This);
    }

    // Check for tuple-to-userset (relation->computed)
    if let Some(arrow_pos) = input.find("->") {
        let tuple_relation = input[..arrow_pos].trim();
        let computed_relation = input[arrow_pos + 2..].trim();

        if tuple_relation.is_empty() {
            return Err(Error::InvalidExpression(
                "empty tuple relation in TupleToUserset".into(),
            ));
        }
        if computed_relation.is_empty() {
            return Err(Error::InvalidExpression(
                "empty computed relation in TupleToUserset".into(),
            ));
        }

        validate_identifier(tuple_relation)?;
        validate_identifier(computed_relation)?;

        return Ok(RelationExpression::TupleToUserset {
            tuple_relation: tuple_relation.into(),
            computed_relation: computed_relation.into(),
        });
    }

    // Otherwise it's a computed userset (just a relation name)
    validate_identifier(input)?;
    Ok(RelationExpression::ComputedUserset {
        relation: input.into(),
    })
}

/// Validate that a string is a valid identifier.
fn validate_identifier(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::InvalidExpression("empty identifier".into()));
    }

    let first = s.chars().next().unwrap();
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(Error::InvalidExpression(format!(
            "identifier must start with letter or underscore: '{}'",
            s
        )));
    }

    for c in s.chars() {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(Error::InvalidExpression(format!(
                "invalid character '{}' in identifier: '{}'",
                c, s
            )));
        }
    }

    Ok(())
}

/// Merge two expressions into a union, flattening nested unions.
fn merge_union(left: RelationExpression, right: RelationExpression) -> RelationExpression {
    let mut exprs = Vec::new();

    match left {
        RelationExpression::Union(inner) => exprs.extend(inner),
        other => exprs.push(other),
    }

    match right {
        RelationExpression::Union(inner) => exprs.extend(inner),
        other => exprs.push(other),
    }

    RelationExpression::Union(exprs)
}

/// Merge two expressions into an intersection, flattening nested intersections.
fn merge_intersection(left: RelationExpression, right: RelationExpression) -> RelationExpression {
    let mut exprs = Vec::new();

    match left {
        RelationExpression::Intersection(inner) => exprs.extend(inner),
        other => exprs.push(other),
    }

    match right {
        RelationExpression::Intersection(inner) => exprs.extend(inner),
        other => exprs.push(other),
    }

    RelationExpression::Intersection(exprs)
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
                write!(f, "{}", parts.join(" + "))
            }
            Self::Intersection(exprs) => {
                let parts: Vec<_> = exprs.iter().map(|e| e.to_string()).collect();
                write!(f, "{}", parts.join(" & "))
            }
            Self::Difference { base, subtract } => {
                write!(f, "{} - {}", base, subtract)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_this_expression() {
        let expr = RelationExpression::this();
        assert!(expr.is_this());
        assert_eq!(expr.to_string(), "_this");
    }

    #[test]
    fn test_computed_userset() {
        let expr = RelationExpression::computed_userset("owner");
        assert!(!expr.is_this());
        assert_eq!(expr.to_string(), "owner");

        match expr {
            RelationExpression::ComputedUserset { relation } => {
                assert_eq!(relation, "owner");
            }
            _ => panic!("expected ComputedUserset"),
        }
    }

    #[test]
    fn test_tuple_to_userset() {
        let expr = RelationExpression::tuple_to_userset("parent", "owner");
        assert_eq!(expr.to_string(), "parent->owner");

        match expr {
            RelationExpression::TupleToUserset {
                tuple_relation,
                computed_relation,
            } => {
                assert_eq!(tuple_relation, "parent");
                assert_eq!(computed_relation, "owner");
            }
            _ => panic!("expected TupleToUserset"),
        }
    }

    #[test]
    fn test_union() {
        let expr = RelationExpression::union(vec![
            RelationExpression::this(),
            RelationExpression::computed_userset("reader"),
        ]);
        assert_eq!(expr.to_string(), "_this + reader");
    }

    #[test]
    fn test_intersection() {
        let expr = RelationExpression::intersection(vec![
            RelationExpression::computed_userset("member"),
            RelationExpression::computed_userset("approved"),
        ]);
        assert_eq!(expr.to_string(), "member & approved");
    }

    #[test]
    fn test_difference() {
        let expr = RelationExpression::difference(
            RelationExpression::computed_userset("member"),
            RelationExpression::computed_userset("banned"),
        );
        assert_eq!(expr.to_string(), "member - banned");
    }

    #[test]
    fn test_parse_this() {
        let expr = RelationExpression::parse("_this").unwrap();
        assert!(expr.is_this());
    }

    #[test]
    fn test_parse_computed_userset() {
        let expr = RelationExpression::parse("owner").unwrap();
        assert_eq!(
            expr,
            RelationExpression::ComputedUserset {
                relation: "owner".into()
            }
        );
    }

    #[test]
    fn test_parse_tuple_to_userset() {
        let expr = RelationExpression::parse("parent->owner").unwrap();
        assert_eq!(
            expr,
            RelationExpression::TupleToUserset {
                tuple_relation: "parent".into(),
                computed_relation: "owner".into()
            }
        );
    }

    #[test]
    fn test_parse_union() {
        let expr = RelationExpression::parse("owner + reader").unwrap();
        match expr {
            RelationExpression::Union(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_parse_union_three() {
        let expr = RelationExpression::parse("owner + reader + editor").unwrap();
        match expr {
            RelationExpression::Union(exprs) => {
                assert_eq!(exprs.len(), 3);
            }
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_parse_intersection() {
        let expr = RelationExpression::parse("member & approved").unwrap();
        match expr {
            RelationExpression::Intersection(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("expected Intersection"),
        }
    }

    #[test]
    fn test_parse_difference() {
        let expr = RelationExpression::parse("member - banned").unwrap();
        match expr {
            RelationExpression::Difference { .. } => {}
            _ => panic!("expected Difference"),
        }
    }

    #[test]
    fn test_parse_complex() {
        // owner + parent->owner
        let expr = RelationExpression::parse("owner + parent->owner").unwrap();
        match expr {
            RelationExpression::Union(exprs) => {
                assert_eq!(exprs.len(), 2);
                assert_eq!(
                    exprs[0],
                    RelationExpression::ComputedUserset {
                        relation: "owner".into()
                    }
                );
                assert_eq!(
                    exprs[1],
                    RelationExpression::TupleToUserset {
                        tuple_relation: "parent".into(),
                        computed_relation: "owner".into()
                    }
                );
            }
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_parse_whitespace() {
        let expr = RelationExpression::parse("  owner  +  reader  ").unwrap();
        match expr {
            RelationExpression::Union(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("expected Union"),
        }
    }

    #[test]
    fn test_parse_empty_error() {
        assert!(RelationExpression::parse("").is_err());
        assert!(RelationExpression::parse("   ").is_err());
    }

    #[test]
    fn test_parse_invalid_identifier() {
        assert!(RelationExpression::parse("123invalid").is_err());
        assert!(RelationExpression::parse("has space").is_err());
    }

    #[test]
    fn test_serde_roundtrip() {
        let expressions = vec![
            RelationExpression::this(),
            RelationExpression::computed_userset("owner"),
            RelationExpression::tuple_to_userset("parent", "owner"),
            RelationExpression::union(vec![
                RelationExpression::this(),
                RelationExpression::computed_userset("reader"),
            ]),
            RelationExpression::intersection(vec![
                RelationExpression::computed_userset("member"),
                RelationExpression::computed_userset("approved"),
            ]),
            RelationExpression::difference(
                RelationExpression::computed_userset("member"),
                RelationExpression::computed_userset("banned"),
            ),
        ];

        for expr in expressions {
            let json = serde_json::to_string(&expr).unwrap();
            let parsed: RelationExpression = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, expr, "roundtrip failed for: {:?}", expr);
        }
    }
}
