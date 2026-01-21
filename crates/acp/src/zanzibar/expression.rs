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
    ///
    /// All operators have equal precedence and are evaluated left-to-right,
    /// matching Go zanzi behavior. Use parentheses to override.
    ///
    /// Example: `a + b & c` parses as `(a + b) & c` (left-to-right)
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.is_empty() {
            return Err(Error::InvalidExpression("empty expression".into()));
        }

        // Handle parenthesized expressions
        if input.starts_with('(') && input.ends_with(')') {
            // Check if the entire expression is wrapped in matching parens
            if is_fully_parenthesized(input) {
                return Self::parse(&input[1..input.len() - 1]);
            }
        }

        // Find the RIGHTMOST operator at depth 0 (left-to-right evaluation)
        // This ensures `a + b & c` is parsed as `(a + b) & c`
        if let Some((pos, op)) = find_rightmost_operator(input) {
            let left = Self::parse(&input[..pos])?;
            let right = Self::parse(&input[pos + 1..])?;

            return match op {
                '+' => Ok(merge_union(left, right)),
                '&' => Ok(merge_intersection(left, right)),
                '-' => Ok(Self::Difference {
                    base: Box::new(left),
                    subtract: Box::new(right),
                }),
                _ => unreachable!(),
            };
        }

        // No operators, parse as single term
        parse_term(input)
    }
}

/// Check if the expression is fully wrapped in matching parentheses.
/// e.g., "(a + b)" returns true, but "(a) + (b)" returns false.
fn is_fully_parenthesized(input: &str) -> bool {
    if !input.starts_with('(') || !input.ends_with(')') {
        return false;
    }

    let mut depth = 0;
    let chars: Vec<char> = input.chars().collect();
    for (i, &c) in chars.iter().enumerate() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                // If depth reaches 0 before the end, the outer parens don't wrap everything
                if depth == 0 && i < chars.len() - 1 {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}

/// Find the RIGHTMOST operator (+, &, or -) at depth 0.
/// Returns the position and the operator character.
/// This implements left-to-right evaluation with equal precedence.
///
/// For '-', ensures it's not part of '->' (tuple-to-userset).
fn find_rightmost_operator(input: &str) -> Option<(usize, char)> {
    let mut depth = 0;
    let chars: Vec<char> = input.chars().collect();
    let mut rightmost: Option<(usize, char)> = None;

    for i in 0..chars.len() {
        match chars[i] {
            '(' => depth += 1,
            ')' => depth -= 1,
            '+' | '&' if depth == 0 => {
                rightmost = Some((i, chars[i]));
            }
            '-' if depth == 0 => {
                // Check if this is part of '->'
                if i + 1 < chars.len() && chars[i + 1] == '>' {
                    // This is '->', skip
                    continue;
                }
                rightmost = Some((i, '-'));
            }
            _ => {}
        }
    }
    rightmost
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

    // ==========================================================================
    // Left-to-right precedence tests (matching Go zanzi behavior)
    // ==========================================================================

    #[test]
    fn test_left_to_right_union_then_intersection() {
        // `a + b & c` should parse as `(a + b) & c` (left-to-right)
        let expr = RelationExpression::parse("a + b & c").unwrap();

        // Expected: Intersection([Union([a, b]), c])
        match expr {
            RelationExpression::Intersection(exprs) => {
                assert_eq!(exprs.len(), 2);
                // First element should be Union([a, b])
                match &exprs[0] {
                    RelationExpression::Union(inner) => {
                        assert_eq!(inner.len(), 2);
                        assert_eq!(
                            inner[0],
                            RelationExpression::ComputedUserset {
                                relation: "a".into()
                            }
                        );
                        assert_eq!(
                            inner[1],
                            RelationExpression::ComputedUserset {
                                relation: "b".into()
                            }
                        );
                    }
                    _ => panic!("expected Union as first element"),
                }
                // Second element should be c
                assert_eq!(
                    exprs[1],
                    RelationExpression::ComputedUserset {
                        relation: "c".into()
                    }
                );
            }
            _ => panic!("expected Intersection, got {:?}", expr),
        }
    }

    #[test]
    fn test_left_to_right_intersection_then_union() {
        // `a & b + c` should parse as `(a & b) + c` (left-to-right)
        let expr = RelationExpression::parse("a & b + c").unwrap();

        // Expected: Union([Intersection([a, b]), c])
        match expr {
            RelationExpression::Union(exprs) => {
                assert_eq!(exprs.len(), 2);
                // First element should be Intersection([a, b])
                match &exprs[0] {
                    RelationExpression::Intersection(inner) => {
                        assert_eq!(inner.len(), 2);
                        assert_eq!(
                            inner[0],
                            RelationExpression::ComputedUserset {
                                relation: "a".into()
                            }
                        );
                        assert_eq!(
                            inner[1],
                            RelationExpression::ComputedUserset {
                                relation: "b".into()
                            }
                        );
                    }
                    _ => panic!("expected Intersection as first element"),
                }
                // Second element should be c
                assert_eq!(
                    exprs[1],
                    RelationExpression::ComputedUserset {
                        relation: "c".into()
                    }
                );
            }
            _ => panic!("expected Union, got {:?}", expr),
        }
    }

    #[test]
    fn test_left_to_right_difference_then_intersection() {
        // `a - b & c` should parse as `(a - b) & c` (left-to-right)
        let expr = RelationExpression::parse("a - b & c").unwrap();

        // Expected: Intersection([Difference(a, b), c])
        match expr {
            RelationExpression::Intersection(exprs) => {
                assert_eq!(exprs.len(), 2);
                // First element should be Difference(a, b)
                match &exprs[0] {
                    RelationExpression::Difference { base, subtract } => {
                        assert_eq!(
                            **base,
                            RelationExpression::ComputedUserset {
                                relation: "a".into()
                            }
                        );
                        assert_eq!(
                            **subtract,
                            RelationExpression::ComputedUserset {
                                relation: "b".into()
                            }
                        );
                    }
                    _ => panic!("expected Difference as first element"),
                }
                // Second element should be c
                assert_eq!(
                    exprs[1],
                    RelationExpression::ComputedUserset {
                        relation: "c".into()
                    }
                );
            }
            _ => panic!("expected Intersection, got {:?}", expr),
        }
    }

    #[test]
    fn test_left_to_right_three_operators() {
        // `a + b - c & d` should parse as `((a + b) - c) & d` (left-to-right)
        let expr = RelationExpression::parse("a + b - c & d").unwrap();

        // Expected: Intersection([Difference(Union([a, b]), c), d])
        match expr {
            RelationExpression::Intersection(exprs) => {
                assert_eq!(exprs.len(), 2);
                // First element should be Difference(Union([a, b]), c)
                match &exprs[0] {
                    RelationExpression::Difference { base, subtract } => {
                        // base should be Union([a, b])
                        match &**base {
                            RelationExpression::Union(inner) => {
                                assert_eq!(inner.len(), 2);
                            }
                            _ => panic!("expected Union as base"),
                        }
                        // subtract should be c
                        assert_eq!(
                            **subtract,
                            RelationExpression::ComputedUserset {
                                relation: "c".into()
                            }
                        );
                    }
                    _ => panic!("expected Difference as first element"),
                }
                // Second element should be d
                assert_eq!(
                    exprs[1],
                    RelationExpression::ComputedUserset {
                        relation: "d".into()
                    }
                );
            }
            _ => panic!("expected Intersection, got {:?}", expr),
        }
    }

    #[test]
    fn test_parentheses_override_left_to_right() {
        // `a + (b & c)` should parse with intersection evaluated first
        let expr = RelationExpression::parse("a + (b & c)").unwrap();

        // Expected: Union([a, Intersection([b, c])])
        match expr {
            RelationExpression::Union(exprs) => {
                assert_eq!(exprs.len(), 2);
                // First element should be a
                assert_eq!(
                    exprs[0],
                    RelationExpression::ComputedUserset {
                        relation: "a".into()
                    }
                );
                // Second element should be Intersection([b, c])
                match &exprs[1] {
                    RelationExpression::Intersection(inner) => {
                        assert_eq!(inner.len(), 2);
                    }
                    _ => panic!("expected Intersection as second element"),
                }
            }
            _ => panic!("expected Union, got {:?}", expr),
        }
    }

    #[test]
    fn test_nested_parentheses() {
        // `(a + (b - c)) & d` should parse correctly
        let expr = RelationExpression::parse("(a + (b - c)) & d").unwrap();

        // Expected: Intersection([Union([a, Difference(b, c)]), d])
        match expr {
            RelationExpression::Intersection(exprs) => {
                assert_eq!(exprs.len(), 2);
            }
            _ => panic!("expected Intersection, got {:?}", expr),
        }
    }

    #[test]
    fn test_tuple_to_userset_not_confused_with_difference() {
        // Ensure `parent->owner` is not confused with difference operator
        let expr = RelationExpression::parse("a + parent->owner & c").unwrap();

        // Should be: Intersection([Union([a, TTU(parent, owner)]), c])
        match expr {
            RelationExpression::Intersection(exprs) => {
                assert_eq!(exprs.len(), 2);
                match &exprs[0] {
                    RelationExpression::Union(inner) => {
                        assert_eq!(inner.len(), 2);
                        match &inner[1] {
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
                    _ => panic!("expected Union"),
                }
            }
            _ => panic!("expected Intersection, got {:?}", expr),
        }
    }
}
