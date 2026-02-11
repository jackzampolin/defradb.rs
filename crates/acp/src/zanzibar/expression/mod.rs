//! Relation expression types and parsing.
//!
//! Defines the RelationExpression enum for userset rewrite rules
//! and provides parsing from string format.

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
        assert_eq!(expr.to_string(), "(_this + reader)");
    }

    #[test]
    fn test_intersection() {
        let expr = RelationExpression::intersection(vec![
            RelationExpression::computed_userset("member"),
            RelationExpression::computed_userset("approved"),
        ]);
        assert_eq!(expr.to_string(), "(member & approved)");
    }

    #[test]
    fn test_difference() {
        let expr = RelationExpression::difference(
            RelationExpression::computed_userset("member"),
            RelationExpression::computed_userset("banned"),
        );
        assert_eq!(expr.to_string(), "(member - banned)");
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

    // ==========================================================================
    // Go Domain Type Tests (matching zanzi/pkg/domain/relation_expression_tree_test.go)
    // These tests verify that RelationExpression.to_string() matches Go's
    // RelationExpression() output exactly.
    // ==========================================================================

    #[test]
    fn test_go_this_to_relation_expression() {
        // Go: TestThisToRelationExpression
        let tree = RelationExpression::this();
        let rel_expr = tree.to_string();
        assert_eq!(rel_expr, "_this");
    }

    #[test]
    fn test_go_computed_userset_to_relation_expression() {
        // Go: TestComputedUsersetToRelationExpression
        let tree = RelationExpression::computed_userset("relation");
        let rel_expr = tree.to_string();
        assert_eq!(rel_expr, "relation");
    }

    #[test]
    fn test_go_tuple_to_userset_to_relation_expression() {
        // Go: TestTupleToUsersetToRelationExpression
        let tree = RelationExpression::tuple_to_userset("parent", "owner");
        let rel_expr = tree.to_string();
        assert_eq!(rel_expr, "parent->owner");
    }

    #[test]
    fn test_go_union_to_relation_expression() {
        // Go: TestUnionToRelationExpression
        let tree = RelationExpression::union(vec![
            RelationExpression::computed_userset("left"),
            RelationExpression::computed_userset("right"),
        ]);
        let rel_expr = tree.to_string();
        assert_eq!(rel_expr, "(left + right)");
    }

    #[test]
    fn test_go_intersection_to_relation_expression() {
        // Go: TestIntersectionToRelationExpression
        let tree = RelationExpression::intersection(vec![
            RelationExpression::computed_userset("left"),
            RelationExpression::computed_userset("right"),
        ]);
        let rel_expr = tree.to_string();
        assert_eq!(rel_expr, "(left & right)");
    }

    #[test]
    fn test_go_difference_to_relation_expression() {
        // Go: TestDifferenceToRelationExpression
        let tree = RelationExpression::difference(
            RelationExpression::computed_userset("left"),
            RelationExpression::computed_userset("right"),
        );
        let rel_expr = tree.to_string();
        assert_eq!(rel_expr, "(left - right)");
    }

    // ==========================================================================
    // Parse-Display roundtrip tests
    // Verify that parsing and displaying produces consistent results.
    // ==========================================================================

    #[test]
    fn test_parse_display_roundtrip_this() {
        let original = "_this";
        let parsed = RelationExpression::parse(original).unwrap();
        let displayed = parsed.to_string();
        assert_eq!(displayed, original);
    }

    #[test]
    fn test_parse_display_roundtrip_computed_userset() {
        let original = "owner";
        let parsed = RelationExpression::parse(original).unwrap();
        let displayed = parsed.to_string();
        assert_eq!(displayed, original);
    }

    #[test]
    fn test_parse_display_roundtrip_ttu() {
        let original = "parent->owner";
        let parsed = RelationExpression::parse(original).unwrap();
        let displayed = parsed.to_string();
        assert_eq!(displayed, original);
    }

    #[test]
    fn test_parse_display_roundtrip_union() {
        // Parsing "a + b" produces Union([a, b])
        // Display outputs "(a + b)"
        // Re-parsing "(a + b)" should produce the same structure
        let expr = RelationExpression::parse("a + b").unwrap();
        let displayed = expr.to_string();
        assert_eq!(displayed, "(a + b)");
        let reparsed = RelationExpression::parse(&displayed).unwrap();
        assert_eq!(expr, reparsed);
    }

    #[test]
    fn test_parse_display_roundtrip_intersection() {
        let expr = RelationExpression::parse("a & b").unwrap();
        let displayed = expr.to_string();
        assert_eq!(displayed, "(a & b)");
        let reparsed = RelationExpression::parse(&displayed).unwrap();
        assert_eq!(expr, reparsed);
    }

    #[test]
    fn test_parse_display_roundtrip_difference() {
        let expr = RelationExpression::parse("a - b").unwrap();
        let displayed = expr.to_string();
        assert_eq!(displayed, "(a - b)");
        let reparsed = RelationExpression::parse(&displayed).unwrap();
        assert_eq!(expr, reparsed);
    }

    #[test]
    fn test_parse_display_roundtrip_complex() {
        // Complex expression: (a + b) & c
        // Due to left-to-right precedence, "a + b & c" parses as "(a + b) & c"
        let expr = RelationExpression::parse("a + b & c").unwrap();
        let displayed = expr.to_string();
        // Should output "((a + b) & c)" with nested parens
        assert_eq!(displayed, "((a + b) & c)");
        let reparsed = RelationExpression::parse(&displayed).unwrap();
        assert_eq!(expr, reparsed);
    }

    #[test]
    fn test_parse_display_roundtrip_nested_ttu() {
        // Complex: owner + parent->admin
        let expr = RelationExpression::parse("owner + parent->admin").unwrap();
        let displayed = expr.to_string();
        assert_eq!(displayed, "(owner + parent->admin)");
        let reparsed = RelationExpression::parse(&displayed).unwrap();
        assert_eq!(expr, reparsed);
    }
}
