//! Tests for Zanzibar relation expressions.

use acp::RelationExpression;

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

// Left-to-right precedence tests (matching Go zanzi behavior)

#[test]
fn test_left_to_right_union_then_intersection() {
    let expr = RelationExpression::parse("a + b & c").unwrap();

    match expr {
        RelationExpression::Intersection(exprs) => {
            assert_eq!(exprs.len(), 2);
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
    let expr = RelationExpression::parse("a & b + c").unwrap();

    match expr {
        RelationExpression::Union(exprs) => {
            assert_eq!(exprs.len(), 2);
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
    let expr = RelationExpression::parse("a - b & c").unwrap();

    match expr {
        RelationExpression::Intersection(exprs) => {
            assert_eq!(exprs.len(), 2);
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
    let expr = RelationExpression::parse("a + b - c & d").unwrap();

    match expr {
        RelationExpression::Intersection(exprs) => {
            assert_eq!(exprs.len(), 2);
            match &exprs[0] {
                RelationExpression::Difference { base, subtract } => {
                    match &**base {
                        RelationExpression::Union(inner) => {
                            assert_eq!(inner.len(), 2);
                        }
                        _ => panic!("expected Union as base"),
                    }
                    assert_eq!(
                        **subtract,
                        RelationExpression::ComputedUserset {
                            relation: "c".into()
                        }
                    );
                }
                _ => panic!("expected Difference as first element"),
            }
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
    let expr = RelationExpression::parse("a + (b & c)").unwrap();

    match expr {
        RelationExpression::Union(exprs) => {
            assert_eq!(exprs.len(), 2);
            assert_eq!(
                exprs[0],
                RelationExpression::ComputedUserset {
                    relation: "a".into()
                }
            );
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
    let expr = RelationExpression::parse("(a + (b - c)) & d").unwrap();

    match expr {
        RelationExpression::Intersection(exprs) => {
            assert_eq!(exprs.len(), 2);
        }
        _ => panic!("expected Intersection, got {:?}", expr),
    }
}

#[test]
fn test_tuple_to_userset_not_confused_with_difference() {
    let expr = RelationExpression::parse("a + parent->owner & c").unwrap();

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

// Go compatibility tests (matching zanzi/pkg/domain/relation_expression_tree_test.go)

#[test]
fn test_go_this_to_relation_expression() {
    let tree = RelationExpression::this();
    let rel_expr = tree.to_string();
    assert_eq!(rel_expr, "_this");
}

#[test]
fn test_go_computed_userset_to_relation_expression() {
    let tree = RelationExpression::computed_userset("relation");
    let rel_expr = tree.to_string();
    assert_eq!(rel_expr, "relation");
}

#[test]
fn test_go_tuple_to_userset_to_relation_expression() {
    let tree = RelationExpression::tuple_to_userset("parent", "owner");
    let rel_expr = tree.to_string();
    assert_eq!(rel_expr, "parent->owner");
}

#[test]
fn test_go_union_to_relation_expression() {
    let tree = RelationExpression::union(vec![
        RelationExpression::computed_userset("left"),
        RelationExpression::computed_userset("right"),
    ]);
    let rel_expr = tree.to_string();
    assert_eq!(rel_expr, "(left + right)");
}

#[test]
fn test_go_intersection_to_relation_expression() {
    let tree = RelationExpression::intersection(vec![
        RelationExpression::computed_userset("left"),
        RelationExpression::computed_userset("right"),
    ]);
    let rel_expr = tree.to_string();
    assert_eq!(rel_expr, "(left & right)");
}

#[test]
fn test_go_difference_to_relation_expression() {
    let tree = RelationExpression::difference(
        RelationExpression::computed_userset("left"),
        RelationExpression::computed_userset("right"),
    );
    let rel_expr = tree.to_string();
    assert_eq!(rel_expr, "(left - right)");
}

// Parse-display roundtrip tests

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
    let expr = RelationExpression::parse("a + b & c").unwrap();
    let displayed = expr.to_string();
    assert_eq!(displayed, "((a + b) & c)");
    let reparsed = RelationExpression::parse(&displayed).unwrap();
    assert_eq!(expr, reparsed);
}

#[test]
fn test_parse_display_roundtrip_nested_ttu() {
    let expr = RelationExpression::parse("owner + parent->admin").unwrap();
    let displayed = expr.to_string();
    assert_eq!(displayed, "(owner + parent->admin)");
    let reparsed = RelationExpression::parse(&displayed).unwrap();
    assert_eq!(expr, reparsed);
}
