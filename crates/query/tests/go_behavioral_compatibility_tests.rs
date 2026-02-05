//! Go Behavioral Compatibility Tests
//!
//! These tests verify that the Rust query engine produces the same results
//! as Go DefraDB for equivalent queries. Each test documents:
//! - The expected Go behavior
//! - The scenario being tested
//! - Why this matters for compatibility
//!
//! P0 = Same query produces different results (critical)
//! P1 = Different error handling behavior
//! P2 = Edge cases unlikely in production

use query::document::DocumentMapping;
use query::mapper::Filter;
use query::plan::{AverageNode, CountNode, ScanNode, SumNode};
use query::planner::{Doc, PlanNode};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use serde_json::json;
use std::collections::HashMap;

// =============================================================================
// P0: AVG of Empty Set
// =============================================================================
// Go DefraDB: Returns 0 (float64)
// Rust (before fix): Returns null
// Rust (after fix): Should return 0
// =============================================================================

#[tokio::test]
async fn test_p0_avg_empty_set_returns_zero() {
    // Go DefraDB behavior: AVG of empty set returns 0
    // This is important for calculations that expect numeric results
    let collection = CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    );

    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "name");
    mapping.add(2, "age");
    mapping.add(3, "_avg");
    mapping.add_render_key(3, "_avg");

    // Empty document set
    let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
    let mut avg_node = AverageNode::new(Box::new(scan), mapping, 2, 3);

    avg_node.init().await.unwrap();
    assert!(avg_node.next().await.unwrap());

    let result = avg_node.value();
    let avg_value = result.get(3).unwrap();

    // Go DefraDB returns 0 for AVG of empty set
    // This should be a number, not null
    assert!(
        avg_value.is_number(),
        "AVG of empty set should return 0, not null. Got: {:?}",
        avg_value
    );
    assert_eq!(
        avg_value.as_f64().unwrap(),
        0.0,
        "AVG of empty set should be 0.0"
    );

    avg_node.close().await.unwrap();
}

#[tokio::test]
async fn test_p0_avg_all_nulls_returns_zero() {
    // When all values are null, AVG should still return 0 (Go behavior)
    let collection = CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    );

    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "name");
    mapping.add(2, "age");
    mapping.add(3, "_avg");
    mapping.add_render_key(3, "_avg");

    // All documents have null age
    let docs = vec![
        Doc::with_fields(vec![
            Some(json!("doc1")),
            Some(json!("Alice")),
            None, // null age
        ]),
        Doc::with_fields(vec![
            Some(json!("doc2")),
            Some(json!("Bob")),
            None, // null age
        ]),
    ];

    let scan = ScanNode::new(collection, mapping.clone()).with_docs(docs);
    let mut avg_node = AverageNode::new(Box::new(scan), mapping, 2, 3);

    avg_node.init().await.unwrap();
    assert!(avg_node.next().await.unwrap());

    let result = avg_node.value();
    let avg_value = result.get(3).unwrap();

    // Go DefraDB: when all values are null (skipped), returns 0
    assert!(
        avg_value.is_number(),
        "AVG with all null values should return 0, not null. Got: {:?}",
        avg_value
    );
    assert_eq!(avg_value.as_f64().unwrap(), 0.0);

    avg_node.close().await.unwrap();
}

// =============================================================================
// P0: Null Comparison Behavior
// =============================================================================
// Go DefraDB: null _gt 5 returns false (silent)
// Rust (before fix): Returns TypeMismatch error
// Rust (after fix): Should return false
// =============================================================================

fn make_filter_mapping() -> DocumentMapping {
    let mut m = DocumentMapping::new();
    m.add(0, "_docID");
    m.add(1, "name");
    m.add(2, "age");
    m.add(3, "score");
    m
}

#[test]
fn test_p0_null_gt_comparison_returns_false() {
    // Go DefraDB behavior: null _gt 5 returns false (not error)
    // From gt.go: if data is nil, returns false
    let filter = Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 25}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(null)), // age is explicitly null
        Some(json!(100)),
    ];

    // Should return false (Go behavior), not error
    let result = filter.matches(&fields, &mapping);
    assert!(
        result.is_ok(),
        "null _gt comparison should not error, but got: {:?}",
        result
    );
    assert!(!result.unwrap(), "null _gt 25 should return false");
}

#[test]
fn test_p0_null_lt_comparison_returns_false() {
    // Go DefraDB behavior: null _lt 5 returns false
    let filter = Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_lt": 25}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(null)), // age is null
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok(), "null _lt comparison should not error");
    assert!(!result.unwrap(), "null _lt 25 should return false");
}

#[test]
fn test_p0_null_gte_comparison_returns_false() {
    let filter = Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gte": 25}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(null)),
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(!result.unwrap(), "null _gte 25 should return false");
}

#[test]
fn test_p0_null_lte_comparison_returns_false() {
    let filter = Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_lte": 25}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(null)),
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(!result.unwrap(), "null _lte 25 should return false");
}

#[test]
fn test_p0_missing_field_gt_returns_false() {
    // Missing field (None) should also return false, not error
    let filter = Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 25}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        None, // age field is completely missing
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(
        result.is_ok(),
        "missing field _gt comparison should not error"
    );
    assert!(!result.unwrap(), "missing field _gt 25 should return false");
}

// =============================================================================
// P0: Numeric Type Coercion
// =============================================================================
// Go DefraDB: Uses numbers.TryUpcast to convert int64 to float64 for comparison
// Rust (before fix): Returns TypeMismatch error
// Rust (after fix): Should upcast and compare successfully
// =============================================================================

#[test]
fn test_p0_int_gt_float_coercion() {
    // Go DefraDB: Comparing int field against float condition works
    // From gt.go: numbers.TryUpcast handles int64 -> float64 conversion
    let filter = Filter::from_conditions(HashMap::from([(
        "age".to_string(),
        json!({"_gt": 25.5}), // float condition
    )]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(30)), // integer value
        Some(json!(100)),
    ];

    // Should work: 30 > 25.5 = true
    let result = filter.matches(&fields, &mapping);
    assert!(
        result.is_ok(),
        "int vs float comparison should work, got: {:?}",
        result
    );
    assert!(result.unwrap(), "30 > 25.5 should be true");
}

#[test]
fn test_p0_float_gt_int_coercion() {
    // Float field against int condition
    let filter = Filter::from_conditions(HashMap::from([(
        "score".to_string(),
        json!({"_gt": 90}), // int condition
    )]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(30)),
        Some(json!(95.5)), // float value
    ];

    // Should work: 95.5 > 90 = true
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok(), "float vs int comparison should work");
    assert!(result.unwrap(), "95.5 > 90 should be true");
}

#[test]
fn test_p0_int_eq_float_coercion() {
    // Integer 30 should equal float 30.0
    let filter = Filter::from_conditions(HashMap::from([(
        "age".to_string(),
        json!({"_eq": 30.0}), // float condition
    )]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(30)), // integer value
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok(), "int _eq float comparison should work");
    assert!(result.unwrap(), "30 == 30.0 should be true");
}

#[test]
fn test_p0_int_lt_float_boundary() {
    // Boundary test: 30 < 30.1 should be true
    let filter =
        Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_lt": 30.1}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(30)),
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(result.unwrap(), "30 < 30.1 should be true");
}

// =============================================================================
// P1: Verified Compatible Behaviors
// =============================================================================
// These tests document behaviors that ARE compatible with Go DefraDB

#[test]
fn test_compatible_null_eq_null() {
    // Both Go and Rust: null == null is true
    let filter =
        Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_eq": null}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(null)),
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(result.unwrap(), "null == null should be true");
}

#[test]
fn test_compatible_null_ne_value() {
    // Both Go and Rust: null != 25 is true
    let filter = Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_ne": 25}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(null)),
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(result.unwrap(), "null != 25 should be true");
}

#[test]
fn test_compatible_value_eq_null_false() {
    // Both Go and Rust: 30 == null is false
    let filter =
        Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_eq": null}))]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(30)),
        Some(json!(100)),
    ];

    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(!result.unwrap(), "30 == null should be false");
}

#[tokio::test]
async fn test_compatible_sum_empty_returns_zero() {
    // Both Go and Rust: SUM of empty set returns 0
    let collection = CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    );

    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "name");
    mapping.add(2, "age");
    mapping.add(3, "_sum");
    mapping.add_render_key(3, "_sum");

    let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
    let mut sum_node = SumNode::new(Box::new(scan), mapping, 2, 3);

    sum_node.init().await.unwrap();
    assert!(sum_node.next().await.unwrap());

    let result = sum_node.value();
    let sum_value = result.get(3).unwrap();

    // Both Go and Rust return 0 for SUM of empty set
    assert!(sum_value.is_number());
    assert_eq!(sum_value.as_i64().unwrap(), 0);

    sum_node.close().await.unwrap();
}

#[tokio::test]
async fn test_compatible_count_empty_returns_zero() {
    // Both Go and Rust: COUNT of empty set returns 0
    let collection = CollectionVersion::new(
        "Users",
        "v1",
        "coll-1",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "age", FieldKind::int()),
        ],
    );

    let mut mapping = DocumentMapping::new();
    mapping.add(0, "_docID");
    mapping.add(1, "name");
    mapping.add(2, "age");
    mapping.add(3, "_count");
    mapping.add_render_key(3, "_count");

    let scan = ScanNode::new(collection, mapping.clone()).with_docs(vec![]);
    let mut count_node = CountNode::new(Box::new(scan), mapping, 3);

    count_node.init().await.unwrap();
    assert!(count_node.next().await.unwrap());

    let result = count_node.value();
    let count_value = result.get(3).unwrap();

    assert!(count_value.is_number());
    assert_eq!(count_value.as_i64().unwrap(), 0);

    count_node.close().await.unwrap();
}

// =============================================================================
// P2: String vs Number Comparison (Go Compatibility)
// =============================================================================
// Go DefraDB: String "Alice" vs int 5 comparison returns false (no match)
// Rust matches Go behavior: mismatched types don't match

#[test]
fn test_p2_string_vs_int_comparison_returns_false() {
    let filter = Filter::from_conditions(HashMap::from([(
        "name".to_string(), // string field
        json!({"_gt": 5}),  // numeric comparison
    )]));

    let mapping = make_filter_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")), // string value
        Some(json!(30)),
        Some(json!(100)),
    ];

    // Matches Go behavior: type mismatch returns false (no match)
    let result = filter.matches(&fields, &mapping);
    assert_eq!(result.unwrap(), false);
}
