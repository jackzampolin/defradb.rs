use serde_json::{json, Value as JsonValue};

use super::Filter;
use super::FilterOp;
use crate::document::DocumentMapping;

fn map<const N: usize>(entries: [(String, JsonValue); N]) -> serde_json::Map<String, JsonValue> {
    entries.into_iter().collect()
}

fn make_mapping() -> DocumentMapping {
    let mut m = DocumentMapping::new();
    m.add(0, "_docID");
    m.add(1, "name");
    m.add(2, "age");
    m.add(3, "active");
    m
}

fn make_fields() -> Vec<Option<JsonValue>> {
    vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(30)),
        Some(json!(true)),
    ]
}

#[test]
fn test_empty_filter_matches_all() {
    let filter = Filter::new();
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_eq_filter() {
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_eq": "Alice"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());

    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_eq": "Bob"}))]));
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ne_filter() {
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ne": "Bob"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_gt_filter() {
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_gt": 25}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());

    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_gt": 35}))]));
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_in_filter() {
    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        json!({"_in": ["Alice", "Bob", "Charlie"]}),
    )]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());

    let filter = Filter::from_conditions(map([(
        "name".to_string(),
        json!({"_in": ["Bob", "Charlie"]}),
    )]));
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_and_filter() {
    let filter = Filter::from_conditions(map([(
        "_and".to_string(),
        json!([
            {"name": {"_eq": "Alice"}},
            {"age": {"_gte": 18}}
        ]),
    )]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());

    let filter = Filter::from_conditions(map([(
        "_and".to_string(),
        json!([
            {"name": {"_eq": "Alice"}},
            {"age": {"_lt": 18}}
        ]),
    )]));
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_or_filter() {
    let filter = Filter::from_conditions(map([(
        "_or".to_string(),
        json!([
            {"name": {"_eq": "Bob"}},
            {"age": {"_eq": 30}}
        ]),
    )]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());

    let filter = Filter::from_conditions(map([(
        "_or".to_string(),
        json!([
            {"name": {"_eq": "Bob"}},
            {"age": {"_eq": 25}}
        ]),
    )]));
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_not_filter() {
    let filter =
        Filter::from_conditions(map([("_not".to_string(), json!({"name": {"_eq": "Bob"}}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_null_logical_filters_are_valid_and_match_all() {
    let mapping = make_mapping();
    let fields = make_fields();

    for op in ["_and", "_or", "_not"] {
        let filter = Filter::from_conditions(map([(op.to_string(), json!(null))]));
        assert!(filter.validate_depth().is_ok(), "{op} should be valid");
        assert!(
            filter.matches(&fields, &mapping).unwrap(),
            "{op} should match all"
        );
    }
}

#[test]
fn test_alias_null_filter_is_valid_and_matches_none() {
    let filter = Filter::from_conditions(map([("_alias".to_string(), json!(null))]));
    let mapping = make_mapping();
    let fields = make_fields();

    assert!(filter.validate_depth().is_ok());
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_alias_scalar_filter_is_valid_and_matches_none() {
    let filter = Filter::from_conditions(map([("_alias".to_string(), json!(1))]));
    let mapping = make_mapping();
    let fields = make_fields();

    assert!(filter.validate_depth().is_ok());
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_custom_filter_depth_limit() {
    let filter = Filter::from_conditions(map([(
        "_and".to_string(),
        json!([{ "_and": [{ "name": { "_eq": "Alice" } }] }]),
    )]))
    .with_max_depth(1);
    let mapping = make_mapping();
    let fields = make_fields();

    let err = filter.matches(&fields, &mapping).unwrap_err();
    assert!(err
        .to_string()
        .contains("filter exceeds maximum nesting depth of 1"));
}

#[test]
fn test_like_filter() {
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_like": "Ali%"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());

    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_like": "%ice"}))]));
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ilike_filter_case_insensitive_prefix() {
    // Pattern "ALI%" should match "Alice" (case-insensitive)
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "ALI%"}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // name = "Alice"
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ilike_filter_case_insensitive_suffix() {
    // Pattern "%ICE" should match "Alice" (case-insensitive)
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "%ICE"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ilike_filter_case_insensitive_contains() {
    // Pattern "%LIC%" should match "Alice" (case-insensitive)
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "%LIC%"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ilike_filter_case_insensitive_exact() {
    // Pattern "ALICE" should match "Alice" (case-insensitive exact)
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "ALICE"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ilike_filter_no_match() {
    // Pattern "BOB%" should NOT match "Alice"
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "BOB%"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_nilike_filter() {
    // Negated: pattern "BOB%" should NOT match "Alice", so nilike returns true
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_nilike": "BOB%"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());

    // Negated: pattern "ALI%" WOULD match "Alice", so nilike returns false
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_nilike": "ALI%"}))]));
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ilike_underscore_as_literal() {
    // Underscore is treated as literal character (matches Go behavior)
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "Al_ce"}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // name = "Alice"
                                // "Al_ce" should NOT match "Alice" because _ is literal, not wildcard
    assert!(!filter.matches(&fields, &mapping).unwrap());

    // But "Al_ce" should match "Al_ce" exactly
    let mut fields_with_underscore = make_fields();
    fields_with_underscore[1] = Some(json!("Al_ce"));
    assert!(filter.matches(&fields_with_underscore, &mapping).unwrap());
}

#[test]
fn test_ilike_prefix_suffix_pattern() {
    // Pattern "Ali%ce" should match "Alice" (starts with "ali" AND ends with "ce")
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "ALI%CE"}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // name = "Alice"
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_like_prefix_suffix_pattern() {
    // Pattern "Ali%ce" should match "Alice" (case-sensitive)
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_like": "Ali%ce"}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // name = "Alice"
    assert!(filter.matches(&fields, &mapping).unwrap());

    // Wrong case should NOT match
    let filter_wrong_case =
        Filter::from_conditions(map([("name".to_string(), json!({"_like": "ALI%CE"}))]));
    assert!(!filter_wrong_case.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ilike_prefix_suffix_no_match() {
    // Pattern "Bob%son" should NOT match "Alice"
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "Bob%son"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_ilike_null_field_returns_false() {
    // Null field should return false for _ilike (not error), matching Go behavior
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_ilike": "Ali%"}))]));
    let mapping = make_mapping();
    let mut fields = make_fields();
    fields[1] = Some(json!(null)); // name is null
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_nilike_null_field_returns_true() {
    // Go's nilike = !ilike. For null data, ilike returns false (non-string),
    // so nilike returns !false = true. Null doesn't match pattern → negation is true.
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_nilike": "Ali%"}))]));
    let mapping = make_mapping();
    let mut fields = make_fields();
    fields[1] = Some(json!(null)); // name is null
    assert!(filter.matches(&fields, &mapping).unwrap());
}

// Helper to create mapping with array and object fields for testing
fn make_extended_mapping() -> DocumentMapping {
    let mut m = DocumentMapping::new();
    m.add(0, "_docID");
    m.add(1, "name");
    m.add(2, "age");
    m.add(3, "active");
    m.add(4, "tags"); // Array field
    m.add(5, "metadata"); // Object field
    m
}

fn make_extended_fields() -> Vec<Option<JsonValue>> {
    vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(30)),
        Some(json!(true)),
        Some(json!(["rust", "database", "graphql"])), // tags array
        Some(json!({"version": "1.0", "author": "Alice"})), // metadata object
    ]
}

#[test]
fn test_contains_filter_match() {
    let filter = Filter::from_conditions(map([("tags".to_string(), json!({"_contains": "rust"}))]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_contains_filter_no_match() {
    let filter =
        Filter::from_conditions(map([("tags".to_string(), json!({"_contains": "python"}))]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields();
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_contains_filter_non_array_error() {
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_contains": "rust"}))]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields();
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("requires array field"));
}

#[test]
fn test_contained_in_filter_match() {
    // All elements of tags are in the given array
    let filter = Filter::from_conditions(map([(
        "tags".to_string(),
        json!({"_contained_in": ["rust", "database", "graphql", "sql", "nosql"]}),
    )]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_contained_in_filter_no_match() {
    // Not all elements of tags are in the given array (missing "graphql")
    let filter = Filter::from_conditions(map([(
        "tags".to_string(),
        json!({"_contained_in": ["rust", "database"]}),
    )]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields();
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_contained_in_filter_empty_field_array() {
    // Empty array is contained in any array
    let filter = Filter::from_conditions(map([(
        "tags".to_string(),
        json!({"_contained_in": ["anything"]}),
    )]));
    let mapping = make_extended_mapping();
    let mut fields = make_extended_fields();
    fields[4] = Some(json!([])); // Empty tags array
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_has_key_filter_match() {
    let filter = Filter::from_conditions(map([(
        "metadata".to_string(),
        json!({"_has_key": "version"}),
    )]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_has_key_filter_no_match() {
    let filter = Filter::from_conditions(map([(
        "metadata".to_string(),
        json!({"_has_key": "nonexistent"}),
    )]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields();
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_has_key_filter_non_object_error() {
    let filter =
        Filter::from_conditions(map([("tags".to_string(), json!({"_has_key": "version"}))]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields();
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("requires object field"));
}

#[test]
fn test_filter_op_parse() {
    assert_eq!(FilterOp::parse("_eq"), Some(FilterOp::Eq));
    assert_eq!(FilterOp::parse("_and"), Some(FilterOp::And));
    assert_eq!(FilterOp::parse("_ilike"), Some(FilterOp::Ilike));
    assert_eq!(FilterOp::parse("_nilike"), Some(FilterOp::Nilike));
    assert_eq!(FilterOp::parse("_contains"), Some(FilterOp::Contains));
    assert_eq!(
        FilterOp::parse("_contained_in"),
        Some(FilterOp::ContainedIn)
    );
    assert_eq!(FilterOp::parse("_has_key"), Some(FilterOp::HasKey));
    assert_eq!(FilterOp::parse("invalid"), None);
}

#[test]
fn test_null_field_comparison() {
    // When a field is null/None, comparisons should handle it gracefully
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_eq": null}))]));
    let mapping = make_mapping();
    // Field at index 2 (age) is None
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        None, // age is null
        Some(json!(true)),
    ];
    // Null equals null
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_null_field_gt_comparison_returns_false() {
    // Go DefraDB behavior: null _gt 25 returns false (null is "smaller" than any value)
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_gt": 25}))]));
    let mapping = make_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        None, // age is null
        Some(json!(true)),
    ];
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok(), "null _gt comparison should not error");
    assert!(!result.unwrap(), "null _gt 25 should return false");
}

#[test]
fn test_value_gt_null_returns_true() {
    // Go DefraDB behavior: 25 _gt null returns true (any non-null value > null)
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_gt": null}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // age = 30
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(result.unwrap(), "value _gt null should return true");
}

#[test]
fn test_value_ge_null_returns_true() {
    // Go DefraDB behavior: any value >= null returns true
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_ge": null}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // age = 30
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(result.unwrap(), "value _ge null should return true");
}

#[test]
fn test_null_ge_null_returns_true() {
    // Go DefraDB behavior: null >= null returns true
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_ge": null}))]));
    let mapping = make_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        None, // age is null
        Some(json!(true)),
    ];
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(result.unwrap(), "null _ge null should return true");
}

#[test]
fn test_value_lt_null_returns_false() {
    // Go DefraDB behavior: value _lt null returns false (no value is less than null)
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_lt": null}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // age = 30
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(!result.unwrap(), "value _lt null should return false");
}

#[test]
fn test_null_le_null_returns_true() {
    // Go DefraDB behavior: null <= null returns true
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_le": null}))]));
    let mapping = make_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        None, // age is null
        Some(json!(true)),
    ];
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(result.unwrap(), "null _le null should return true");
}

#[test]
fn test_value_le_null_returns_false() {
    // Go DefraDB behavior: value _le null returns false (only null <= null)
    let filter = Filter::from_conditions(map([("age".to_string(), json!({"_le": null}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // age = 30
    let result = filter.matches(&fields, &mapping);
    assert!(result.is_ok());
    assert!(!result.unwrap(), "value _le null should return false");
}

#[test]
fn test_nested_and_or_operators() {
    // Test _and containing _or: match if (name=Alice OR name=Bob) AND age>=18
    let filter = Filter::from_conditions(map([(
        "_and".to_string(),
        json!([
            {"_or": [
                {"name": {"_eq": "Alice"}},
                {"name": {"_eq": "Bob"}}
            ]},
            {"age": {"_gte": 18}}
        ]),
    )]));
    let mapping = make_mapping();

    // Alice, age 30 - should match
    let fields_alice = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(30)),
        Some(json!(true)),
    ];
    assert!(filter.matches(&fields_alice, &mapping).unwrap());

    // Charlie, age 25 - should NOT match (name not Alice or Bob)
    let fields_charlie = vec![
        Some(json!("doc2")),
        Some(json!("Charlie")),
        Some(json!(25)),
        Some(json!(true)),
    ];
    assert!(!filter.matches(&fields_charlie, &mapping).unwrap());
}

#[test]
fn test_nested_not_and_operators() {
    // Test _not containing _and: match if NOT (name=Alice AND age<18)
    let filter = Filter::from_conditions(map([(
        "_not".to_string(),
        json!({"_and": [
            {"name": {"_eq": "Alice"}},
            {"age": {"_lt": 18}}
        ]}),
    )]));
    let mapping = make_mapping();

    // Alice, age 30 - should match (Alice but NOT age<18)
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());

    // Alice, age 15 - should NOT match
    let fields_young = vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!(15)),
        Some(json!(true)),
    ];
    assert!(!filter.matches(&fields_young, &mapping).unwrap());
}

#[test]
fn test_like_underscore_as_literal() {
    // Underscore is treated as literal character (matches Go behavior)
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_like": "Al_ce"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    // "Al_ce" should NOT match "Alice" because _ is literal
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_like_complex_pattern() {
    // Multiple % should be handled by the DP algorithm
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_like": "%li%ce"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    let result = filter.matches(&fields, &mapping);
    // DP algorithm handles arbitrary % patterns
    assert!(result.is_ok());
}

// =========================================================================
// Null field handling tests for array/object operators
// =========================================================================

#[test]
fn test_contains_null_field_returns_false() {
    // When field is null, _contains should return false (not error)
    let filter = Filter::from_conditions(map([("tags".to_string(), json!({"_contains": "rust"}))]));
    let mapping = make_extended_mapping();
    let mut fields = make_extended_fields();
    fields[4] = Some(json!(null)); // tags is null
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_contained_in_null_field_returns_false() {
    // When field is null, _contained_in should return false (not error)
    let filter = Filter::from_conditions(map([(
        "tags".to_string(),
        json!({"_contained_in": ["rust", "go"]}),
    )]));
    let mapping = make_extended_mapping();
    let mut fields = make_extended_fields();
    fields[4] = Some(json!(null)); // tags is null
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_has_key_null_field_returns_false() {
    // When field is null, _has_key should return false (not error)
    let filter = Filter::from_conditions(map([(
        "metadata".to_string(),
        json!({"_has_key": "version"}),
    )]));
    let mapping = make_extended_mapping();
    let mut fields = make_extended_fields();
    fields[5] = Some(json!(null)); // metadata is null
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_contains_with_null_in_array() {
    // Array contains null, searching for null should find it
    let filter = Filter::from_conditions(map([("tags".to_string(), json!({"_contains": null}))]));
    let mapping = make_extended_mapping();
    let mut fields = make_extended_fields();
    fields[4] = Some(json!(["rust", null, "graphql"])); // Array with null
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_contains_null_not_in_array() {
    // Array doesn't contain null, searching for null should not find it
    let filter = Filter::from_conditions(map([("tags".to_string(), json!({"_contains": null}))]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields(); // No null in tags
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

// =========================================================================
// Empty array edge cases
// =========================================================================

#[test]
fn test_contains_empty_array() {
    // Empty array should never contain anything
    let filter = Filter::from_conditions(map([("tags".to_string(), json!({"_contains": "rust"}))]));
    let mapping = make_extended_mapping();
    let mut fields = make_extended_fields();
    fields[4] = Some(json!([])); // Empty array
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_contained_in_empty_expected_array() {
    // Non-empty field array vs empty expected array
    // [a,b,c] is NOT contained in [] (no elements of expected contain the actuals)
    let filter = Filter::from_conditions(map([("tags".to_string(), json!({"_contained_in": []}))]));
    let mapping = make_extended_mapping();
    let fields = make_extended_fields(); // tags = ["rust", "database", "graphql"]
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_has_key_empty_string_key() {
    // Empty string keys are valid in JSON objects
    let filter = Filter::from_conditions(map([("metadata".to_string(), json!({"_has_key": ""}))]));
    let mapping = make_extended_mapping();
    let mut fields = make_extended_fields();
    fields[5] = Some(json!({"": "empty key value", "version": "1.0"}));
    assert!(filter.matches(&fields, &mapping).unwrap());
}

// =========================================================================
// Pattern matching edge cases
// =========================================================================

#[test]
fn test_like_pattern_only_percent() {
    // Pattern "%" should match any non-empty string (suffix after empty prefix)
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_like": "%"}))]));
    let mapping = make_mapping();
    let fields = make_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_like_empty_pattern() {
    // Empty pattern should only match empty string
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_like": ""}))]));
    let mapping = make_mapping();
    let fields = make_fields(); // name = "Alice"
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_like_empty_pattern_matches_empty_string() {
    // Empty pattern should match empty string
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_like": ""}))]));
    let mapping = make_mapping();
    let mut fields = make_fields();
    fields[1] = Some(json!("")); // empty name
    assert!(filter.matches(&fields, &mapping).unwrap());
}

// Tests for is_complex()
#[test]
fn test_is_complex_simple_scalar() {
    // Simple scalar filter is NOT complex
    let filter = Filter::from_conditions(map([("name".to_string(), json!({"_eq": "Alice"}))]));
    assert!(!filter.is_complex());
}

#[test]
fn test_is_complex_simple_relation_at_root() {
    // Relation filter at root level (no logical wrapper) is NOT complex
    let filter = Filter::from_conditions(map([(
        "author".to_string(),
        json!({"verified": {"_eq": true}}),
    )]));
    assert!(!filter.is_complex());
}

#[test]
fn test_is_complex_and_with_only_scalars() {
    // _and with only scalar conditions is NOT complex
    let filter = Filter::from_conditions(map([(
        "_and".to_string(),
        json!([
            {"name": {"_eq": "Alice"}},
            {"age": {"_gt": 25}}
        ]),
    )]));
    assert!(!filter.is_complex());
}

#[test]
fn test_is_complex_and_with_relation() {
    // _and containing a relation filter IS complex
    let filter = Filter::from_conditions(map([(
        "_and".to_string(),
        json!([
            {"rating": {"_ge": 4.0}},
            {"author": {"verified": {"_eq": true}}}
        ]),
    )]));
    assert!(filter.is_complex());
}

#[test]
fn test_is_complex_or_with_relation() {
    // _or containing a relation filter IS complex
    let filter = Filter::from_conditions(map([(
        "_or".to_string(),
        json!([
            {"rating": {"_ge": 4.0}},
            {"author": {"verified": {"_eq": true}}}
        ]),
    )]));
    assert!(filter.is_complex());
}

#[test]
fn test_is_complex_not_with_relation() {
    // _not containing a relation filter IS complex
    let filter = Filter::from_conditions(map([(
        "_not".to_string(),
        json!({"author": {"verified": {"_eq": true}}}),
    )]));
    assert!(filter.is_complex());
}

// =========================================================================
// Multi-level relation path detection tests
// =========================================================================

#[test]
fn test_get_multi_level_relation_paths_simple_relation() {
    // Single-level relation like {author: {verified: {_eq: true}}}
    // Path is ["author"] which has length 1, so should NOT be in multi-level paths
    let filter = Filter::from_conditions(map([(
        "author".to_string(),
        json!({"verified": {"_eq": true}}),
    )]));
    let paths = filter.get_multi_level_relation_paths();
    assert!(
        paths.is_empty(),
        "Single-level relation should not return multi-level paths"
    );
}

#[test]
fn test_get_multi_level_relation_paths_two_level() {
    // Two-level relation like {author: {published: {rating: {_eq: 4.9}}}}
    // Path is ["author", "published"] which has length 2
    let filter = Filter::from_conditions(map([(
        "author".to_string(),
        json!({"published": {"rating": {"_eq": 4.9}}}),
    )]));
    let paths = filter.get_multi_level_relation_paths();
    assert_eq!(paths.len(), 1);
    assert_eq!(
        paths[0],
        vec!["author".to_string(), "published".to_string()]
    );
}

#[test]
fn test_get_multi_level_relation_paths_three_level() {
    // Three-level relation like {author: {publisher: {country: {name: {_eq: "USA"}}}}}
    let filter = Filter::from_conditions(map([(
        "author".to_string(),
        json!({"publisher": {"country": {"name": {"_eq": "USA"}}}}),
    )]));
    let paths = filter.get_multi_level_relation_paths();
    assert_eq!(paths.len(), 1);
    assert_eq!(
        paths[0],
        vec![
            "author".to_string(),
            "publisher".to_string(),
            "country".to_string()
        ]
    );
}

#[test]
fn test_get_multi_level_relation_paths_no_relation() {
    // Scalar filter, no relations
    let filter = Filter::from_conditions(map([("rating".to_string(), json!({"_eq": 4.9}))]));
    let paths = filter.get_multi_level_relation_paths();
    assert!(paths.is_empty());
}

#[test]
fn test_extract_filter_at_path_single_level() {
    // Extract filter at ["author"] from {author: {verified: {_eq: true}}}
    let filter = Filter::from_conditions(map([(
        "author".to_string(),
        json!({"verified": {"_eq": true}}),
    )]));
    let extracted = filter.extract_filter_at_path(&["author".to_string()]);
    assert!(extracted.is_some());
    let extracted = extracted.unwrap();
    // Should contain {verified: {_eq: true}}
    assert!(extracted.conditions().contains_key("verified"));
}

#[test]
fn test_extract_filter_at_path_two_level() {
    // Extract filter at ["author", "published"] from {author: {published: {rating: {_eq: 4.9}}}}
    let filter = Filter::from_conditions(map([(
        "author".to_string(),
        json!({"published": {"rating": {"_eq": 4.9}}}),
    )]));
    let extracted = filter.extract_filter_at_path(&["author".to_string(), "published".to_string()]);
    assert!(extracted.is_some());
    let extracted = extracted.unwrap();
    // Should contain {rating: {_eq: 4.9}}
    assert!(extracted.conditions().contains_key("rating"));
}

#[test]
fn test_extract_filter_at_path_empty_path() {
    // Empty path should return the full filter
    let filter = Filter::from_conditions(map([(
        "author".to_string(),
        json!({"verified": {"_eq": true}}),
    )]));
    let extracted = filter.extract_filter_at_path(&[]);
    assert!(extracted.is_some());
    let extracted = extracted.unwrap();
    assert!(extracted.conditions().contains_key("author"));
}

#[test]
fn test_extract_filter_at_path_nonexistent() {
    // Path that doesn't exist should return None
    let filter = Filter::from_conditions(map([(
        "author".to_string(),
        json!({"verified": {"_eq": true}}),
    )]));
    let extracted = filter.extract_filter_at_path(&["nonexistent".to_string()]);
    assert!(extracted.is_none());
}

// =========================================================================
// Array element operator tests (_any, _all, _none)
// =========================================================================

fn make_scores_mapping() -> DocumentMapping {
    let mut m = DocumentMapping::new();
    m.add(0, "_docID");
    m.add(1, "name");
    m.add(2, "testScores"); // Array of integers
    m
}

fn make_scores_fields() -> Vec<Option<JsonValue>> {
    vec![
        Some(json!("doc1")),
        Some(json!("Alice")),
        Some(json!([85, 90, 75, 95])), // testScores
    ]
}

#[test]
fn test_any_filter_match() {
    // _any: {_gt: 90} should match because 95 > 90
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_any": {"_gt": 90}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = make_scores_fields();
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_any_filter_no_match() {
    // _any: {_gt: 100} should not match because no score > 100
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_any": {"_gt": 100}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = make_scores_fields();
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_any_filter_empty_array() {
    // _any on empty array should return false
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_any": {"_gt": 50}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Bob")),
        Some(json!([])), // Empty array
    ];
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_any_filter_null_field() {
    // _any on null field should return false
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_any": {"_gt": 50}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Bob")),
        Some(json!(null)), // Null field
    ];
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_all_filter_match() {
    // _all: {_gte: 70} should match because all scores >= 70
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_all": {"_gte": 70}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = make_scores_fields(); // [85, 90, 75, 95]
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_all_filter_no_match() {
    // _all: {_gte: 80} should not match because 75 < 80
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_all": {"_gte": 80}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = make_scores_fields(); // [85, 90, 75, 95]
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_all_filter_empty_array() {
    // _all on empty array should return true (vacuous truth)
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_all": {"_gt": 100}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Bob")),
        Some(json!([])), // Empty array
    ];
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_all_filter_null_field() {
    // _all on null field should return false
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_all": {"_gt": 50}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Bob")),
        Some(json!(null)), // Null field
    ];
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_none_filter_match() {
    // _none: {_lt: 70} should match because no score < 70
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_none": {"_lt": 70}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = make_scores_fields(); // [85, 90, 75, 95]
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_none_filter_no_match() {
    // _none: {_lt: 80} should not match because 75 < 80
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_none": {"_lt": 80}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = make_scores_fields(); // [85, 90, 75, 95]
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_none_filter_empty_array() {
    // _none on empty array should return true (no elements match)
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_none": {"_lt": 100}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Bob")),
        Some(json!([])), // Empty array
    ];
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_none_filter_null_field() {
    // _none on null field should return false
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_none": {"_gt": 50}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = vec![
        Some(json!("doc1")),
        Some(json!("Bob")),
        Some(json!(null)), // Null field
    ];
    assert!(!filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_any_with_multiple_conditions() {
    // _any: {_gt: 80, _lt: 92} should match 85 and 90
    let filter = Filter::from_conditions(map([(
        "testScores".to_string(),
        json!({"_any": {"_gt": 80, "_lt": 92}}),
    )]));
    let mapping = make_scores_mapping();
    let fields = make_scores_fields(); // [85, 90, 75, 95]
    assert!(filter.matches(&fields, &mapping).unwrap());
}

#[test]
fn test_filter_op_parse_array_ops() {
    assert_eq!(FilterOp::parse("_any"), Some(FilterOp::Any));
    assert_eq!(FilterOp::parse("_all"), Some(FilterOp::All));
    assert_eq!(FilterOp::parse("_none"), Some(FilterOp::None));
}

#[test]
fn test_filter_op_is_array_element_op() {
    assert!(FilterOp::Any.is_array_element_op());
    assert!(FilterOp::All.is_array_element_op());
    assert!(FilterOp::None.is_array_element_op());
    assert!(!FilterOp::Eq.is_array_element_op());
    assert!(!FilterOp::Contains.is_array_element_op());
}

#[test]
fn test_to_explain_json_without_docid_preserves_shape() {
    let filter = Filter::from_conditions(map([(
        "_and".to_string(),
        json!([
            {"_docID": {"_eq": "doc-1"}},
            {
                "_or": [
                    {"_docID": {"_eq": "doc-2"}},
                    {"name": {"_eq": "Alice"}}
                ]
            }
        ]),
    )]));

    assert_eq!(
        filter.to_explain_json_without_docid(),
        json!({"name": {"_eq": "Alice"}})
    );
}

#[test]
fn test_to_explain_json_without_docid_returns_null_when_only_docid_remains() {
    let filter = Filter::from_conditions(map([(
        "_and".to_string(),
        json!([
            {"_docID": {"_eq": "doc-1"}},
            {"_or": [{"_docID": {"_eq": "doc-2"}}]}
        ]),
    )]));

    assert_eq!(filter.to_explain_json_without_docid(), JsonValue::Null);
}
