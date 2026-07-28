use super::*;
use serde_json::json;

fn make_filter(value: JsonValue) -> crate::mapper::Filter {
    crate::mapper::Filter::from_conditions(serde_json::from_value(value).unwrap())
}

#[test]
fn test_extract_commits_height_range_simple_window() {
    let filter = make_filter(json!({
        "height": {
            "_gte": 2,
            "_lt": 5
        }
    }));

    assert_eq!(
        extract_commits_height_range(&filter),
        HeightRangeExtraction::Range(super::super::commits_height::CommitsHeightRange {
            start: 2,
            end: Some(5),
        })
    );
}

#[test]
fn test_extract_commits_height_range_merges_and_conditions() {
    let filter = make_filter(json!({
        "_and": [
            { "height": { "_gte": 2 } },
            { "height": { "_lt": 4 } },
            { "fieldName": { "_eq": "_C" } }
        ]
    }));

    assert_eq!(
        extract_commits_height_range(&filter),
        HeightRangeExtraction::Range(super::super::commits_height::CommitsHeightRange {
            start: 2,
            end: Some(4),
        })
    );
}

#[test]
fn test_extract_commits_height_range_ignores_non_height_or_clauses() {
    let filter = make_filter(json!({
        "height": { "_gte": 2 },
        "_or": [
            { "fieldName": { "_eq": "_C" } },
            { "fieldName": { "_eq": "age" } }
        ]
    }));

    assert_eq!(
        extract_commits_height_range(&filter),
        HeightRangeExtraction::Range(super::super::commits_height::CommitsHeightRange {
            start: 2,
            end: None,
        })
    );
}

#[test]
fn test_extract_commits_height_range_rejects_disjunctive_height_filters() {
    let filter = make_filter(json!({
        "_or": [
            { "height": { "_eq": 1 } },
            { "height": { "_eq": 3 } }
        ]
    }));

    assert_eq!(
        extract_commits_height_range(&filter),
        HeightRangeExtraction::Unsupported
    );
}

#[test]
fn test_extract_commits_height_range_detects_empty_window() {
    let filter = make_filter(json!({
        "height": {
            "_gt": 10,
            "_lt": 5
        }
    }));

    assert_eq!(
        extract_commits_height_range(&filter),
        HeightRangeExtraction::Empty
    );
}

#[test]
fn test_commit_sum_preserves_large_int_precision() {
    let values = [
        CommitNumericValue::Int(9_007_199_254_740_992),
        CommitNumericValue::Int(1),
    ];
    let sum = sum_commit_numeric_values(&values);
    assert_eq!(sum.as_i64(), Some(9_007_199_254_740_993));
}

#[test]
fn test_commit_min_max_preserve_large_int_precision() {
    let values = [
        CommitNumericValue::Int(9_007_199_254_740_993),
        CommitNumericValue::Int(9_007_199_254_740_992),
    ];
    assert_eq!(
        min_commit_numeric_values(&values).as_i64(),
        Some(9_007_199_254_740_992)
    );
    assert_eq!(
        max_commit_numeric_values(&values).as_i64(),
        Some(9_007_199_254_740_993)
    );
}
