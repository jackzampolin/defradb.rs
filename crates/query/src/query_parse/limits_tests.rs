use crate::limits::QueryLimits;

use super::{parse_request_with_limits, ParsedOperation};

fn limits(max_query_depth: usize, max_query_width: usize, max_filter_depth: usize) -> QueryLimits {
    QueryLimits {
        max_query_depth,
        max_query_width,
        max_filter_depth,
    }
}

#[test]
fn custom_query_depth_limit_rejects_deep_selects() {
    let result = parse_request_with_limits(
        "{ Users { name posts { title } } }",
        None,
        None,
        limits(1, 100, 50),
    );

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("query exceeds maximum nesting depth of 1"));
}

#[test]
fn custom_query_width_limit_rejects_wide_selects() {
    let result = parse_request_with_limits(
        "{ Users { name age active } }",
        None,
        None,
        limits(20, 2, 50),
    );

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("query exceeds maximum field width of 2 at depth 1"));
}

#[test]
fn custom_filter_depth_limit_rejects_nested_filters() {
    let result = parse_request_with_limits(
        r#"{ Users(filter: {_and: [{_and: [{name: {_eq: "Alice"}}]}]}) { name } }"#,
        None,
        None,
        limits(20, 100, 1),
    );

    assert!(result
        .unwrap_err()
        .to_string()
        .contains("filter exceeds maximum nesting depth of 1"));
}

#[test]
fn zero_query_limits_disable_shape_checks() {
    let parsed = parse_request_with_limits(
        "{ Users { name age posts { title body } } }",
        None,
        None,
        limits(0, 0, 50),
    )
    .unwrap();

    assert!(matches!(parsed, ParsedOperation::Query { .. }));
}
