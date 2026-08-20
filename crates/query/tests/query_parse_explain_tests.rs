//! Integration tests for the GraphQL query parser: explain directives and top-level aggregates.

use query::parse_query;

#[test]
fn test_parse_explain_directive() {
    use query::query_parse::{parse_request, ExplainType, ParsedOperation};

    let query = "query @explain { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query {
            selects, explain, ..
        } => {
            assert!(
                explain.is_some(),
                "Expected explain=Some for @explain directive"
            );
            assert_eq!(explain, Some(ExplainType::Simple));
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_explain_directive_with_type_simple() {
    use query::query_parse::{parse_request, ExplainType, ParsedOperation};

    let query = "query @explain(type: simple) { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query {
            selects, explain, ..
        } => {
            assert_eq!(explain, Some(ExplainType::Simple));
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_explain_directive_with_type_execute() {
    use query::query_parse::{parse_request, ExplainType, ParsedOperation};

    let query = "query @explain(type: execute) { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query {
            selects, explain, ..
        } => {
            assert_eq!(explain, Some(ExplainType::Execute));
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_explain_directive_with_type_debug() {
    use query::query_parse::{parse_request, ExplainType, ParsedOperation};

    let query = "query @explain(type: debug) { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query {
            selects, explain, ..
        } => {
            assert_eq!(explain, Some(ExplainType::Debug));
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_query_without_explain() {
    use query::query_parse::{parse_request, ParsedOperation};

    let query = "query { Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query {
            selects, explain, ..
        } => {
            assert!(
                explain.is_none(),
                "Expected explain=None without @explain directive"
            );
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_bare_query_without_explain() {
    use query::query_parse::{parse_request, ParsedOperation};

    // Bare selection set (no 'query' keyword)
    let query = "{ Users { _docID name } }";
    let result = parse_request(query).unwrap();

    match result {
        ParsedOperation::Query {
            selects, explain, ..
        } => {
            assert!(
                explain.is_none(),
                "Expected explain=None for bare selection set"
            );
            assert_eq!(selects.len(), 1);
        }
        _ => panic!("Expected query"),
    }
}

#[test]
fn test_parse_top_level_aggregate() {
    use query::mapper::Requestable;

    let query = "{ AVG(Users: {field: Age}) }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(
        selects[0].collection_name, "Users",
        "Collection name should be extracted from aggregate target"
    );
    assert_eq!(selects[0].fields.len(), 1);

    // The field should be an aggregate
    match &selects[0].fields[0] {
        Requestable::Aggregate(agg) => {
            assert_eq!(agg.aggregate_type, query::mapper::AggregateType::Average);
            assert_eq!(agg.targets.len(), 1);
            assert_eq!(agg.targets[0].host_name, "Users");
            assert_eq!(agg.targets[0].field_name, Some("Age".to_string()));
        }
        _ => panic!("Expected aggregate"),
    }
}

#[test]
fn test_parse_top_level_count() {
    use query::mapper::Requestable;

    let query = "{ COUNT(Users: {}) }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].collection_name, "Users");

    match &selects[0].fields[0] {
        Requestable::Aggregate(agg) => {
            assert_eq!(agg.aggregate_type, query::mapper::AggregateType::Count);
            assert_eq!(agg.targets.len(), 1);
            assert_eq!(agg.targets[0].host_name, "Users");
        }
        _ => panic!("Expected aggregate"),
    }
}

#[test]
fn test_parse_top_level_aggregate_with_alias() {
    use query::mapper::Requestable;

    let query = "{ average: AVG(Users: {field: Age}) }";
    let selects = parse_query(query).unwrap();

    assert_eq!(selects.len(), 1);
    assert_eq!(selects[0].collection_name, "Users");

    match &selects[0].fields[0] {
        Requestable::Aggregate(agg) => {
            assert_eq!(agg.alias, Some("average".to_string()));
        }
        _ => panic!("Expected aggregate"),
    }
}
