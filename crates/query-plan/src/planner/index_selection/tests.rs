use document::{JsonPathPart, JsonScalarValue, NormalValue};
use schema::{IndexDescription, IndexedFieldDescription};
use serde_json::{json, Map, Value as JsonValue};
use storage::index::Bound;

use query_types::mapper::{Filter, FilterOp};

use super::values::json_to_normal_value;
use super::*;

fn map<const N: usize>(entries: [(String, JsonValue); N]) -> Map<String, JsonValue> {
    entries.into_iter().collect()
}

fn make_filter(conditions: Map<String, JsonValue>) -> Filter {
    Filter::from_conditions(conditions)
}

fn single_field_index(field: &str) -> IndexDescription {
    IndexDescription {
        id: 1,
        name: format!("{}_idx", field),
        unique: false,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: field.to_string(),
            descending: false,
        }],
    }
}

fn composite_index(fields: &[&str]) -> IndexDescription {
    IndexDescription {
        id: 2,
        name: "composite_idx".to_string(),
        unique: false,
        auto_generated: false,
        fields: fields
            .iter()
            .map(|f| IndexedFieldDescription {
                name: f.to_string(),
                descending: false,
            })
            .collect(),
    }
}

fn unique_index(field: &str) -> IndexDescription {
    IndexDescription {
        id: 3,
        name: format!("{}_unique_idx", field),
        unique: true,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: field.to_string(),
            descending: false,
        }],
    }
}

#[test]
fn test_can_use_index_eq() {
    let filter = make_filter(map([("name".to_string(), json!({"_eq": "alice"}))]));
    let index = single_field_index("name");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_can_use_index_wrong_field() {
    let filter = make_filter(map([("age".to_string(), json!({"_eq": 30}))]));
    let index = single_field_index("name");

    assert!(!can_use_index(&filter, &index));
}

#[test]
fn test_can_use_index_range() {
    let filter = make_filter(map([("age".to_string(), json!({"_gt": 18, "_lt": 65}))]));
    let index = single_field_index("age");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_can_use_index_in() {
    let filter = make_filter(map([(
        "status".to_string(),
        json!({"_in": ["active", "pending"]}),
    )]));
    let index = single_field_index("status");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_can_use_index_ne() {
    // _ne uses full index scan (matching Go behavior)
    let filter = make_filter(map([("name".to_string(), json!({"_ne": "alice"}))]));
    let index = single_field_index("name");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_can_use_index_like() {
    // _like uses full index scan (matching Go behavior)
    let filter = make_filter(map([("name".to_string(), json!({"_like": "%alice%"}))]));
    let index = single_field_index("name");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_filter_to_scan_exact_match() {
    let filter = make_filter(map([("name".to_string(), json!({"_eq": "alice"}))]));
    let index = single_field_index("name");

    let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
    assert_eq!(params.index_name, "name_idx");

    match params.scan_type {
        IndexScanType::ExactMatch { values } => {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], NormalValue::String("alice".to_string()));
        }
        _ => panic!("expected ExactMatch scan type"),
    }
}

#[test]
fn test_filter_to_scan_in() {
    let filter = make_filter(map([(
        "status".to_string(),
        json!({"_in": ["active", "pending"]}),
    )]));
    let index = single_field_index("status");

    let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();

    match params.scan_type {
        IndexScanType::InScan { values, .. } => {
            assert_eq!(values.len(), 2);
        }
        _ => panic!("expected InScan scan type"),
    }
}

#[test]
fn test_filter_to_scan_range() {
    let filter = make_filter(map([("age".to_string(), json!({"_gte": 18, "_lt": 65}))]));
    let index = single_field_index("age");

    let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();

    match params.scan_type {
        IndexScanType::RangeScan {
            lower,
            upper,
            prefix_values,
            ..
        } => {
            assert!(prefix_values.is_empty());
            match lower {
                Bound::Inclusive(v) => assert_eq!(v, NormalValue::Int(18)),
                _ => panic!("expected inclusive lower bound"),
            }
            match upper {
                Bound::Exclusive(v) => assert_eq!(v, NormalValue::Int(65)),
                _ => panic!("expected exclusive upper bound"),
            }
        }
        _ => panic!("expected RangeScan scan type"),
    }
}

#[test]
fn test_select_best_index_prefers_eq() {
    let filter = make_filter(map([
        ("name".to_string(), json!({"_eq": "alice"})),
        ("age".to_string(), json!({"_gt": 18})),
    ]));

    let indexes = vec![single_field_index("name"), single_field_index("age")];

    let best = select_best_index(&filter, &indexes).unwrap();
    assert_eq!(best.fields[0].name, "name"); // eq is preferred
}

#[test]
fn test_select_best_index_prefers_unique() {
    let filter = make_filter(map([("email".to_string(), json!({"_eq": "a@b.com"}))]));

    let indexes = vec![single_field_index("email"), unique_index("email")];

    let best = select_best_index(&filter, &indexes).unwrap();
    assert!(best.unique);
}

#[test]
fn test_select_best_index_composite() {
    let filter = make_filter(map([
        ("category".to_string(), json!({"_eq": "electronics"})),
        ("brand".to_string(), json!({"_eq": "sony"})),
    ]));

    let indexes = vec![
        single_field_index("category"),
        composite_index(&["category", "brand"]),
    ];

    let best = select_best_index(&filter, &indexes).unwrap();
    assert_eq!(best.fields.len(), 2); // composite is preferred
}

#[test]
fn test_extract_field_conditions_simple() {
    let filter = make_filter(map([
        ("name".to_string(), json!({"_eq": "alice"})),
        ("age".to_string(), json!({"_gt": 18})),
    ]));

    let conditions = extract_field_conditions(&filter);
    assert_eq!(conditions.len(), 2);

    let name_cond = conditions.iter().find(|c| c.field_name == "name").unwrap();
    assert_eq!(name_cond.op, FilterOp::Eq);

    let age_cond = conditions.iter().find(|c| c.field_name == "age").unwrap();
    assert_eq!(age_cond.op, FilterOp::Gt);
}

#[test]
fn test_extract_field_conditions_and() {
    let filter = make_filter(map([(
        "_and".to_string(),
        json!([
            {"name": {"_eq": "alice"}},
            {"age": {"_gt": 18}}
        ]),
    )]));

    let conditions = extract_field_conditions(&filter);
    assert_eq!(conditions.len(), 2);
}

#[test]
fn test_json_to_normal_value() {
    assert_eq!(json_to_normal_value(&json!(null)), Some(NormalValue::Null));
    assert_eq!(
        json_to_normal_value(&json!(true)),
        Some(NormalValue::Bool(true))
    );
    assert_eq!(json_to_normal_value(&json!(42)), Some(NormalValue::Int(42)));
    assert_eq!(
        json_to_normal_value(&json!(3.15)),
        Some(NormalValue::Float64(3.15))
    );
    assert_eq!(
        json_to_normal_value(&json!("hello")),
        Some(NormalValue::String("hello".to_string()))
    );
    assert_eq!(json_to_normal_value(&json!([1, 2, 3])), None); // arrays not supported
}

#[test]
fn test_empty_filter_cannot_use_index() {
    let filter = Filter::new();
    let index = single_field_index("name");

    assert!(!can_use_index(&filter, &index));
}

#[test]
fn test_condition_value_variants() {
    let ops = serde_json::from_str::<serde_json::Map<String, JsonValue>>(
        r#"{"_eq": "alice", "_in": ["a", "b"], "_like": "test%"}"#,
    )
    .unwrap();

    let conditions = FieldCondition::parse("name", &ops);
    assert_eq!(conditions.len(), 3);

    let eq_cond = conditions.iter().find(|c| c.op == FilterOp::Eq).unwrap();
    assert!(matches!(eq_cond.value, ConditionValue::Single(_)));

    let in_cond = conditions.iter().find(|c| c.op == FilterOp::In).unwrap();
    assert!(matches!(in_cond.value, ConditionValue::Multiple(_)));

    let like_cond = conditions.iter().find(|c| c.op == FilterOp::Like).unwrap();
    assert!(matches!(like_cond.value, ConditionValue::Pattern(_)));
}

#[test]
fn test_can_use_index_array_any() {
    // Filter: {numbers: {_any: {_eq: 30}}}
    let filter = make_filter(map([("numbers".to_string(), json!({"_any": {"_eq": 30}}))]));
    let index = single_field_index("numbers");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_can_use_index_array_all() {
    // Filter: {numbers: {_all: {_eq: 30}}}
    let filter = make_filter(map([("numbers".to_string(), json!({"_all": {"_eq": 30}}))]));
    let index = single_field_index("numbers");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_cannot_use_index_array_none() {
    // Filter: {numbers: {_none: {_eq: 30}}} - _none cannot use index
    let filter = make_filter(map([(
        "numbers".to_string(),
        json!({"_none": {"_eq": 30}}),
    )]));
    let index = single_field_index("numbers");

    assert!(!can_use_index(&filter, &index));
}

#[test]
fn test_can_use_index_array_all_with_range_op() {
    // Filter: {numbers: {_all: {_geq: 33}}}
    // _all with range operators (not just _eq/_in) should use index
    // Index provides candidates, residual filter verifies ALL match
    let filter = make_filter(map([(
        "numbers".to_string(),
        json!({"_all": {"_gte": 33}}),
    )]));
    let index = single_field_index("numbers");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_can_use_composite_index_with_none_on_second_field() {
    // Filter: {name: {_eq: "Shahzad"}, numbers: {_none: {_eq: 3}}}
    // Composite index [name, numbers] should be usable because first field has _eq
    // _none on second field is handled by residual filter
    let filter = make_filter(map([
        ("name".to_string(), json!({"_eq": "Shahzad"})),
        ("numbers".to_string(), json!({"_none": {"_eq": 3}})),
    ]));
    let index = composite_index(&["name", "numbers"]);

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_cannot_use_composite_index_with_none_on_first_field() {
    // Filter: {numbers: {_none: {_eq: 3}}, name: {_eq: "Shahzad"}}
    // Composite index [numbers, name] cannot be used because first field has _none
    let filter = make_filter(map([
        ("numbers".to_string(), json!({"_none": {"_eq": 3}})),
        ("name".to_string(), json!({"_eq": "Shahzad"})),
    ]));
    let index = composite_index(&["numbers", "name"]);

    assert!(!can_use_index(&filter, &index));
}

#[test]
fn test_filter_to_scan_array_any() {
    let filter = make_filter(map([("numbers".to_string(), json!({"_any": {"_eq": 30}}))]));
    let index = single_field_index("numbers");

    let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
    assert_eq!(params.index_name, "numbers_idx");

    match params.scan_type {
        IndexScanType::ExactMatch { values } => {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], NormalValue::Int(30));
        }
        _ => panic!("expected ExactMatch scan type for _any with _eq"),
    }
}

#[test]
fn test_extract_array_conditions() {
    // Parse: {_any: {_eq: 30}}
    let ops =
        serde_json::from_str::<serde_json::Map<String, JsonValue>>(r#"{"_any": {"_eq": 30}}"#)
            .unwrap();

    let conditions = FieldCondition::parse("numbers", &ops);
    assert_eq!(conditions.len(), 1);

    let cond = &conditions[0];
    assert_eq!(cond.field_name, "numbers");
    assert_eq!(cond.op, FilterOp::Eq);
    assert_eq!(cond.array_op, Some(FilterOp::Any));
    match &cond.value {
        ConditionValue::Single(v) => assert_eq!(*v, NormalValue::Int(30)),
        _ => panic!("expected single value"),
    }
}

#[test]
fn test_extract_json_path_simple() {
    // Parse: {height: {_gt: 170}} - JSON field filter
    let ops =
        serde_json::from_str::<serde_json::Map<String, JsonValue>>(r#"{"height": {"_gt": 170}}"#)
            .unwrap();

    let conditions = FieldCondition::parse("custom", &ops);
    assert_eq!(conditions.len(), 1);

    let cond = &conditions[0];
    assert_eq!(cond.field_name, "custom");
    assert_eq!(cond.op, FilterOp::Gt);
    assert!(cond.json_path.is_some());

    let path = cond.json_path.as_ref().unwrap();
    assert_eq!(path.0.len(), 1);
    assert_eq!(path.0[0], JsonPathPart::Property("height".to_string()));
}

#[test]
fn test_extract_json_path_nested() {
    // Parse: {profile: {address: {city: {_eq: "NYC"}}}}
    let ops = serde_json::from_str::<serde_json::Map<String, JsonValue>>(
        r#"{"profile": {"address": {"city": {"_eq": "NYC"}}}}"#,
    )
    .unwrap();

    let conditions = FieldCondition::parse("custom", &ops);
    assert_eq!(conditions.len(), 1);

    let cond = &conditions[0];
    assert_eq!(cond.field_name, "custom");
    assert_eq!(cond.op, FilterOp::Eq);
    assert!(cond.json_path.is_some());

    let path = cond.json_path.as_ref().unwrap();
    assert_eq!(path.0.len(), 3);
    assert_eq!(path.0[0], JsonPathPart::Property("profile".to_string()));
    assert_eq!(path.0[1], JsonPathPart::Property("address".to_string()));
    assert_eq!(path.0[2], JsonPathPart::Property("city".to_string()));
}

#[test]
fn test_can_use_index_json_path() {
    // Filter: {custom: {height: {_gt: 170}}}
    let filter = make_filter(map([(
        "custom".to_string(),
        json!({"height": {"_gt": 170}}),
    )]));
    let index = single_field_index("custom");

    assert!(can_use_index(&filter, &index));
}

#[test]
fn test_filter_to_scan_json_path_eq() {
    // Filter: {custom: {height: {_eq: 168}}}
    let filter = make_filter(map([(
        "custom".to_string(),
        json!({"height": {"_eq": 168}}),
    )]));
    let index = single_field_index("custom");

    let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
    assert_eq!(params.index_name, "custom_idx");

    match params.scan_type {
        IndexScanType::ExactMatch { values } => {
            assert_eq!(values.len(), 1);
            // The value should be wrapped in JsonLeaf with the path
            match &values[0] {
                NormalValue::JsonLeaf(leaf) => {
                    assert_eq!(leaf.path.0.len(), 1);
                    assert_eq!(leaf.path.0[0], JsonPathPart::Property("height".to_string()));
                    assert_eq!(leaf.value, JsonScalarValue::Number(168.0));
                }
                _ => panic!("expected JsonLeaf value, got {:?}", values[0]),
            }
        }
        _ => panic!("expected ExactMatch scan type"),
    }
}

#[test]
fn test_filter_to_scan_json_path_range() {
    // Filter: {custom: {height: {_gt: 170}}}
    let filter = make_filter(map([(
        "custom".to_string(),
        json!({"height": {"_gt": 170}}),
    )]));
    let index = single_field_index("custom");

    let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
    assert_eq!(params.index_name, "custom_idx");

    match params.scan_type {
        IndexScanType::RangeScan { lower, upper, .. } => {
            // Lower bound should be wrapped in JsonLeaf
            match lower {
                Bound::Exclusive(v) => match v {
                    NormalValue::JsonLeaf(leaf) => {
                        assert_eq!(leaf.path.0[0], JsonPathPart::Property("height".to_string()));
                        assert_eq!(leaf.value, JsonScalarValue::Number(170.0));
                    }
                    _ => panic!("expected JsonLeaf value, got {:?}", v),
                },
                _ => panic!("expected Exclusive lower bound"),
            }
            // Upper bound should be constrained to PathMax for the JSON path
            match upper {
                Bound::Exclusive(v) => match v {
                    NormalValue::JsonLeaf(leaf) => {
                        assert_eq!(leaf.path.0[0], JsonPathPart::Property("height".to_string()));
                        assert_eq!(leaf.value, JsonScalarValue::PathMax);
                    }
                    _ => panic!("expected JsonLeaf value for upper bound, got {:?}", v),
                },
                _ => panic!("expected Exclusive upper bound with PathMax"),
            }
        }
        _ => panic!("expected RangeScan scan type"),
    }
}

#[test]
fn test_filter_to_scan_json_path_in() {
    // Filter: {custom: {status: {_in: ["active", "pending"]}}}
    let filter = make_filter(map([(
        "custom".to_string(),
        json!({"status": {"_in": ["active", "pending"]}}),
    )]));
    let index = single_field_index("custom");

    let params = filter_to_index_scan(&filter, &index, None, &[], None, 0).unwrap();
    assert_eq!(params.index_name, "custom_idx");

    match params.scan_type {
        IndexScanType::InScan { values, .. } => {
            assert_eq!(values.len(), 2);
            // All values should be wrapped in JsonLeaf with the path
            for value in &values {
                match value {
                    NormalValue::JsonLeaf(leaf) => {
                        assert_eq!(leaf.path.0.len(), 1);
                        assert_eq!(leaf.path.0[0], JsonPathPart::Property("status".to_string()));
                    }
                    _ => panic!("expected JsonLeaf value"),
                }
            }
        }
        _ => panic!("expected InScan scan type"),
    }
}
