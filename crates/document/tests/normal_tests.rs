//! Integration tests for NormalValue

use document::{JsonLeafValue, JsonPath, JsonScalarValue, NormalValue};

#[test]
fn test_is_nil() {
    assert!(NormalValue::Null.is_nil());
    assert!(NormalValue::NillableBool(None).is_nil());
    assert!(NormalValue::NillableString(None).is_nil());
    assert!(!NormalValue::Bool(true).is_nil());
    assert!(!NormalValue::NillableBool(Some(true)).is_nil());
}

#[test]
fn test_is_nillable() {
    assert!(NormalValue::Null.is_nillable());
    assert!(NormalValue::NillableBool(Some(true)).is_nillable());
    assert!(!NormalValue::Bool(true).is_nillable());
    assert!(!NormalValue::Int(42).is_nillable());
}

#[test]
fn test_is_array() {
    assert!(NormalValue::IntArray(vec![1, 2, 3]).is_array());
    assert!(NormalValue::StringArray(vec!["a".into()]).is_array());
    assert!(!NormalValue::Int(42).is_array());
    assert!(!NormalValue::String("hello".into()).is_array());
}

#[test]
fn test_classifier_edge_cases() {
    let nillable_array = NormalValue::NillableStringArray(None);
    assert!(nillable_array.is_nil());
    assert!(nillable_array.is_nillable());
    assert!(nillable_array.is_array());

    let nillable_elements = NormalValue::NillableIntElementArray(vec![Some(1), None]);
    assert!(!nillable_elements.is_nil());
    assert!(nillable_elements.is_nillable());
    assert!(nillable_elements.is_array());

    let json_leaf = NormalValue::JsonLeaf(JsonLeafValue::new(
        JsonPath::new(),
        JsonScalarValue::String("value".into()),
    ));
    assert!(!json_leaf.is_nil());
    assert!(!json_leaf.is_nillable());
    assert!(!json_leaf.is_array());
}

#[test]
fn test_as_bool() {
    assert_eq!(NormalValue::Bool(true).as_bool(), Some(true));
    assert_eq!(
        NormalValue::NillableBool(Some(false)).as_bool(),
        Some(false)
    );
    assert_eq!(NormalValue::NillableBool(None).as_bool(), None);
    assert_eq!(NormalValue::Int(1).as_bool(), None);
}

#[test]
fn test_as_int() {
    assert_eq!(NormalValue::Int(42).as_int(), Some(42));
    assert_eq!(NormalValue::NillableInt(Some(100)).as_int(), Some(100));
    assert_eq!(NormalValue::NillableInt(None).as_int(), None);
    assert_eq!(NormalValue::String("42".into()).as_int(), None);
}

#[test]
fn test_as_str() {
    assert_eq!(NormalValue::String("hello".into()).as_str(), Some("hello"));
    assert_eq!(
        NormalValue::NillableString(Some("world".into())).as_str(),
        Some("world")
    );
    assert_eq!(NormalValue::NillableString(None).as_str(), None);
    assert_eq!(NormalValue::Int(42).as_str(), None);
}

#[test]
fn test_from_implementations() {
    assert_eq!(NormalValue::from(true), NormalValue::Bool(true));
    assert_eq!(NormalValue::from(42i64), NormalValue::Int(42));
    assert_eq!(NormalValue::from(3.15f64), NormalValue::Float64(3.15));
    assert_eq!(
        NormalValue::from("hello"),
        NormalValue::String("hello".into())
    );
}

#[test]
fn test_default() {
    assert_eq!(NormalValue::default(), NormalValue::Null);
}

#[test]
fn test_json_leaves_null() {
    let json = NormalValue::Json(serde_json::Value::Null);
    let leaves = json.json_leaves();
    assert_eq!(leaves.len(), 1);
    assert!(matches!(leaves[0], NormalValue::Null));
}

#[test]
fn test_json_leaves_scalar() {
    let json = NormalValue::Json(serde_json::json!(42));
    let leaves = json.json_leaves();
    assert_eq!(leaves.len(), 1);
    if let NormalValue::JsonLeaf(leaf) = &leaves[0] {
        assert!(leaf.path.is_empty());
        assert_eq!(leaf.value, JsonScalarValue::Number(42.0));
    } else {
        panic!("expected JsonLeaf");
    }
}

#[test]
fn test_json_leaves_simple_object() {
    let json = NormalValue::Json(serde_json::json!({"height": 168, "weight": 70}));
    let leaves = json.json_leaves();
    assert_eq!(leaves.len(), 2);
    // Both should be JsonLeaf with single-part paths
    for leaf in &leaves {
        if let NormalValue::JsonLeaf(l) = leaf {
            assert_eq!(l.path.len(), 1);
        } else {
            panic!("expected JsonLeaf");
        }
    }
}

#[test]
fn test_json_leaves_nested_object() {
    let json = NormalValue::Json(serde_json::json!({"custom": {"height": 168}}));
    let leaves = json.json_leaves();
    assert_eq!(leaves.len(), 1);
    if let NormalValue::JsonLeaf(leaf) = &leaves[0] {
        assert_eq!(leaf.path.len(), 2);
        assert_eq!(leaf.value, JsonScalarValue::Number(168.0));
    } else {
        panic!("expected JsonLeaf");
    }
}

#[test]
fn test_json_leaves_array() {
    let json = NormalValue::Json(serde_json::json!({"tags": ["a", "b", "c"]}));
    let leaves = json.json_leaves();
    assert_eq!(leaves.len(), 3);
    // Each leaf should have path [Property("tags"), Index]
    for leaf in &leaves {
        if let NormalValue::JsonLeaf(l) = leaf {
            assert_eq!(l.path.len(), 2);
        } else {
            panic!("expected JsonLeaf");
        }
    }
}

#[test]
fn test_json_leaves_empty_object() {
    let json = NormalValue::Json(serde_json::json!({}));
    let leaves = json.json_leaves();
    assert!(leaves.is_empty());
}

#[test]
fn test_json_leaves_empty_array() {
    let json = NormalValue::Json(serde_json::json!([]));
    let leaves = json.json_leaves();
    assert!(leaves.is_empty());
}

#[test]
fn test_json_leaves_non_json() {
    let val = NormalValue::Int(42);
    let leaves = val.json_leaves();
    assert!(leaves.is_empty());
}
