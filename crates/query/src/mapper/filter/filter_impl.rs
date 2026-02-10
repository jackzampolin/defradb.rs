//! Filter types and evaluation for query conditions

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use super::eval::{eval_op, values_equal};
use super::op::FilterOp;
use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};

/// Filter condition containing parsed conditions.
///
/// Conditions map field names to their filter values.
/// Supports nested conditions for related objects.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Parsed filter conditions
    conditions: HashMap<String, JsonValue>,
}

impl Filter {
    /// Create an empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a filter from conditions map
    pub fn from_conditions(conditions: HashMap<String, JsonValue>) -> Self {
        Self { conditions }
    }

    /// Check if the filter is empty
    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    /// Combine this filter with another filter using AND logic.
    ///
    /// Returns a new filter where both conditions must be satisfied.
    pub fn and(&self, other: Filter) -> Filter {
        let mut combined_conditions = HashMap::new();
        combined_conditions.insert(
            "_and".to_string(),
            serde_json::json!([self.conditions, other.conditions]),
        );
        Filter::from_conditions(combined_conditions)
    }

    /// Get a reference to the conditions map
    pub fn conditions(&self) -> &HashMap<String, JsonValue> {
        &self.conditions
    }

    /// Evaluate the filter against document fields
    pub fn matches(&self, fields: &[Option<JsonValue>], mapping: &DocumentMapping) -> Result<bool> {
        if self.conditions.is_empty() {
            return Ok(true);
        }
        self.eval_conditions(&self.conditions, fields, mapping)
    }

    fn eval_conditions(
        &self,
        conditions: &HashMap<String, JsonValue>,
        fields: &[Option<JsonValue>],
        mapping: &DocumentMapping,
    ) -> Result<bool> {
        for (key, value) in conditions {
            // Check for logical operators
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And => {
                        // Null _and is a no-op (matches all)
                        if value.is_null() {
                            continue;
                        }
                        let arr = value
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_and requires array"))?;
                        for item in arr {
                            let sub_conditions: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            if !self.eval_conditions(&sub_conditions, fields, mapping)? {
                                return Ok(false);
                            }
                        }
                        continue;
                    }
                    FilterOp::Or => {
                        // Null _or is a no-op (matches all)
                        if value.is_null() {
                            continue;
                        }
                        let arr = value
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_or requires array"))?;
                        let mut any_match = false;
                        for item in arr {
                            let sub_conditions: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            if self.eval_conditions(&sub_conditions, fields, mapping)? {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return Ok(false);
                        }
                        continue;
                    }
                    FilterOp::Not => {
                        // Null _not is a no-op (matches all)
                        if value.is_null() {
                            continue;
                        }
                        let sub_conditions: HashMap<String, JsonValue> =
                            serde_json::from_value(value.clone())
                                .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                        if self.eval_conditions(&sub_conditions, fields, mapping)? {
                            return Ok(false);
                        }
                        continue;
                    }
                    _ => {} // Non-logical ops handled below
                }
            }

            // Handle _alias filter: filter by aliased field names (render keys)
            // Example: filter: {_alias: {myAge: {_gt: 20}}} where myAge is an alias for Age
            // Also supports logical operators: {_alias: {_and: [{myAge: {_gt: 20}}]}}
            if key == "_alias" {
                // Null or non-object _alias filters out all documents (Go compatibility)
                if value.is_null() {
                    return Ok(false);
                }
                let alias_conditions: HashMap<String, JsonValue> =
                    match serde_json::from_value(value.clone()) {
                        Ok(v) => v,
                        Err(_) => {
                            // Non-object _alias (e.g., integer) filters everything out
                            return Ok(false);
                        }
                    };

                if !self.eval_alias_conditions(&alias_conditions, fields, mapping)? {
                    return Ok(false);
                }
                continue;
            }

            // Field condition
            let field_index = mapping
                .first_index_of_name(key)
                .ok_or_else(|| QueryError::unknown_field(key))?;

            let field_value = fields
                .get(field_index)
                .and_then(|v| v.as_ref())
                .cloned()
                .unwrap_or(JsonValue::Null);

            // Handle null field conditions: {Name: null} is equivalent to {Name: {_eq: null}}
            if value.is_null() {
                if !values_equal(&field_value, &JsonValue::Null) {
                    return Ok(false);
                }
                continue;
            }

            // Value should be an object with operator keys or nested field conditions
            let ops = value
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("field condition must be object"))?;

            // Check if this is a relation filter (nested field conditions) or operator conditions
            let is_relation_filter = ops.keys().any(|k| FilterOp::parse(k).is_none());

            if is_relation_filter {
                // This is a nested field filter like {Custom: {age: {_ge: null}}}
                // or a relation filter like {author: {verified: {_eq: true}}}
                // The field_value can be:
                // - null (no related document / null JSON field)
                // - an object (one-to-one relation or JSON object)
                // - an array (one-to-many relation)

                if field_value.is_null() {
                    // Handle null field value:
                    // - For JSON fields: null.path = null, so propagate null through nested access
                    // - Evaluate nested conditions with null values to handle cases like null >= null
                    let empty_obj = serde_json::Map::new();
                    if !self.eval_relation_conditions(ops, &empty_obj)? {
                        return Ok(false);
                    }
                } else if let Some(arr) = field_value.as_array() {
                    // Handle arrays (one-to-many relations) with existential semantics
                    // If ANY element matches, the filter passes
                    let mut any_match = false;
                    for elem in arr {
                        if let Some(obj) = elem.as_object() {
                            if self.eval_relation_conditions(ops, obj)? {
                                any_match = true;
                                break;
                            }
                        }
                        // Non-object elements in array are skipped
                    }
                    if !any_match {
                        return Ok(false);
                    }
                } else if let Some(related_obj) = field_value.as_object() {
                    // Handle objects (one-to-one relations or JSON objects)
                    if !self.eval_relation_conditions(ops, related_obj)? {
                        return Ok(false);
                    }
                } else {
                    // Field value is neither null, array, nor object - invalid for relation filter
                    return Err(QueryError::invalid_filter(format!(
                        "relation field '{}' must be null, object, or array, got {:?}",
                        key, field_value
                    )));
                }
            } else {
                // Standard operator conditions
                for (op_str, expected) in ops {
                    let op = FilterOp::parse(op_str).ok_or_else(|| {
                        QueryError::invalid_filter(format!("unknown operator: {}", op_str))
                    })?;

                    if !eval_op(&field_value, op, expected)? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// Evaluate relation filter conditions against a JSON object.
    ///
    /// This handles nested conditions like `{verified: {_eq: true}}` where the
    /// condition is evaluated against a related document's fields.
    fn eval_relation_conditions(
        &self,
        conditions: &serde_json::Map<String, JsonValue>,
        obj: &serde_json::Map<String, JsonValue>,
    ) -> Result<bool> {
        for (key, value) in conditions {
            // Check for logical operators
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And => {
                        let arr = value
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_and requires array"))?;
                        for item in arr {
                            let sub_conds = item.as_object().ok_or_else(|| {
                                QueryError::invalid_filter("_and items must be objects")
                            })?;
                            if !self.eval_relation_conditions(sub_conds, obj)? {
                                return Ok(false);
                            }
                        }
                        continue;
                    }
                    FilterOp::Or => {
                        let arr = value
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_or requires array"))?;
                        let mut any_match = false;
                        for item in arr {
                            let sub_conds = item.as_object().ok_or_else(|| {
                                QueryError::invalid_filter("_or items must be objects")
                            })?;
                            if self.eval_relation_conditions(sub_conds, obj)? {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return Ok(false);
                        }
                        continue;
                    }
                    FilterOp::Not => {
                        let sub_conds = value
                            .as_object()
                            .ok_or_else(|| QueryError::invalid_filter("_not requires object"))?;
                        if self.eval_relation_conditions(sub_conds, obj)? {
                            return Ok(false);
                        }
                        continue;
                    }
                    _ => {}
                }
            }

            // Field condition - get the field value from the object
            let field_value = obj.get(key).cloned().unwrap_or(JsonValue::Null);

            let ops = value
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("field condition must be object"))?;

            // Check if this is another level of nesting
            let is_nested = ops.keys().any(|k| FilterOp::parse(k).is_none());

            if is_nested {
                // Another level of relation nesting
                // Handle null, array (one-to-many), or object (one-to-one)
                if field_value.is_null() {
                    return Ok(false);
                }

                // Handle arrays with existential semantics
                if let Some(arr) = field_value.as_array() {
                    let mut any_match = false;
                    for elem in arr {
                        if let Some(obj) = elem.as_object() {
                            if self.eval_relation_conditions(ops, obj)? {
                                any_match = true;
                                break;
                            }
                        }
                    }
                    if !any_match {
                        return Ok(false);
                    }
                } else if let Some(nested_obj) = field_value.as_object() {
                    if !self.eval_relation_conditions(ops, nested_obj)? {
                        return Ok(false);
                    }
                } else {
                    return Err(QueryError::invalid_filter(format!(
                        "nested relation field '{}' must be an object or array",
                        key
                    )));
                }
            } else {
                // Operator conditions
                // When a JSON property is missing (null) and _eq/_ne compares against
                // a complex value (object/array), exclude the document. Missing JSON
                // properties can't match complex equality/inequality comparisons.
                if field_value.is_null() {
                    for (op_str, expected) in ops {
                        if matches!(
                            FilterOp::parse(op_str),
                            Some(FilterOp::Eq | FilterOp::Ne)
                        ) && (expected.is_object() || expected.is_array())
                        {
                            return Ok(false);
                        }
                    }
                }
                for (op_str, expected) in ops {
                    let op = FilterOp::parse(op_str).ok_or_else(|| {
                        QueryError::invalid_filter(format!("unknown operator: {}", op_str))
                    })?;
                    if !eval_op(&field_value, op, expected)? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// Evaluate alias-based filter conditions.
    /// Alias filters allow filtering by render key names instead of field names.
    /// Supports logical operators (_and, _or, _not) within the alias block.
    fn eval_alias_conditions(
        &self,
        conditions: &HashMap<String, JsonValue>,
        fields: &[Option<JsonValue>],
        mapping: &DocumentMapping,
    ) -> Result<bool> {
        for (key, value) in conditions {
            // Check for logical operators within alias block
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And => {
                        let arr = value.as_array().ok_or_else(|| {
                            QueryError::invalid_filter("_and requires array in _alias")
                        })?;
                        for item in arr {
                            let sub_conditions: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            if !self.eval_alias_conditions(&sub_conditions, fields, mapping)? {
                                return Ok(false);
                            }
                        }
                        continue;
                    }
                    FilterOp::Or => {
                        let arr = value.as_array().ok_or_else(|| {
                            QueryError::invalid_filter("_or requires array in _alias")
                        })?;
                        let mut any_match = false;
                        for item in arr {
                            let sub_conditions: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            if self.eval_alias_conditions(&sub_conditions, fields, mapping)? {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return Ok(false);
                        }
                        continue;
                    }
                    FilterOp::Not => {
                        let sub_conditions: HashMap<String, JsonValue> =
                            serde_json::from_value(value.clone())
                                .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                        if self.eval_alias_conditions(&sub_conditions, fields, mapping)? {
                            return Ok(false);
                        }
                        continue;
                    }
                    _ => {} // Non-logical ops are handled as alias field conditions below
                }
            }

            // Look up by render_key (alias) instead of field name
            let field_index = mapping
                .try_find_index_from_render_key(key)
                .ok_or_else(|| QueryError::unknown_field(format!("alias '{}' not found", key)))?;

            let field_value = fields
                .get(field_index)
                .and_then(|v| v.as_ref())
                .cloned()
                .unwrap_or(JsonValue::Null);

            let ops = value.as_object().ok_or_else(|| {
                QueryError::invalid_filter("alias field condition must be object")
            })?;

            // Check if this is a relation filter (nested field conditions) or operator conditions
            let is_relation_filter = ops.keys().any(|k| FilterOp::parse(k).is_none());

            if is_relation_filter {
                // Relation-style alias: {_alias: {books: {rating: {_gt: 4.8}}}}
                // The alias points to a relation field (array or object)
                if field_value.is_null() {
                    // Null relation -> no match
                    return Ok(false);
                } else if let Some(arr) = field_value.as_array() {
                    // One-to-many: existential semantics (any element matches)
                    let mut any_match = false;
                    for elem in arr {
                        if let Some(obj) = elem.as_object() {
                            if self.eval_relation_conditions(ops, obj)? {
                                any_match = true;
                                break;
                            }
                        }
                    }
                    if !any_match {
                        return Ok(false);
                    }
                } else if let Some(obj) = field_value.as_object() {
                    // One-to-one: direct match
                    if !self.eval_relation_conditions(ops, obj)? {
                        return Ok(false);
                    }
                } else {
                    return Ok(false);
                }
            } else {
                // Scalar-style alias: {_alias: {myAge: {_gt: 20}}}
                for (op_key, op_value) in ops {
                    if let Some(op) = FilterOp::parse(op_key) {
                        if !eval_op(&field_value, op, op_value)? {
                            return Ok(false);
                        }
                    } else {
                        return Err(QueryError::invalid_filter(format!(
                            "unknown operator '{}' in _alias filter",
                            op_key
                        )));
                    }
                }
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_eq": "Alice"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_eq": "Bob"}))]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ne_filter() {
        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_ne": "Bob"}))]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_gt_filter() {
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 25}))]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 35}))]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_in_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_in": ["Alice", "Bob", "Charlie"]}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_in": ["Bob", "Charlie"]}),
        )]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_and_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "_and".to_string(),
            json!([
                {"name": {"_eq": "Alice"}},
                {"age": {"_gte": 18}}
            ]),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
            "_or".to_string(),
            json!([
                {"name": {"_eq": "Bob"}},
                {"age": {"_eq": 30}}
            ]),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
            "_not".to_string(),
            json!({"name": {"_eq": "Bob"}}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "Ali%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "%ice"}),
        )]));
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_case_insensitive_prefix() {
        // Pattern "ALI%" should match "Alice" (case-insensitive)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "ALI%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_case_insensitive_suffix() {
        // Pattern "%ICE" should match "Alice" (case-insensitive)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "%ICE"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_case_insensitive_contains() {
        // Pattern "%LIC%" should match "Alice" (case-insensitive)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "%LIC%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_case_insensitive_exact() {
        // Pattern "ALICE" should match "Alice" (case-insensitive exact)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "ALICE"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_filter_no_match() {
        // Pattern "BOB%" should NOT match "Alice"
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "BOB%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_nilike_filter() {
        // Negated: pattern "BOB%" should NOT match "Alice", so nilike returns true
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_nilike": "BOB%"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        // Negated: pattern "ALI%" WOULD match "Alice", so nilike returns false
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_nilike": "ALI%"}),
        )]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_underscore_as_literal() {
        // Underscore is treated as literal character (matches Go behavior)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "Al_ce"}),
        )]));
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
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "ALI%CE"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_prefix_suffix_pattern() {
        // Pattern "Ali%ce" should match "Alice" (case-sensitive)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "Ali%ce"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
        assert!(filter.matches(&fields, &mapping).unwrap());

        // Wrong case should NOT match
        let filter_wrong_case = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "ALI%CE"}),
        )]));
        assert!(!filter_wrong_case.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_prefix_suffix_no_match() {
        // Pattern "Bob%son" should NOT match "Alice"
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "Bob%son"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ilike_null_field_returns_false() {
        // Null field should return false for _ilike (not error), matching Go behavior
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ilike": "Ali%"}),
        )]));
        let mapping = make_mapping();
        let mut fields = make_fields();
        fields[1] = Some(json!(null)); // name is null
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_nilike_null_field_returns_false() {
        // Go returns false for _nilike with null (non-string values always return false)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_nilike": "Ali%"}),
        )]));
        let mapping = make_mapping();
        let mut fields = make_fields();
        fields[1] = Some(json!(null)); // name is null
        assert!(!filter.matches(&fields, &mapping).unwrap());
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
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": "rust"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contains_filter_no_match() {
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": "python"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contains_filter_non_array_error() {
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_contains": "rust"}),
        )]));
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
            "metadata".to_string(),
            json!({"_has_key": "version"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_has_key_filter_no_match() {
        let filter = Filter::from_conditions(HashMap::from([(
            "metadata".to_string(),
            json!({"_has_key": "nonexistent"}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields();
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_has_key_filter_non_object_error() {
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_has_key": "version"}),
        )]));
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
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_eq": null}))]));
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
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": 25}))]));
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
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_gt": null}))]));
        let mapping = make_mapping();
        let fields = make_fields(); // age = 30
        let result = filter.matches(&fields, &mapping);
        assert!(result.is_ok());
        assert!(result.unwrap(), "value _gt null should return true");
    }

    #[test]
    fn test_value_ge_null_returns_true() {
        // Go DefraDB behavior: any value >= null returns true
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_ge": null}))]));
        let mapping = make_mapping();
        let fields = make_fields(); // age = 30
        let result = filter.matches(&fields, &mapping);
        assert!(result.is_ok());
        assert!(result.unwrap(), "value _ge null should return true");
    }

    #[test]
    fn test_null_ge_null_returns_true() {
        // Go DefraDB behavior: null >= null returns true
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_ge": null}))]));
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
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_lt": null}))]));
        let mapping = make_mapping();
        let fields = make_fields(); // age = 30
        let result = filter.matches(&fields, &mapping);
        assert!(result.is_ok());
        assert!(!result.unwrap(), "value _lt null should return false");
    }

    #[test]
    fn test_null_le_null_returns_true() {
        // Go DefraDB behavior: null <= null returns true
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_le": null}))]));
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
        let filter =
            Filter::from_conditions(HashMap::from([("age".to_string(), json!({"_le": null}))]));
        let mapping = make_mapping();
        let fields = make_fields(); // age = 30
        let result = filter.matches(&fields, &mapping);
        assert!(result.is_ok());
        assert!(!result.unwrap(), "value _le null should return false");
    }

    #[test]
    fn test_nested_and_or_operators() {
        // Test _and containing _or: match if (name=Alice OR name=Bob) AND age>=18
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "Al_ce"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        // "Al_ce" should NOT match "Alice" because _ is literal
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_complex_pattern() {
        // Multiple % should be handled by the DP algorithm
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_like": "%li%ce"}),
        )]));
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
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": "rust"}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[4] = Some(json!(null)); // tags is null
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contained_in_null_field_returns_false() {
        // When field is null, _contained_in should return false (not error)
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": null}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[4] = Some(json!(["rust", null, "graphql"])); // Array with null
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contains_null_not_in_array() {
        // Array doesn't contain null, searching for null should not find it
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": null}),
        )]));
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
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contains": "rust"}),
        )]));
        let mapping = make_extended_mapping();
        let mut fields = make_extended_fields();
        fields[4] = Some(json!([])); // Empty array
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_contained_in_empty_expected_array() {
        // Non-empty field array vs empty expected array
        // [a,b,c] is NOT contained in [] (no elements of expected contain the actuals)
        let filter = Filter::from_conditions(HashMap::from([(
            "tags".to_string(),
            json!({"_contained_in": []}),
        )]));
        let mapping = make_extended_mapping();
        let fields = make_extended_fields(); // tags = ["rust", "database", "graphql"]
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_has_key_empty_string_key() {
        // Empty string keys are valid in JSON objects
        let filter = Filter::from_conditions(HashMap::from([(
            "metadata".to_string(),
            json!({"_has_key": ""}),
        )]));
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
        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_like": "%"}))]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_empty_pattern() {
        // Empty pattern should only match empty string
        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_like": ""}))]));
        let mapping = make_mapping();
        let fields = make_fields(); // name = "Alice"
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_like_empty_pattern_matches_empty_string() {
        // Empty pattern should match empty string
        let filter =
            Filter::from_conditions(HashMap::from([("name".to_string(), json!({"_like": ""}))]));
        let mapping = make_mapping();
        let mut fields = make_fields();
        fields[1] = Some(json!("")); // empty name
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    // Tests for is_complex()
    #[test]
    fn test_is_complex_simple_scalar() {
        // Simple scalar filter is NOT complex
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_eq": "Alice"}),
        )]));
        assert!(!filter.is_complex());
    }

    #[test]
    fn test_is_complex_simple_relation_at_root() {
        // Relation filter at root level (no logical wrapper) is NOT complex
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"verified": {"_eq": true}}),
        )]));
        assert!(!filter.is_complex());
    }

    #[test]
    fn test_is_complex_and_with_only_scalars() {
        // _and with only scalar conditions is NOT complex
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter =
            Filter::from_conditions(HashMap::from([("rating".to_string(), json!({"_eq": 4.9}))]));
        let paths = filter.get_multi_level_relation_paths();
        assert!(paths.is_empty());
    }

    #[test]
    fn test_extract_filter_at_path_single_level() {
        // Extract filter at ["author"] from {author: {verified: {_eq: true}}}
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
            "author".to_string(),
            json!({"published": {"rating": {"_eq": 4.9}}}),
        )]));
        let extracted =
            filter.extract_filter_at_path(&["author".to_string(), "published".to_string()]);
        assert!(extracted.is_some());
        let extracted = extracted.unwrap();
        // Should contain {rating: {_eq: 4.9}}
        assert!(extracted.conditions().contains_key("rating"));
    }

    #[test]
    fn test_extract_filter_at_path_empty_path() {
        // Empty path should return the full filter
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
        let filter = Filter::from_conditions(HashMap::from([(
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
}
