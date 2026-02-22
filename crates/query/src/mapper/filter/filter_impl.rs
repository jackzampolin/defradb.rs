//! Filter types and evaluation for query conditions

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use super::eval::{eval_op, values_equal};
use super::op::FilterOp;
use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};

/// Maximum recursive depth for filter evaluation.
///
/// Filters with `_and`/`_or`/`_not` can nest arbitrarily. This limit prevents
/// stack exhaustion from pathologically deep filter trees.
const MAX_FILTER_DEPTH: usize = 50;

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
        self.eval_conditions(&self.conditions, fields, mapping, 0)
    }

    fn eval_conditions(
        &self,
        conditions: &HashMap<String, JsonValue>,
        fields: &[Option<JsonValue>],
        mapping: &DocumentMapping,
        depth: usize,
    ) -> Result<bool> {
        if depth > MAX_FILTER_DEPTH {
            return Err(QueryError::invalid_filter(format!(
                "filter exceeds maximum nesting depth of {}",
                MAX_FILTER_DEPTH
            )));
        }

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
                            if !self.eval_conditions(&sub_conditions, fields, mapping, depth + 1)? {
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
                            if self.eval_conditions(&sub_conditions, fields, mapping, depth + 1)? {
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
                        if self.eval_conditions(&sub_conditions, fields, mapping, depth + 1)? {
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

            // Check for _similarity misuse in filter (it's a select field, not a filter operator)
            if ops.contains_key("SIMILARITY") {
                return Err(QueryError::invalid_filter(format!(
                    "_similarity cannot be used as a filter on '{}'. Use it as a select field with _alias filtering: \
                     {{ Type(filter: {{_alias: {{sim: {{_gt: 0.8}}}}}}, order: {{_alias: {{sim: DESC}}}}, limit: K) \
                     {{ sim: _similarity({}: {{vector: [...]}}) ... }} }}",
                    key, key
                )));
            }

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
}
