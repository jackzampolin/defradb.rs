//! Filter types and evaluation for query conditions

use serde_json::{Map, Value as JsonValue};
use tracing::instrument;

use super::eval::{eval_op, values_equal};
use super::op::FilterOp;
use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::limits::DEFAULT_MAX_FILTER_DEPTH;

/// Filter condition containing parsed conditions.
///
/// Conditions map field names to their filter values.
/// Supports nested conditions for related objects.
#[derive(Debug, Clone)]
pub struct Filter {
    /// Parsed filter conditions
    conditions: Map<String, JsonValue>,
    /// Maximum recursive depth for this filter. `0` disables the limit.
    max_depth: usize,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            conditions: Map::new(),
            max_depth: DEFAULT_MAX_FILTER_DEPTH,
        }
    }
}

impl Filter {
    /// Create an empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a filter from conditions map
    pub fn from_conditions(conditions: Map<String, JsonValue>) -> Self {
        Self {
            conditions,
            max_depth: DEFAULT_MAX_FILTER_DEPTH,
        }
    }

    /// Create a filter from conditions map with a custom recursive depth limit.
    pub fn from_conditions_with_max_depth(
        conditions: Map<String, JsonValue>,
        max_depth: usize,
    ) -> Self {
        Self {
            conditions,
            max_depth,
        }
    }

    /// Set the maximum recursive depth for this filter. `0` disables the limit.
    pub fn set_max_depth(&mut self, max_depth: usize) {
        self.max_depth = max_depth;
    }

    /// Return a copy of this filter with a custom recursive depth limit.
    pub fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.set_max_depth(max_depth);
        self
    }

    /// Get the maximum recursive depth configured for this filter.
    pub fn max_depth(&self) -> usize {
        self.max_depth
    }

    /// Check if the filter is empty
    pub fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }

    /// Combine this filter with another filter using AND logic.
    ///
    /// Returns a new filter where both conditions must be satisfied.
    pub fn and(&self, other: Filter) -> Filter {
        let max_depth = match (self.max_depth, other.max_depth) {
            (0, _) | (_, 0) => 0,
            (left, right) => left.min(right),
        };
        let mut combined_conditions = Map::new();
        combined_conditions.insert(
            "_and".to_string(),
            serde_json::json!([self.conditions.clone(), other.conditions]),
        );
        Filter::from_conditions_with_max_depth(combined_conditions, max_depth)
    }

    /// Get a reference to the conditions map
    pub fn conditions(&self) -> &Map<String, JsonValue> {
        &self.conditions
    }

    /// Convert this filter to explain JSON using the existing raw filter shape.
    pub fn to_explain_json(&self) -> JsonValue {
        if self.conditions.is_empty() {
            JsonValue::Null
        } else {
            JsonValue::Object(self.conditions.clone())
        }
    }

    /// Convert this filter to explain JSON while stripping `_docID` conditions.
    ///
    /// Explain output represents docID lookups separately, so `_docID` should not
    /// appear in the rendered filter value. The returned JSON preserves the same
    /// object/array shape used elsewhere in explain output.
    pub fn to_explain_json_without_docid(&self) -> JsonValue {
        Self::strip_docid_for_explain(&self.to_explain_json())
    }

    /// Evaluate the filter against document fields
    #[instrument(level = "trace", skip(self, fields, mapping))]
    pub fn matches(&self, fields: &[Option<JsonValue>], mapping: &DocumentMapping) -> Result<bool> {
        if self.conditions.is_empty() {
            return Ok(true);
        }
        self.eval_conditions(&self.conditions, fields, mapping, 0)
    }

    /// Validate this filter's recursive structure against its configured depth limit.
    pub fn validate_depth(&self) -> Result<()> {
        self.validate_conditions_depth(&self.conditions, 0)
    }

    pub(crate) fn check_depth(&self, depth: usize) -> Result<()> {
        if self.max_depth > 0 && depth > self.max_depth {
            return Err(QueryError::invalid_filter(format!(
                "filter exceeds maximum nesting depth of {}",
                self.max_depth
            )));
        }
        Ok(())
    }

    fn validate_conditions_depth(
        &self,
        conditions: &Map<String, JsonValue>,
        depth: usize,
    ) -> Result<()> {
        self.check_depth(depth)?;

        for (key, value) in conditions {
            match FilterOp::parse(key) {
                Some(FilterOp::And | FilterOp::Or) => {
                    let items = value.as_array().ok_or_else(|| {
                        QueryError::invalid_filter(format!("{} requires array", key))
                    })?;
                    for item in items {
                        let sub_conditions = item.as_object().ok_or_else(|| {
                            QueryError::invalid_filter(format!("{} items must be objects", key))
                        })?;
                        self.validate_conditions_depth(sub_conditions, depth + 1)?;
                    }
                }
                Some(FilterOp::Not) => {
                    let sub_conditions = value
                        .as_object()
                        .ok_or_else(|| QueryError::invalid_filter("_not requires object"))?;
                    self.validate_conditions_depth(sub_conditions, depth + 1)?;
                }
                _ if key == "_alias" => {
                    if let Some(alias_conditions) = value.as_object() {
                        self.validate_conditions_depth(alias_conditions, depth + 1)?;
                    }
                }
                _ => {
                    if let Some(obj) = value.as_object() {
                        if obj.keys().any(|k| FilterOp::parse(k).is_none()) {
                            self.validate_conditions_depth(obj, depth + 1)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn eval_conditions(
        &self,
        conditions: &Map<String, JsonValue>,
        fields: &[Option<JsonValue>],
        mapping: &DocumentMapping,
        depth: usize,
    ) -> Result<bool> {
        self.check_depth(depth)?;

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
                            let sub_conditions = item.as_object().ok_or_else(|| {
                                QueryError::invalid_filter("_and items must be objects")
                            })?;
                            if !self.eval_conditions(sub_conditions, fields, mapping, depth + 1)? {
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
                            let sub_conditions = item.as_object().ok_or_else(|| {
                                QueryError::invalid_filter("_or items must be objects")
                            })?;
                            if self.eval_conditions(sub_conditions, fields, mapping, depth + 1)? {
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
                        let sub_conditions = value
                            .as_object()
                            .ok_or_else(|| QueryError::invalid_filter("_not requires object"))?;
                        if self.eval_conditions(sub_conditions, fields, mapping, depth + 1)? {
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
                let alias_conditions = match value.as_object() {
                    Some(v) => v,
                    None => {
                        // Non-object _alias (e.g., integer) filters everything out
                        return Ok(false);
                    }
                };

                if !self.eval_alias_conditions_at_depth(
                    alias_conditions,
                    fields,
                    mapping,
                    depth + 1,
                )? {
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

    fn strip_docid_for_explain(value: &JsonValue) -> JsonValue {
        match value {
            JsonValue::Object(map) => {
                let mut result = Map::new();

                for (key, val) in map {
                    if key == "_docID" {
                        continue;
                    }

                    match FilterOp::parse(key) {
                        Some(FilterOp::And | FilterOp::Or) => {
                            if let JsonValue::Array(arr) = val {
                                let filtered: Vec<JsonValue> = arr
                                    .iter()
                                    .map(Self::strip_docid_for_explain)
                                    .filter(|item| {
                                        !item.is_null()
                                            && !item
                                                .as_object()
                                                .map(|object| object.is_empty())
                                                .unwrap_or(false)
                                    })
                                    .collect();

                                match filtered.len() {
                                    0 => {}
                                    1 => {
                                        if let Some(JsonValue::Object(inner)) =
                                            filtered.into_iter().next()
                                        {
                                            for (inner_key, inner_value) in inner {
                                                result.insert(inner_key, inner_value);
                                            }
                                        }
                                    }
                                    _ => {
                                        result.insert(key.clone(), JsonValue::Array(filtered));
                                    }
                                }
                            } else {
                                result.insert(key.clone(), val.clone());
                            }
                        }
                        _ => {
                            result.insert(key.clone(), val.clone());
                        }
                    }
                }

                if result.is_empty() {
                    JsonValue::Null
                } else {
                    JsonValue::Object(result)
                }
            }
            _ => value.clone(),
        }
    }
}
