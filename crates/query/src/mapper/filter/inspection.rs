//! Filter inspection methods - boolean queries about filter structure

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use super::filter_impl::Filter;
use super::op::FilterOp;

impl Filter {
    /// Check if the filter has field-based conditions (as opposed to operator-only conditions).
    ///
    /// Field-based: `{rating: {_gt: 4.8}}` - the key "rating" is a field name
    /// Operator-only: `{_gt: 4.8}` - the key "_gt" is an operator
    ///
    /// This is used to determine whether to use `matches_json_object` (field-based)
    /// or `matches_scalar_value` (operator-only) when filtering relation aggregates.
    pub fn has_field_conditions(&self) -> bool {
        self.conditions()
            .keys()
            .any(|k| FilterOp::parse(k).is_none())
    }

    /// Get all field names referenced by this filter
    pub fn referenced_fields(&self) -> Vec<String> {
        let mut fields = Vec::new();
        Self::collect_fields(self.conditions(), &mut fields);
        fields
    }

    fn collect_fields(conditions: &HashMap<String, JsonValue>, fields: &mut Vec<String>) {
        for (key, value) in conditions {
            // Skip logical operators
            if FilterOp::parse(key).is_some() {
                match value {
                    JsonValue::Array(arr) => {
                        for item in arr {
                            if let JsonValue::Object(obj) = item {
                                let nested: HashMap<String, JsonValue> =
                                    obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                Self::collect_fields(&nested, fields);
                            }
                        }
                    }
                    JsonValue::Object(obj) => {
                        let nested: HashMap<String, JsonValue> =
                            obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                        Self::collect_fields(&nested, fields);
                    }
                    _ => {}
                }
            } else {
                // This is a field name
                if !fields.contains(key) {
                    fields.push(key.clone());
                }
            }
        }
    }

    /// Check if this filter contains relation filters (filters through nested objects).
    ///
    /// Relation filters are conditions like `{author: {verified: {_eq: true}}}` where
    /// the first level field is a relation and the nested object contains field conditions
    /// rather than operators.
    pub fn has_relation_filters(&self) -> bool {
        Self::check_for_relation_filters(self.conditions())
    }

    /// Check if a conditions map contains relation filters.
    ///
    /// Shared helper used by both inspection and split methods.
    pub(crate) fn check_for_relation_filters(conditions: &HashMap<String, JsonValue>) -> bool {
        for (key, value) in conditions {
            // Check logical operators recursively
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or => {
                        if let JsonValue::Array(arr) = value {
                            for item in arr {
                                if let JsonValue::Object(obj) = item {
                                    let nested: HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    if Self::check_for_relation_filters(&nested) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = value {
                            let nested: HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            if Self::check_for_relation_filters(&nested) {
                                return true;
                            }
                        }
                    }
                    _ => {}
                }
            } else if key == "_alias" {
                // _alias is a special filter directive, not a relation filter
                continue;
            } else if let JsonValue::Object(obj) = value {
                if obj.contains_key("_similarity") {
                    continue;
                }
                // This is a field condition - check if it contains operators or nested fields
                // If any key in the object is NOT an operator, it's a relation filter
                for nested_key in obj.keys() {
                    if FilterOp::parse(nested_key).is_none() {
                        // This is a nested field name, not an operator - it's a relation filter
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Check if this filter contains an `_alias` directive.
    ///
    /// `_alias` filters reference aliased fields/relations and must be evaluated after joins
    /// because the alias may reference a relation whose data hasn't been joined yet.
    pub fn has_alias_filter(&self) -> bool {
        self.conditions().contains_key("_alias")
    }

    /// Check if this filter is "complex" - contains relation conditions inside logical operators.
    ///
    /// A filter is complex when `_and`, `_or`, or `_not` contains a mix of scalar and relation
    /// conditions. Complex filters cannot be split and must be evaluated as a whole after
    /// the join when the merged document is available.
    ///
    /// Examples:
    /// - `{_and: [{rating: {_ge: 4.0}}, {author: {verified: {_eq: true}}}]}` -> COMPLEX
    /// - `{author: {verified: {_eq: true}}}` -> NOT COMPLEX (relation at root, no logical wrapper)
    /// - `{_and: [{rating: {_ge: 4.0}}, {age: {_gt: 25}}]}` -> NOT COMPLEX (only scalars)
    pub fn is_complex(&self) -> bool {
        Self::check_for_complex_filters(self.conditions())
    }

    fn check_for_complex_filters(conditions: &HashMap<String, JsonValue>) -> bool {
        for (key, value) in conditions {
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And | FilterOp::Or => {
                        if let JsonValue::Array(arr) = value {
                            // Check if this logical block contains any relation filters
                            for item in arr {
                                if let JsonValue::Object(obj) = item {
                                    let nested: HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    // If any item in the logical block has a relation filter, it's complex
                                    if Self::check_for_relation_filters(&nested) {
                                        return true;
                                    }
                                    // Also check recursively for nested complex filters
                                    if Self::check_for_complex_filters(&nested) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = value {
                            let nested: HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            if Self::check_for_relation_filters(&nested) {
                                return true;
                            }
                            if Self::check_for_complex_filters(&nested) {
                                return true;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        false
    }
}
