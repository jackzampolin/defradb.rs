//! JSON matching utilities - evaluate filters against JSON values

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use super::eval::eval_op;
use super::filter_impl::Filter;
use super::op::FilterOp;
use crate::error::{QueryError, Result};

impl Filter {
    /// Evaluate the filter's conditions directly against a scalar JSON value.
    ///
    /// Used for inline array aggregate filters where each array element is tested
    /// against operator conditions like `{_gt: 0}` or `{_and: [{_gt: -2}, {_lt: 2}]}`.
    ///
    /// Null values don't match comparison filters (they are excluded from aggregation).
    pub fn matches_scalar_value(&self, value: &JsonValue) -> Result<bool> {
        if self.conditions().is_empty() {
            return Ok(true);
        }
        // Null scalar values don't pass comparison filters in inline array context
        if value.is_null() {
            return Ok(false);
        }
        for (key, expected) in self.conditions() {
            if let Some(op) = FilterOp::parse(key) {
                match op {
                    FilterOp::And => {
                        let arr = expected
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_and requires array"))?;
                        for item in arr {
                            let sub: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            let f = Filter::from_conditions(sub);
                            if !f.matches_scalar_value(value)? {
                                return Ok(false);
                            }
                        }
                    }
                    FilterOp::Or => {
                        let arr = expected
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_or requires array"))?;
                        let mut any_match = false;
                        for item in arr {
                            let sub: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            let f = Filter::from_conditions(sub);
                            if f.matches_scalar_value(value)? {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return Ok(false);
                        }
                    }
                    FilterOp::Not => {
                        let sub: HashMap<String, JsonValue> =
                            serde_json::from_value(expected.clone())
                                .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                        let f = Filter::from_conditions(sub);
                        if f.matches_scalar_value(value)? {
                            return Ok(false);
                        }
                    }
                    _ => {
                        if !eval_op(value, op, expected)? {
                            return Ok(false);
                        }
                    }
                }
            } else {
                return Err(QueryError::invalid_filter(format!(
                    "unknown operator in scalar filter: {}",
                    key
                )));
            }
        }
        Ok(true)
    }

    /// Evaluate the filter against a JSON object (document).
    ///
    /// Used for relation aggregate filters where each related document is tested
    /// against field-based conditions like `{rating: {_gt: 4.8}}`.
    ///
    /// Unlike `matches_scalar_value` which only handles operator conditions,
    /// this method handles:
    /// - Field-based conditions: `{rating: {_gt: 4.8}}` extracts `rating` from the object
    /// - Operator-only conditions: `{_gt: 4.8}` (falls back to matches_scalar_value)
    /// - Compound conditions: `{_and: [...]}`, `{_or: [...]}`, `{_not: {...}}`
    pub fn matches_json_object(&self, obj: &JsonValue) -> Result<bool> {
        if self.conditions().is_empty() {
            return Ok(true);
        }

        for (key, expected) in self.conditions() {
            if let Some(op) = FilterOp::parse(key) {
                // Operator condition
                match op {
                    FilterOp::And => {
                        let arr = expected
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_and requires array"))?;
                        for item in arr {
                            let sub: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            let f = Filter::from_conditions(sub);
                            if !f.matches_json_object(obj)? {
                                return Ok(false);
                            }
                        }
                    }
                    FilterOp::Or => {
                        let arr = expected
                            .as_array()
                            .ok_or_else(|| QueryError::invalid_filter("_or requires array"))?;
                        let mut any_match = false;
                        for item in arr {
                            let sub: HashMap<String, JsonValue> =
                                serde_json::from_value(item.clone())
                                    .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                            let f = Filter::from_conditions(sub);
                            if f.matches_json_object(obj)? {
                                any_match = true;
                                break;
                            }
                        }
                        if !any_match {
                            return Ok(false);
                        }
                    }
                    FilterOp::Not => {
                        let sub: HashMap<String, JsonValue> =
                            serde_json::from_value(expected.clone())
                                .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
                        let f = Filter::from_conditions(sub);
                        if f.matches_json_object(obj)? {
                            return Ok(false);
                        }
                    }
                    _ => {
                        // Direct operator on the object itself - use scalar matching
                        if !eval_op(obj, op, expected)? {
                            return Ok(false);
                        }
                    }
                }
            } else {
                // Field-based condition: key is a field name
                let field_value = obj
                    .as_object()
                    .and_then(|o| o.get(key))
                    .unwrap_or(&JsonValue::Null);

                // expected should be an object with operator conditions
                if let Some(conditions_obj) = expected.as_object() {
                    // Check if the nested conditions are operators or field-based
                    // If they're operators, use matches_scalar_value on the field value
                    // If they're field-based, recursively use matches_json_object
                    let has_field_conditions =
                        conditions_obj.keys().any(|k| FilterOp::parse(k).is_none());
                    let sub_conditions: HashMap<String, JsonValue> = conditions_obj
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    let f = Filter::from_conditions(sub_conditions);
                    if has_field_conditions {
                        // Nested field access - recursively match
                        if !f.matches_json_object(field_value)? {
                            return Ok(false);
                        }
                    } else {
                        // Operator conditions on the field value
                        if !f.matches_scalar_value(field_value)? {
                            return Ok(false);
                        }
                    }
                } else {
                    // Direct equality check: {field: value}
                    if field_value != expected {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }
}
