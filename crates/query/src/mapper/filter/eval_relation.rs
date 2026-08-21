//! Relation condition evaluation for nested document filters

use serde_json::Value as JsonValue;

use super::eval::eval_op;
use super::op::FilterOp;
use crate::error::{QueryError, Result};

use super::Filter;

impl Filter {
    /// Evaluate relation filter conditions against a JSON object.
    ///
    /// This handles nested conditions like `{verified: {_eq: true}}` where the
    /// condition is evaluated against a related document's fields.
    pub(crate) fn eval_relation_conditions(
        &self,
        conditions: &serde_json::Map<String, JsonValue>,
        obj: &serde_json::Map<String, JsonValue>,
    ) -> Result<bool> {
        self.eval_relation_conditions_at_depth(conditions, obj, 0)
    }

    fn eval_relation_conditions_at_depth(
        &self,
        conditions: &serde_json::Map<String, JsonValue>,
        obj: &serde_json::Map<String, JsonValue>,
        depth: usize,
    ) -> Result<bool> {
        self.check_depth(depth)?;

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
                            if !self.eval_relation_conditions_at_depth(sub_conds, obj, depth + 1)? {
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
                            if self.eval_relation_conditions_at_depth(sub_conds, obj, depth + 1)? {
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
                        if self.eval_relation_conditions_at_depth(sub_conds, obj, depth + 1)? {
                            return Ok(false);
                        }
                        continue;
                    }
                    _ => {}
                }
            }

            // Field condition - get the field value from the object.
            // Track whether the field actually exists vs defaulting to null,
            // because Go treats missing JSON sub-fields differently from explicit nulls.
            let field_missing = !obj.contains_key(key);
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
                            if self.eval_relation_conditions_at_depth(ops, obj, depth + 1)? {
                                any_match = true;
                                break;
                            }
                        }
                    }
                    if !any_match {
                        return Ok(false);
                    }
                } else if let Some(nested_obj) = field_value.as_object() {
                    if !self.eval_relation_conditions_at_depth(ops, nested_obj, depth + 1)? {
                        return Ok(false);
                    }
                } else {
                    return Err(QueryError::invalid_filter(format!(
                        "nested relation field '{}' must be an object or array",
                        key
                    )));
                }
            } else {
                // Operator conditions.
                // When a field doesn't exist in a JSON object and the operator
                // compares against a non-null value, the document doesn't match
                // (Go compatibility: missing JSON sub-field != not-equal).
                // But when comparing against null (e.g. _gte null), null >= null
                // is valid and should proceed normally.
                for (op_str, expected) in ops {
                    let op = FilterOp::parse(op_str).ok_or_else(|| {
                        QueryError::invalid_filter(format!("unknown operator: {}", op_str))
                    })?;
                    if field_missing && !expected.is_null() {
                        // Go treats missing nested JSON fields as implicit nulls for
                        // membership checks. Other operators keep the stricter miss.
                        if !matches!(op, FilterOp::In | FilterOp::Nin) {
                            return Ok(false);
                        }
                    }
                    if !eval_op(&field_value, op, expected)? {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }
}
