use serde_json::Value as JsonValue;

use crate::txn::TransactionRegistry;

use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Check if a JSON object matches a filter for nested commit selections.
    ///
    /// This is a simplified filter matcher for nested selections like `links(filter: {fieldName: {_eq: "Age"}})`.
    /// The filter conditions are stored as `{field_name: {_op: value}}`.
    pub(super) fn json_item_matches_filter(
        &self,
        item: &JsonValue,
        filter: &crate::mapper::Filter,
    ) -> bool {
        use crate::mapper::FilterOp;

        // Get the filter conditions - a map of field_name -> operator conditions
        let conditions = filter.conditions();

        for (field_name, condition_value) in conditions {
            // Check if this is a logical operator (_and, _or, _not)
            if let Some(op) = FilterOp::parse(field_name) {
                match op {
                    FilterOp::And => {
                        if let JsonValue::Array(arr) = condition_value {
                            for sub_cond in arr {
                                if let JsonValue::Object(obj) = sub_cond {
                                    let sub_filter =
                                        crate::mapper::Filter::from_conditions(obj.clone());
                                    if !self.json_item_matches_filter(item, &sub_filter) {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                    FilterOp::Or => {
                        if let JsonValue::Array(arr) = condition_value {
                            let mut any_match = false;
                            for sub_cond in arr {
                                if let JsonValue::Object(obj) = sub_cond {
                                    let sub_filter =
                                        crate::mapper::Filter::from_conditions(obj.clone());
                                    if self.json_item_matches_filter(item, &sub_filter) {
                                        any_match = true;
                                        break;
                                    }
                                }
                            }
                            if !any_match {
                                return false;
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = condition_value {
                            let sub_filter = crate::mapper::Filter::from_conditions(obj.clone());
                            if self.json_item_matches_filter(item, &sub_filter) {
                                return false;
                            }
                        }
                    }
                    _ => {}
                }
                continue;
            }

            // This is a field condition: field_name -> {_op: value}
            let item_value = item.get(field_name);

            // The condition_value should be an object like {"_eq": "Age"}
            if let JsonValue::Object(ops) = condition_value {
                for (op_name, expected_value) in ops {
                    if let Some(op) = FilterOp::parse(op_name) {
                        let matches = self.check_filter_op(item_value, op, expected_value);
                        if !matches {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }

    /// Check if an item value matches a filter operator condition.
    pub(super) fn check_filter_op(
        &self,
        item_value: Option<&JsonValue>,
        op: crate::mapper::FilterOp,
        expected: &JsonValue,
    ) -> bool {
        use crate::mapper::FilterOp;

        match op {
            FilterOp::Eq => match (item_value, expected) {
                (Some(JsonValue::String(a)), JsonValue::String(b)) => a == b,
                (Some(JsonValue::Number(a)), JsonValue::Number(b)) => a == b,
                (Some(JsonValue::Bool(a)), JsonValue::Bool(b)) => a == b,
                (Some(JsonValue::Null), JsonValue::Null) => true,
                (None, JsonValue::Null) => true,
                _ => false,
            },
            FilterOp::Ne => match (item_value, expected) {
                (Some(JsonValue::String(a)), JsonValue::String(b)) => a != b,
                (Some(JsonValue::Number(a)), JsonValue::Number(b)) => a != b,
                (Some(JsonValue::Bool(a)), JsonValue::Bool(b)) => a != b,
                (Some(JsonValue::Null), JsonValue::Null) => false,
                (None, _) => true,
                _ => true,
            },
            FilterOp::Gt => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a > b,
                _ => false,
            },
            FilterOp::Gte => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a >= b,
                _ => false,
            },
            FilterOp::Lt => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a < b,
                _ => false,
            },
            FilterOp::Lte => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a <= b,
                _ => false,
            },
            FilterOp::In => {
                if let JsonValue::Array(values) = expected {
                    item_value.map(|v| values.contains(v)).unwrap_or(false)
                } else {
                    false
                }
            }
            FilterOp::Nin => {
                if let JsonValue::Array(values) = expected {
                    item_value.map(|v| !values.contains(v)).unwrap_or(true)
                } else {
                    true
                }
            }
            _ => true, // For unsupported operators, default to matching
        }
    }
}
