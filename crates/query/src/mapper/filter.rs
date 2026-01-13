//! Filter types and evaluation for query conditions

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};

/// Filter operators for condition matching
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FilterOp {
    /// Equal (_eq)
    #[serde(rename = "_eq")]
    Eq,
    /// Not equal (_ne)
    #[serde(rename = "_ne")]
    Ne,
    /// Greater than (_gt)
    #[serde(rename = "_gt")]
    Gt,
    /// Greater than or equal (_gte)
    #[serde(rename = "_gte")]
    Gte,
    /// Less than (_lt)
    #[serde(rename = "_lt")]
    Lt,
    /// Less than or equal (_lte)
    #[serde(rename = "_lte")]
    Lte,
    /// In array (_in)
    #[serde(rename = "_in")]
    In,
    /// Not in array (_nin)
    #[serde(rename = "_nin")]
    Nin,
    /// Pattern match (_like)
    #[serde(rename = "_like")]
    Like,
    /// Negated pattern match (_nlike)
    #[serde(rename = "_nlike")]
    Nlike,
    /// Logical AND (_and)
    #[serde(rename = "_and")]
    And,
    /// Logical OR (_or)
    #[serde(rename = "_or")]
    Or,
    /// Logical NOT (_not)
    #[serde(rename = "_not")]
    Not,
}

impl FilterOp {
    /// Parse a filter operator from string
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "_eq" => Some(Self::Eq),
            "_ne" => Some(Self::Ne),
            "_gt" => Some(Self::Gt),
            "_gte" => Some(Self::Gte),
            "_lt" => Some(Self::Lt),
            "_lte" => Some(Self::Lte),
            "_in" => Some(Self::In),
            "_nin" => Some(Self::Nin),
            "_like" => Some(Self::Like),
            "_nlike" => Some(Self::Nlike),
            "_and" => Some(Self::And),
            "_or" => Some(Self::Or),
            "_not" => Some(Self::Not),
            _ => None,
        }
    }

    /// Get the string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Eq => "_eq",
            Self::Ne => "_ne",
            Self::Gt => "_gt",
            Self::Gte => "_gte",
            Self::Lt => "_lt",
            Self::Lte => "_lte",
            Self::In => "_in",
            Self::Nin => "_nin",
            Self::Like => "_like",
            Self::Nlike => "_nlike",
            Self::And => "_and",
            Self::Or => "_or",
            Self::Not => "_not",
        }
    }

    /// Check if this is a logical operator
    pub fn is_logical(&self) -> bool {
        matches!(self, Self::And | Self::Or | Self::Not)
    }
}

/// Filter condition containing parsed conditions.
///
/// Conditions map field names to their filter values.
/// Supports nested conditions for related objects.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Parsed filter conditions
    pub conditions: HashMap<String, JsonValue>,
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

            // Field condition
            let field_index = mapping
                .first_index_of_name(key)
                .ok_or_else(|| QueryError::unknown_field(key))?;

            let field_value = fields
                .get(field_index)
                .and_then(|v| v.as_ref())
                .cloned()
                .unwrap_or(JsonValue::Null);

            // Value should be an object with operator keys
            let ops = value
                .as_object()
                .ok_or_else(|| QueryError::invalid_filter("field condition must be object"))?;

            for (op_str, expected) in ops {
                let op = FilterOp::parse(op_str)
                    .ok_or_else(|| QueryError::invalid_filter(format!("unknown operator: {}", op_str)))?;

                if !self.eval_op(&field_value, op, expected)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn eval_op(&self, actual: &JsonValue, op: FilterOp, expected: &JsonValue) -> Result<bool> {
        match op {
            FilterOp::Eq => Ok(Self::values_equal(actual, expected)),
            FilterOp::Ne => Ok(!Self::values_equal(actual, expected)),
            FilterOp::Gt => self.compare(actual, expected).map(|ord| ord.is_gt()),
            FilterOp::Gte => self.compare(actual, expected).map(|ord| ord.is_ge()),
            FilterOp::Lt => self.compare(actual, expected).map(|ord| ord.is_lt()),
            FilterOp::Lte => self.compare(actual, expected).map(|ord| ord.is_le()),
            FilterOp::In => {
                let arr = expected
                    .as_array()
                    .ok_or_else(|| QueryError::invalid_filter("_in requires array"))?;
                Ok(arr.iter().any(|v| Self::values_equal(actual, v)))
            }
            FilterOp::Nin => {
                let arr = expected
                    .as_array()
                    .ok_or_else(|| QueryError::invalid_filter("_nin requires array"))?;
                Ok(!arr.iter().any(|v| Self::values_equal(actual, v)))
            }
            FilterOp::Like => self.like_match(actual, expected, false),
            FilterOp::Nlike => self.like_match(actual, expected, true),
            FilterOp::And | FilterOp::Or | FilterOp::Not => {
                Err(QueryError::internal("logical ops should be handled at top level"))
            }
        }
    }

    fn values_equal(a: &JsonValue, b: &JsonValue) -> bool {
        match (a, b) {
            (JsonValue::Null, JsonValue::Null) => true,
            (JsonValue::Bool(a), JsonValue::Bool(b)) => a == b,
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                // Handle int/float comparison
                if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                    a == b
                } else if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
                    (a - b).abs() < f64::EPSILON
                } else {
                    false
                }
            }
            (JsonValue::String(a), JsonValue::String(b)) => a == b,
            (JsonValue::Array(a), JsonValue::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(a, b)| Self::values_equal(a, b))
            }
            _ => false,
        }
    }

    fn compare(&self, a: &JsonValue, b: &JsonValue) -> Result<std::cmp::Ordering> {
        match (a, b) {
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let a = a.as_f64().unwrap_or(0.0);
                let b = b.as_f64().unwrap_or(0.0);
                Ok(a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal))
            }
            (JsonValue::String(a), JsonValue::String(b)) => Ok(a.cmp(b)),
            _ => Err(QueryError::TypeMismatch {
                expected: "comparable types".to_string(),
                actual: format!("{:?} vs {:?}", a, b),
            }),
        }
    }

    fn like_match(&self, actual: &JsonValue, pattern: &JsonValue, negate: bool) -> Result<bool> {
        let actual_str = actual
            .as_str()
            .ok_or_else(|| QueryError::invalid_filter("_like requires string field"))?;
        let pattern_str = pattern
            .as_str()
            .ok_or_else(|| QueryError::invalid_filter("_like requires string pattern"))?;

        // Convert SQL LIKE pattern to simple matching
        // % = any characters, _ = single character
        let regex_pattern = pattern_str
            .replace('%', ".*")
            .replace('_', ".");

        // Simple pattern matching (not full regex for performance)
        let matches = if let Some(inner) = regex_pattern
            .strip_prefix(".*")
            .and_then(|s| s.strip_suffix(".*"))
        {
            actual_str.contains(inner)
        } else if let Some(suffix) = regex_pattern.strip_prefix(".*") {
            actual_str.ends_with(suffix)
        } else if let Some(prefix) = regex_pattern.strip_suffix(".*") {
            actual_str.starts_with(prefix)
        } else {
            actual_str == pattern_str
        };

        Ok(if negate { !matches } else { matches })
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

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_eq": "Bob"}),
        )]));
        assert!(!filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_ne_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            json!({"_ne": "Bob"}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());
    }

    #[test]
    fn test_gt_filter() {
        let filter = Filter::from_conditions(HashMap::from([(
            "age".to_string(),
            json!({"_gt": 25}),
        )]));
        let mapping = make_mapping();
        let fields = make_fields();
        assert!(filter.matches(&fields, &mapping).unwrap());

        let filter = Filter::from_conditions(HashMap::from([(
            "age".to_string(),
            json!({"_gt": 35}),
        )]));
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
    fn test_filter_op_parse() {
        assert_eq!(FilterOp::parse("_eq"), Some(FilterOp::Eq));
        assert_eq!(FilterOp::parse("_and"), Some(FilterOp::And));
        assert_eq!(FilterOp::parse("invalid"), None);
    }
}
