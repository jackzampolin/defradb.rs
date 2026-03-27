//! Numeric aggregation for commits queries.

use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Copy)]
pub(crate) enum CommitNumericValue {
    Int(i64),
    Float(f64),
}

impl CommitNumericValue {
    pub(crate) fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
        }
    }

    pub(crate) fn is_float(self) -> bool {
        matches!(self, Self::Float(_))
    }
}

pub(crate) fn sum_commit_numeric_values(values: &[CommitNumericValue]) -> JsonValue {
    if values.iter().any(|value| value.is_float()) {
        let sum = values.iter().map(|value| value.as_f64()).sum::<f64>();
        serde_json::Number::from_f64(sum)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null)
    } else {
        values
            .iter()
            .try_fold(0i64, |sum, value| match value {
                CommitNumericValue::Int(value) => sum.checked_add(*value),
                CommitNumericValue::Float(_) => None,
            })
            .map(|sum| JsonValue::Number(sum.into()))
            .unwrap_or(JsonValue::Null)
    }
}

pub(crate) fn min_commit_numeric_values(values: &[CommitNumericValue]) -> JsonValue {
    if values.iter().any(|value| value.is_float()) {
        let min = values
            .iter()
            .map(|value| value.as_f64())
            .fold(None, |current: Option<f64>, value| {
                Some(current.map_or(value, |min| min.min(value)))
            });
        min.and_then(serde_json::Number::from_f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null)
    } else {
        values
            .iter()
            .filter_map(|value| match value {
                CommitNumericValue::Int(value) => Some(*value),
                CommitNumericValue::Float(_) => None,
            })
            .min()
            .map(|value| JsonValue::Number(value.into()))
            .unwrap_or(JsonValue::Null)
    }
}

pub(crate) fn max_commit_numeric_values(values: &[CommitNumericValue]) -> JsonValue {
    if values.iter().any(|value| value.is_float()) {
        let max = values
            .iter()
            .map(|value| value.as_f64())
            .fold(None, |current: Option<f64>, value| {
                Some(current.map_or(value, |max| max.max(value)))
            });
        max.and_then(serde_json::Number::from_f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null)
    } else {
        values
            .iter()
            .filter_map(|value| match value {
                CommitNumericValue::Int(value) => Some(*value),
                CommitNumericValue::Float(_) => None,
            })
            .max()
            .map(|value| JsonValue::Number(value.into()))
            .unwrap_or(JsonValue::Null)
    }
}
