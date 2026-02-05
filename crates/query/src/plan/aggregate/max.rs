//! MAX aggregate operation

use serde_json::Value as JsonValue;

use super::AggregateOp;
use super::NumericSourceMeta;

/// Source metadata alias for MAX
pub type MaxSourceMeta = NumericSourceMeta;

/// MAX accumulator: tracks current max and whether float
#[derive(Default, Clone)]
pub struct MaxAccumulator {
    max: Option<f64>,
    has_float: bool,
}

/// MAX aggregate operation
pub struct MaxOp;

impl MaxOp {
    /// Extract numeric value from JSON, returning (value, is_float)
    fn extract_numeric(value: Option<&JsonValue>) -> Option<(f64, bool)> {
        match value {
            Some(JsonValue::Number(n)) => n
                .as_i64()
                .map(|i| (i as f64, false))
                .or_else(|| n.as_f64().map(|f| (f, true))),
            _ => None,
        }
    }

    /// Convert max to JSON
    fn max_to_json(max: Option<f64>, has_float: bool) -> JsonValue {
        match max {
            None => JsonValue::Null,
            Some(val) if has_float => serde_json::Number::from_f64(val)
                .map(JsonValue::Number)
                .unwrap_or(JsonValue::Null),
            Some(val) => JsonValue::Number((val as i64).into()),
        }
    }
}

impl AggregateOp for MaxOp {
    type Accumulator = MaxAccumulator;
    type SourceMeta = MaxSourceMeta;
    const REQUIRES_FIELD_INDEX: bool = true;

    fn init_accumulator() -> Self::Accumulator {
        MaxAccumulator::default()
    }

    fn accumulate(acc: &mut Self::Accumulator, value: Option<&JsonValue>) {
        if let Some((val, is_float)) = Self::extract_numeric(value) {
            acc.max = Some(match acc.max {
                None => val,
                Some(current) => current.max(val),
            });
            acc.has_float = acc.has_float || is_float;
        }
    }

    fn accumulate_from_group(acc: &mut Self::Accumulator, items: &[JsonValue], field_name: &str) {
        for item in items {
            if let JsonValue::Object(obj) = item {
                if let Some(val) = obj.get(field_name) {
                    if let Some(i) = val.as_i64() {
                        let v = i as f64;
                        acc.max = Some(acc.max.map_or(v, |m: f64| m.max(v)));
                    } else if let Some(f) = val.as_f64() {
                        acc.max = Some(acc.max.map_or(f, |m: f64| m.max(f)));
                        acc.has_float = true;
                    }
                }
            }
        }
    }

    fn finalize(acc: &Self::Accumulator) -> JsonValue {
        Self::max_to_json(acc.max, acc.has_float)
    }

    fn kind() -> &'static str {
        "maxNode"
    }

    fn build_explain_sources(sources: &[Self::SourceMeta]) -> Vec<JsonValue> {
        sources
            .iter()
            .map(|s| {
                let mut source_obj = serde_json::Map::new();
                source_obj.insert(
                    "fieldName".to_string(),
                    JsonValue::String(s.field_name.clone()),
                );
                match &s.child_field_name {
                    Some(child_name) => {
                        source_obj.insert(
                            "childFieldName".to_string(),
                            JsonValue::String(child_name.clone()),
                        );
                    }
                    None => {
                        source_obj.insert("childFieldName".to_string(), serde_json::Value::Null);
                    }
                }
                if let Some(ref filter) = s.filter {
                    let conditions = filter.conditions();
                    if conditions.is_empty() {
                        source_obj.insert("filter".to_string(), serde_json::Value::Null);
                    } else {
                        source_obj.insert("filter".to_string(), serde_json::json!(conditions));
                    }
                } else {
                    source_obj.insert("filter".to_string(), serde_json::Value::Null);
                }
                JsonValue::Object(source_obj)
            })
            .collect()
    }
}
