//! COUNT aggregate operation

use serde_json::Value as JsonValue;

use query_types::mapper::Filter;

use super::AggregateOp;

/// Source metadata for COUNT explain output.
#[derive(Debug, Clone)]
pub struct CountSourceMeta {
    /// Field name (collection name or relation field name)
    pub field_name: String,
    /// Optional filter on this source
    pub filter: Option<Filter>,
    /// Whether this is an inline array aggregate
    pub is_inline_array: bool,
}

/// COUNT accumulator: just a running count
#[derive(Default, Clone)]
pub struct CountAccumulator {
    count: i64,
}

/// COUNT aggregate operation
pub struct CountOp;

impl AggregateOp for CountOp {
    type Accumulator = CountAccumulator;
    type SourceMeta = CountSourceMeta;
    const REQUIRES_FIELD_INDEX: bool = false;

    fn init_accumulator() -> Self::Accumulator {
        CountAccumulator::default()
    }

    fn accumulate(acc: &mut Self::Accumulator, _value: Option<&JsonValue>) {
        acc.count += 1;
    }

    fn accumulate_from_group(acc: &mut Self::Accumulator, items: &[JsonValue], _field_name: &str) {
        acc.count = items.len() as i64;
    }

    fn finalize(acc: &Self::Accumulator) -> JsonValue {
        JsonValue::Number(acc.count.into())
    }

    fn kind() -> &'static str {
        "countNode"
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
