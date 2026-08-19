use super::types::*;
use crate::error::{Error, Result};
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use document::NormalValue;
use schema::{CollectionVersion, QuerySource, ScalarKind};
use std::collections::{BTreeMap, HashMap, HashSet};

pub(super) fn supported_downsample_aggregate_fields() -> [AggregateField; 5] {
    [
        AggregateField::Count,
        AggregateField::Sum,
        AggregateField::Avg,
        AggregateField::Min,
        AggregateField::Max,
    ]
}

pub(super) fn aggregate_field_name(field: AggregateField) -> &'static str {
    match field {
        AggregateField::Count => "count",
        AggregateField::Sum => "sum",
        AggregateField::Avg => "avg",
        AggregateField::Min => "min",
        AggregateField::Max => "max",
    }
}

pub(super) fn normal_value_to_numeric(value: &NormalValue) -> Option<NumericValue> {
    value
        .as_int()
        .map(NumericValue::Int)
        .or_else(|| value.as_float64().map(NumericValue::Float))
        .or_else(|| {
            value
                .as_float32()
                .map(|value| NumericValue::Float(value as f64))
        })
}

pub(super) fn normal_value_to_time(value: &NormalValue) -> Option<DateTime<FixedOffset>> {
    value
        .as_time()
        .cloned()
        .or_else(|| value.as_str().and_then(document::parse_rfc3339))
}

fn decode_commit_delta_value(commit: &document::Document) -> Option<NormalValue> {
    use base64::Engine;

    let delta_base64 = commit.get("delta")?.as_str()?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(delta_base64)
        .ok()?;
    ciborium::from_reader::<NormalValue, _>(&bytes[..]).ok()
}

pub(super) fn scalar_kind_name(kind: ScalarKind) -> &'static str {
    match kind.base_kind() {
        ScalarKind::None => "None",
        ScalarKind::DocID => "DocID",
        ScalarKind::Bool => "Bool",
        ScalarKind::Int => "Int",
        ScalarKind::Float64 => "Float",
        ScalarKind::Float32 => "Float32",
        ScalarKind::DateTime => "DateTime",
        ScalarKind::String => "String",
        ScalarKind::Blob => "Blob",
        ScalarKind::Json => "JSON",
        _ => unreachable!(),
    }
}

pub(super) fn require_scalar_kind(
    collection: &CollectionVersion,
    field_name: &str,
    allowed: &[ScalarKind],
) -> Result<()> {
    let field = collection.field_by_name(field_name).ok_or_else(|| {
        Error::Other(format!(
            "downsample collection '{}' must define field '{}'",
            collection.name, field_name
        ))
    })?;
    let Some(kind) = field.kind.as_scalar() else {
        return Err(Error::Other(format!(
            "downsample field '{}.{}' must be a scalar",
            collection.name, field_name
        )));
    };
    if allowed.contains(&kind.base_kind()) {
        Ok(())
    } else {
        let expected = allowed
            .iter()
            .map(|kind| scalar_kind_name(*kind))
            .collect::<Vec<_>>()
            .join(" or ");
        Err(Error::Other(format!(
            "downsample field '{}.{}' must be {}, found {}",
            collection.name,
            field_name,
            expected,
            scalar_kind_name(kind)
        )))
    }
}

pub(super) fn require_numeric_field(
    collection: &CollectionVersion,
    field_name: &str,
) -> Result<()> {
    let field = collection.field_by_name(field_name).ok_or_else(|| {
        Error::Other(format!(
            "downsample collection '{}' must define field '{}'",
            collection.name, field_name
        ))
    })?;
    if field.kind.is_numeric() {
        Ok(())
    } else {
        Err(Error::Other(format!(
            "downsample field '{}.{}' must be numeric",
            collection.name, field_name
        )))
    }
}

pub(super) fn parse_go_duration_nanos(s: &str) -> std::result::Result<i64, String> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Ok(0);
    }

    let (negative, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    };

    if s.chars().all(|c| c.is_ascii_digit()) {
        let secs: i64 = s.parse().map_err(|_| format!("invalid number: {}", s))?;
        let nanos = secs
            .checked_mul(1_000_000_000)
            .ok_or_else(|| format!("duration overflow: {}", s))?;
        return Ok(if negative { -nanos } else { nanos });
    }

    let mut total_nanos: i64 = 0;
    let mut remaining = s;

    while !remaining.is_empty() {
        let num_end = remaining
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(remaining.len());
        if num_end == 0 {
            return Err(format!("invalid duration: {}", s));
        }

        let num_str = &remaining[..num_end];
        remaining = &remaining[num_end..];

        let unit_end = remaining
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(remaining.len());
        if unit_end == 0 {
            return Err(format!("missing unit in duration: {}", s));
        }

        let unit = &remaining[..unit_end];
        remaining = &remaining[unit_end..];

        let num: f64 = num_str
            .parse()
            .map_err(|_| format!("invalid number in duration: {}", num_str))?;
        let nanos_per_unit: f64 = match unit {
            "ns" => 1.0,
            "us" | "µs" | "μs" => 1_000.0,
            "ms" => 1_000_000.0,
            "s" => 1_000_000_000.0,
            "m" => 60.0 * 1_000_000_000.0,
            "h" => 3600.0 * 1_000_000_000.0,
            _ => return Err(format!("unknown unit in duration: {}", unit)),
        };

        total_nanos = total_nanos
            .checked_add((num * nanos_per_unit) as i64)
            .ok_or_else(|| format!("duration overflow: {}", s))?;
    }

    Ok(if negative { -total_nanos } else { total_nanos })
}

pub(super) fn parse_positive_interval_nanos(raw: &str) -> std::result::Result<i64, String> {
    let nanos = parse_go_duration_nanos(raw)?;
    if nanos <= 0 {
        Err(format!("downsample interval '{}' must be positive", raw))
    } else {
        Ok(nanos)
    }
}

pub(super) fn parse_positive_retention_nanos(raw: &str) -> std::result::Result<i64, String> {
    let nanos = parse_go_duration_nanos(raw)?;
    if nanos <= 0 {
        Err(format!("downsample retention '{}' must be positive", raw))
    } else {
        Ok(nanos)
    }
}

pub(super) fn parse_downsample_source_query(
    query_source: &QuerySource,
) -> Result<ParsedSourceQuery> {
    if query_source.transform.is_some() {
        return Err(Error::Other(
            "downsample source queries do not support lens transforms".to_string(),
        ));
    }

    for unsupported in [
        "Filter", "Limit", "Offset", "OrderBy", "DocIDs", "CID", "GroupBy",
    ] {
        if query_source
            .query
            .get(unsupported)
            .is_some_and(|value| !value.is_null())
        {
            return Err(Error::Other(format!(
                "downsample source queries must be flat collection selects without {}",
                unsupported
            )));
        }
    }

    if query_source
        .query
        .get("ShowDeleted")
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
    {
        return Err(Error::Other(
            "downsample source queries do not support ShowDeleted".to_string(),
        ));
    }

    let collection_name = query_source
        .query
        .get("Name")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::Other("downsample source queries must select a source collection".to_string())
        })?
        .to_string();

    let fields = query_source
        .query
        .get("Fields")
        .and_then(|value| value.as_array())
        .ok_or_else(|| {
            Error::Other("downsample source queries must list selected fields".to_string())
        })?;

    let mut selected_fields = HashSet::new();
    for field in fields {
        if field.get("Fields").is_some() || field.get("Targets").is_some() {
            return Err(Error::Other(
                "downsample source queries only support flat field selections".to_string(),
            ));
        }
        if field.get("Alias").is_some_and(|alias| !alias.is_null()) {
            return Err(Error::Other(
                "downsample source queries do not support field aliases".to_string(),
            ));
        }

        let field_name = field
            .get("Name")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                Error::Other("downsample source queries contain an unnamed field".to_string())
            })?;
        selected_fields.insert(field_name.to_string());
    }

    if selected_fields.is_empty() {
        return Err(Error::Other(
            "downsample source queries must select at least one field".to_string(),
        ));
    }

    Ok(ParsedSourceQuery {
        collection_name,
        selected_fields,
    })
}

pub(super) fn group_commit_values_by_height(
    commits: Vec<document::Document>,
    fields: &HashSet<&str>,
) -> BTreeMap<u64, HashMap<String, NormalValue>> {
    let mut values_by_height: BTreeMap<u64, HashMap<String, NormalValue>> = BTreeMap::new();

    for commit in commits {
        let Some(field_name) = commit.get("fieldName").and_then(|value| value.as_str()) else {
            continue;
        };
        if !fields.contains(field_name) {
            continue;
        }
        let Some(height) = commit
            .get("height")
            .and_then(|value| value.as_int())
            .and_then(|value| u64::try_from(value).ok())
        else {
            continue;
        };
        let Some(value) = decode_commit_delta_value(&commit) else {
            continue;
        };
        values_by_height
            .entry(height)
            .or_default()
            .insert(field_name.to_string(), value);
    }

    values_by_height
}

pub(super) fn numeric_value_to_normal_value(
    collection: &CollectionVersion,
    field_name: &str,
    value: NumericValue,
) -> Result<NormalValue> {
    let field = collection.field_by_name(field_name).ok_or_else(|| {
        Error::Other(format!(
            "downsample collection '{}' must define field '{}'",
            collection.name, field_name
        ))
    })?;
    match field.kind.as_scalar().map(ScalarKind::base_kind) {
        Some(ScalarKind::Int) => match value {
            NumericValue::Int(value) => Ok(NormalValue::Int(value)),
            NumericValue::Float(_) => Err(Error::Other(format!(
                "downsample field '{}.{}' can not store floating-point values",
                collection.name, field_name
            ))),
        },
        Some(ScalarKind::Float32) => Ok(NormalValue::Float32(value.as_f64() as f32)),
        Some(ScalarKind::Float64) => Ok(NormalValue::Float64(value.as_f64())),
        _ => Err(Error::Other(format!(
            "downsample field '{}.{}' must be numeric",
            collection.name, field_name
        ))),
    }
}

pub(super) fn average_value_to_normal_value(
    collection: &CollectionVersion,
    field_name: &str,
    value: f64,
) -> Result<NormalValue> {
    let field = collection.field_by_name(field_name).ok_or_else(|| {
        Error::Other(format!(
            "downsample collection '{}' must define field '{}'",
            collection.name, field_name
        ))
    })?;
    match field.kind.as_scalar().map(ScalarKind::base_kind) {
        Some(ScalarKind::Float32) => Ok(NormalValue::Float32(value as f32)),
        Some(ScalarKind::Float64) => Ok(NormalValue::Float64(value)),
        _ => Err(Error::Other(format!(
            "downsample field '{}.{}' must be Float or Float32",
            collection.name, field_name
        ))),
    }
}

pub(super) fn timestamp_nanos(value: &DateTime<FixedOffset>) -> Result<i64> {
    value
        .with_timezone(&Utc)
        .timestamp_nanos_opt()
        .ok_or_else(|| {
            Error::Other(format!(
                "downsample timestamp '{}' can not be represented in nanoseconds",
                value
            ))
        })
}

pub(super) fn datetime_from_nanos(nanos: i64) -> Result<DateTime<FixedOffset>> {
    let secs = nanos.div_euclid(1_000_000_000);
    let nanos = nanos.rem_euclid(1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|value| value.fixed_offset())
        .ok_or_else(|| Error::Other("failed to construct downsample DateTime".to_string()))
}

pub(super) fn bucket_start_nanos(
    value: &DateTime<FixedOffset>,
    interval_nanos: i64,
) -> Result<i64> {
    Ok(timestamp_nanos(value)?.div_euclid(interval_nanos) * interval_nanos)
}

pub(super) fn source_sample_retention_time_nanos(sample: &SourceSample) -> Result<i64> {
    match &sample.coverage_end {
        Some(coverage_end) => timestamp_nanos(coverage_end),
        None => timestamp_nanos(&sample.time),
    }
}
