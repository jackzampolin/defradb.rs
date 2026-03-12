use crate::auto_commit_mutator::AutoCommitMutator;
use crate::error::{Error, Result};
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use datastore::NamespaceView;
use document::{DocID, Document, NormalValue};
use events::EventName;
use query::fetcher::CommitsQueryOptions;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use schema::{CollectionVersion, QuerySource, ScalarKind};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use storage::keys::headstore::HeadstoreDocKey;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateField {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
enum SourceKind {
    Raw { measure_field: String },
    Downsample,
}

#[derive(Debug, Clone)]
struct ParsedSourceQuery {
    collection_name: String,
    selected_fields: HashSet<String>,
}

#[derive(Debug, Clone)]
struct DownsamplePlan {
    target: CollectionVersion,
    source: CollectionVersion,
    interval_raw: String,
    interval_nanos: i64,
    time_field: String,
    passthrough_fields: Vec<String>,
    aggregate_fields: Vec<AggregateField>,
    source_kind: SourceKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum NumericValue {
    Int(i64),
    Float(f64),
}

impl NumericValue {
    fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
        }
    }

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Self::Int(left + right),
            (left, right) => Self::Float(left.as_f64() + right.as_f64()),
        }
    }

    fn min(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Self::Int(left.min(right)),
            (left, right) => Self::Float(left.as_f64().min(right.as_f64())),
        }
    }

    fn max(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Self::Int(left.max(right)),
            (left, right) => Self::Float(left.as_f64().max(right.as_f64())),
        }
    }
}

#[derive(Debug, Clone)]
struct SourceSample {
    height: u64,
    time: DateTime<FixedOffset>,
    coverage_end: Option<DateTime<FixedOffset>>,
    count: i64,
    sum: Option<NumericValue>,
    min: Option<NumericValue>,
    max: Option<NumericValue>,
}

#[derive(Debug, Clone)]
struct PendingWindowAggregate {
    count: i64,
    sum: Option<NumericValue>,
    min: Option<NumericValue>,
    max: Option<NumericValue>,
    source_height: u64,
    max_coverage_end_nanos: Option<i64>,
    window_end_nanos: i64,
}

#[derive(Debug, Clone)]
struct WindowAggregate {
    count: i64,
    sum: Option<NumericValue>,
    avg: Option<f64>,
    min: Option<NumericValue>,
    max: Option<NumericValue>,
    window_start: DateTime<FixedOffset>,
    window_end: DateTime<FixedOffset>,
    source_height: i64,
}

fn decode_priority_varint(buf: &[u8]) -> u64 {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for &byte in buf {
        if shift >= 64 {
            return 0;
        }
        value |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return value;
        }
        shift += 7;
    }
    value
}

fn supported_downsample_aggregate_fields() -> [AggregateField; 5] {
    [
        AggregateField::Count,
        AggregateField::Sum,
        AggregateField::Avg,
        AggregateField::Min,
        AggregateField::Max,
    ]
}

fn aggregate_field_name(field: AggregateField) -> &'static str {
    match field {
        AggregateField::Count => "count",
        AggregateField::Sum => "sum",
        AggregateField::Avg => "avg",
        AggregateField::Min => "min",
        AggregateField::Max => "max",
    }
}

fn normal_value_to_numeric(value: &NormalValue) -> Option<NumericValue> {
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

fn normal_value_to_time(value: &NormalValue) -> Option<DateTime<FixedOffset>> {
    value.as_time().cloned().or_else(|| {
        value
            .as_str()
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
    })
}

fn decode_commit_delta_value(commit: &Document) -> Option<NormalValue> {
    use base64::Engine;

    let delta_base64 = commit.get("delta")?.as_str()?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(delta_base64)
        .ok()?;
    ciborium::from_reader::<NormalValue, _>(&bytes[..]).ok()
}

fn scalar_kind_name(kind: ScalarKind) -> &'static str {
    match kind {
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
    }
}

fn require_scalar_kind(
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
    if allowed.contains(&kind) {
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

fn require_numeric_field(collection: &CollectionVersion, field_name: &str) -> Result<()> {
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

fn parse_go_duration_nanos(s: &str) -> std::result::Result<i64, String> {
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

fn parse_positive_interval_nanos(raw: &str) -> std::result::Result<i64, String> {
    let nanos = parse_go_duration_nanos(raw)?;
    if nanos <= 0 {
        Err(format!("downsample interval '{}' must be positive", raw))
    } else {
        Ok(nanos)
    }
}

fn parse_downsample_source_query(query_source: &QuerySource) -> Result<ParsedSourceQuery> {
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

fn group_commit_values_by_height(
    commits: Vec<Document>,
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

fn numeric_value_to_normal_value(
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
    match field.kind.as_scalar() {
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

fn average_value_to_normal_value(
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
    match field.kind.as_scalar() {
        Some(ScalarKind::Float32) => Ok(NormalValue::Float32(value as f32)),
        Some(ScalarKind::Float64) => Ok(NormalValue::Float64(value)),
        _ => Err(Error::Other(format!(
            "downsample field '{}.{}' must be Float or Float32",
            collection.name, field_name
        ))),
    }
}

fn timestamp_nanos(value: &DateTime<FixedOffset>) -> Result<i64> {
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

fn datetime_from_nanos(nanos: i64) -> Result<DateTime<FixedOffset>> {
    let secs = nanos.div_euclid(1_000_000_000);
    let nanos = nanos.rem_euclid(1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|value| value.fixed_offset())
        .ok_or_else(|| Error::Other("failed to construct downsample DateTime".to_string()))
}

fn bucket_start_nanos(value: &DateTime<FixedOffset>, interval_nanos: i64) -> Result<i64> {
    Ok(timestamp_nanos(value)?.div_euclid(interval_nanos) * interval_nanos)
}

impl<S: Store> crate::database::DB<S> {
    fn downsample_depth(
        &self,
        collection_name: &str,
        memo: &mut HashMap<String, usize>,
        visiting: &mut HashSet<String>,
    ) -> Result<usize> {
        if let Some(depth) = memo.get(collection_name) {
            return Ok(*depth);
        }
        if !visiting.insert(collection_name.to_string()) {
            return Err(Error::Other(format!(
                "detected a downsample dependency cycle involving '{}'",
                collection_name
            )));
        }

        let depth = match self.get_collection(collection_name)? {
            Some(collection) => match &collection.schema().downsample_source {
                Some(source) => {
                    let parsed = parse_downsample_source_query(source)?;
                    1 + self.downsample_depth(&parsed.collection_name, memo, visiting)?
                }
                None => 0,
            },
            None => 0,
        };

        visiting.remove(collection_name);
        memo.insert(collection_name.to_string(), depth);
        Ok(depth)
    }

    fn validate_downsample_cycle(&self, target_name: &str, source_name: &str) -> Result<()> {
        let mut seen = HashSet::new();
        let mut current_name = source_name.to_string();

        while seen.insert(current_name.clone()) {
            if current_name == target_name {
                return Err(Error::Other(format!(
                    "downsample collection '{}' can not depend on itself",
                    target_name
                )));
            }

            let Some(collection) = self.get_collection(&current_name)? else {
                break;
            };
            let Some(query_source) = &collection.schema().downsample_source else {
                break;
            };

            current_name = parse_downsample_source_query(query_source)?.collection_name;
        }

        Ok(())
    }

    fn build_downsample_plan(&self, target: &CollectionVersion) -> Result<DownsamplePlan> {
        let interval_raw = target.downsample_interval.clone().ok_or_else(|| {
            Error::Other(format!(
                "collection '{}' is not configured as a downsample collection",
                target.name
            ))
        })?;
        let interval_nanos = parse_positive_interval_nanos(&interval_raw).map_err(Error::Other)?;
        let time_field = target.downsample_time_field.clone().ok_or_else(|| {
            Error::Other(format!(
                "downsample collection '{}' is missing its time field",
                target.name
            ))
        })?;
        let query_source = target.downsample_source.as_ref().ok_or_else(|| {
            Error::Other(format!(
                "downsample collection '{}' is missing its source query",
                target.name
            ))
        })?;
        let parsed_source = parse_downsample_source_query(query_source)?;
        self.validate_downsample_cycle(&target.name, &parsed_source.collection_name)?;

        let source_collection = self
            .get_collection(&parsed_source.collection_name)?
            .ok_or_else(|| Error::CollectionNotFound(parsed_source.collection_name.clone()))?;
        let source = source_collection.schema().clone();

        if !parsed_source.selected_fields.contains(time_field.as_str()) {
            return Err(Error::Other(format!(
                "downsample time field '{}.{}' must be selected from '{}'",
                source.name, time_field, source.name
            )));
        }
        require_scalar_kind(&source, &time_field, &[ScalarKind::DateTime])?;

        require_scalar_kind(
            target,
            "source_doc_id",
            &[ScalarKind::String, ScalarKind::DocID],
        )?;
        require_scalar_kind(target, "source_height", &[ScalarKind::Int])?;
        require_scalar_kind(target, "window_start", &[ScalarKind::DateTime])?;
        require_scalar_kind(target, "window_end", &[ScalarKind::DateTime])?;

        let mut aggregate_fields = Vec::new();
        for field in supported_downsample_aggregate_fields() {
            let field_name = aggregate_field_name(field);
            if target.field_by_name(field_name).is_none() {
                continue;
            }

            match field {
                AggregateField::Count => {
                    require_scalar_kind(target, field_name, &[ScalarKind::Int])?;
                }
                AggregateField::Avg => {
                    require_scalar_kind(
                        target,
                        field_name,
                        &[ScalarKind::Float64, ScalarKind::Float32],
                    )?;
                }
                AggregateField::Sum | AggregateField::Min | AggregateField::Max => {
                    require_numeric_field(target, field_name)?;
                }
            }
            aggregate_fields.push(field);
        }

        if aggregate_fields.is_empty() {
            return Err(Error::Other(format!(
                "downsample collection '{}' must define at least one aggregate field (count, sum, avg, min, max)",
                target.name
            )));
        }

        let passthrough_fields: Vec<String> = target
            .fields
            .iter()
            .filter(|field| {
                !matches!(
                    field.name.as_str(),
                    "_docID"
                        | "source_doc_id"
                        | "source_height"
                        | "window_start"
                        | "window_end"
                        | "count"
                        | "sum"
                        | "avg"
                        | "min"
                        | "max"
                )
            })
            .map(|field| field.name.clone())
            .collect();

        if passthrough_fields.iter().any(|field| field == &time_field) {
            return Err(Error::Other(format!(
                "downsample time field '{}.{}' can not also be a passthrough field",
                source.name, time_field
            )));
        }

        for field_name in &passthrough_fields {
            if !parsed_source.selected_fields.contains(field_name) {
                return Err(Error::Other(format!(
                    "downsample passthrough field '{}.{}' must be selected from '{}'",
                    target.name, field_name, source.name
                )));
            }

            let source_field = source.field_by_name(field_name).ok_or_else(|| {
                Error::Other(format!(
                    "downsample source collection '{}' does not define field '{}'",
                    source.name, field_name
                ))
            })?;
            let target_field = target.field_by_name(field_name).ok_or_else(|| {
                Error::Other(format!(
                    "downsample collection '{}' does not define field '{}'",
                    target.name, field_name
                ))
            })?;

            if source_field.kind != target_field.kind {
                return Err(Error::Other(format!(
                    "downsample passthrough field '{}.{}' must match the source field type",
                    target.name, field_name
                )));
            }
        }

        let source_kind = if source.downsample_interval.is_some() {
            if !parsed_source.selected_fields.contains("window_end") {
                return Err(Error::Other(format!(
                    "downsample source query for '{}' must select 'window_end' when chaining from another downsample collection",
                    target.name
                )));
            }
            require_scalar_kind(&source, "source_height", &[ScalarKind::Int])?;
            require_scalar_kind(&source, "window_start", &[ScalarKind::DateTime])?;
            require_scalar_kind(&source, "window_end", &[ScalarKind::DateTime])?;

            if aggregate_fields
                .iter()
                .any(|field| matches!(field, AggregateField::Count | AggregateField::Avg))
            {
                require_scalar_kind(&source, "count", &[ScalarKind::Int])?;
            }
            if aggregate_fields
                .iter()
                .any(|field| matches!(field, AggregateField::Sum | AggregateField::Avg))
            {
                require_numeric_field(&source, "sum")?;
            }
            if aggregate_fields.contains(&AggregateField::Min) {
                require_numeric_field(&source, "min")?;
            }
            if aggregate_fields.contains(&AggregateField::Max) {
                require_numeric_field(&source, "max")?;
            }

            SourceKind::Downsample
        } else {
            let passthrough_set: HashSet<&str> =
                passthrough_fields.iter().map(String::as_str).collect();
            let mut numeric_candidates: Vec<String> = parsed_source
                .selected_fields
                .iter()
                .filter(|field_name| field_name.as_str() != time_field)
                .filter(|field_name| !passthrough_set.contains(field_name.as_str()))
                .filter(|field_name| {
                    source
                        .field_by_name(field_name)
                        .map(|field| field.kind.is_numeric())
                        .unwrap_or(false)
                })
                .cloned()
                .collect();
            numeric_candidates.sort();

            let measure_field = if numeric_candidates.iter().any(|field| field == "value") {
                "value".to_string()
            } else if numeric_candidates.len() == 1 {
                numeric_candidates[0].clone()
            } else {
                return Err(Error::Other(format!(
                    "downsample source '{}' must select exactly one numeric measure field",
                    source.name
                )));
            };

            if passthrough_set.contains(measure_field.as_str()) {
                return Err(Error::Other(format!(
                    "downsample measure field '{}.{}' can not also be a passthrough field",
                    source.name, measure_field
                )));
            }

            SourceKind::Raw { measure_field }
        };

        Ok(DownsamplePlan {
            target: target.clone(),
            source,
            interval_raw,
            interval_nanos,
            time_field,
            passthrough_fields,
            aggregate_fields,
            source_kind,
        })
    }

    fn downsample_plans(
        &self,
        names_filter: Option<&HashSet<String>>,
        source_name_filter: Option<&str>,
    ) -> Result<Vec<DownsamplePlan>> {
        let collections: Vec<CollectionVersion> = {
            let cache = self.collections.read().map_err(|_| {
                Error::Other(
                    "failed to acquire collections lock while planning downsamples".to_string(),
                )
            })?;
            cache
                .values()
                .map(|collection| collection.schema().clone())
                .collect()
        };

        let mut plans = Vec::new();
        for collection in collections {
            if collection.downsample_interval.is_none() {
                continue;
            }
            if names_filter.is_some_and(|names| !names.contains(&collection.name)) {
                continue;
            }

            let plan = self.build_downsample_plan(&collection)?;
            if source_name_filter.is_some_and(|name| name != plan.source.name) {
                continue;
            }
            plans.push(plan);
        }

        Ok(plans)
    }

    pub fn validate_downsample_collection(&self, collection: &CollectionVersion) -> Result<()> {
        let _ = self.build_downsample_plan(collection)?;
        Ok(())
    }
}

impl<S: Store + 'static> crate::database::DB<S> {
    pub async fn validate_downsample_write(
        &self,
        datastore: &NamespaceView,
        source_collection: &CollectionVersion,
        source_doc: &Document,
    ) -> Result<()> {
        let plans = self.downsample_plans(None, Some(&source_collection.name))?;
        if plans.is_empty() {
            return Ok(());
        }

        let series_doc_id = self.series_doc_id(source_doc)?;

        for plan in plans {
            let source_time = source_doc
                .get(&plan.time_field)
                .and_then(normal_value_to_time)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "downsample source '{}.{}' must contain a valid RFC3339 timestamp",
                        source_collection.name, plan.time_field
                    ))
                })?;
            let source_window_start_nanos = bucket_start_nanos(&source_time, plan.interval_nanos)?;
            let target_doc_id = DocID::new_v0_from_seed(&format!(
                "{}:{}",
                plan.target.collection_id, series_doc_id
            ));

            let Some(target_collection) = self.get_collection(&plan.target.name)? else {
                continue;
            };
            let Some(target_doc) = target_collection
                .get_with_datastore(datastore, &target_doc_id)
                .await?
            else {
                continue;
            };

            let current_window_start = target_doc
                .get("window_start")
                .and_then(normal_value_to_time)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "downsample target '{}.window_start' is missing or invalid",
                        plan.target.name
                    ))
                })?;
            let current_window_start_nanos = timestamp_nanos(&current_window_start)?;

            if source_window_start_nanos <= current_window_start_nanos {
                return Err(Error::Other(format!(
                    "late data is not accepted for downsample source '{}': timestamp '{}' falls in closed bucket '{}'",
                    source_collection.name, source_time, current_window_start
                )));
            }
        }

        Ok(())
    }

    async fn latest_doc_priority(&self, doc_id: &str) -> Result<u64> {
        let txn = self.new_txn(true).await?;
        let headstore = txn.headstore()?;
        let mut iter = headstore
            .iterator(IterOptions::new().with_prefix(HeadstoreDocKey::document_prefix(doc_id)))
            .await
            .map_err(Error::Storage)?;

        let mut max_priority = 0;
        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            max_priority = max_priority.max(decode_priority_varint(&pair.value));
        }
        iter.close().await.map_err(Error::Storage)?;

        let _ = txn.discard();
        Ok(max_priority)
    }

    async fn load_source_documents(&self, collection: &CollectionVersion) -> Result<Vec<Document>> {
        let source_collection = self
            .get_collection(&collection.name)?
            .ok_or_else(|| Error::CollectionNotFound(collection.name.clone()))?;
        let txn = self.new_txn(true).await?;
        let datastore = txn.datastore()?;
        let result = source_collection.get_all_with_datastore(&datastore).await;
        let _ = txn.discard();
        result
    }

    fn build_raw_source_samples(
        &self,
        plan: &DownsamplePlan,
        measure_field: &str,
        commits: Vec<Document>,
    ) -> Result<Vec<SourceSample>> {
        let needed = HashSet::from([plan.time_field.as_str(), measure_field]);
        let grouped = group_commit_values_by_height(commits, &needed);
        let mut samples = Vec::new();

        for (height, values) in grouped {
            let time = values
                .get(&plan.time_field)
                .and_then(normal_value_to_time)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "downsample source '{}.{}' must be updated alongside '{}' at commit height {}",
                        plan.source.name, plan.time_field, measure_field, height
                    ))
                })?;
            let measure = values
                .get(measure_field)
                .and_then(normal_value_to_numeric)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "downsample source '{}.{}' must be numeric at commit height {}",
                        plan.source.name, measure_field, height
                    ))
                })?;

            samples.push(SourceSample {
                height,
                time,
                coverage_end: None,
                count: 1,
                sum: Some(measure),
                min: Some(measure),
                max: Some(measure),
            });
        }

        Ok(samples)
    }

    fn build_rollup_source_samples(
        &self,
        plan: &DownsamplePlan,
        commits: Vec<Document>,
    ) -> Result<Vec<SourceSample>> {
        let mut needed = HashSet::from([plan.time_field.as_str(), "window_end"]);
        for field in &plan.aggregate_fields {
            match field {
                AggregateField::Count => {
                    needed.insert("count");
                }
                AggregateField::Sum => {
                    needed.insert("sum");
                }
                AggregateField::Avg => {
                    needed.insert("count");
                    needed.insert("sum");
                }
                AggregateField::Min => {
                    needed.insert("min");
                }
                AggregateField::Max => {
                    needed.insert("max");
                }
            }
        }

        let grouped = group_commit_values_by_height(commits, &needed);
        let mut samples = Vec::new();

        for (height, values) in grouped {
            let time = values
                .get(&plan.time_field)
                .and_then(normal_value_to_time)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "downsample source '{}.{}' must be present at commit height {}",
                        plan.source.name, plan.time_field, height
                    ))
                })?;
            let coverage_end = values
                .get("window_end")
                .and_then(normal_value_to_time)
                .ok_or_else(|| {
                    Error::Other(format!(
                        "downsample source '{}.window_end' must be present at commit height {}",
                        plan.source.name, height
                    ))
                })?;

            let count = if plan
                .aggregate_fields
                .iter()
                .any(|field| matches!(field, AggregateField::Count | AggregateField::Avg))
            {
                values
                    .get("count")
                    .and_then(|value| value.as_int())
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "downsample source '{}.count' must be present at commit height {}",
                            plan.source.name, height
                        ))
                    })?
            } else {
                0
            };

            let sum = if plan
                .aggregate_fields
                .iter()
                .any(|field| matches!(field, AggregateField::Sum | AggregateField::Avg))
            {
                Some(
                    values
                        .get("sum")
                        .and_then(normal_value_to_numeric)
                        .ok_or_else(|| {
                            Error::Other(format!(
                                "downsample source '{}.sum' must be numeric at commit height {}",
                                plan.source.name, height
                            ))
                        })?,
                )
            } else {
                None
            };

            let min = if plan.aggregate_fields.contains(&AggregateField::Min) {
                Some(
                    values
                        .get("min")
                        .and_then(normal_value_to_numeric)
                        .ok_or_else(|| {
                            Error::Other(format!(
                                "downsample source '{}.min' must be numeric at commit height {}",
                                plan.source.name, height
                            ))
                        })?,
                )
            } else {
                None
            };

            let max = if plan.aggregate_fields.contains(&AggregateField::Max) {
                Some(
                    values
                        .get("max")
                        .and_then(normal_value_to_numeric)
                        .ok_or_else(|| {
                            Error::Other(format!(
                                "downsample source '{}.max' must be numeric at commit height {}",
                                plan.source.name, height
                            ))
                        })?,
                )
            } else {
                None
            };

            samples.push(SourceSample {
                height,
                time,
                coverage_end: Some(coverage_end),
                count,
                sum,
                min,
                max,
            });
        }

        Ok(samples)
    }

    fn build_source_samples(
        &self,
        plan: &DownsamplePlan,
        commits: Vec<Document>,
    ) -> Result<Vec<SourceSample>> {
        match &plan.source_kind {
            SourceKind::Raw { measure_field } => {
                self.build_raw_source_samples(plan, measure_field, commits)
            }
            SourceKind::Downsample => self.build_rollup_source_samples(plan, commits),
        }
    }

    fn aggregate_samples_into_windows(
        &self,
        plan: &DownsamplePlan,
        samples: Vec<SourceSample>,
        complete_through: DateTime<FixedOffset>,
    ) -> Result<Vec<WindowAggregate>> {
        let complete_through_nanos = timestamp_nanos(&complete_through)?;
        let mut pending: BTreeMap<i64, PendingWindowAggregate> = BTreeMap::new();

        for sample in samples {
            let start_nanos = bucket_start_nanos(&sample.time, plan.interval_nanos)?;
            let window_end_nanos =
                start_nanos
                    .checked_add(plan.interval_nanos)
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "downsample interval '{}' overflowed a bucket boundary",
                            plan.interval_raw
                        ))
                    })?;

            let entry = pending
                .entry(start_nanos)
                .or_insert(PendingWindowAggregate {
                    count: 0,
                    sum: None,
                    min: None,
                    max: None,
                    source_height: sample.height,
                    max_coverage_end_nanos: None,
                    window_end_nanos,
                });
            entry.count += sample.count;
            entry.source_height = entry.source_height.max(sample.height);
            entry.window_end_nanos = window_end_nanos;
            if let Some(coverage_end) = sample.coverage_end {
                let coverage_end_nanos = timestamp_nanos(&coverage_end)?;
                entry.max_coverage_end_nanos = Some(
                    entry
                        .max_coverage_end_nanos
                        .map_or(coverage_end_nanos, |current| {
                            current.max(coverage_end_nanos)
                        }),
                );
            }

            if let Some(value) = sample.sum {
                entry.sum = Some(entry.sum.map_or(value, |sum| sum.add(value)));
            }
            if let Some(value) = sample.min {
                entry.min = Some(entry.min.map_or(value, |min| min.min(value)));
            }
            if let Some(value) = sample.max {
                entry.max = Some(entry.max.map_or(value, |max| max.max(value)));
            }
        }

        let mut windows = Vec::new();
        for (window_start_nanos, aggregate) in pending {
            let is_complete = match &plan.source_kind {
                SourceKind::Raw { .. } => aggregate.window_end_nanos <= complete_through_nanos,
                SourceKind::Downsample => aggregate
                    .max_coverage_end_nanos
                    .is_some_and(|coverage_end| coverage_end >= aggregate.window_end_nanos),
            };
            if !is_complete {
                continue;
            }

            let avg = match (aggregate.sum, aggregate.count) {
                (Some(sum), count) if count > 0 => Some(sum.as_f64() / count as f64),
                _ => None,
            };

            windows.push(WindowAggregate {
                count: aggregate.count,
                sum: aggregate.sum,
                avg,
                min: aggregate.min,
                max: aggregate.max,
                window_start: datetime_from_nanos(window_start_nanos)?,
                window_end: datetime_from_nanos(aggregate.window_end_nanos)?,
                source_height: i64::try_from(aggregate.source_height).map_err(|_| {
                    Error::Other("downsample source height exceeded i64".to_string())
                })?,
            });
        }

        Ok(windows)
    }

    fn series_doc_id(&self, source_doc: &Document) -> Result<String> {
        source_doc
            .get("source_doc_id")
            .and_then(|value| value.as_str())
            .map(str::to_string)
            .or_else(|| source_doc.id().map(ToString::to_string))
            .ok_or_else(|| Error::Other("downsample source document is missing an id".to_string()))
    }

    fn set_field(
        &self,
        doc: &mut Document,
        modified_fields: &mut HashSet<String>,
        field_name: &str,
        value: NormalValue,
        force_write: bool,
    ) {
        if force_write || doc.get(field_name) != Some(&value) {
            doc.set(field_name, value);
            modified_fields.insert(field_name.to_string());
        }
    }

    fn apply_window_to_target_doc(
        &self,
        plan: &DownsamplePlan,
        source_doc: &Document,
        doc: &mut Document,
        aggregate: &WindowAggregate,
        series_doc_id: &str,
    ) -> Result<HashSet<String>> {
        let mut modified_fields = HashSet::new();

        self.set_field(
            doc,
            &mut modified_fields,
            "source_doc_id",
            NormalValue::String(series_doc_id.to_string()),
            false,
        );
        self.set_field(
            doc,
            &mut modified_fields,
            "source_height",
            NormalValue::Int(aggregate.source_height),
            true,
        );
        self.set_field(
            doc,
            &mut modified_fields,
            "window_start",
            NormalValue::Time(aggregate.window_start),
            true,
        );
        self.set_field(
            doc,
            &mut modified_fields,
            "window_end",
            NormalValue::Time(aggregate.window_end),
            true,
        );

        for field_name in &plan.passthrough_fields {
            let value = source_doc.get(field_name).cloned().ok_or_else(|| {
                Error::Other(format!(
                    "downsample source '{}' is missing passthrough field '{}'",
                    plan.source.name, field_name
                ))
            })?;
            self.set_field(doc, &mut modified_fields, field_name, value, false);
        }

        for field in &plan.aggregate_fields {
            match field {
                AggregateField::Count => {
                    self.set_field(
                        doc,
                        &mut modified_fields,
                        "count",
                        NormalValue::Int(aggregate.count),
                        true,
                    );
                }
                AggregateField::Sum => {
                    let value = aggregate.sum.ok_or_else(|| {
                        Error::Other(format!(
                            "downsample window for '{}' is missing a sum value",
                            plan.target.name
                        ))
                    })?;
                    self.set_field(
                        doc,
                        &mut modified_fields,
                        "sum",
                        numeric_value_to_normal_value(&plan.target, "sum", value)?,
                        true,
                    );
                }
                AggregateField::Avg => {
                    let value = aggregate.avg.ok_or_else(|| {
                        Error::Other(format!(
                            "downsample window for '{}' is missing an avg value",
                            plan.target.name
                        ))
                    })?;
                    self.set_field(
                        doc,
                        &mut modified_fields,
                        "avg",
                        average_value_to_normal_value(&plan.target, "avg", value)?,
                        true,
                    );
                }
                AggregateField::Min => {
                    let value = aggregate.min.ok_or_else(|| {
                        Error::Other(format!(
                            "downsample window for '{}' is missing a min value",
                            plan.target.name
                        ))
                    })?;
                    self.set_field(
                        doc,
                        &mut modified_fields,
                        "min",
                        numeric_value_to_normal_value(&plan.target, "min", value)?,
                        true,
                    );
                }
                AggregateField::Max => {
                    let value = aggregate.max.ok_or_else(|| {
                        Error::Other(format!(
                            "downsample window for '{}' is missing a max value",
                            plan.target.name
                        ))
                    })?;
                    self.set_field(
                        doc,
                        &mut modified_fields,
                        "max",
                        numeric_value_to_normal_value(&plan.target, "max", value)?,
                        true,
                    );
                }
            }
        }

        Ok(modified_fields)
    }

    async fn persist_window_update(
        self: &Arc<Self>,
        plan: &DownsamplePlan,
        source_doc: &Document,
        series_doc_id: &str,
        target_doc_id: &DocID,
        aggregate: &WindowAggregate,
    ) -> Result<()> {
        let mutator = AutoCommitMutator::new(self.clone());
        let maybe_existing = mutator
            .get_for_update(&plan.target.name, target_doc_id)
            .await
            .map_err(Error::Query)?;

        match maybe_existing {
            Some(mut doc) => {
                let modified_fields = self.apply_window_to_target_doc(
                    plan,
                    source_doc,
                    &mut doc,
                    aggregate,
                    series_doc_id,
                )?;

                if modified_fields.is_empty() {
                    return Ok(());
                }

                doc.set_collection(plan.target.clone());
                doc.set_schema_version_id(plan.target.version_id.clone());
                doc.set_id(target_doc_id.clone());

                mutator
                    .update(&plan.target.name, doc, modified_fields)
                    .await
                    .map_err(Error::Query)?;
            }
            None => {
                let mut doc = Document::with_id(target_doc_id.clone());
                doc.set_collection(plan.target.clone());
                doc.set_schema_version_id(plan.target.version_id.clone());

                self.apply_window_to_target_doc(
                    plan,
                    source_doc,
                    &mut doc,
                    aggregate,
                    series_doc_id,
                )?;

                mutator
                    .create(&plan.target.name, doc)
                    .await
                    .map_err(Error::Query)?;
            }
        }

        Ok(())
    }

    async fn process_source_doc_for_plan(
        self: &Arc<Self>,
        plan: &DownsamplePlan,
        source_doc: &Document,
        complete_through: DateTime<FixedOffset>,
    ) -> Result<()> {
        let Some(source_doc_id) = source_doc.id() else {
            return Ok(());
        };
        let source_doc_id = source_doc_id.to_string();
        let latest_source_priority = self.latest_doc_priority(&source_doc_id).await?;
        if latest_source_priority == 0 {
            return Ok(());
        }

        let series_doc_id = self.series_doc_id(source_doc)?;
        let target_doc_id =
            DocID::new_v0_from_seed(&format!("{}:{}", plan.target.collection_id, series_doc_id));

        let mutator = AutoCommitMutator::new(self.clone());
        let current_target = mutator
            .get_for_update(&plan.target.name, &target_doc_id)
            .await
            .map_err(Error::Query)?;
        let processed_height = current_target
            .as_ref()
            .and_then(|doc| doc.get("source_height"))
            .and_then(|value| value.as_int())
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(0);
        let current_window_start = current_target
            .as_ref()
            .and_then(|doc| doc.get("window_start"))
            .and_then(normal_value_to_time);

        if processed_height >= latest_source_priority {
            return Ok(());
        }

        let fetcher = crate::LensedAutoCommitFetcher::new(self.clone());
        let commits = fetcher
            .get_commits(&CommitsQueryOptions {
                doc_id: Some(source_doc_id.clone()),
                height_start: Some(processed_height + 1),
                height_end: Some(latest_source_priority + 1),
                ..Default::default()
            })
            .await
            .map_err(Error::Query)?;

        let samples = self.build_source_samples(plan, commits)?;
        if samples.is_empty() {
            return Ok(());
        }

        let windows = self.aggregate_samples_into_windows(plan, samples, complete_through)?;
        if windows.is_empty() {
            return Ok(());
        }

        if let Some(current_window_start) = current_window_start {
            let current_window_start_nanos = timestamp_nanos(&current_window_start)?;
            let first_window_start_nanos = timestamp_nanos(&windows[0].window_start)?;
            if first_window_start_nanos <= current_window_start_nanos {
                return Err(Error::Other(format!(
                    "downsample source '{}' produced out-of-order timestamps for series '{}'; late data for already-closed windows is not yet supported",
                    plan.source.name, series_doc_id
                )));
            }
        }

        for aggregate in windows {
            self.persist_window_update(
                plan,
                source_doc,
                &series_doc_id,
                &target_doc_id,
                &aggregate,
            )
            .await?;
        }

        Ok(())
    }

    async fn bootstrap_downsample_plan(self: &Arc<Self>, plan: &DownsamplePlan) -> Result<()> {
        let now = Utc::now().fixed_offset();
        for source_doc in self.load_source_documents(&plan.source).await? {
            self.process_source_doc_for_plan(plan, &source_doc, now)
                .await?;
        }
        Ok(())
    }

    pub async fn bootstrap_downsamples(self: &Arc<Self>, names: Option<&[String]>) -> Result<()> {
        let name_set = names.map(|names| names.iter().cloned().collect::<HashSet<_>>());
        let mut plans = self.downsample_plans(name_set.as_ref(), None)?;

        let mut memo = HashMap::new();
        let mut visiting = HashSet::new();
        plans.sort_by_key(|plan| {
            self.downsample_depth(&plan.target.name, &mut memo, &mut visiting)
                .unwrap_or(usize::MAX)
        });

        for plan in plans {
            self.bootstrap_downsample_plan(&plan).await?;
        }

        Ok(())
    }

    async fn process_downsample_update(
        self: &Arc<Self>,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<()> {
        if doc_id.is_empty() {
            return Ok(());
        }

        let source_collection = match self.find_collection_by_id(collection_id)? {
            Some(collection) => collection.schema().clone(),
            None => return Ok(()),
        };

        let plans = self.downsample_plans(None, Some(&source_collection.name))?;
        if plans.is_empty() {
            return Ok(());
        }

        let source_doc_id =
            DocID::from_string(doc_id).map_err(|error| Error::Other(error.to_string()))?;
        let fetcher = AutoCommitMutator::new(self.clone());
        let Some(source_doc) = fetcher
            .get_for_update(&source_collection.name, &source_doc_id)
            .await
            .map_err(Error::Query)?
        else {
            return Ok(());
        };

        let now = Utc::now().fixed_offset();
        for plan in plans {
            self.process_source_doc_for_plan(&plan, &source_doc, now)
                .await?;
        }

        Ok(())
    }

    pub fn start_downsample_task(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = time::interval(std::time::Duration::from_millis(250));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            if let Err(error) = self.bootstrap_downsamples(None).await {
                tracing::warn!(error = %error, "Failed to bootstrap downsample collections");
            }

            let Some(bus) = self.event_bus().cloned() else {
                loop {
                    ticker.tick().await;
                    if let Err(error) = self.bootstrap_downsamples(None).await {
                        tracing::warn!(error = %error, "Failed to refresh downsample collections");
                    }
                }
            };

            let mut subscription = bus.subscribe(&[EventName::Update]);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(error) = self.bootstrap_downsamples(None).await {
                            tracing::warn!(error = %error, "Failed to refresh downsample collections");
                        }
                    }
                    message = subscription.recv() => {
                        let Some(message) = message else {
                            continue;
                        };

                        if subscription.check_and_reset_dropped() > 0 {
                            tracing::warn!(
                                "Downsample event subscription dropped messages; rebuilding all downsample collections"
                            );
                            if let Err(error) = self.bootstrap_downsamples(None).await {
                                tracing::warn!(
                                    error = %error,
                                    "Failed to rebuild downsample collections after dropped events"
                                );
                            }
                        }

                        let Some(update) = message.as_update() else {
                            continue;
                        };

                        if let Err(error) = self
                            .process_downsample_update(&update.collection_id, &update.doc_id)
                            .await
                        {
                            tracing::warn!(
                                collection_id = %update.collection_id,
                                doc_id = %update.doc_id,
                                error = %error,
                                "Failed to process downsample update"
                            );
                        }
                    }
                }
            }
        })
    }
}
