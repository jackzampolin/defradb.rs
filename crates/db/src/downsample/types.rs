use chrono::{DateTime, FixedOffset};
use schema::CollectionVersion;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AggregateField {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone)]
pub(super) enum SourceKind {
    Raw { measure_field: String },
    Downsample,
}

#[derive(Debug, Clone)]
pub(super) struct ParsedSourceQuery {
    pub collection_name: String,
    pub selected_fields: HashSet<String>,
}

#[derive(Debug, Clone)]
pub(super) struct DownsamplePlan {
    pub target: CollectionVersion,
    pub source: CollectionVersion,
    pub interval_raw: String,
    pub interval_nanos: i64,
    pub time_field: String,
    pub retention_nanos: Option<i64>,
    pub passthrough_fields: Vec<String>,
    pub aggregate_fields: Vec<AggregateField>,
    pub source_kind: SourceKind,
}

/// Options for explicit/manual downsample history garbage collection.
#[derive(Debug, Clone, Default)]
pub struct GcDownsampleHistoriesOptions {
    /// Only apply retention policies for these downsample targets (None = all).
    pub names: Option<Vec<String>>,
}

impl GcDownsampleHistoriesOptions {
    pub fn all() -> Self {
        Self { names: None }
    }

    pub fn with_names(names: Vec<String>) -> Self {
        Self { names: Some(names) }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum NumericValue {
    Int(i64),
    Float(f64),
}

impl NumericValue {
    pub fn as_f64(self) -> f64 {
        match self {
            Self::Int(value) => value as f64,
            Self::Float(value) => value,
        }
    }

    pub fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Self::Int(left + right),
            (left, right) => Self::Float(left.as_f64() + right.as_f64()),
        }
    }

    pub fn min(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Self::Int(left.min(right)),
            (left, right) => Self::Float(left.as_f64().min(right.as_f64())),
        }
    }

    pub fn max(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int(left), Self::Int(right)) => Self::Int(left.max(right)),
            (left, right) => Self::Float(left.as_f64().max(right.as_f64())),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct SourceSample {
    pub height: u64,
    pub time: DateTime<FixedOffset>,
    pub coverage_end: Option<DateTime<FixedOffset>>,
    pub count: i64,
    pub sum: Option<NumericValue>,
    pub min: Option<NumericValue>,
    pub max: Option<NumericValue>,
}

#[derive(Debug, Clone)]
pub(super) struct PendingWindowAggregate {
    pub count: i64,
    pub sum: Option<NumericValue>,
    pub min: Option<NumericValue>,
    pub max: Option<NumericValue>,
    pub source_height: u64,
    pub max_coverage_end_nanos: Option<i64>,
    pub window_end_nanos: i64,
}

#[derive(Debug, Clone)]
pub(super) struct WindowAggregate {
    pub count: i64,
    pub sum: Option<NumericValue>,
    pub avg: Option<f64>,
    pub min: Option<NumericValue>,
    pub max: Option<NumericValue>,
    pub window_start: DateTime<FixedOffset>,
    pub window_end: DateTime<FixedOffset>,
    pub source_height: i64,
}
