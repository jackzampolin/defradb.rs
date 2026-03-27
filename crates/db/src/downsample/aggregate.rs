//! Source sample building and window aggregation for downsample execution.

use super::parse::*;
use super::types::*;
use crate::error::{Error, Result};
use chrono::{DateTime, FixedOffset};
use document::Document;
use std::collections::{BTreeMap, HashSet};
use storage::corekv::Store;

impl<S: Store + 'static> crate::database::DB<S> {
    pub(super) fn build_raw_source_samples(
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

    pub(super) fn build_rollup_source_samples(
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

    pub(super) fn build_source_samples(
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

    pub(super) fn aggregate_samples_into_windows(
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
}
