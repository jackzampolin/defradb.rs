use super::parse::*;
use super::types::*;
use crate::error::{Error, Result};
use schema::{CollectionVersion, ScalarKind};
use std::collections::{HashMap, HashSet};
use storage::corekv::Store;

impl<S: Store> crate::database::DB<S> {
    pub(super) fn downsample_depth(
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

    pub(super) fn build_downsample_plan(
        &self,
        target: &CollectionVersion,
    ) -> Result<DownsamplePlan> {
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
        let retention_nanos = target
            .downsample_retention
            .as_deref()
            .map(parse_positive_retention_nanos)
            .transpose()
            .map_err(Error::Other)?;
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
            retention_nanos,
            passthrough_fields,
            aggregate_fields,
            source_kind,
        })
    }

    pub(super) fn downsample_plans(
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

    pub fn local_downsample_targets_for_source(
        &self,
        source_collection_name: &str,
    ) -> Result<Vec<String>> {
        let mut targets = self
            .downsample_plans(None, Some(source_collection_name))?
            .into_iter()
            .map(|plan| plan.target.name)
            .collect::<Vec<_>>();
        targets.sort();
        targets.dedup();
        Ok(targets)
    }

    pub fn replicated_downsample_source_skip_reason(
        &self,
        source_collection: &CollectionVersion,
    ) -> Result<Option<String>> {
        let targets = self.local_downsample_targets_for_source(&source_collection.name)?;
        if targets.is_empty() {
            return Ok(None);
        }

        Ok(Some(format!(
            "replicated writes into downsample source '{}' are not supported; downsample source collections are local-only (targets: {})",
            source_collection.name,
            targets.join(", ")
        )))
    }
}
