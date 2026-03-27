//! Downsample write validation.

use super::parse::*;
use super::types::*;
use crate::error::{Error, Result};
use datastore::NamespaceView;
use document::Document;
use schema::CollectionVersion;
use std::collections::HashSet;
use storage::corekv::Store;

impl<S: Store + 'static> crate::database::DB<S> {
    pub async fn validate_downsample_write(
        &self,
        datastore: &NamespaceView,
        source_collection: &CollectionVersion,
        source_doc: &Document,
        modified_fields: Option<&HashSet<String>>,
    ) -> Result<()> {
        let plans = self.downsample_plans(None, Some(&source_collection.name))?;
        if plans.is_empty() {
            return Ok(());
        }

        let series_doc_id = self.series_doc_id(source_doc)?;

        for plan in plans {
            if let SourceKind::Raw { measure_field } = &plan.source_kind {
                let time_changed =
                    modified_fields.is_some_and(|fields| fields.contains(&plan.time_field));
                let measure_changed =
                    modified_fields.is_some_and(|fields| fields.contains(measure_field));

                if modified_fields.is_some() && !time_changed && !measure_changed {
                    continue;
                }

                if time_changed != measure_changed {
                    return Err(Error::Other(format!(
                        "downsample source '{}': '{}' and '{}' must be updated together",
                        source_collection.name, plan.time_field, measure_field
                    )));
                }

                source_doc
                    .get(&plan.time_field)
                    .and_then(normal_value_to_time)
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "downsample source '{}.{}' must contain a valid RFC3339 timestamp",
                            source_collection.name, plan.time_field
                        ))
                    })?;
                source_doc
                    .get(measure_field)
                    .and_then(normal_value_to_numeric)
                    .ok_or_else(|| {
                        Error::Other(format!(
                            "downsample source '{}.{}' must contain a numeric value",
                            source_collection.name, measure_field
                        ))
                    })?;
            }

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
            let target_doc_id = document::DocID::new_v0_from_seed(&format!(
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
}
