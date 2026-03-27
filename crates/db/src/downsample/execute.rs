//! Downsample execution: per-document processing, persistence, and bootstrapping.

use super::parse::*;
use super::types::*;
use crate::error::{Error, Result};
use chrono::Utc;
use document::{DocID, Document, NormalValue};
use query::fetcher::CommitsQueryOptions;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use schema::CollectionVersion;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use storage::keys::headstore::HeadstoreDocKey;

impl<S: Store + 'static> crate::database::DB<S> {
    pub(super) async fn latest_doc_priority(&self, doc_id: &str) -> Result<u64> {
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

    pub(super) async fn load_source_documents(
        &self,
        collection: &CollectionVersion,
    ) -> Result<Vec<Document>> {
        let source_collection = self
            .get_collection(&collection.name)?
            .ok_or_else(|| Error::CollectionNotFound(collection.name.clone()))?;
        let txn = self.new_txn(true).await?;
        let datastore = txn.datastore()?;
        let result = source_collection.get_all_with_datastore(&datastore).await;
        let _ = txn.discard();
        result
    }

    pub(super) fn series_doc_id(&self, source_doc: &Document) -> Result<String> {
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
        let mutator = crate::auto_commit_mutator::AutoCommitMutator::new(self.clone());
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
        complete_through: chrono::DateTime<chrono::FixedOffset>,
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

        let mutator = crate::auto_commit_mutator::AutoCommitMutator::new(self.clone());
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

    pub(super) async fn process_downsample_update(
        self: &Arc<Self>,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<()> {
        let collection = self
            .find_collection_by_id(collection_id)?
            .ok_or_else(|| Error::CollectionNotFound(collection_id.to_string()))?;

        let plans = self.downsample_plans(None, Some(collection.name()))?;
        if plans.is_empty() {
            return Ok(());
        }

        let doc_id = DocID::from_string(doc_id)
            .map_err(|e| Error::Other(format!("invalid doc_id '{}': {}", doc_id, e)))?;
        let fetcher = crate::auto_commit_mutator::AutoCommitMutator::new(self.clone());
        let source_doc = match fetcher
            .get_for_update(collection.name(), &doc_id)
            .await
            .map_err(Error::Query)?
        {
            Some(doc) => doc,
            None => {
                tracing::debug!(
                    collection = %collection.name(),
                    doc_id = %doc_id,
                    "Source document not found for downsample update"
                );
                return Ok(());
            }
        };

        let now = Utc::now().fixed_offset();
        for plan in plans {
            self.process_source_doc_for_plan(&plan, &source_doc, now)
                .await?;
        }

        Ok(())
    }
}
