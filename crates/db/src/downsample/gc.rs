use super::parse::*;
use super::types::*;
use crate::error::{Error, Result};
use cid::Cid;
use defra_core::block::Block;
use document::DocID;
use query::fetcher::CommitsQueryOptions;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use std::collections::{HashMap, HashSet};
use std::str;
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use storage::keys::headstore::{HeadstoreDocKey, HeadstorePriorityKey};

impl<S: Store + 'static> crate::database::DB<S> {
    async fn downsample_gc_cutoff_nanos(
        self: &Arc<Self>,
        plan: &DownsamplePlan,
        series_doc_id: &str,
    ) -> Result<Option<i64>> {
        let Some(retention_nanos) = plan.retention_nanos else {
            return Ok(None);
        };

        let target_doc_id =
            DocID::new_v0_from_seed(&format!("{}:{}", plan.target.collection_id, series_doc_id));
        let fetcher = crate::auto_commit_mutator::AutoCommitMutator::new(self.clone());
        let Some(target_doc) = fetcher
            .get_for_update(&plan.target.name, &target_doc_id)
            .await
            .map_err(Error::Query)?
        else {
            return Ok(None);
        };

        let window_end = target_doc
            .get("window_end")
            .and_then(normal_value_to_time)
            .ok_or_else(|| {
                Error::Other(format!(
                    "downsample target '{}.window_end' is missing or invalid",
                    plan.target.name
                ))
            })?;

        Ok(Some(
            timestamp_nanos(&window_end)?.saturating_sub(retention_nanos),
        ))
    }

    fn pruneable_source_heights(
        &self,
        plan: &DownsamplePlan,
        commits: &[document::Document],
        cutoff_nanos: i64,
    ) -> Result<HashSet<u64>> {
        let samples = self.build_source_samples(plan, commits.to_vec())?;
        let mut heights = HashSet::new();
        for sample in samples {
            if source_sample_retention_time_nanos(&sample)? < cutoff_nanos {
                heights.insert(sample.height);
            }
        }
        Ok(heights)
    }

    async fn current_head_cids(&self, doc_id: &str) -> Result<HashSet<Cid>> {
        let txn = self.new_txn(true).await?;
        let headstore = txn.headstore()?;
        let mut iter = headstore
            .iterator(IterOptions::new().with_prefix(HeadstoreDocKey::document_prefix(doc_id)))
            .await
            .map_err(Error::Storage)?;

        let mut cids = HashSet::new();
        while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
            let Some(cid_str) = pair
                .key
                .split(|byte| *byte == b'/')
                .next_back()
                .and_then(|segment| str::from_utf8(segment).ok())
            else {
                continue;
            };
            let Ok(cid) = Cid::try_from(cid_str) else {
                continue;
            };
            cids.insert(cid);
        }
        iter.close().await.map_err(Error::Storage)?;

        drop(headstore);
        let _ = txn.discard();
        Ok(cids)
    }

    fn parse_priority_entry(doc_id: &str, key: &[u8]) -> Option<(u64, Cid)> {
        let doc_prefix_len = HeadstorePriorityKey::document_prefix(doc_id).len();
        let cid_offset = HeadstorePriorityKey::cid_offset(doc_id);
        let priority_hex = key
            .get(doc_prefix_len..doc_prefix_len + 16)
            .and_then(|bytes| str::from_utf8(bytes).ok())?;
        let priority = u64::from_str_radix(priority_hex, 16).ok()?;
        let cid = key
            .get(cid_offset..)
            .and_then(|bytes| Cid::try_from(bytes).ok())?;
        Some((priority, cid))
    }

    async fn prune_source_doc_history(
        &self,
        doc_id: &str,
        pruneable_heights: &HashSet<u64>,
    ) -> Result<()> {
        if pruneable_heights.is_empty() {
            return Ok(());
        }

        let current_head_cids = self.current_head_cids(doc_id).await?;
        let txn = self.new_txn(false).await?;
        let headstore = txn.headstore()?;
        let blockstore = txn.blockstore()?;

        let result: Result<()> = async {
            let mut iter = headstore
                .iterator(
                    IterOptions::new().with_prefix(HeadstorePriorityKey::document_prefix(doc_id)),
                )
                .await
                .map_err(Error::Storage)?;

            let mut keys_to_delete = Vec::new();
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                let Some((priority, cid)) = Self::parse_priority_entry(doc_id, &pair.key) else {
                    continue;
                };
                if !pruneable_heights.contains(&priority) || current_head_cids.contains(&cid) {
                    continue;
                }
                keys_to_delete.push((pair.key, cid));
            }
            iter.close().await.map_err(Error::Storage)?;

            let mut deleted_commits = HashSet::new();
            let mut deleted_signatures = HashSet::new();

            for (key, cid) in keys_to_delete {
                headstore.delete(&key).await.map_err(Error::Storage)?;

                if !deleted_commits.insert(cid) {
                    continue;
                }

                let block_key = cid.to_bytes();
                if let Some(bytes) = blockstore.get(&block_key).await.map_err(Error::Storage)? {
                    if let Ok(block) = Block::from_dag_cbor(&bytes) {
                        if let Some(signature_cid) = block.signature {
                            if deleted_signatures.insert(signature_cid) {
                                let _ = blockstore.delete(&signature_cid.to_bytes()).await;
                            }
                        }
                    }
                }
                let _ = blockstore.delete(&block_key).await;
            }

            Ok(())
        }
        .await;

        drop(headstore);
        drop(blockstore);

        match result {
            Ok(()) => {
                txn.commit().await?;
                Ok(())
            }
            Err(error) => {
                if let Err(discard_error) = txn.discard() {
                    tracing::warn!(
                        error = %discard_error,
                        original_error = %error,
                        "Transaction discard failed during downsample GC"
                    );
                }
                Err(error)
            }
        }
    }

    async fn gc_source_doc_for_plans(
        self: &Arc<Self>,
        plans: &[DownsamplePlan],
        source_doc: &document::Document,
    ) -> Result<()> {
        let Some(source_doc_id) = source_doc.id() else {
            return Ok(());
        };
        let source_doc_id = source_doc_id.to_string();
        let latest_source_priority = self.latest_doc_priority(&source_doc_id).await?;
        if latest_source_priority == 0 {
            return Ok(());
        }

        let commits = crate::LensedAutoCommitFetcher::new(self.clone())
            .get_commits(&CommitsQueryOptions {
                doc_id: Some(source_doc_id.clone()),
                height_start: Some(1),
                height_end: Some(latest_source_priority + 1),
                ..Default::default()
            })
            .await
            .map_err(Error::Query)?;
        if commits.is_empty() {
            return Ok(());
        }

        let series_doc_id = self.series_doc_id(source_doc)?;
        let mut effective_pruneable_heights: Option<HashSet<u64>> = None;

        for plan in plans {
            let Some(cutoff_nanos) = self
                .downsample_gc_cutoff_nanos(plan, &series_doc_id)
                .await?
            else {
                return Ok(());
            };
            let plan_pruneable_heights =
                self.pruneable_source_heights(plan, &commits, cutoff_nanos)?;
            effective_pruneable_heights = Some(match effective_pruneable_heights {
                Some(current) => current
                    .intersection(&plan_pruneable_heights)
                    .copied()
                    .collect(),
                None => plan_pruneable_heights,
            });
            if effective_pruneable_heights
                .as_ref()
                .is_some_and(HashSet::is_empty)
            {
                return Ok(());
            }
        }

        if let Some(pruneable_heights) = effective_pruneable_heights {
            self.prune_source_doc_history(&source_doc_id, &pruneable_heights)
                .await?;
        }

        Ok(())
    }

    pub async fn gc_downsample_histories(
        self: &Arc<Self>,
        options: Option<GcDownsampleHistoriesOptions>,
    ) -> Result<()> {
        let names_filter = options
            .as_ref()
            .and_then(|options| options.names.as_ref())
            .map(|names| names.iter().cloned().collect::<HashSet<_>>());

        let bootstrap_names = names_filter
            .as_ref()
            .map(|names| names.iter().cloned().collect::<Vec<_>>());
        self.bootstrap_downsamples(bootstrap_names.as_deref())
            .await?;

        let mut grouped_plans: HashMap<String, Vec<DownsamplePlan>> = HashMap::new();
        for plan in self.downsample_plans(None, None)? {
            grouped_plans
                .entry(plan.source.name.clone())
                .or_default()
                .push(plan);
        }

        for plans in grouped_plans.into_values() {
            if names_filter
                .as_ref()
                .is_some_and(|names| !plans.iter().any(|plan| names.contains(&plan.target.name)))
            {
                continue;
            }

            if plans.iter().any(|plan| plan.retention_nanos.is_none()) {
                tracing::debug!(
                    source = %plans[0].source.name,
                    "Skipping downsample GC because at least one local consumer has no retention policy"
                );
                continue;
            }

            let source = plans[0].source.clone();
            for source_doc in self.load_source_documents(&source).await? {
                self.gc_source_doc_for_plans(&plans, &source_doc).await?;
            }
        }

        Ok(())
    }
}
