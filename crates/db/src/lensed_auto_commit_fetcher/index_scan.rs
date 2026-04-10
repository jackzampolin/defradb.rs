//! Index scan implementation for LensedAutoCommitFetcher.

use std::collections::HashSet;

use defra_core::thread_bounds::MaybeBoxFuture;
use query::planner::index_selection::{IndexScanParams, IndexScanType};
use storage::corekv::Store;
use storage::index::IndexIterator;

use crate::index_manager::IndexManager;

use super::LensedAutoCommitFetcher;

impl<S: Store + 'static> LensedAutoCommitFetcher<S> {
    pub(super) fn get_by_index_scan_impl<'a>(
        &'a self,
        collection_name: &'a str,
        params: &'a IndexScanParams,
    ) -> MaybeBoxFuture<'a, query::error::Result<query::fetcher::IndexScanResult>> {
        Box::pin(self.get_by_index_scan_inner(collection_name, params))
    }

    async fn get_by_index_scan_inner(
        &self,
        collection_name: &str,
        params: &IndexScanParams,
    ) -> query::error::Result<query::fetcher::IndexScanResult> {
        let collection = self
            .db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))?;

        let txn = self.db.new_txn(true).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;

        let datastore = txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get datastore: {}", e))
        })?;

        let short_id = collection.resolved_root_id();
        let index_manager =
            IndexManager::from_collection(short_id, collection.schema()).map_err(|e| {
                query::error::QueryError::execution(format!(
                    "failed to create index manager for collection '{}': {}",
                    collection_name, e
                ))
            })?;

        let index = index_manager.get_index(&params.index_name).ok_or_else(|| {
            query::error::QueryError::execution(format!(
                "index '{}' not found on collection '{}'",
                params.index_name, collection_name
            ))
        })?;

        let limit = params.limit;
        let offset = params.offset;

        async fn collect_with_limit<I: IndexIterator>(
            iter: &mut I,
            limit: Option<u64>,
            offset: u64,
            value_filter: Option<&query::planner::index_selection::ScanValueFilter>,
        ) -> Result<(Vec<String>, u64), query::error::QueryError> {
            let mut entries = Vec::new();
            let mut skipped = 0u64;
            let mut total_iterated = 0u64;

            while let Some(entry) = iter.next().await.map_err(|e| {
                query::error::QueryError::execution(format!("index iteration error: {}", e))
            })? {
                total_iterated += 1;

                if let Some(filter) = value_filter {
                    if let Some(first_value) = entry.values.first() {
                        if !filter.matches_value(first_value) {
                            continue;
                        }
                    }
                }

                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                entries.push(entry.doc_id);

                if let Some(lim) = limit {
                    if entries.len() >= lim as usize {
                        break;
                    }
                }
            }

            Ok((entries, total_iterated))
        }

        let vf = params.value_filter.as_ref();
        let (raw_doc_ids, raw_fetches): (Vec<String>, u64) = match &params.scan_type {
            IndexScanType::ExactMatch { values } => {
                let mut iter = index.get(&datastore, values).await.map_err(|e| {
                    query::error::QueryError::execution(format!("index error: {}", e))
                })?;
                collect_with_limit(&mut iter, limit, offset, vf).await?
            }
            IndexScanType::InScan {
                values,
                suffix_values,
            } => {
                let is_composite = index.description().fields.len() > 1;
                let has_full_key = !suffix_values.is_empty()
                    && suffix_values.len() == index.description().fields.len() - 1;
                let mut all_doc_ids = Vec::new();
                for value in values {
                    if has_full_key {
                        let mut key_values = vec![value.clone()];
                        key_values.extend(suffix_values.iter().cloned());
                        let mut iter = index.get(&datastore, &key_values).await.map_err(|e| {
                            query::error::QueryError::execution(format!("index error: {}", e))
                        })?;
                        let entries = iter.collect_all().await.map_err(|e| {
                            query::error::QueryError::execution(format!(
                                "index iteration error: {}",
                                e
                            ))
                        })?;
                        all_doc_ids.extend(entries.into_iter().map(|e| e.doc_id));
                    } else if is_composite {
                        let mut iter = index
                            .scan_prefix(&datastore, std::slice::from_ref(value), false)
                            .await
                            .map_err(|e| {
                                query::error::QueryError::execution(format!("index error: {}", e))
                            })?;
                        let entries = iter.collect_all().await.map_err(|e| {
                            query::error::QueryError::execution(format!(
                                "index iteration error: {}",
                                e
                            ))
                        })?;
                        all_doc_ids.extend(entries.into_iter().map(|e| e.doc_id));
                    } else {
                        let mut iter = index
                            .get(&datastore, std::slice::from_ref(value))
                            .await
                            .map_err(|e| {
                                query::error::QueryError::execution(format!("index error: {}", e))
                            })?;
                        let entries = iter.collect_all().await.map_err(|e| {
                            query::error::QueryError::execution(format!(
                                "index iteration error: {}",
                                e
                            ))
                        })?;
                        all_doc_ids.extend(entries.into_iter().map(|e| e.doc_id));
                    }
                }
                let count = all_doc_ids.len() as u64;
                (all_doc_ids, count)
            }
            IndexScanType::PrefixScan {
                prefix_values,
                reverse,
            } => {
                let mut iter = index
                    .scan_prefix(&datastore, prefix_values, *reverse)
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!("index error: {}", e))
                    })?;
                collect_with_limit(&mut iter, limit, offset, vf).await?
            }
            IndexScanType::RangeScan {
                prefix_values,
                lower,
                upper,
                reverse,
            } => {
                let mut iter = index
                    .scan_range(
                        &datastore,
                        prefix_values,
                        lower.clone(),
                        upper.clone(),
                        *reverse,
                    )
                    .await
                    .map_err(|e| {
                        query::error::QueryError::execution(format!("index error: {}", e))
                    })?;
                collect_with_limit(&mut iter, limit, offset, vf).await?
            }
            IndexScanType::OrScan { branches } => {
                let _ = txn.discard();
                let mut all_doc_ids = Vec::new();
                let mut total_raw_fetches = 0u64;
                for branch in branches {
                    let branch_params = IndexScanParams {
                        index_name: params.index_name.clone(),
                        scan_type: branch.clone(),
                        limit: None,
                        offset: 0,
                        value_filter: None,
                    };
                    let branch_result = self
                        .get_by_index_scan_impl(collection_name, &branch_params)
                        .await?;
                    total_raw_fetches += branch_result.raw_fetches();
                    all_doc_ids.extend(branch_result.doc_ids().iter().cloned());
                }
                let mut seen = HashSet::new();
                let doc_ids: Vec<String> = all_doc_ids
                    .into_iter()
                    .filter(|id| seen.insert(id.clone()))
                    .collect();
                return Ok(query::fetcher::IndexScanResult::with_raw_count(
                    doc_ids,
                    total_raw_fetches,
                ));
            }
            _ => unreachable!(),
        };

        let _ = txn.discard();

        let mut seen = HashSet::new();
        let doc_ids: Vec<String> = raw_doc_ids
            .into_iter()
            .filter(|id| seen.insert(id.clone()))
            .collect();

        Ok(query::fetcher::IndexScanResult::with_raw_count(
            doc_ids,
            raw_fetches,
        ))
    }
}
