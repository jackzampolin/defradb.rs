//! Index scan implementation for LensedDocFetcher.

use std::collections::HashSet;

use defra_core::thread_bounds::MaybeBoxFuture;
use query::planner::index_selection::{IndexScanParams, IndexScanType};
use storage::corekv::Store;
use storage::index::IndexIterator;

use crate::collection::loader::get_collection_with_lazy_load;
use crate::index::IndexManager;
use crate::read::seek::apply_cursor_seek_to_iterator;

use super::LensedDocFetcher;

impl<S: Store + 'static> LensedDocFetcher<S> {
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
        let (collection, datastore, systemstore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        let short_id = collection.resolved_root_id();
        let index_manager =
            IndexManager::from_indexes(short_id, collection.schema(), collection.write_indexes())
                .map_err(|e| {
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
        ) -> Result<(Vec<u64>, u64), query::error::QueryError> {
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

                entries.push(entry.doc_short_id);

                if let Some(lim) = limit {
                    if entries.len() >= lim as usize {
                        break;
                    }
                }
            }

            Ok((entries, total_iterated))
        }

        let vf = params.value_filter.as_ref();
        let (raw_doc_short_ids, raw_fetches, group_lens): (Vec<u64>, u64, Vec<usize>) =
            match &params.scan_type {
                IndexScanType::ExactMatch { values } => {
                    // Cursor seek is intentionally not applied: ExactMatch fetches a single
                    // value; pagination over a single value is meaningless.
                    let mut iter = index.get(&datastore, values).await.map_err(|e| {
                        query::error::QueryError::execution(format!("index error: {}", e))
                    })?;
                    // Full equal-key set: public-DocID tie-break sorts before offset/limit (#1602).
                    let (ids, n) = collect_with_limit(&mut iter, None, 0, vf).await?;
                    (ids, n, Vec::new())
                }
                IndexScanType::InScan {
                    values,
                    suffix_values,
                } => {
                    // Cursor seek is intentionally not applied: InScan fetches a fixed set
                    // of values; pagination over an unordered set is meaningless.
                    let is_composite = index.description().fields.len() > 1;
                    let has_full_key = !suffix_values.is_empty()
                        && suffix_values.len() == index.description().fields.len() - 1;
                    let mut all_doc_short_ids = Vec::new();
                    let mut group_lens = Vec::new();
                    let mut seen_short_ids = HashSet::new();
                    let mut raw_count = 0u64;
                    for value in values {
                        let entries = if has_full_key {
                            let mut key_values = vec![value.clone()];
                            key_values.extend(suffix_values.iter().cloned());
                            let mut iter =
                                index.get(&datastore, &key_values).await.map_err(|e| {
                                    query::error::QueryError::execution(format!(
                                        "index error: {}",
                                        e
                                    ))
                                })?;
                            iter.collect_all().await.map_err(|e| {
                                query::error::QueryError::execution(format!(
                                    "index iteration error: {}",
                                    e
                                ))
                            })?
                        } else if is_composite {
                            let mut iter = index
                                .scan_prefix(&datastore, std::slice::from_ref(value), false)
                                .await
                                .map_err(|e| {
                                    query::error::QueryError::execution(format!(
                                        "index error: {}",
                                        e
                                    ))
                                })?;
                            iter.collect_all().await.map_err(|e| {
                                query::error::QueryError::execution(format!(
                                    "index iteration error: {}",
                                    e
                                ))
                            })?
                        } else {
                            let mut iter = index
                                .get(&datastore, std::slice::from_ref(value))
                                .await
                                .map_err(|e| {
                                    query::error::QueryError::execution(format!(
                                        "index error: {}",
                                        e
                                    ))
                                })?;
                            iter.collect_all().await.map_err(|e| {
                                query::error::QueryError::execution(format!(
                                    "index iteration error: {}",
                                    e
                                ))
                            })?
                        };
                        raw_count += entries.len() as u64;
                        crate::read::index_tiebreak::extend_equal_key_group(
                            &mut all_doc_short_ids,
                            &mut group_lens,
                            &mut seen_short_ids,
                            entries.into_iter().map(|e| e.doc_short_id),
                        );
                    }
                    (all_doc_short_ids, raw_count, group_lens)
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
                    apply_cursor_seek_to_iterator(
                        &mut iter,
                        &params.cursor_seek,
                        &systemstore,
                        short_id,
                    )
                    .await?;
                    let (ids, n) = collect_with_limit(&mut iter, limit, offset, vf).await?;
                    (ids, n, Vec::new())
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
                    apply_cursor_seek_to_iterator(
                        &mut iter,
                        &params.cursor_seek,
                        &systemstore,
                        short_id,
                    )
                    .await?;
                    let (ids, n) = collect_with_limit(&mut iter, limit, offset, vf).await?;
                    (ids, n, Vec::new())
                }
                IndexScanType::OrScan { branches } => {
                    // Cursor seek is intentionally not applied: each branch gets cursor_seek: None
                    // because OrScan merges independent sets; applying a cursor to individual
                    // branches would arbitrarily exclude results from other branches.
                    let mut all_doc_ids = Vec::new();
                    let mut total_raw_fetches = 0u64;
                    for branch in branches {
                        let branch_params = IndexScanParams {
                            index_name: params.index_name.clone(),
                            scan_type: branch.clone(),
                            limit: None,
                            offset: 0,
                            value_filter: None,
                            cursor_seek: None,
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

        let mut seen = HashSet::new();
        let doc_short_ids: Vec<u64> = raw_doc_short_ids
            .into_iter()
            .filter(|id| seen.insert(*id))
            .collect();

        let mut doc_ids = crate::docid::map::resolve_doc_ids(&systemstore, &doc_short_ids)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("doc ID resolution error: {}", e))
            })?;
        crate::read::index_tiebreak::apply_equal_key_doc_id_tie_break(
            &params.scan_type,
            &mut doc_ids,
            offset,
            limit,
            &group_lens,
        );

        Ok(query::fetcher::IndexScanResult::with_raw_count(
            doc_ids,
            raw_fetches,
        ))
    }
}
