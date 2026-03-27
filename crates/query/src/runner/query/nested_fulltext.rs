//! Full-text search utilities for nested queries.
//!
//! Handles BM25 score precomputation, scoped relation scoring,
//! and sorting/top-k selection for nested relation FTS fields.

use bm25::{Document as Bm25Document, Language, SearchEngineBuilder};
use schema::{CollectionVersion, FieldDescription};
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::error::{QueryError, Result};
use crate::mapper::{FullTextSearch, Requestable, Select};
use crate::plan::{compare_json_values, resolve_nested_field};
use crate::planner::Planner;
use crate::txn::TransactionRegistry;

use super::super::super::fetcher::DocFetcher;
use super::super::QueryRunner;
use super::nested_profile::ScopedFulltextProfile;

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    pub(crate) async fn precompute_fulltext_scores(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        collections_map: &HashMap<String, Arc<CollectionVersion>>,
    ) -> Result<HashMap<String, HashMap<String, f64>>> {
        let root_collection = collections_map
            .get(&select.collection_name)
            .cloned()
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        let mut fts_scores: HashMap<String, HashMap<String, f64>> = HashMap::new();
        let mut worklist: Vec<(&Select, Arc<CollectionVersion>, Vec<String>)> = vec![(
            select,
            root_collection,
            vec![select.field.output_name().to_string()],
        )];

        while let Some((current_select, current_collection, scope_path)) = worklist.pop() {
            for field in &current_select.fields {
                match field {
                    Requestable::FullTextSearch(fts) => {
                        if Self::should_defer_relation_scoped_fulltext(
                            current_select,
                            current_collection.as_ref(),
                            fts,
                            scope_path.len(),
                        ) {
                            continue;
                        }

                        let mut combined_scores: HashMap<String, f64> = HashMap::new();
                        for target_field in &fts.target_fields {
                            if let Ok(scores) = self
                                .compute_fulltext_path_scores(
                                    current_collection.clone(),
                                    target_field,
                                    &fts.query,
                                    fetcher,
                                    collections_map,
                                )
                                .await
                            {
                                for (doc_id, score) in scores {
                                    *combined_scores.entry(doc_id).or_insert(0.0) += score;
                                }
                            }
                        }

                        let score_key = Planner::fts_score_key(&scope_path, fts.output_name());
                        fts_scores.insert(score_key, combined_scores);
                    }
                    Requestable::Select(nested_select) => {
                        if nested_select.field.name == "GROUP" {
                            continue;
                        }

                        let Some(relation_field) =
                            current_collection.field_by_name(&nested_select.field.name)
                        else {
                            continue;
                        };
                        if !relation_field.kind.is_relation() {
                            continue;
                        }

                        let target_collection = Self::resolve_relation_target_collection(
                            &current_collection,
                            relation_field,
                            collections_map,
                        )
                        .ok_or_else(|| {
                            QueryError::execution(format!(
                                "Unable to resolve BM25 relation target '{}.{}'",
                                current_collection.name, nested_select.field.name
                            ))
                        })?;

                        let mut child_scope_path = scope_path.clone();
                        child_scope_path.push(nested_select.field.output_name().to_string());
                        worklist.push((nested_select, target_collection, child_scope_path));
                    }
                    _ => {}
                }
            }
        }

        Ok(fts_scores)
    }

    pub(crate) async fn compute_fulltext_path_scores(
        &self,
        root_collection: Arc<CollectionVersion>,
        path: &str,
        query: &str,
        fetcher: &dyn DocFetcher,
        collections_map: &HashMap<String, Arc<CollectionVersion>>,
    ) -> Result<HashMap<String, f64>> {
        let path_segments: Vec<&str> = path
            .split('.')
            .filter(|segment| !segment.is_empty())
            .collect();
        if path_segments.is_empty() {
            return Err(QueryError::execution(
                "BM25 field path must not be empty".to_string(),
            ));
        }

        let mut current_collection = root_collection;
        let mut relation_chain: Vec<(
            Arc<CollectionVersion>,
            FieldDescription,
            Arc<CollectionVersion>,
        )> = Vec::new();

        for relation_name in &path_segments[..path_segments.len() - 1] {
            let relation_field = current_collection
                .field_by_name(relation_name)
                .ok_or_else(|| QueryError::unknown_field(*relation_name))?;

            if !relation_field.kind.is_relation() {
                return Err(QueryError::execution(format!(
                    "BM25 relation path segment '{}' on collection '{}' is not a relation",
                    relation_name, current_collection.name
                )));
            }

            let target_collection = Self::resolve_relation_target_collection(
                &current_collection,
                relation_field,
                collections_map,
            )
            .ok_or_else(|| {
                QueryError::execution(format!(
                    "Unable to resolve BM25 relation target '{}.{}'",
                    current_collection.name, relation_name
                ))
            })?;

            relation_chain.push((
                current_collection.clone(),
                relation_field.clone(),
                target_collection.clone(),
            ));
            current_collection = target_collection;
        }

        let leaf_field = path_segments[path_segments.len() - 1];
        let mut scores = fetcher
            .search_fulltext_scored(&current_collection.name, leaf_field, query)
            .await?;

        for (parent_collection, relation_field, target_collection) in
            relation_chain.into_iter().rev()
        {
            scores = self
                .lift_fulltext_scores_to_parent(
                    &parent_collection,
                    &relation_field,
                    &target_collection,
                    scores,
                    fetcher,
                )
                .await?;
        }

        Ok(scores)
    }

    pub(crate) fn should_defer_relation_scoped_fulltext(
        select: &Select,
        collection: &CollectionVersion,
        fts: &FullTextSearch,
        scope_depth: usize,
    ) -> bool {
        scope_depth > 1
            && select.group_by.is_none()
            && select
                .filter
                .as_ref()
                .map(|filter| !filter.has_alias_filter())
                .unwrap_or(true)
            && fts
                .target_fields
                .iter()
                .all(|field| !field.contains('.') && collection.field_by_name(field).is_some())
    }

    pub(crate) fn resolve_relation_target_collection(
        current_collection: &Arc<CollectionVersion>,
        relation_field: &FieldDescription,
        collections_map: &HashMap<String, Arc<CollectionVersion>>,
    ) -> Option<Arc<CollectionVersion>> {
        let target_collection_id = relation_field.kind.relation_collection_id()?;

        if target_collection_id.is_empty() {
            return Some(current_collection.clone());
        }

        collections_map
            .get(target_collection_id)
            .cloned()
            .or_else(|| {
                collections_map.values().find_map(|collection| {
                    (collection.collection_id == target_collection_id
                        || collection.version_id == target_collection_id)
                        .then(|| collection.clone())
                })
            })
            .or_else(|| {
                let relation_name = relation_field.relation_name.as_deref()?;
                collections_map.values().find_map(|collection| {
                    if collection.name == current_collection.name {
                        return None;
                    }
                    collection
                        .fields
                        .iter()
                        .any(|field| field.relation_name.as_deref() == Some(relation_name))
                        .then(|| collection.clone())
                })
            })
    }

    async fn lift_fulltext_scores_to_parent(
        &self,
        parent_collection: &Arc<CollectionVersion>,
        relation_field: &FieldDescription,
        target_collection: &Arc<CollectionVersion>,
        child_scores: HashMap<String, f64>,
        fetcher: &dyn DocFetcher,
    ) -> Result<HashMap<String, f64>> {
        if child_scores.is_empty() {
            return Ok(HashMap::new());
        }

        // Primary non-array relations hold the FK on the parent document itself.
        if !relation_field.kind.is_array() && relation_field.is_primary {
            let fk_field_name = CollectionVersion::relation_id_field_name(&relation_field.name);
            let parent_docs = fetcher.get_all(&parent_collection.name).await?;
            let mut parent_scores: HashMap<String, f64> = HashMap::new();

            for doc in parent_docs {
                let Some(parent_id) = doc.id().map(|id| id.to_string()) else {
                    continue;
                };
                let Some(target_id) = doc.get(&fk_field_name).and_then(|value| value.as_str())
                else {
                    continue;
                };
                let Some(score) = child_scores.get(target_id) else {
                    continue;
                };
                *parent_scores.entry(parent_id).or_insert(0.0) += *score;
            }

            return Ok(parent_scores);
        }

        let target_relation_field = relation_field
            .relation_name
            .as_ref()
            .and_then(|relation_name| {
                target_collection.field_by_relation(
                    relation_name,
                    &parent_collection.name,
                    &relation_field.name,
                )
            })
            .ok_or_else(|| {
                QueryError::execution(format!(
                    "Unable to resolve reverse BM25 relation '{}.{}'",
                    parent_collection.name, relation_field.name
                ))
            })?;

        if target_relation_field.kind.is_array() || !target_relation_field.is_primary {
            return Err(QueryError::execution(format!(
                "BM25 relation path '{}.{}' does not resolve to a primary foreign key holder",
                parent_collection.name, relation_field.name
            )));
        }

        let fk_field_name = CollectionVersion::relation_id_field_name(&target_relation_field.name);
        let child_doc_ids: Vec<String> = child_scores.keys().cloned().collect();
        let child_docs = fetcher
            .get_by_ids(&target_collection.name, &child_doc_ids)
            .await?
            .into_docs();

        let mut parent_scores: HashMap<String, f64> = HashMap::new();
        for doc in child_docs {
            let Some(child_id) = doc.id().map(|id| id.to_string()) else {
                continue;
            };
            let Some(parent_id) = doc.get(&fk_field_name).and_then(|value| value.as_str()) else {
                continue;
            };
            let Some(score) = child_scores.get(&child_id) else {
                continue;
            };
            *parent_scores.entry(parent_id.to_string()).or_insert(0.0) += *score;
        }

        Ok(parent_scores)
    }

    /// Apply scoped BM25 scoring to already-joined nested relation data.
    ///
    /// This is intentionally a post-processing step over rendered nested relation objects,
    /// limited to relation-local BM25 fields. It avoids the full-corpus precompute path for
    /// high-cardinality child collections while preserving the existing planner path for
    /// top-level and dotted relation BM25 fields.
    #[cfg(test)]
    pub(crate) fn apply_scoped_relation_fulltext(
        results: Vec<JsonValue>,
        select: &Select,
    ) -> Vec<JsonValue> {
        let mut profile = ScopedFulltextProfile::default();
        Self::apply_scoped_relation_fulltext_with_profile(results, select, &mut profile)
    }

    pub(crate) fn apply_scoped_relation_fulltext_with_profile(
        mut results: Vec<JsonValue>,
        select: &Select,
        profile: &mut ScopedFulltextProfile,
    ) -> Vec<JsonValue> {
        for result in &mut results {
            Self::apply_scoped_relation_fulltext_to_value(result, select, profile);
        }

        results
    }

    fn apply_scoped_relation_fulltext_to_value(
        value: &mut JsonValue,
        select: &Select,
        profile: &mut ScopedFulltextProfile,
    ) {
        let JsonValue::Object(obj) = value else {
            return;
        };

        for requestable in &select.fields {
            let Requestable::Select(nested_select) = requestable else {
                continue;
            };
            if nested_select.field.name == "GROUP" {
                continue;
            }

            if let Some(relation_value) = obj.get_mut(nested_select.field.output_name()) {
                Self::apply_scoped_relation_fulltext_to_relation(
                    relation_value,
                    nested_select,
                    profile,
                );
            }
        }
    }

    fn apply_scoped_relation_fulltext_to_relation(
        value: &mut JsonValue,
        select: &Select,
        profile: &mut ScopedFulltextProfile,
    ) {
        match value {
            JsonValue::Array(items) => {
                if !Self::apply_scoped_relation_fulltext_top_k(items, select, profile) {
                    Self::score_scoped_relation_items(items, select, profile);
                }
                for item in items.iter_mut() {
                    Self::apply_scoped_relation_fulltext_to_value(item, select, profile);
                }
            }
            JsonValue::Object(_) => {
                Self::score_scoped_relation_items(std::slice::from_mut(value), select, profile);
                Self::apply_scoped_relation_fulltext_to_value(value, select, profile);
            }
            _ => {}
        }
    }

    fn apply_scoped_relation_fulltext_top_k(
        items: &mut Vec<JsonValue>,
        select: &Select,
        profile: &mut ScopedFulltextProfile,
    ) -> bool {
        if items.is_empty() || select.group_by.is_some() {
            return false;
        }
        if select
            .filter
            .as_ref()
            .map(|filter| filter.has_alias_filter())
            .unwrap_or(false)
        {
            return false;
        }

        let scoped_fulltext = Self::collect_scoped_relation_fulltext(select);
        let Some((order_field, keep_count)) =
            Self::scoped_relation_fulltext_top_k(select, &scoped_fulltext, items.len())
        else {
            return false;
        };

        let Some(ranked_fts) = scoped_fulltext
            .iter()
            .find(|fts| fts.output_name() == order_field)
        else {
            return false;
        };

        let top_k_start = Instant::now();
        let scores =
            Self::compute_scoped_fulltext_scores(items, ranked_fts, profile, Some(keep_count));
        Self::retain_relation_top_k_by_score(items, order_field.as_str(), &scores, keep_count);

        for fts in scoped_fulltext {
            if fts.output_name() == order_field {
                continue;
            }

            let scores = Self::compute_scoped_fulltext_scores(items, fts, profile, None);
            Self::inject_scoped_fulltext_scores(items, fts.output_name(), &scores);
        }

        profile.top_k_calls += 1;
        profile.top_k_elapsed += top_k_start.elapsed();
        true
    }

    fn collect_scoped_relation_fulltext(select: &Select) -> Vec<&FullTextSearch> {
        select
            .fields
            .iter()
            .filter_map(|requestable| match requestable {
                Requestable::FullTextSearch(fts)
                    if fts.target_fields.iter().all(|field| !field.contains('.')) =>
                {
                    Some(fts)
                }
                _ => None,
            })
            .collect()
    }

    fn scoped_relation_fulltext_top_k(
        select: &Select,
        scoped_fulltext: &[&FullTextSearch],
        item_count: usize,
    ) -> Option<(String, usize)> {
        let limit = select.limit.as_ref()?;
        let limit_count = limit.limit? as usize;
        if limit_count == 0 {
            return None;
        }

        let keep_count = limit_count + limit.offset as usize;
        if keep_count == 0 || keep_count >= item_count {
            return None;
        }

        let order_by = select.order_by.as_ref()?;
        if order_by.conditions.len() != 1 {
            return None;
        }

        let condition = order_by.conditions.first()?;
        if condition.direction != crate::mapper::OrderDirection::Desc || condition.fields.len() != 1
        {
            return None;
        }

        let order_field = condition.fields.first()?;
        scoped_fulltext
            .iter()
            .find(|fts| fts.output_name() == order_field)
            .map(|_| (order_field.clone(), keep_count))
    }

    fn retain_relation_top_k_by_score(
        items: &mut Vec<JsonValue>,
        output_name: &str,
        scores: &[f64],
        keep_count: usize,
    ) {
        if keep_count == 0 || items.len() <= keep_count {
            Self::inject_scoped_fulltext_scores(items, output_name, scores);
            return;
        }

        let mut selected: Vec<(usize, JsonValue)> = Vec::with_capacity(keep_count);

        for (original_index, mut item) in std::mem::take(items).into_iter().enumerate() {
            let score = scores.get(original_index).copied().unwrap_or(0.0);
            Self::inject_scoped_fulltext_score(&mut item, output_name, score);

            if selected.len() < keep_count {
                selected.push((original_index, item));
                continue;
            }

            let worst_index = selected
                .iter()
                .enumerate()
                .max_by(|(_, (index_a, item_a)), (_, (index_b, item_b))| {
                    Self::compare_top_k_scored_items(
                        item_a,
                        *index_a,
                        item_b,
                        *index_b,
                        output_name,
                    )
                })
                .map(|(index, _)| index)
                .expect("selected is non-empty when searching for the worst top-k candidate");

            if Self::compare_top_k_scored_items(
                &item,
                original_index,
                &selected[worst_index].1,
                selected[worst_index].0,
                output_name,
            ) == Ordering::Less
            {
                selected[worst_index] = (original_index, item);
            }
        }

        selected.sort_by(|(index_a, item_a), (index_b, item_b)| {
            Self::compare_top_k_scored_items(item_a, *index_a, item_b, *index_b, output_name)
        });

        *items = selected.into_iter().map(|(_, item)| item).collect();
    }

    fn compare_top_k_scored_items(
        item_a: &JsonValue,
        index_a: usize,
        item_b: &JsonValue,
        index_b: usize,
        output_name: &str,
    ) -> Ordering {
        let value_a = item_a.as_object().and_then(|obj| obj.get(output_name));
        let value_b = item_b.as_object().and_then(|obj| obj.get(output_name));
        let cmp = compare_json_values(value_a, value_b).reverse();
        if cmp == Ordering::Equal {
            index_a.cmp(&index_b)
        } else {
            cmp
        }
    }

    pub(crate) fn score_scoped_relation_items(
        items: &mut [JsonValue],
        select: &Select,
        profile: &mut ScopedFulltextProfile,
    ) {
        if items.is_empty() || select.group_by.is_some() {
            return;
        }
        if select
            .filter
            .as_ref()
            .map(|filter| filter.has_alias_filter())
            .unwrap_or(false)
        {
            return;
        }

        let scoped_fulltext = Self::collect_scoped_relation_fulltext(select);

        if scoped_fulltext.is_empty() {
            return;
        }

        for fts in &scoped_fulltext {
            let scores = Self::compute_scoped_fulltext_scores(items, fts, profile, None);
            Self::inject_scoped_fulltext_scores(items, fts.output_name(), &scores);
        }

        if let Some(order_by) = &select.order_by {
            if scoped_fulltext.iter().any(|fts| {
                order_by.conditions.iter().any(|condition| {
                    condition
                        .fields
                        .first()
                        .map(|field| field == fts.output_name())
                        .unwrap_or(false)
                })
            }) {
                let sort_start = Instant::now();
                Self::sort_relation_items(items, order_by);
                profile.sort_calls += 1;
                profile.sort_elapsed += sort_start.elapsed();
            }
        }
    }

    pub(crate) fn compute_scoped_fulltext_scores(
        items: &[JsonValue],
        fts: &FullTextSearch,
        profile: &mut ScopedFulltextProfile,
        limit: Option<usize>,
    ) -> Vec<f64> {
        if fts.query.trim().is_empty() {
            return vec![0.0; items.len()];
        }

        let scoring_start = Instant::now();
        let mut combined_scores = vec![0.0; items.len()];
        profile.scoring_calls += 1;
        profile.items_seen += items.len();
        profile.target_fields_seen += fts.target_fields.len();
        let doc_positions = items
            .iter()
            .enumerate()
            .filter_map(|(item_index, item)| {
                let obj = item.as_object()?;
                let doc_id = obj.get("_docID")?.as_str()?;
                Some((doc_id, item_index))
            })
            .collect::<HashMap<_, _>>();

        for target_field in &fts.target_fields {
            let documents: Vec<Bm25Document<String>> = items
                .iter()
                .filter_map(|item| {
                    let obj = item.as_object()?;
                    let doc_id = obj.get("_docID")?.as_str()?.to_string();
                    let contents = obj.get(target_field)?.as_str()?;
                    if contents.trim().is_empty() {
                        return None;
                    }

                    Some(Bm25Document::new(doc_id, contents))
                })
                .collect();

            if documents.is_empty() {
                continue;
            }

            profile.docs_indexed += documents.len();
            let search_engine =
                SearchEngineBuilder::<String>::with_documents(Language::English, documents).build();
            for result in search_engine.search(&fts.query, limit) {
                if let Some(item_index) = doc_positions.get(result.document.id.as_str()).copied() {
                    combined_scores[item_index] += result.score as f64;
                }
            }
        }

        profile.scoring_elapsed += scoring_start.elapsed();
        combined_scores
    }

    fn inject_scoped_fulltext_scores(items: &mut [JsonValue], output_name: &str, scores: &[f64]) {
        for (index, item) in items.iter_mut().enumerate() {
            let score = scores.get(index).copied().unwrap_or(0.0);
            Self::inject_scoped_fulltext_score(item, output_name, score);
        }
    }

    fn inject_scoped_fulltext_score(item: &mut JsonValue, output_name: &str, score: f64) {
        let JsonValue::Object(obj) = item else {
            return;
        };

        let json_score = serde_json::Number::from_f64(score)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null);
        obj.insert(output_name.to_string(), json_score);
    }

    pub(crate) fn sort_relation_items(items: &mut [JsonValue], order_by: &crate::mapper::OrderBy) {
        items.sort_by(|a, b| {
            for condition in &order_by.conditions {
                let value_a = resolve_nested_field(Some(a), &condition.fields);
                let value_b = resolve_nested_field(Some(b), &condition.fields);
                let cmp = compare_json_values(value_a.as_ref(), value_b.as_ref());
                let cmp = match condition.direction {
                    crate::mapper::OrderDirection::Asc => cmp,
                    crate::mapper::OrderDirection::Desc => cmp.reverse(),
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }

            Ordering::Equal
        });
    }
}
