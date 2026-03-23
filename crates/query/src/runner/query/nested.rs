//! Planner orchestration and post-processing for nested queries.

use bm25::{Document as Bm25Document, Language, SearchEngineBuilder};
use identity::Did;
use schema::{CollectionVersion, FieldDescription};
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use crate::error::{QueryError, Result};
use crate::mapper::{FullTextSearch, Requestable, Select};
use crate::plan::{compare_json_values, resolve_nested_field};
use crate::planner::Planner;
use crate::txn::TransactionRegistry;
use std::collections::HashMap;

use super::super::fetcher::FetcherWrapper;
use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a query with nested selections using the Planner.
    ///
    /// The Planner builds a proper join plan with TypeJoinOne/TypeJoinMany nodes.
    /// ScanNodes fetch their own data via the attached fetcher.
    /// ACP permission filtering is applied per-collection via PermissionFilterNode in the plan.
    pub(crate) async fn execute_nested_select_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Create a fetcher wrapper that can be shared across plan nodes
        // We need to wrap the reference in an Arc-compatible struct
        let fetcher_arc = FetcherWrapper::new(fetcher);

        // Build the plan using the Planner with fetcher support
        // Get all collections from provider for join planning
        let collections_map = self.collections_map().await?;
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        // Pre-compute FTS scores from the inverted index before planning.
        // Supports dotted relation paths like `file.name` and `functions.content`
        // by querying the leaf collection's BM25 index and lifting scores back
        // onto the root collection through relation foreign keys.
        let fts_scores = self
            .precompute_fulltext_scores(select, fetcher, &collections_map)
            .await?;

        let mut planner = Planner::new(collections).with_fetcher(Arc::new(fetcher_arc));
        if !fts_scores.is_empty() {
            planner = planner.with_fts_scores(fts_scores);
        }
        if let Some(ref acp) = self.acp {
            planner = planner.with_acp(acp.clone(), identity);
        }
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        let mut plan = plan_result.plan;
        let ordering_only_fields = plan_result.ordering_only_fields;
        let aggregate_internal_keys = plan_result.aggregate_internal_keys;

        // Get the mapping from the plan
        let mapping = plan.document_map().clone();

        // Execute the plan and collect results
        plan.init().await?;
        plan.start().await?;

        let mut results = Vec::new();

        while plan.next().await? {
            let doc = plan.value();
            let mut json = self.doc_to_json(doc, &mapping)?;

            // Strip ordering-only fields from nested objects.
            // These fields were added for ORDER BY but shouldn't appear in output.
            for (relation_field, nested_field) in &ordering_only_fields {
                if let Some(obj) = json.as_object_mut() {
                    if let Some(relation_value) = obj.get_mut(relation_field) {
                        if let Some(nested_obj) = relation_value.as_object_mut() {
                            nested_obj.remove(nested_field);
                        }
                    }
                }
            }

            results.push(json);
        }

        plan.close().await?;

        // Post-process relation-based aggregates
        // For aggregates like _count(books: {}), compute the value from joined data
        let results =
            self.compute_relation_aggregates(results, select, &aggregate_internal_keys)?;

        // For nested relation-local BM25, score the already-joined relation scope instead of
        // precomputing against the full leaf collection. This preserves correct nested ordering
        // while avoiding a full-corpus BM25 pass for session-scoped child queries.
        let results = Self::apply_scoped_relation_fulltext(results, select);

        // Strip fields from relation data that were added for filter evaluation
        // but not explicitly requested in the selection set.
        let results = Self::clean_filter_only_relation_fields(results, select);

        // Apply deferred limit/offset to relation fields.
        // TypeJoinMany stores ALL children (for aggregates to count), so we apply
        // the select's limit/offset here after aggregates have been computed.
        let results = Self::apply_relation_limits(results, select);

        Ok(JsonValue::Array(results))
    }

    async fn precompute_fulltext_scores(
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

    async fn compute_fulltext_path_scores(
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

    fn should_defer_relation_scoped_fulltext(
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

    fn resolve_relation_target_collection(
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
    fn apply_scoped_relation_fulltext(
        mut results: Vec<JsonValue>,
        select: &Select,
    ) -> Vec<JsonValue> {
        for result in &mut results {
            Self::apply_scoped_relation_fulltext_to_value(result, select);
        }

        results
    }

    fn apply_scoped_relation_fulltext_to_value(value: &mut JsonValue, select: &Select) {
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
                Self::apply_scoped_relation_fulltext_to_relation(relation_value, nested_select);
            }
        }
    }

    fn apply_scoped_relation_fulltext_to_relation(value: &mut JsonValue, select: &Select) {
        match value {
            JsonValue::Array(items) => {
                if !Self::apply_scoped_relation_fulltext_top_k(items, select) {
                    Self::score_scoped_relation_items(items, select);
                }
                for item in items.iter_mut() {
                    Self::apply_scoped_relation_fulltext_to_value(item, select);
                }
            }
            JsonValue::Object(_) => {
                Self::score_scoped_relation_items(std::slice::from_mut(value), select);
                Self::apply_scoped_relation_fulltext_to_value(value, select);
            }
            _ => {}
        }
    }

    fn apply_scoped_relation_fulltext_top_k(items: &mut Vec<JsonValue>, select: &Select) -> bool {
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

        let scores = Self::compute_scoped_fulltext_scores(items, ranked_fts);
        Self::retain_relation_top_k_by_score(items, order_field.as_str(), &scores, keep_count);

        for fts in scoped_fulltext {
            if fts.output_name() == order_field {
                continue;
            }

            let scores = Self::compute_scoped_fulltext_scores(items, fts);
            Self::inject_scoped_fulltext_scores(items, fts.output_name(), &scores);
        }

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
        scores: &HashMap<String, f64>,
        keep_count: usize,
    ) {
        if keep_count == 0 || items.len() <= keep_count {
            Self::inject_scoped_fulltext_scores(items, output_name, scores);
            return;
        }

        let mut selected: Vec<(usize, JsonValue)> = Vec::with_capacity(keep_count);

        for (original_index, mut item) in std::mem::take(items).into_iter().enumerate() {
            let score = item
                .as_object()
                .and_then(|obj| obj.get("_docID"))
                .and_then(|value| value.as_str())
                .and_then(|doc_id| scores.get(doc_id))
                .copied()
                .unwrap_or(0.0);
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

    fn score_scoped_relation_items(items: &mut [JsonValue], select: &Select) {
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
            let scores = Self::compute_scoped_fulltext_scores(items, fts);
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
                Self::sort_relation_items(items, order_by);
            }
        }
    }

    fn compute_scoped_fulltext_scores(
        items: &[JsonValue],
        fts: &FullTextSearch,
    ) -> HashMap<String, f64> {
        if fts.query.trim().is_empty() {
            return HashMap::new();
        }

        let mut combined_scores = HashMap::new();

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

            let search_engine =
                SearchEngineBuilder::<String>::with_documents(Language::English, documents).build();

            for result in search_engine.search(&fts.query, None) {
                *combined_scores.entry(result.document.id).or_insert(0.0) += result.score as f64;
            }
        }

        combined_scores
    }

    fn inject_scoped_fulltext_scores(
        items: &mut [JsonValue],
        output_name: &str,
        scores: &HashMap<String, f64>,
    ) {
        for item in items {
            let score = item
                .as_object()
                .and_then(|obj| obj.get("_docID"))
                .and_then(|value| value.as_str())
                .and_then(|doc_id| scores.get(doc_id))
                .copied()
                .unwrap_or(0.0);

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

    fn sort_relation_items(items: &mut [JsonValue], order_by: &crate::mapper::OrderBy) {
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

    /// Strip filter-only fields from relation data in query results.
    ///
    /// When the planner adds relation joins for filter evaluation (e.g., filtering
    /// Author by book.publisher.yearOpened), those relations get render_keys so
    /// the filter can evaluate on rendered JSON. This causes the relation field to
    /// appear in output even though the user didn't request it. This function
    /// retains only the fields explicitly listed in each nested Select.
    fn clean_filter_only_relation_fields(
        mut results: Vec<JsonValue>,
        select: &Select,
    ) -> Vec<JsonValue> {
        // Build map of relation output_name → allowed sub-field names
        let mut relation_allowed_fields: Vec<(String, HashSet<String>)> = Vec::new();

        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "GROUP" {
                    continue;
                }
                let mut allowed = HashSet::new();
                // _docID is always implicit
                allowed.insert("_docID".to_string());
                for sub_field in &nested_select.fields {
                    match sub_field {
                        Requestable::Field(f) => {
                            allowed.insert(f.output_name().to_string());
                        }
                        Requestable::Select(s) => {
                            allowed.insert(s.field.output_name().to_string());
                        }
                        Requestable::Aggregate(a) => {
                            allowed.insert(a.output_name().to_string());
                        }
                        Requestable::Similarity(s) => {
                            allowed.insert(s.output_name().to_string());
                        }
                        Requestable::FullTextSearch(fts) => {
                            allowed.insert(fts.output_name().to_string());
                        }
                    }
                }
                relation_allowed_fields
                    .push((nested_select.field.output_name().to_string(), allowed));
            }
        }

        if relation_allowed_fields.is_empty() {
            return results;
        }

        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (relation_name, allowed_fields) in &relation_allowed_fields {
                    if let Some(relation_data) = obj.get_mut(relation_name.as_str()) {
                        match relation_data {
                            JsonValue::Array(items) => {
                                for item in items.iter_mut() {
                                    if let JsonValue::Object(item_obj) = item {
                                        item_obj.retain(|k, _| allowed_fields.contains(k));
                                    }
                                }
                            }
                            JsonValue::Object(item_obj) => {
                                item_obj.retain(|k, _| allowed_fields.contains(k));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        results
    }

    /// Apply deferred limit/offset to relation fields in query results.
    ///
    /// TypeJoinMany stores ALL children so that relation aggregates (e.g., _count)
    /// can see the full set. This function applies the limit/offset from the select's
    /// nested relation fields after aggregates have been computed.
    fn apply_relation_limits(mut results: Vec<JsonValue>, select: &Select) -> Vec<JsonValue> {
        // Collect relation fields with limits
        let mut relation_limits: Vec<(String, u64, u64)> = Vec::new(); // (field_name, limit, offset)
        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "GROUP" {
                    continue; // _group is handled by GroupByNode
                }
                if let Some(ref limit) = nested_select.limit {
                    let limit_val = limit.limit.unwrap_or(0); // 0 means no limit
                    let offset_val = limit.offset;
                    if limit_val > 0 || offset_val > 0 {
                        relation_limits.push((
                            nested_select.field.output_name().to_string(),
                            limit_val,
                            offset_val,
                        ));
                    }
                }
            }
        }

        if relation_limits.is_empty() {
            return results;
        }

        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (field_name, limit, offset) in &relation_limits {
                    if let Some(JsonValue::Array(items)) = obj.get_mut(field_name) {
                        let offset = *offset as usize;
                        let total = items.len();
                        if offset >= total {
                            *items = Vec::new();
                        } else {
                            let remaining: Vec<JsonValue> = items.drain(offset..).collect();
                            *items = if *limit > 0 {
                                remaining.into_iter().take(*limit as usize).collect()
                            } else {
                                remaining
                            };
                        }
                    }
                }
            }
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use document::Document;
    use schema::FieldKind;
    use std::sync::Mutex;

    use crate::fetcher::FetchByIdsResult;

    #[derive(Default)]
    struct FullTextTestFetcher {
        docs: Mutex<HashMap<String, Vec<Document>>>,
        scores: Mutex<HashMap<(String, String, String), HashMap<String, f64>>>,
    }

    impl FullTextTestFetcher {
        fn add_doc(&self, collection: &str, doc: Document) {
            let mut docs = self.docs.lock().unwrap();
            docs.entry(collection.to_string()).or_default().push(doc);
        }

        fn set_scores(
            &self,
            collection: &str,
            field: &str,
            query: &str,
            scores: HashMap<String, f64>,
        ) {
            self.scores.lock().unwrap().insert(
                (collection.to_string(), field.to_string(), query.to_string()),
                scores,
            );
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl DocFetcher for FullTextTestFetcher {
        async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>> {
            let docs = self.docs.lock().unwrap();
            Ok(docs.get(collection_name).cloned().unwrap_or_default())
        }

        async fn get_by_ids(
            &self,
            collection_name: &str,
            doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            let docs = self.docs.lock().unwrap();
            let all = docs.get(collection_name).cloned().unwrap_or_default();

            let mut found = Vec::new();
            let mut missing = Vec::new();

            for id in doc_ids {
                let doc = all.iter().find(|d| {
                    d.id()
                        .map(|doc_id| doc_id.to_string() == *id)
                        .unwrap_or(false)
                });
                match doc {
                    Some(doc) => found.push(doc.clone()),
                    None => missing.push(id.clone()),
                }
            }

            Ok(FetchByIdsResult::partial(found, missing))
        }

        async fn get_by_field_value(
            &self,
            collection_name: &str,
            field_name: &str,
            value: &str,
        ) -> Result<Vec<Document>> {
            let docs = self.docs.lock().unwrap();
            let all = docs.get(collection_name).cloned().unwrap_or_default();

            Ok(all
                .into_iter()
                .filter(|doc| {
                    doc.get(field_name)
                        .and_then(|v| v.as_str())
                        .map(|v| v == value)
                        .unwrap_or(false)
                })
                .collect())
        }

        async fn search_fulltext_scored(
            &self,
            collection_name: &str,
            field_name: &str,
            query: &str,
        ) -> Result<HashMap<String, f64>> {
            let scores = self.scores.lock().unwrap();
            Ok(scores
                .get(&(
                    collection_name.to_string(),
                    field_name.to_string(),
                    query.to_string(),
                ))
                .cloned()
                .unwrap_or_default())
        }
    }

    fn relation_collections() -> (
        CollectionVersion,
        CollectionVersion,
        HashMap<String, Arc<CollectionVersion>>,
    ) {
        let file_collection = CollectionVersion::new(
            "File",
            "v1",
            "coll-file",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "path", FieldKind::string()),
                FieldDescription::new("4", "functions", FieldKind::relation("Function", true))
                    .with_relation_name("file_functions"),
            ],
        );

        let function_collection = CollectionVersion::new(
            "Function",
            "v1",
            "coll-function",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "file", FieldKind::relation("File", false))
                    .with_relation_name("file_functions")
                    .as_primary(),
                FieldDescription::new("4", "_fileID", FieldKind::doc_id())
                    .with_relation_name("file_functions")
                    .as_primary(),
            ],
        );

        let file_collection = Arc::new(file_collection);
        let function_collection = Arc::new(function_collection);
        let collections_map = HashMap::from([
            (file_collection.name.clone(), file_collection.clone()),
            (
                function_collection.name.clone(),
                function_collection.clone(),
            ),
        ]);

        (
            (*file_collection).clone(),
            (*function_collection).clone(),
            collections_map,
        )
    }

    fn parsed_relation_collections() -> (
        CollectionVersion,
        CollectionVersion,
        HashMap<String, Arc<CollectionVersion>>,
    ) {
        let collections = crate::parse_sdl(
            r#"
            type File {
                name: String @fulltext
                path: String @fulltext
                content: String @fulltext
                functions: [Function]
            }

            type Function {
                name: String @fulltext
                content: String @fulltext
                qualifiedName: String
                startLine: Int
                file: File @primary
            }
            "#,
        )
        .unwrap();

        let file_collection = collections
            .iter()
            .find(|c| c.name == "File")
            .unwrap()
            .clone();
        let function_collection = collections
            .iter()
            .find(|c| c.name == "Function")
            .unwrap()
            .clone();
        let file_collection = Arc::new(file_collection);
        let function_collection = Arc::new(function_collection);
        let collections_map = HashMap::from([
            (file_collection.name.clone(), file_collection.clone()),
            (
                function_collection.name.clone(),
                function_collection.clone(),
            ),
        ]);

        (
            (*file_collection).clone(),
            (*function_collection).clone(),
            collections_map,
        )
    }

    fn relation_collections_resolved_by_id() -> (
        CollectionVersion,
        CollectionVersion,
        HashMap<String, Arc<CollectionVersion>>,
    ) {
        let file_collection = CollectionVersion::new(
            "File",
            "vers-file",
            "coll-file",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        );

        let function_collection = CollectionVersion::new(
            "Function",
            "vers-function",
            "coll-function",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "file", FieldKind::relation("coll-file", false))
                    .as_primary(),
                FieldDescription::new("4", "_fileID", FieldKind::doc_id()).as_primary(),
            ],
        );

        let file_collection = Arc::new(file_collection);
        let function_collection = Arc::new(function_collection);
        let collections_map = HashMap::from([
            (file_collection.name.clone(), file_collection.clone()),
            (
                function_collection.name.clone(),
                function_collection.clone(),
            ),
        ]);

        (
            (*file_collection).clone(),
            (*function_collection).clone(),
            collections_map,
        )
    }

    fn doc(json: &str) -> Document {
        Document::from_json_str(json).unwrap()
    }

    #[tokio::test]
    async fn compute_fulltext_path_scores_lifts_parent_relation_scores() {
        let (file_collection, function_collection, collections_map) = relation_collections();
        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let file_2 = "bae-47bd7c29-69cc-5b8a-856f-caaa93d9ace0";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";
        let fn_2 = "bae-daad4cec-56aa-5b13-9502-657f29321b5d";

        fetcher.add_doc(
            "File",
            doc(&format!(
                r#"{{"_docID":"{file_1}","name":"auth.rs","path":"src/auth.rs"}}"#
            )),
        );
        fetcher.add_doc(
            "File",
            doc(&format!(
                r#"{{"_docID":"{file_2}","name":"utils.rs","path":"src/utils.rs"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"handle_request","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_2}","name":"handle_request","_fileID":"{file_2}"}}"#
            )),
        );
        fetcher.set_scores(
            "File",
            "name",
            "auth",
            HashMap::from([(file_1.to_string(), 1.5)]),
        );

        let runner = QueryRunner::new(fetcher, vec![file_collection, function_collection]);
        let scores = runner
            .compute_fulltext_path_scores(
                collections_map.get("Function").unwrap().clone(),
                "file.name",
                "auth",
                runner.fetcher.as_ref(),
                &collections_map,
            )
            .await
            .unwrap();

        assert_eq!(scores.get(fn_1), Some(&1.5));
        assert!(!scores.contains_key(fn_2));
    }

    #[tokio::test]
    async fn compute_fulltext_path_scores_lifts_reverse_relation_scores() {
        let (file_collection, function_collection, collections_map) = relation_collections();
        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let file_2 = "bae-47bd7c29-69cc-5b8a-856f-caaa93d9ace0";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";
        let fn_2 = "bae-daad4cec-56aa-5b13-9502-657f29321b5d";

        fetcher.add_doc(
            "File",
            doc(&format!(
                r#"{{"_docID":"{file_1}","name":"auth.rs","path":"src/auth.rs"}}"#
            )),
        );
        fetcher.add_doc(
            "File",
            doc(&format!(
                r#"{{"_docID":"{file_2}","name":"utils.rs","path":"src/utils.rs"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"parse_token","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_2}","name":"format_output","_fileID":"{file_2}"}}"#
            )),
        );
        fetcher.set_scores(
            "Function",
            "name",
            "parse_token",
            HashMap::from([(fn_1.to_string(), 2.0)]),
        );

        let runner = QueryRunner::new(fetcher, vec![file_collection, function_collection]);
        let scores = runner
            .compute_fulltext_path_scores(
                collections_map.get("File").unwrap().clone(),
                "functions.name",
                "parse_token",
                runner.fetcher.as_ref(),
                &collections_map,
            )
            .await
            .unwrap();

        assert_eq!(scores.get(file_1), Some(&2.0));
        assert!(!scores.contains_key(file_2));
    }

    #[tokio::test]
    async fn compute_fulltext_path_scores_with_parsed_sdl_schema() {
        let (_file_collection, _function_collection, collections_map) =
            parsed_relation_collections();
        let function_collection = collections_map.get("Function").unwrap();
        let file_field = function_collection.field_by_name("file").unwrap();

        assert!(file_field.is_primary);
        assert!(!file_field.kind.is_array());
        assert_eq!(file_field.relation_name.as_deref(), Some("file_function"));

        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let file_2 = "bae-47bd7c29-69cc-5b8a-856f-caaa93d9ace0";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";
        let fn_2 = "bae-daad4cec-56aa-5b13-9502-657f29321b5d";

        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"handle_request","content":"handles inbound requests","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_2}","name":"handle_request","content":"handles inbound requests","_fileID":"{file_2}"}}"#
            )),
        );
        fetcher.set_scores(
            "File",
            "content",
            "auth",
            HashMap::from([(file_1.to_string(), 0.7)]),
        );

        let runner = QueryRunner::new(fetcher, vec![]);
        let scores = runner
            .compute_fulltext_path_scores(
                function_collection.clone(),
                "file.content",
                "auth",
                runner.fetcher.as_ref(),
                &collections_map,
            )
            .await
            .unwrap();

        assert_eq!(scores.get(fn_1), Some(&0.7));
        assert!(!scores.contains_key(fn_2));
    }

    #[tokio::test]
    async fn compute_fulltext_path_scores_resolves_target_collection_by_collection_id() {
        let (file_collection, function_collection, collections_map) =
            relation_collections_resolved_by_id();
        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";

        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"handle_request","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.set_scores(
            "File",
            "name",
            "auth",
            HashMap::from([(file_1.to_string(), 1.25)]),
        );

        let runner = QueryRunner::new(fetcher, vec![file_collection, function_collection]);
        let scores = runner
            .compute_fulltext_path_scores(
                collections_map.get("Function").unwrap().clone(),
                "file.name",
                "auth",
                runner.fetcher.as_ref(),
                &collections_map,
            )
            .await
            .unwrap();

        assert_eq!(scores.get(fn_1), Some(&1.25));
    }

    #[tokio::test]
    async fn precompute_fulltext_scores_scopes_nested_bm25_aliases() {
        let (_file_collection, _function_collection, collections_map) =
            parsed_relation_collections();
        let fetcher = FullTextTestFetcher::default();
        let file_1 = "bae-7b649bba-3168-5c05-827c-514c0f8d56fd";
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";

        fetcher.add_doc(
            "Function",
            doc(&format!(
                r#"{{"_docID":"{fn_1}","name":"handle_request","content":"handles inbound requests","_fileID":"{file_1}"}}"#
            )),
        );
        fetcher.set_scores(
            "File",
            "name",
            "auth",
            HashMap::from([(file_1.to_string(), 1.0)]),
        );

        let select = crate::parse_query(
            r#"query {
                File {
                    score: BM25(query: "auth", fields: ["name"])
                    functions {
                        score: BM25(query: "auth", fields: ["file.name"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let runner = QueryRunner::new(fetcher, vec![]);
        let scores = runner
            .precompute_fulltext_scores(&select, runner.fetcher.as_ref(), &collections_map)
            .await
            .unwrap();

        let root_scope = vec![select.field.output_name().to_string()];
        let child_scope = vec![
            select.field.output_name().to_string(),
            "functions".to_string(),
        ];
        let root_key = Planner::fts_score_key(&root_scope, "score");
        let child_key = Planner::fts_score_key(&child_scope, "score");

        assert_ne!(root_key, child_key);
        assert_eq!(
            scores.get(&root_key).and_then(|m| m.get(file_1)),
            Some(&1.0)
        );
        assert_eq!(scores.get(&child_key).and_then(|m| m.get(fn_1)), Some(&1.0));
    }

    #[tokio::test]
    async fn precompute_fulltext_scores_skips_nested_local_bm25_fields() {
        let (_file_collection, _function_collection, collections_map) =
            parsed_relation_collections();
        let fetcher = FullTextTestFetcher::default();
        let fn_1 = "bae-bdeed30f-a5e4-5952-93df-27eccec5a5b9";

        fetcher.set_scores(
            "Function",
            "name",
            "handle",
            HashMap::from([(fn_1.to_string(), 1.75)]),
        );

        let select = crate::parse_query(
            r#"query {
                File {
                    functions(order: {_alias: {score: DESC}}) {
                        score: BM25(query: "handle", fields: ["name"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let runner = QueryRunner::new(fetcher, vec![]);
        let scores = runner
            .precompute_fulltext_scores(&select, runner.fetcher.as_ref(), &collections_map)
            .await
            .unwrap();

        let child_scope = vec![
            select.field.output_name().to_string(),
            "functions".to_string(),
        ];
        let child_key = Planner::fts_score_key(&child_scope, "score");

        assert!(!scores.contains_key(&child_key));
    }

    #[test]
    fn apply_scoped_relation_fulltext_scores_and_orders_nested_items() {
        let select = crate::parse_query(
            r#"query {
                Session {
                    messages(order: {_alias: {score: DESC}}) {
                        _docID
                        score: BM25(query: "rust", fields: ["content"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let results = vec![serde_json::json!({
            "_docID": "session-1",
            "messages": [
                {"_docID": "msg-1", "content": "rust search"},
                {"_docID": "msg-2", "content": "rust rust rust rust rust"},
                {"_docID": "msg-3", "content": "database tuning"}
            ]
        })];

        let scored =
            QueryRunner::<FullTextTestFetcher>::apply_scoped_relation_fulltext(results, &select);
        let messages = scored[0]["messages"].as_array().unwrap();

        assert_eq!(messages[0]["_docID"], "msg-2");
        assert_eq!(messages[2]["_docID"], "msg-3");
        assert!(messages[0]["score"].as_f64().unwrap() > messages[1]["score"].as_f64().unwrap());
        assert_eq!(messages[2]["score"].as_f64(), Some(0.0));
    }

    #[test]
    fn apply_scoped_relation_fulltext_top_k_preserves_offset_window() {
        let select = crate::parse_query(
            r#"query {
                Session {
                    messages(limit: 1, offset: 1, order: {_alias: {score: DESC}}) {
                        _docID
                        score: BM25(query: "rust", fields: ["content"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let results = vec![serde_json::json!({
            "_docID": "session-1",
            "messages": [
                {"_docID": "msg-1", "content": "rust search"},
                {"_docID": "msg-2", "content": "rust rust rust rust rust"},
                {"_docID": "msg-3", "content": "database tuning"},
                {"_docID": "msg-4", "content": "distributed systems"}
            ]
        })];

        let scored =
            QueryRunner::<FullTextTestFetcher>::apply_scoped_relation_fulltext(results, &select);
        let prelimited_messages = scored[0]["messages"].as_array().unwrap();

        assert_eq!(prelimited_messages.len(), 2);
        assert_eq!(prelimited_messages[0]["_docID"], "msg-2");
        assert_eq!(prelimited_messages[1]["_docID"], "msg-1");

        let limited = QueryRunner::<FullTextTestFetcher>::apply_relation_limits(scored, &select);
        let messages = limited[0]["messages"].as_array().unwrap();

        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["_docID"], "msg-1");
    }

    #[test]
    fn apply_scoped_relation_fulltext_top_k_preserves_original_zero_score_order() {
        let select = crate::parse_query(
            r#"query {
                Session {
                    messages(limit: 2, order: {_alias: {score: DESC}}) {
                        _docID
                        score: BM25(query: "missing", fields: ["content"])
                    }
                }
            }"#,
        )
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

        let results = vec![serde_json::json!({
            "_docID": "session-1",
            "messages": [
                {"_docID": "msg-1", "content": "rust search"},
                {"_docID": "msg-2", "content": "rust rust rust rust rust"},
                {"_docID": "msg-3", "content": "database tuning"}
            ]
        })];

        let scored =
            QueryRunner::<FullTextTestFetcher>::apply_scoped_relation_fulltext(results, &select);
        let messages = scored[0]["messages"].as_array().unwrap();

        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["_docID"], "msg-1");
        assert_eq!(messages[1]["_docID"], "msg-2");
        assert_eq!(messages[0]["score"].as_f64(), Some(0.0));
        assert_eq!(messages[1]["score"].as_f64(), Some(0.0));
    }
}
