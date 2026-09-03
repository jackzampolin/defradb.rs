//! Core query routing and encrypted search.

use acp::{DocumentACP, DocumentPermission, Identity};
use identity::Did;
use serde_json::{Map, Value as JsonValue};

use crate::error::{QueryError, Result};
use crate::executor::GqlWarning;
use crate::mapper::{Filter, Requestable, Select};
use crate::planner::index_selection::{can_be_ordered_by_index, can_or_filter_use_index};
use crate::txn::TransactionRegistry;

use super::super::plan;
use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a single Select operation with a specific fetcher and identity.
    pub(crate) async fn execute_select_internal(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
        warnings: &mut Vec<GqlWarning>,
    ) -> Result<JsonValue> {
        // Handle encrypted search queries (encrypted_<Collection>)
        if select.is_encrypted {
            return self
                .execute_encrypted_select(select, fetcher, caller_identity)
                .await;
        }

        // Handle _commits system collection specially
        if select.collection_name == "_commits" {
            return self
                .execute_commits_query(select, fetcher, caller_identity)
                .await;
        }

        // Check if _version is selected - it needs special handling since it's commit data
        // not a schema field. Extract the _version Select for later use.
        let version_selection: Option<&Select> = select.fields.iter().find_map(|f| {
            if let Requestable::Select(s) = f {
                if s.field.name == "_version" {
                    return Some(s.as_ref());
                }
            }
            None
        });

        // Handle CID-based time-travel queries
        if let Some(ref cids) = select.cid {
            if cids.is_empty() {
                return Ok(JsonValue::Array(vec![]));
            }
            return self
                .execute_cid_query_with_version(select, fetcher, caller_identity, version_selection)
                .await;
        }

        // For queries with _version, execute documents first then add version data
        if version_selection.is_some() {
            return self
                .execute_query_with_version(
                    select,
                    fetcher,
                    caller_identity,
                    version_selection,
                    warnings,
                )
                .await;
        }

        // Get collection schema on-demand from provider
        let collection = self.get_collection(&select.collection_name).await?;

        // Embedded-only types (interface types from view SDL) are not root-queryable
        if collection.is_embedded_only {
            return Err(QueryError::parse(format!(
                "Cannot query field \"{}\" on type \"Query\".",
                select.collection_name
            )));
        }

        // Validate unsupported features and field references
        plan::validate_select(select, &collection)?;

        // Check if this query has nested selections (relations)
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        // Check if the filter references relation fields (e.g., {author: {verified: true}})
        // If so, we need to use the Planner to join the relation for filtering even if
        // the relation field is not in the selection set.
        let filter_has_relations = select
            .filter
            .as_ref()
            .map(|f| f.has_relation_filters())
            .unwrap_or(false);

        // Check if the order references relation fields (e.g., {author: {age: DESC}})
        // If so, we need to use the Planner to join the relation for ordering.
        let order_has_relations = select
            .order_by
            .as_ref()
            .map(|o| o.has_relation_order())
            .unwrap_or(false);

        // Check if this is a top-level aggregate query (e.g., { _avg(Users: {field: Age}) })
        // Top-level aggregates have: only aggregate fields, and all targets have host_name == collection_name
        let is_top_level_aggregate = !select.fields.is_empty()
            && select
                .fields
                .iter()
                .all(|f| matches!(f, Requestable::Aggregate(_)))
            && select.fields.iter().all(|f| {
                if let Requestable::Aggregate(agg) = f {
                    agg.targets
                        .iter()
                        .all(|t| t.host_name == select.collection_name)
                } else {
                    true
                }
            });

        // Check if any aggregates reference relations (e.g., _count(books: {}))
        // Relation-based aggregates have a non-empty host_name that differs from collection_name.
        // Exclude top-level aggregates where host_name == collection_name.
        let aggregates_have_relations = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets
                    .iter()
                    .any(|t| !t.host_name.is_empty() && t.host_name != select.collection_name)
            } else {
                false
            }
        });

        // Check if any aggregate targets have filters with relation conditions.
        // If so, we need the planner to join relation data before filtering.
        let aggregate_filter_has_relations = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets.iter().any(|t| {
                    t.filter
                        .as_ref()
                        .map(|f| f.has_relation_filters())
                        .unwrap_or(false)
                })
            } else {
                false
            }
        });

        // Handle top-level aggregates specially - return single value, not array
        if is_top_level_aggregate {
            if aggregate_filter_has_relations {
                // Use planner path to join relation data before filtering
                return self
                    .execute_top_level_aggregate_with_planner(
                        select,
                        fetcher,
                        caller_identity,
                        warnings,
                    )
                    .await;
            } else {
                return self
                    .execute_top_level_aggregate(select, fetcher, &collection, caller_identity)
                    .await;
            }
        }

        // Check if any secondary relation ID fields are selected (e.g., `_authorID` for a secondary `author` relation).
        // These require a TypeJoin to compute the ID via reverse lookup.
        let has_secondary_relation_id = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                let field_name = &field.name;
                // Check pattern: _<relationName>ID
                if field_name.starts_with('_') && field_name.ends_with("ID") && field_name.len() > 3
                {
                    let relation_name = &field_name[1..field_name.len() - 2];
                    if let Some(relation_field) = collection.field_by_name(relation_name) {
                        // Only secondary relations need a join to compute the ID
                        return relation_field.kind.is_relation() && !relation_field.is_primary;
                    }
                }
            }
            false
        });

        // Check if an ordering-only index can be used (planner needed for IndexScanNode)
        let has_ordering_index = select.order_by.is_some()
            && fetcher.supports_index_queries()
            && !collection.indexes.is_empty()
            && select
                .order_by
                .as_ref()
                .map(|o| {
                    collection
                        .indexes
                        .iter()
                        .any(|idx| can_be_ordered_by_index(o, idx).0)
                })
                .unwrap_or(false);

        // Check if an OR filter can use an index (planner needed for IndexScanNode with OrScan)
        let has_or_filter_index = select.filter.is_some()
            && fetcher.supports_index_queries()
            && !collection.indexes.is_empty()
            && select
                .filter
                .as_ref()
                .map(|f| can_or_filter_use_index(f, &collection.indexes))
                .unwrap_or(false);

        // Check if any similarity fields are present (require SimilarityNode in planner)
        let has_similarity = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Similarity(_)));

        // Check if any BM25 full-text search fields are present (require BM25Node in planner)
        let has_fulltext_search = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::FullTextSearch(_)));

        // Validate similarity fields against the collection schema
        if has_similarity {
            for field in &select.fields {
                if let Requestable::Similarity(sim) = field {
                    let target = &sim.target_field;
                    let schema_field = collection.field_by_name(target);
                    // Check that the target field exists and is a numeric array
                    let element_kind = schema_field.and_then(|f| {
                        if let schema::FieldKind::ScalarArray(arr) = &f.kind {
                            let ek = arr.element_kind();
                            match ek {
                                schema::ScalarKind::Int
                                | schema::ScalarKind::Float32
                                | schema::ScalarKind::Float64 => Some(ek),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    });

                    let element_kind = match element_kind {
                        Some(ek) => ek,
                        None => {
                            return Err(QueryError::execution(format!(
                                "Unknown argument \"{}\" on field \"SIMILARITY\" of type \"{}\".",
                                target, collection.name
                            )));
                        }
                    };

                    // For Int fields, validate that vector values are integers
                    if element_kind == schema::ScalarKind::Int {
                        let non_int_values: Vec<String> = sim
                            .vector
                            .iter()
                            .filter(|v| v.fract() != 0.0)
                            .map(|v| format!("{}", v))
                            .collect();
                        if !non_int_values.is_empty() {
                            let vector_repr = format!(
                                "[{}]",
                                sim.vector
                                    .iter()
                                    .map(|v| format!("{}", v))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            );
                            let mut msg = format!(
                                "Argument \"{}\" has invalid value {{vector: {}}}.",
                                target, vector_repr
                            );
                            for v in &non_int_values {
                                msg.push_str(&format!(
                                    "\nIn field \"vector\": In element #1: Expected type \"Int\", found {}.",
                                    v
                                ));
                            }
                            return Err(QueryError::execution(msg));
                        }
                    }
                }
            }
        }

        // Views must always use the planner because they don't store data directly -
        // the planner creates a ViewNode that executes the underlying query.
        let is_view = collection.query.is_some();

        // Use Planner if there are nested selections, filter through relations,
        // order through relations, aggregates on relations, aggregate filters with relations,
        // secondary relation ID fields, similarity computations, when an index can provide
        // ordering, or when querying a view
        let needs_planner = is_view
            || has_nested
            || filter_has_relations
            || order_has_relations
            || aggregates_have_relations
            || aggregate_filter_has_relations
            || has_secondary_relation_id
            || has_ordering_index
            || has_or_filter_index
            || has_similarity
            || has_fulltext_search
            // Cursor queries must go through the planner so CursorNode wraps the top.
            || select.is_cursor;

        if needs_planner {
            // Use the Planner for queries with nested selections (joins) or relation filters.
            self.execute_nested_select_with_planner(select, fetcher, caller_identity, warnings)
                .await
        } else {
            // Use the optimized path for simple queries
            self.execute_simple_select(select, fetcher, &collection, caller_identity)
                .await
        }
    }

    /// Execute an encrypted search query (`encrypted_<Collection>`).
    ///
    /// Validates encrypted index exists, then fetches documents, applies _eq filter
    /// conditions, filters through ACP, and returns Go-compatible `[{"docIDs": [...]}]` format.
    async fn execute_encrypted_select(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Get collection to check for encrypted indexes
        let collection = self.get_collection(&select.collection_name).await?;

        // Validate collection has encrypted indexes (Go-compatible error)
        if collection.encrypted_indexes.is_empty() {
            return Err(QueryError::execution(format!(
                "Cannot query field \"encrypted_{}\" on type \"Query\".",
                collection.name
            )));
        }

        // Extract filtered field names and validate they have encrypted indexes
        if let Some(ref filter) = select.filter {
            let filtered_fields = filter.referenced_fields();
            for field_name in &filtered_fields {
                let has_index = collection
                    .encrypted_indexes
                    .iter()
                    .any(|idx| idx.field_name == *field_name);
                if !has_index {
                    return Err(QueryError::execution(
                        "Argument \"filter\" has invalid value".to_string(),
                    ));
                }
            }
        }

        // Remote-only SE path (Go semantics): when a transport is wired, fan the
        // search tags out to replicators instead of scanning local plaintext.
        // The querying node is the document owner; replicators serve docIDs from
        // the artifacts the owner pushed. Zero replicators yields empty.
        if let Some(transport) = self.se_transport.clone() {
            let eq_conditions = select
                .filter
                .as_ref()
                .map(Self::extract_eq_conditions)
                .unwrap_or_default();

            let ids = transport
                .query_doc_ids(&collection.collection_id, eq_conditions)
                .await
                .map_err(QueryError::execution)?;

            let filtered_ids =
                Self::filter_ids_by_acp(&ids, self.acp.as_ref(), &collection, caller_identity)
                    .await;

            return Ok(serde_json::json!([{"docIDs": filtered_ids}]));
        }

        let docs = fetcher.get_all(&select.collection_name).await?;

        let matching_ids: Vec<String> = if let Some(ref filter) = select.filter {
            let mut ids = Vec::new();
            for doc in &docs {
                let json_map = doc
                    .to_map()
                    .map_err(|e| QueryError::internal(e.to_string()))?;
                let json_obj =
                    JsonValue::Object(json_map.into_iter().collect::<Map<String, JsonValue>>());
                if filter.matches_json_object(&json_obj)? {
                    if let Some(id) = doc.id() {
                        ids.push(id.to_string());
                    }
                }
            }
            ids
        } else {
            docs.iter()
                .filter_map(|doc| doc.id().map(|id| id.to_string()))
                .collect()
        };

        // Apply ACP filtering: remove document IDs the caller cannot read.
        let filtered_ids = Self::filter_ids_by_acp(
            &matching_ids,
            self.acp.as_ref(),
            &collection,
            caller_identity,
        )
        .await;

        Ok(serde_json::json!([{"docIDs": filtered_ids}]))
    }

    /// Extract `(field, value)` equality conditions from an `encrypted_*`
    /// filter. SE supports only top-level `{field: {_eq: value}}` conditions
    /// (Go's `QueryDocIDsByValues` likewise builds one tag per field/value).
    /// Non-`_eq` operators and nested/logical conditions are skipped.
    fn extract_eq_conditions(filter: &Filter) -> Vec<(String, JsonValue)> {
        let mut pairs = Vec::new();
        for (field, value) in filter.conditions() {
            if let Some(ops) = value.as_object() {
                if let Some(eq_value) = ops.get("_eq") {
                    pairs.push((field.clone(), eq_value.clone()));
                }
            }
        }
        pairs
    }

    /// Filter document IDs through ACP, removing those the caller cannot read.
    ///
    /// If no ACP is configured or the collection has no policy, all IDs pass through.
    /// Fail-closed: any ACP check error results in the document being excluded.
    async fn filter_ids_by_acp(
        doc_ids: &[String],
        acp: &dyn DocumentACP,
        collection: &schema::CollectionVersion,
        caller_identity: Option<Did>,
    ) -> Vec<String> {
        let policy = match &collection.policy {
            Some(policy) => policy,
            None => return doc_ids.to_vec(),
        };

        let identity = Identity::from(caller_identity);
        let mut allowed = Vec::with_capacity(doc_ids.len());

        for doc_id in doc_ids {
            let has_access = crate::txn::check_doc_access_with_overlay(
                acp,
                &identity,
                DocumentPermission::Read,
                &policy.id,
                &policy.resource_name,
                doc_id,
            )
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    doc_id = %doc_id,
                    error = %e,
                    "ACP check failed for encrypted search result, denying access"
                );
                false
            });

            if has_access {
                allowed.push(doc_id.clone());
            }
        }

        allowed
    }
}
