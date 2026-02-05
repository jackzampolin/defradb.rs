//! Version and time-travel query execution
//!
//! Contains methods for executing queries against historical document versions:
//! - `execute_cid_query_with_version()` - Query by commit CID
//! - `execute_query_with_version()` - Query with version/time-travel support
//! - `resolve_relation_target_name()` - Resolve relation target collection names

use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;

use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::txn::TransactionRegistry;

use super::plan;
use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    ///
    /// Reconstructs the document as it existed at the specified commit CID
    /// by walking the merkle DAG backwards and replaying CRDT deltas.
    ///
    /// CID queries require `cid` argument and optionally `docID` for validation.
    /// For document CIDs, returns a single-element array. For collection CIDs
    /// (branchable collections), returns all documents visible at that state.
    pub(crate) async fn execute_cid_query_with_version(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        _caller_identity: Option<Did>,
        version_selection: Option<&Select>,
    ) -> Result<JsonValue> {
        let cid = select.cid.as_ref().ok_or_else(|| {
            QueryError::internal("execute_cid_query called without CID - this is a bug")
        })?;

        // Get expected docID from select.doc_ids (optional validation)
        let expected_doc_id = select.doc_ids.as_ref().and_then(|ids| ids.first());

        // Fetch document(s) at the specified CID.
        // For collection-level CIDs (branchable), this returns multiple documents.
        // For document-level CIDs, this returns a single document.
        let documents = match fetcher
            .get_documents_at_cid(cid, expected_doc_id.map(|s| s.as_str()))
            .await
        {
            Ok(docs) => docs,
            Err(e) => {
                let err_msg = e.to_string();
                // docID mismatch: Go returns empty results
                if err_msg.contains("cid either does not exist or belong to document") {
                    return Ok(JsonValue::Array(vec![]));
                }
                // Block not found in blockstore: propagate as error (Go does the same)
                return Err(e);
            }
        };

        // Get collection schema for building the mapping
        let collection = self.get_collection(&select.collection_name).await?;

        // Separate nested selects (relation fields) from scalar fields.
        // build_mapping can't handle nested selects, so we strip them and resolve relations separately.
        let mut nested_selects: Vec<&Select> = Vec::new();
        let scalar_fields: Vec<Requestable> = select
            .fields
            .iter()
            .filter(|f| {
                if let Requestable::Select(s) = f {
                    if s.field.name == "_version" {
                        return false;
                    }
                    nested_selects.push(s);
                    return false;
                }
                true
            })
            .cloned()
            .collect();

        let select_for_mapping = Select {
            fields: scalar_fields,
            ..select.clone()
        };

        // Build mapping for scalar fields only
        let mapping = plan::build_mapping(&select_for_mapping, &collection)?;

        // Process each document into a JSON object
        let mut result_array = Vec::new();

        for document in &documents {
            // Convert the document to JSON with only the requested scalar fields
            let mut obj = serde_json::Map::new();

            for render_key in &mapping.render_keys {
                let field_name = mapping
                    .try_find_name_from_index(render_key.index)
                    .unwrap_or("");

                let value = if field_name == "__typename" {
                    JsonValue::String(select.collection_name.clone())
                } else if field_name == "_docID" {
                    document
                        .id()
                        .map(|id| JsonValue::String(id.to_string()))
                        .unwrap_or(JsonValue::Null)
                } else if field_name == "_deleted" {
                    JsonValue::Bool(document.is_deleted())
                } else if let Some(nv) = document.get(field_name) {
                    crate::json_convert::normal_value_to_json(nv).unwrap_or(JsonValue::Null)
                } else {
                    JsonValue::Null
                };

                obj.insert(render_key.key.clone(), value);
            }

            // Resolve nested selects (relation fields like `author { name }`)
            for nested_select in &nested_selects {
                let relation_name = &nested_select.field.name;
                let output_name = nested_select.field.output_name();

                // Resolve the actual collection name from the relation field.
                // nested_select.collection_name is the field name (e.g., "author"),
                // but we need the collection type name (e.g., "Author").
                let related_collection = self
                    .resolve_relation_target_name(&collection, relation_name)
                    .await
                    .unwrap_or_else(|| nested_select.collection_name.clone());

                // Many-to-one: parent has FK field (e.g., Book._authorID → Author)
                let fk_field_name = CollectionVersion::relation_id_field_name(relation_name);
                if let Some(fk_value) = document.get(&fk_field_name) {
                    let fk_doc_id = crate::json_convert::normal_value_to_json(fk_value)
                        .ok()
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default();

                    if !fk_doc_id.is_empty() {
                        let result = fetcher
                            .get_by_ids(&related_collection, &[fk_doc_id])
                            .await?;

                        if let Some(related_doc) = result.docs().first() {
                            let related_obj =
                                self.render_document_fields(related_doc, nested_select);
                            obj.insert(output_name.to_string(), JsonValue::Object(related_obj));
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Null);
                        }
                    } else {
                        obj.insert(output_name.to_string(), JsonValue::Null);
                    }
                } else {
                    // One-to-many or no FK found: return null for now
                    obj.insert(output_name.to_string(), JsonValue::Null);
                }
            }

            // Add _version data if requested
            if let Some(version_select) = version_selection {
                let doc_id = document.id().map(|id| id.to_string());
                if let Some(doc_id_str) = doc_id {
                    let version_data = self
                        .fetch_version_data(fetcher, &doc_id_str, version_select, Some(cid))
                        .await?;
                    let output_name = version_select.field.output_name();
                    obj.insert(output_name.to_string(), version_data);
                }
            }

            result_array.push(JsonValue::Object(obj));
        }

        Ok(JsonValue::Array(result_array))
    }

    /// Execute a regular query with _version field support.
    ///
    /// This handles queries that include _version selection but don't have a CID argument.
    /// For each document result, fetches the commit history and adds _version data.
    pub(crate) async fn execute_query_with_version(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
        version_selection: Option<&Select>,
    ) -> Result<JsonValue> {
        // Get collection schema
        let collection = self.get_collection(&select.collection_name).await?;

        // Check if _docID is already in the selection (we need it to fetch version data)
        let has_doc_id = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                field.name == "_docID"
            } else {
                false
            }
        });

        // Build a modified select without _version for the regular query
        // (We'll add _version data after fetching documents)
        // Also add _docID if not already present (needed to fetch version data)
        let mut fields_without_version: Vec<Requestable> = select
            .fields
            .iter()
            .filter(|f| {
                if let Requestable::Select(s) = f {
                    s.field.name != "_version"
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        // Add _docID field if not already present
        if !has_doc_id {
            fields_without_version.push(Requestable::Field(crate::mapper::Field {
                name: "_docID".to_string(),
                alias: None,
            }));
        }

        let select_without_version = Select {
            fields: fields_without_version,
            ..select.clone()
        };

        // Check if remaining fields need the Planner (has real nested selections)
        let has_nested = select_without_version
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        let filter_has_relations = select
            .filter
            .as_ref()
            .map(|f| f.has_relation_filters())
            .unwrap_or(false);

        let order_has_relations = select
            .order_by
            .as_ref()
            .map(|o| o.has_relation_order())
            .unwrap_or(false);

        // Views always need the planner (they execute queries, not storage reads)
        let is_view = collection.query.is_some();

        // Execute the query for document data (without _version)
        let result = if is_view || has_nested || filter_has_relations || order_has_relations {
            self.execute_nested_select_with_planner(
                &select_without_version,
                fetcher,
                caller_identity,
            )
            .await?
        } else {
            self.execute_simple_select(
                &select_without_version,
                fetcher,
                &collection,
                caller_identity,
            )
            .await?
        };

        // If no _version selection, return as-is
        let version_select = match version_selection {
            Some(v) => v,
            None => return Ok(result),
        };

        // Add _version data to each document result
        let results = result
            .as_array()
            .ok_or_else(|| QueryError::internal("Expected array result"))?;

        let mut enriched_results = Vec::new();
        for doc_json in results {
            let mut doc_obj = doc_json
                .as_object()
                .ok_or_else(|| QueryError::internal("Expected object in result"))?
                .clone();

            // Get document ID from the result
            let doc_id = doc_obj
                .get("_docID")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            // Remove _docID if it wasn't originally requested
            if !has_doc_id {
                doc_obj.remove("_docID");
            }

            if let Some(doc_id_str) = doc_id {
                let version_data = self
                    .fetch_version_data(fetcher, &doc_id_str, version_select, None)
                    .await?;
                let output_name = version_select.field.output_name();
                doc_obj.insert(output_name.to_string(), version_data);
            } else {
                // No docID available - return empty version array
                let output_name = version_select.field.output_name();
                doc_obj.insert(output_name.to_string(), JsonValue::Array(vec![]));
            }

            enriched_results.push(JsonValue::Object(doc_obj));
        }

        Ok(JsonValue::Array(enriched_results))
    }

    /// Resolve the actual collection name for a relation field on a parent collection.
    ///
    /// Nested selects have collection_name set to the field name (e.g., "author"),
    /// but fetcher operations need the collection type name (e.g., "Author").
    /// This method resolves the target collection name by looking at the parent
    /// collection's relation field metadata.
    async fn resolve_relation_target_name(
        &self,
        parent_collection: &CollectionVersion,
        relation_field_name: &str,
    ) -> Option<String> {
        let field = parent_collection.field_by_name(relation_field_name)?;
        let target_id = field.kind.relation_collection_id()?;

        // For Named fields, target_id is the collection name directly
        if let Ok(Some(coll)) = self.collection_provider.get_collection(target_id).await {
            return Some(coll.name.clone());
        }

        // For Relation fields, target_id is a collection_id/version_id — search by ID
        if let Ok(names) = self.collection_provider.list_collections().await {
            for name in names {
                if let Ok(Some(coll)) = self.collection_provider.get_collection(&name).await {
                    if coll.collection_id == target_id || coll.version_id == target_id {
                        return Some(coll.name.clone());
                    }
                }
            }
        }
        None
    }
}
