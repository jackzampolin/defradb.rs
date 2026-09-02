//! Input construction helpers for mutation operations.

use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::document::{document_to_plan_doc, DocumentMapping};
use crate::error::Result;
use crate::mapper::{Field, Filter, Mutation, Requestable, Select};
use crate::plan::{CreateInput, UpdateInput, UpsertInput};
use crate::txn::TransactionRegistry;

use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Return the document IDs matching a filter, capped to `limit` results.
    pub async fn matching_doc_ids(
        &self,
        collection_name: &str,
        filter: Filter,
        limit: usize,
        show_deleted: bool,
    ) -> Result<Vec<String>> {
        let mut mapping = DocumentMapping::new();
        mapping.add(0, "_docID");
        mapping.add_render_key(0, "_docID");

        let mut select = Select::new(collection_name)
            .with_field(Field::new("_docID"))
            .with_filter(filter)
            .with_limit(limit as u64);
        select.document_mapping = mapping;
        select.show_deleted = show_deleted;

        let mut warnings = Vec::new();
        let result = self
            .execute_select_internal(&select, self.fetcher.as_ref(), None, &mut warnings)
            .await?;
        let items = result.as_array().ok_or_else(|| {
            crate::error::QueryError::internal("filtered document selection returned a non-list")
        })?;

        items
            .iter()
            .map(|item| {
                item.get("_docID")
                    .and_then(JsonValue::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        crate::error::QueryError::internal(
                            "filtered document selection returned an invalid document ID",
                        )
                    })
            })
            .collect()
    }

    fn normalize_mutation_input_fields(
        &self,
        collection: &schema::CollectionVersion,
        input: &HashMap<String, JsonValue>,
    ) -> HashMap<String, JsonValue> {
        let mut normalized = HashMap::with_capacity(input.len());

        for (field_name, value) in input {
            let mapped_name = collection
                .field_by_name(field_name)
                .filter(|field| field.kind.is_object() && !field.kind.is_array())
                .and_then(|field| {
                    let fk_field_name =
                        schema::CollectionVersion::relation_id_field_name(&field.name);
                    collection
                        .field_by_name(&fk_field_name)
                        .map(|_| fk_field_name)
                })
                .unwrap_or_else(|| field_name.clone());

            normalized.insert(mapped_name, value.clone());
        }

        normalized
    }

    /// Resolve a filter to document IDs by querying the collection.
    ///
    /// This is used for filter-based mutations where we need to first
    /// find matching documents, then perform the mutation on them.
    pub(crate) async fn resolve_filter_to_doc_ids(
        &self,
        mutation: &Mutation,
        fetcher: &dyn crate::fetcher::DocFetcher,
    ) -> Result<Option<Vec<String>>> {
        // Only resolve if there's a filter but no explicit doc_ids
        let filter = match (&mutation.filter, &mutation.doc_ids) {
            (Some(filter), None) => filter,
            _ => return Ok(None),
        };

        // Get the collection schema on-demand from provider
        let collection = self.get_collection(&mutation.collection_name).await?;

        // Build mapping from collection schema
        let mut mapping = DocumentMapping::new();
        for (i, field) in collection.fields.iter().enumerate() {
            mapping.add(i, &field.name);
        }

        // Get all documents from the collection
        let all_docs = fetcher.get_all(&mutation.collection_name).await?;

        // Apply filter to find matching documents
        let mut matching_ids = Vec::new();
        for doc in &all_docs {
            // Convert Document to fields array for filter matching
            let plan_doc = document_to_plan_doc(doc, &mapping)?;
            let fields = plan_doc.fields();

            if filter.matches(fields, &mapping)? {
                if let Some(id) = doc.id() {
                    matching_ids.push(id.to_string());
                }
            }
        }

        Ok(Some(matching_ids))
    }

    /// Build document mapping for mutation result fields.
    pub(crate) fn build_mutation_mapping(&self, mutation: &Mutation) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Always reserve index 0 for _docID (matches Go DefraDB DocumentMapping pattern).
        // This ensures set_doc_id() at index 0 doesn't collide with requested field values.
        mapping.add(0, "_docID");

        // Add requested fields (starting at index 1+ since 0 is reserved for _docID)
        let mut has_docid_render = false;
        for field in mutation.requested_fields() {
            if field.name == "_docID" {
                // _docID is already at index 0, just add render key
                mapping.add_render_key(0, field.output_name());
                has_docid_render = true;
                continue;
            }
            let index = mapping.next_index();
            mapping.add(index, &field.name);
            mapping.add_render_key(index, field.output_name());
        }

        // When _version or relation sub-selects are requested, ensure _docID
        // is always rendered (needed to look up version/commit data and to
        // re-query relation data for each document after the mutation)
        let has_sub_select = mutation
            .fields
            .iter()
            .any(|r| matches!(r, Requestable::Select(_)));
        if has_sub_select && !has_docid_render {
            mapping.add_render_key(0, "_docID");
        }

        // If no fields explicitly requested, render _docID by default
        if mapping.render_keys.is_empty() {
            mapping.add_render_key(0, "_docID");
        }

        Ok(mapping)
    }

    /// Build CreateInput objects from mutation input.
    pub(crate) fn build_create_inputs(
        &self,
        mutation: &Mutation,
        collection: &schema::CollectionVersion,
    ) -> Result<Vec<CreateInput>> {
        let mut inputs = Vec::new();

        for doc_input in &mutation.create_input {
            let mut create_input = CreateInput::new();
            let normalized = self.normalize_mutation_input_fields(collection, doc_input);
            for (field_name, value) in normalized {
                create_input = create_input.with_field(field_name.clone(), value.clone());
            }
            inputs.push(create_input);
        }

        Ok(inputs)
    }

    /// Build UpdateInput from mutation input.
    pub(crate) fn build_update_input(
        &self,
        mutation: &Mutation,
        collection: &schema::CollectionVersion,
    ) -> Result<UpdateInput> {
        let mut update_input = UpdateInput::new();

        let normalized = self.normalize_mutation_input_fields(collection, &mutation.update_input);
        for (field_name, value) in &normalized {
            update_input = update_input.with_field(field_name.clone(), value.clone());
        }

        Ok(update_input)
    }

    /// Build UpsertInput from a field-value map.
    pub(crate) fn build_upsert_input_from_map(
        &self,
        collection: &schema::CollectionVersion,
        input: &std::collections::HashMap<String, JsonValue>,
    ) -> Result<UpsertInput> {
        let mut upsert_input = UpsertInput::new();

        let normalized = self.normalize_mutation_input_fields(collection, input);
        for (field_name, value) in &normalized {
            upsert_input = upsert_input.with_field(field_name.clone(), value.clone());
        }

        Ok(upsert_input)
    }
}
