//! Document mapping utilities for query planning
//!
//! Contains methods for building DocumentMapping structures that track
//! field positions and rendering keys for query results.

use schema::CollectionVersion;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};

use super::builder::Planner;

impl Planner {
    /// Build the document mapping for a Select operation.
    ///
    /// IMPORTANT: _docID is ALWAYS placed at index 0 because Doc::doc_id() expects it there.
    /// TypeJoinOne/TypeJoinMany use doc_id() to match related documents.
    pub(super) fn build_mapping(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        let mut doc_id_requested = false;
        let mut doc_id_alias: Option<String> = None;

        for requestable in &select.fields {
            if let Requestable::Field(field) = requestable {
                if field.name == "_docID" {
                    doc_id_requested = true;
                    doc_id_alias = Some(field.output_name().to_string());
                    break;
                }
            }
        }

        mapping.add(0, "_docID");
        if doc_id_requested {
            mapping.add_render_key(0, doc_id_alias.as_deref().unwrap_or("_docID"));
        }

        for requestable in &select.fields {
            match requestable {
                Requestable::Field(field) => {
                    if field.name == "_docID" {
                        continue;
                    }
                    if field.name == "_group" {
                        let index = mapping.next_index();
                        mapping.add(index, "_group");
                        mapping.add_render_key(index, field.output_name());
                        continue;
                    }
                    if field.name == "__typename" {
                        mapping.set_type_name(&select.collection_name);
                        let index = mapping.first_index_of_name("__typename").unwrap();
                        mapping.add_render_key(index, field.output_name());
                        continue;
                    }
                    if field.name == "_deleted" {
                        let index = mapping.next_index();
                        mapping.add(index, "_deleted");
                        mapping.add_render_key(index, field.output_name());
                        continue;
                    }
                    if collection.field_by_name(&field.name).is_none() {
                        return Err(QueryError::unknown_field(&field.name));
                    }
                    let index = mapping.next_index();
                    mapping.add(index, &field.name);
                    mapping.add_render_key(index, field.output_name());
                }
                Requestable::Select(nested_select) => {
                    if nested_select.field.name == "_group" {
                        let index = mapping.next_index();
                        mapping.add(index, "_group");
                        mapping.add_render_key(index, nested_select.field.output_name());

                        let child_mapping =
                            self.build_group_child_mapping(nested_select, collection)?;
                        mapping.set_child_at(index, child_mapping);
                        continue;
                    }
                    let index = mapping.next_index();
                    mapping.add(index, &nested_select.field.name);
                    mapping.add_render_key(index, nested_select.field.output_name());
                }
                Requestable::Aggregate(agg) => {
                    let index = mapping.next_index();
                    let name = agg.aggregate_type.as_str();
                    mapping.add(index, name);
                    mapping.add_render_key(index, agg.output_name());
                }
                Requestable::Similarity(sim) => {
                    let index = mapping.next_index();
                    mapping.add(index, "_similarity");
                    mapping.add_render_key(index, sim.output_name());
                }
            }
        }

        if !doc_id_requested && mapping.next_index() == 1 {
            for (i, field) in collection.fields.iter().enumerate() {
                if field.name != "_docID" {
                    mapping.add(i, &field.name);
                    mapping.add_render_key(i, &field.name);
                } else if !doc_id_requested {
                    mapping.add_render_key(0, "_docID");
                }
            }
        }

        if let Some(ref filter) = select.filter {
            for field_name in filter.referenced_fields() {
                if mapping.first_index_of_name(&field_name).is_none() {
                    if collection.field_by_name(&field_name).is_some() {
                        let index = mapping.next_index();
                        mapping.add(index, &field_name);
                    }
                }
            }
        }

        Ok(mapping)
    }

    /// Build a child mapping for the _group virtual field.
    pub(super) fn build_group_child_mapping(
        &self,
        group_select: &Select,
        collection: &CollectionVersion,
    ) -> Result<DocumentMapping> {
        let mut child_mapping = DocumentMapping::new();

        for requestable in &group_select.fields {
            match requestable {
                Requestable::Field(field) => {
                    if field.name == "__typename" {
                        child_mapping.set_type_name(&collection.name);
                        let index = child_mapping.first_index_of_name("__typename").unwrap();
                        child_mapping.add_render_key(index, field.output_name());
                        continue;
                    }

                    if field.name == "_deleted" {
                        let index = child_mapping.next_index();
                        child_mapping.add(index, "_deleted");
                        child_mapping.add_render_key(index, field.output_name());
                        continue;
                    }

                    let schema_idx = if field.name == "_docID" {
                        0
                    } else {
                        let schema_field = collection.field_by_name(&field.name);
                        if schema_field.is_none() {
                            return Err(QueryError::unknown_field(&field.name));
                        }
                        collection
                            .fields
                            .iter()
                            .position(|f| f.name == field.name)
                            .unwrap_or(0)
                    };

                    child_mapping.add(schema_idx, &field.name);
                    child_mapping.add_render_key(schema_idx, field.output_name());
                }
                Requestable::Select(nested_select) => {
                    if nested_select.field.name == "_group" {
                        let index = child_mapping.next_index();
                        child_mapping.add(index, "_group");
                        child_mapping.add_render_key(index, nested_select.field.output_name());

                        let inner_child_mapping =
                            self.build_group_child_mapping(nested_select, collection)?;
                        child_mapping.set_child_at(index, inner_child_mapping);
                    } else {
                        let index = child_mapping.next_index();
                        child_mapping.add(index, &nested_select.field.name);
                        child_mapping.add_render_key(index, nested_select.field.output_name());
                    }
                }
                Requestable::Aggregate(_) => {}
                Requestable::Similarity(_) => {}
            }
        }

        Ok(child_mapping)
    }
}
