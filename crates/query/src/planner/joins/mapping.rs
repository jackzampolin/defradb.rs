//! Scan mapping construction for join child plans.

use std::collections::HashSet;

use schema::CollectionVersion;

use crate::document::DocumentMapping;

use super::super::builder::Planner;

impl Planner {
    /// Build a scan mapping for join child plans that includes ALL fields at schema indices.
    ///
    /// TypeJoin nodes use `JoinSide::relation_id_field_index()` which returns the FK field's
    /// position in the collection schema. For FK lookups to work correctly, documents must
    /// have fields at their schema positions. This method ensures the mapping includes all
    /// schema fields, while render_keys only include the user-selected fields.
    ///
    /// # Aliased Relation Fields
    ///
    /// When multiple aliases reference the same relation field (e.g., `p1: published` and
    /// `p2: published`), each alias MUST get a unique index. This is critical because
    /// TypeJoinMany nodes use their relation_field_index to set children on the parent
    /// document. If aliases share the same index, later joins overwrite earlier ones.
    ///
    /// The solution: track which indices already have render_keys. If a schema_index
    /// already has a render_key, allocate a new index for subsequent aliases.
    pub(in crate::planner) fn build_scan_mapping_for_join(
        &self,
        collection: &CollectionVersion,
        render_mapping: &DocumentMapping,
    ) -> DocumentMapping {
        let mut mapping = DocumentMapping::new();

        // _docID is ALWAYS at index 0 so filters referencing _docID (from docID/docIDs
        // arguments on child relations) can evaluate against the document's ID.
        mapping.add(0, "_docID");

        // Add ALL fields from the schema at their schema indices
        for (i, field) in collection.fields.iter().enumerate() {
            mapping.add(i, &field.name);
        }

        // Add _docID so that docID/docIDs filters on child relations work.
        // _docID is a virtual field not in the schema, so allocate a new index
        // after all schema fields to avoid collisions with schema positions.
        if mapping.first_index_of_name("_docID").is_none() {
            let doc_id_index = mapping.next_index();
            mapping.add(doc_id_index, "_docID");
        }

        // Track which schema indices already have render_keys assigned
        let mut indices_with_render_keys = HashSet::new();

        // Map render_keys from render_mapping to schema indices.
        // render_mapping uses sparse indices (0, 1, 2, ...) for only selected fields,
        // but scan_mapping uses schema indices which may differ.
        //
        // IMPORTANT: render_key.key may be an alias (e.g., "headline" for field "title").
        // We must look up the *field name* from render_mapping to find the schema index,
        // then use render_key.key (the alias) as the output key.
        //
        // For aliased fields referencing the same underlying field, each alias gets
        // its own unique index to prevent TypeJoinMany nodes from overwriting each other.
        for render_key in &render_mapping.render_keys {
            // Find the field name that corresponds to this render_key's index in render_mapping
            if let Some(field_name) = render_mapping.try_find_name_from_index(render_key.index) {
                // Look up the schema index for this field name in the new mapping
                if let Some(schema_index) = mapping.first_index_of_name(field_name) {
                    if indices_with_render_keys.contains(&schema_index) {
                        // This schema_index already has a render_key (aliased field).
                        // Allocate a new index to avoid TypeJoinMany nodes overwriting each other.
                        let new_index = mapping.next_index();
                        mapping.add(new_index, field_name);
                        mapping.add_render_key(new_index, &render_key.key);
                        indices_with_render_keys.insert(new_index);
                    } else {
                        // First render_key for this schema_index
                        mapping.add_render_key(schema_index, &render_key.key);
                        indices_with_render_keys.insert(schema_index);
                    }
                }
            }
        }

        // Copy BM25 virtual fields from render_mapping.
        // These are not in the schema, so they need dedicated slots in the join mapping.
        for render_key in &render_mapping.render_keys {
            if let Some(field_name) = render_mapping.try_find_name_from_index(render_key.index) {
                if field_name == "BM25" {
                    let new_index = mapping.next_index();
                    mapping.add(new_index, "BM25");
                    mapping.add_render_key(new_index, &render_key.key);
                }
            }
        }

        // Copy _deleted virtual field from render_mapping if present.
        // _deleted is not in the schema, so it must be explicitly added to the scan mapping.
        if let Some(deleted_render_idx) = render_mapping.first_index_of_name("_deleted") {
            let new_index = mapping.next_index();
            mapping.add(new_index, "_deleted");
            for rk in &render_mapping.render_keys {
                if rk.index == deleted_render_idx && rk.key == "_deleted" {
                    mapping.add_render_key(new_index, &rk.key);
                    break;
                }
            }
        }

        // Copy type_info from render_mapping if set (for __typename support)
        // Also need to copy the __typename render_key since it's a virtual field not in schema
        // IMPORTANT: Use collection.name as the type name, not the one from render_mapping,
        // because nested selects have collection_name=field_name (e.g., "author") not the
        // actual collection name (e.g., "Author")
        if render_mapping.type_name().is_some() {
            mapping.set_type_name(&collection.name);
            // Find the __typename render_key in render_mapping and copy it
            if let Some(typename_index) = mapping.first_index_of_name("__typename") {
                for rk in &render_mapping.render_keys {
                    // Find the render_key for __typename (key might be __typename or an alias)
                    if let Some(field_name) = render_mapping.try_find_name_from_index(rk.index) {
                        if field_name == "__typename" {
                            mapping.add_render_key(typename_index, &rk.key);
                            break;
                        }
                    }
                }
            }
        }

        mapping
    }
}
