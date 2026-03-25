//! Collection building methods
//!
//! Contains SdlParser methods for building CollectionVersion schemas:
//! - `collect_primary_directives()` - Gather @primary directive info
//! - `build_collections()` - Main entry point for building all collections
//! - `detect_collection_set()` - Detect circular relation sets
//! - `build_collection()` - Build a single collection schema
//! - `resolve_field_kind()` - Resolve field types to FieldKind

use cid::Cid;

use crate::error::{QueryError, Result};
use schema::{
    CType, CollectionVersion, FieldDescription, FieldKind, IndexDescription,
    IndexedFieldDescription, VectorEmbeddingDescription,
};
use std::collections::HashMap;

use super::directives::IndexDirection;
use super::helpers::{
    generate_collection_id, generate_field_id, generate_index_name, generate_relation_name,
    graphql_to_scalar_kind,
};
use super::parser::{ParsedType, ParsedTypeDef, SdlParser};

impl<'a> SdlParser<'a> {
    pub(super) fn collect_primary_directives(
        &self,
        type_names: &std::collections::HashSet<String>,
    ) -> std::collections::HashMap<(String, String), bool> {
        let mut result = std::collections::HashMap::new();

        for (type_name, type_def) in &self.type_defs {
            for field in &type_def.fields {
                let target = &field.field_type.base_type;

                // Only consider relations to other types in the schema
                if type_names.contains(target) {
                    // Key: (source_type, target_type) -> has_primary directive
                    // Use OR logic: if ANY field in this (source, target) pair has @primary, it's true.
                    // This handles self-referencing types where multiple fields share the same key.
                    let entry = result
                        .entry((type_name.clone(), target.clone()))
                        .or_insert(false);
                    if field.directives.is_primary {
                        *entry = true;
                    }
                }
            }
        }

        result
    }

    /// Validate parsed types before building collections.
    /// Checks for NonNull fields, one-one relation primary constraints, and default value constraints.
    pub(super) fn build_collections(&self) -> Result<Vec<CollectionVersion>> {
        // Build collection names set for relation detection, including external types
        let mut type_names: std::collections::HashSet<_> = self.type_defs.keys().cloned().collect();
        type_names.extend(self.known_external_types.iter().cloned());

        // Pre-validate all field types to accumulate multiple errors (Go compatibility).
        // Go collects ALL "no type found" errors before returning, rather than stopping at first.
        let scalar_types: std::collections::HashSet<&str> = [
            "String", "Int", "Float", "Float64", "Float32", "Boolean", "ID", "DateTime", "JSON",
            "Blob", "Self",
        ]
        .into_iter()
        .collect();
        let mut field_type_errors = Vec::new();
        for type_def in self.type_defs.values() {
            for field in &type_def.fields {
                let base = &field.field_type.base_type;
                if !scalar_types.contains(base.as_str())
                    && !type_names.contains(base)
                    && base != &type_def.name
                {
                    field_type_errors.push(format!(
                        "no type found for given name. Field: {}, Kind: {}",
                        field.name, base
                    ));
                }
            }
        }
        if !field_type_errors.is_empty() {
            field_type_errors.sort();
            return Err(QueryError::parse(field_type_errors.join("\n")));
        }

        // Collect @primary directive information for determining actual primaryness
        let primary_directives = self.collect_primary_directives(&type_names);

        // Detect circular relation sets - types that form TRUE cycles
        // A cycle only occurs if BOTH sides of a mutual reference are PRIMARY
        // (i.e., neither has @primary making the other secondary)
        let (collection_set, collection_set_groups) =
            self.detect_collection_set(&type_names, &primary_directives);

        // Process types in alphabetical order (Go behavior)
        let mut sorted_type_names: Vec<_> = self.type_defs.keys().cloned().collect();
        sorted_type_names.sort();

        // TOPOLOGICAL ORDER APPROACH (matches Go behavior):
        // Process types in topological order based on CID dependencies.
        //
        // A type A depends on type B if A has a PRIMARY relation field to B
        // (meaning B's CollectionID must be known to calculate A's CID).
        //
        // Types are sorted by:
        // 1. Dependency order (types with fewer dependencies first)
        // 2. Alphabetical order as tiebreaker
        //
        // This ensures that when we process a type, all types it depends on
        // have already been processed and their CollectionIDs are known.

        // Build dependency graph: which types does each type's CID depend on?
        let mut dependencies: std::collections::HashMap<String, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for type_name in &sorted_type_names {
            let type_def = self.type_defs.get(type_name).ok_or_else(|| {
                QueryError::internal(format!("unknown type in dependency graph: {type_name}"))
            })?;
            let mut deps = std::collections::HashSet::new();

            for field in &type_def.fields {
                let target = &field.field_type.base_type;
                if !type_names.contains(target) || target == type_name {
                    continue; // Not a relation to another type in schema, or self-ref
                }
                if self.known_external_types.contains(target) {
                    continue; // External type already exists, no CID dependency
                }

                // Check if this field is PRIMARY (included in CID calculation)
                let is_array = field.field_type.is_list;
                if is_array {
                    continue; // Arrays are secondary, not in CID
                }

                let has_primary = field.directives.is_primary;
                let counterpart_has_primary = primary_directives
                    .get(&(target.clone(), type_name.clone()))
                    .copied()
                    .unwrap_or(false);

                let is_field_primary = has_primary || !counterpart_has_primary;
                if is_field_primary {
                    // If both types are in the same collection set, they use SelfRef
                    // (relative_id) instead of Relation (CID), so no CID dependency.
                    let same_set = match (
                        collection_set.get(type_name.as_str()),
                        collection_set.get(target.as_str()),
                    ) {
                        (Some(&(_, g1)), Some(&(_, g2))) => g1 == g2,
                        _ => false,
                    };
                    if !same_set {
                        deps.insert(target.clone());
                    }
                }
            }

            dependencies.insert(type_name.clone(), deps);
        }

        // Topological sort using Kahn's algorithm
        // In-degree = number of types this type depends on (not how many depend on it).
        // Types with in-degree 0 have no unresolved dependencies and can be processed.
        let mut in_degree: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (type_name, deps) in &dependencies {
            in_degree.insert(type_name.clone(), deps.len());
        }

        // Queue starts with types that have no dependencies
        let mut queue: Vec<&String> = sorted_type_names
            .iter()
            .filter(|name| {
                dependencies
                    .get(*name)
                    .map(|d| d.is_empty())
                    .unwrap_or(true)
            })
            .collect();
        // Sort queue alphabetically for determinism
        queue.sort();

        let mut processing_order = Vec::new();
        while !queue.is_empty() {
            // Sort queue alphabetically for deterministic ordering
            queue.sort();
            let current = queue.remove(0);
            processing_order.push(current.clone());

            // For each type that depends on current, decrease its in-degree
            for (type_name, deps) in &dependencies {
                if deps.contains(current) {
                    let Some(degree) = in_degree.get_mut(type_name) else {
                        continue;
                    };
                    *degree = degree.saturating_sub(1);
                    if *degree == 0 && !processing_order.contains(type_name) {
                        queue.push(type_name);
                    }
                }
            }
        }

        // If there's a cycle, fall back to alphabetical order for remaining types
        for type_name in &sorted_type_names {
            if !processing_order.contains(type_name) {
                processing_order.push(type_name.clone());
            }
        }

        // TWO-PASS APPROACH:
        // Pass 1: Calculate CollectionIDs in topological order
        // This ensures CID dependencies are resolved correctly.
        // Also simulates Go's headstore to replicate prefix collision behavior.
        let mut all_collection_ids: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut headstore: HashMap<String, (Cid, u64)> = HashMap::new();

        for type_name in &processing_order {
            let type_def = self.type_defs.get(type_name).ok_or_else(|| {
                QueryError::internal(format!("unknown type in processing order: {type_name}"))
            })?;
            let collection = self.build_collection(
                type_def,
                &type_names,
                &collection_set,
                &all_collection_ids, // Pass already-calculated CollectionIDs
                &primary_directives,
                &headstore,
            )?;
            // Store this type's CollectionID for later types to reference
            all_collection_ids.insert(type_name.clone(), collection.collection_id.clone());

            // Update simulated headstore: store this collection's CID with height=1
            // (Go stores collection definition CIDs at prefix /g/<CollectionName>)
            if let Ok(cid) = collection.collection_id.parse::<Cid>() {
                // Determine height: check if any prefix collision occurred
                let prefix = format!("/g/{}", type_name);
                let max_height: u64 = headstore
                    .iter()
                    .filter(|(k, _)| format!("/g/{}", k).starts_with(&prefix))
                    .map(|(_, (_, h))| *h)
                    .max()
                    .unwrap_or(0);
                let height = max_height + 1;
                headstore.insert(type_name.clone(), (cid, height));
            }
        }
        // Compute CollectionSetIDs for multi-type circular groups
        let mut collection_set_map: HashMap<String, schema::CollectionSetDescription> =
            HashMap::new();
        for group in &collection_set_groups {
            if group.len() < 2 {
                continue;
            }
            let collection_cids: Vec<Cid> = group
                .iter()
                .filter_map(|name| all_collection_ids.get(name))
                .filter_map(|id| id.parse::<Cid>().ok())
                .collect();
            if collection_cids.len() != group.len() {
                continue;
            }
            if let Ok(set_cid) = schema::generate_collection_set_cid(&collection_cids) {
                let set_id = set_cid.to_string();
                for name in group {
                    if let Some(&(relative_id, _)) = collection_set.get(name) {
                        collection_set_map.insert(
                            name.clone(),
                            schema::CollectionSetDescription::new(&set_id, relative_id),
                        );
                    }
                }
            }
        }

        // Pass 2: Rebuild all collections with ALL CollectionIDs known
        // This ensures all relation fields have proper CollectionKind (not NamedKind)
        // for query resolution, even for secondary fields pointing to later types.
        // Return in SDL definition order to match Go's parser behavior (Go's sequential
        // prefix counter assigns IDs in the order types appear in the SDL).
        //
        // IMPORTANT: Use the collection_id/version_id from Pass 1, not the regenerated
        // ones from Pass 2. Pass 1 computes CIDs in topological order with headstore
        // simulation matching Go's behavior. Pass 2 may produce different field CIDs
        // (because Named → Relation resolution changes the field kind), which would
        // change the collection CID incorrectly.
        let mut collections = Vec::new();

        for type_name in &self.definition_order {
            let type_def = self.type_defs.get(type_name).ok_or_else(|| {
                QueryError::internal(format!("unknown type in definition order: {type_name}"))
            })?;
            let mut collection = self.build_collection(
                type_def,
                &type_names,
                &collection_set,
                &all_collection_ids, // Now has ALL CollectionIDs
                &primary_directives,
                &headstore,
            )?;

            // Override with Pass 1's CID (computed in topological order with headstore)
            if let Some(pass1_id) = all_collection_ids.get(type_name) {
                collection.collection_id = pass1_id.clone();
                collection.version_id = pass1_id.clone();
            }

            // Assign CollectionSetDescription for multi-type circular groups
            if let Some(set_desc) = collection_set_map.get(type_name) {
                collection.collection_set = Some(set_desc.clone());
            }

            // Interface types are embedded-only (not root-queryable)
            if type_def.is_interface {
                collection.is_embedded_only = true;
            }

            collections.push(collection);
        }

        Ok(collections)
    }

    /// Detect types that form circular relation sets.
    /// Returns a map from type name to its sorted index within its connected cycle group.
    /// Types with circular relations use SelfKind with relative indices for CID generation.
    ///
    /// IMPORTANT: A cycle only exists if BOTH sides of a mutual reference are PRIMARY.
    /// If one side has @primary and the other doesn't, the side WITHOUT @primary is SECONDARY
    /// and gets empty FieldID (not included in CID calculation), breaking the cycle.
    ///
    /// IMPORTANT: Only types that are ACTUALLY part of a cycle are included.
    /// Different cycle groups (e.g., Employee self-ref) are treated as separate collection sets.
    #[allow(clippy::type_complexity)]
    pub(super) fn detect_collection_set(
        &self,
        type_names: &std::collections::HashSet<String>,
        primary_directives: &std::collections::HashMap<(String, String), bool>,
    ) -> (
        std::collections::HashMap<String, (i32, usize)>,
        Vec<Vec<String>>,
    ) {
        // Helper to check if a relation field from source->target is actually primary
        // (will be included in CID calculation)
        let is_field_primary = |source: &str, target: &str, is_array: bool| -> bool {
            if is_array {
                // Arrays are always secondary
                return false;
            }

            // Check if this field has @primary directive
            let has_primary = primary_directives
                .get(&(source.to_string(), target.to_string()))
                .copied()
                .unwrap_or(false);

            if has_primary {
                return true;
            }

            // Check if the counterpart (target->source) has @primary
            // If it does, this side is SECONDARY
            let counterpart_has_primary = primary_directives
                .get(&(target.to_string(), source.to_string()))
                .copied()
                .unwrap_or(false);

            if counterpart_has_primary {
                // Counterpart has @primary, so this side is secondary
                return false;
            }

            // Neither side has @primary - single-object relation defaults to primary
            true
        };

        // Build relation graph: which types reference which other types via PRIMARY relations only
        let mut references: std::collections::HashMap<String, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for (type_name, type_def) in &self.type_defs {
            let mut refs = std::collections::HashSet::new();
            for field in &type_def.fields {
                let target = &field.field_type.base_type;
                // Only consider relations to other types that are:
                // 1. In the current schema (type_names)
                // 2. Actually PRIMARY (will be included in CID)
                if type_names.contains(target)
                    && is_field_primary(type_name, target, field.field_type.is_list)
                {
                    refs.insert(target.clone());
                }
            }
            references.insert(type_name.clone(), refs);
        }

        // Find strongly connected components (SCCs) using Tarjan's algorithm.
        // An SCC is a maximal set where every node can reach every other node via
        // directed primary edges. This correctly detects cycles like A→B→C→D→A even
        // when not all edges are bidirectional.
        let components = find_sccs(&references);

        if components.is_empty() {
            return (std::collections::HashMap::new(), Vec::new());
        }

        // Build result: each type gets (relative_id, group_index)
        let mut relative_ids = std::collections::HashMap::new();
        let mut groups = Vec::new();
        for mut members in components {
            members.sort(); // Sort alphabetically within component
            let group_idx = groups.len();
            for (idx, name) in members.iter().enumerate() {
                relative_ids.insert(name.clone(), (idx as i32, group_idx));
            }
            groups.push(members);
        }

        (relative_ids, groups)
    }

    pub(super) fn build_collection(
        &self,
        type_def: &ParsedTypeDef,
        type_names: &std::collections::HashSet<String>,
        collection_set: &std::collections::HashMap<String, (i32, usize)>,
        known_collection_ids: &std::collections::HashMap<String, String>,
        primary_directives: &std::collections::HashMap<(String, String), bool>,
        headstore: &HashMap<String, (Cid, u64)>,
    ) -> Result<CollectionVersion> {
        // collection_id will be generated after fields are created (like Go)
        let mut fields = Vec::new();
        let mut indexes = Vec::new();
        let mut existing_index_names: Vec<String> = Vec::new();
        let mut index_id_counter = 0u32;

        // Track primary relation FK fields that may need auto-created indexes.
        // The bool indicates whether the FK index must be unique (one-to-one) or not
        // (one-to-many / unidirectional). We defer auto-index creation until after
        // type-level indexes are processed so user-defined covering indexes win.
        let mut auto_fk_indexes: Vec<(String, bool)> = Vec::new();

        // Add implicit _docID field
        // NOTE: Go uses CType::None (0) for _docID, not LwwRegister (1)
        let doc_id_kind = FieldKind::doc_id();
        let doc_id_field_id = generate_field_id("_docID", &doc_id_kind, CType::None);
        fields.push(
            FieldDescription::new(&doc_id_field_id, "_docID", doc_id_kind)
                .with_crdt_type(CType::None),
        );

        // Process user-defined fields
        for parsed_field in &type_def.fields {
            let kind = self.resolve_field_kind(
                &parsed_field.field_type,
                &parsed_field.name,
                type_names,
                &type_def.name,
                collection_set,
                known_collection_ids,
            )?;

            // Determine if this relation creates an implicit _id field (FK):
            // - Single-object relations (not arrays) get an implicit {field}_id field
            // - But only on the PRIMARY side (the side with @primary OR the default primary)
            let creates_fk_field = kind.is_relation() && !kind.is_array();

            // Check if this is a self-reference relation (field type == current type)
            let is_self_ref_relation =
                kind.is_relation() && parsed_field.field_type.base_type == type_def.name;

            // Determine the actual primary status for this relation field
            // In Go, a field is PRIMARY if:
            // 1. It has explicit @primary directive, OR
            // 2. It's a single-object relation AND the counterpart does NOT have @primary
            // A field is SECONDARY if:
            // 1. It's an array relation, OR
            // 2. The counterpart (target->source) has @primary
            let is_primary = if kind.is_relation() {
                let target_type = &parsed_field.field_type.base_type;
                let source_type = &type_def.name;

                // Check if this field has @primary directive
                let has_primary_directive = parsed_field.directives.is_primary;

                // Check if counterpart has @primary directive
                let counterpart_has_primary = primary_directives
                    .get(&(target_type.clone(), source_type.clone()))
                    .copied()
                    .unwrap_or(false);

                if kind.is_array() {
                    // Arrays are always secondary
                    false
                } else if has_primary_directive {
                    // Explicit @primary makes this primary
                    true
                } else if counterpart_has_primary {
                    // Counterpart has @primary, so this is secondary
                    false
                } else {
                    // Neither has @primary - single-object defaults to primary
                    true
                }
            } else {
                // Non-relation fields: use explicit @primary directive
                parsed_field.directives.is_primary
            };

            // Determine CRDT type: directive overrides > relation defaults > LwwRegister
            // Go uses NONE_CRDT (Typ=0) for ALL relation object fields, not just single-object
            let crdt_type = if let Some(ct) = parsed_field.directives.crdt_type {
                ct
            } else if kind.is_relation() {
                // All relation object fields use NONE_CRDT in Go
                CType::None
            } else {
                CType::LwwRegister
            };

            // Generate field ID using actual kind and CRDT type.
            // Go assigns empty FieldID to:
            // - Secondary (non-primary) relation object fields
            // - Self-referencing relation object fields with empty RelativeID
            //   (Go's Delta() skips them because strconv.Atoi("") fails)
            let field_id = if is_self_ref_relation || (kind.is_relation() && !is_primary) {
                String::new()
            } else {
                generate_field_id(&parsed_field.name, &kind, crdt_type)
            };

            let mut field = FieldDescription::new(&field_id, &parsed_field.name, kind.clone())
                .with_crdt_type(crdt_type);

            // Set is_primary based on our earlier computation
            if is_primary {
                field = field.as_primary();
            }
            if let Some(ref default_value) = parsed_field.directives.default_value {
                field = field.with_default(default_value.clone());
            }
            if let Some(size) = parsed_field.directives.size_constraint {
                field = field.with_size(size);
            }

            // Set relation name - use explicit @relation(name:) if provided, otherwise auto-generate
            if kind.is_relation() {
                let relation_name = parsed_field
                    .directives
                    .relation_name
                    .clone()
                    .unwrap_or_else(|| {
                        // Go uses lexicographic sort of type names for auto-generated relation names
                        generate_relation_name(
                            &type_def.name,
                            &parsed_field.name,
                            &parsed_field.field_type.base_type,
                        )
                    });
                field = field.with_relation_name(relation_name.clone());

                // For single-object relations (not arrays), Go automatically creates an
                // implicit _{field}ID field to store the foreign key.
                // The FK field has the SAME is_primary status as the main relation field:
                // - If main field is PRIMARY, FK field is also PRIMARY (non-empty FieldID)
                // - If main field is SECONDARY, FK field is also SECONDARY (empty FieldID)
                if creates_fk_field {
                    let id_field_name = format!("_{}ID", parsed_field.name);
                    let id_field_kind = FieldKind::doc_id();
                    let id_field_crdt = CType::LwwRegister;

                    // FK field gets a FieldID only if primary.
                    // Go's Delta() skips secondary fields: RelationName.HasValue() && !IsPrimary.
                    // This applies to both self-ref and cross-type secondary FK fields.
                    let id_field_id = if is_primary {
                        generate_field_id(&id_field_name, &id_field_kind, id_field_crdt)
                    } else {
                        String::new()
                    };
                    // FK field has same is_primary status as relation object field
                    let mut id_field =
                        FieldDescription::new(&id_field_id, &id_field_name, id_field_kind)
                            .with_crdt_type(id_field_crdt)
                            .with_relation_name(relation_name.clone());
                    if is_primary {
                        id_field = id_field.as_primary();

                        // Match finalize_relations(): find the counterpart relation by
                        // relation name, excluding this field for self-references.
                        let counterpart_field = self
                            .type_defs
                            .get(&parsed_field.field_type.base_type)
                            .and_then(|target_def| {
                                target_def.fields.iter().find(|candidate| {
                                    let candidate_relation_name =
                                        candidate.directives.relation_name.clone().unwrap_or_else(
                                            || {
                                                generate_relation_name(
                                                    &target_def.name,
                                                    &candidate.name,
                                                    &candidate.field_type.base_type,
                                                )
                                            },
                                        );

                                    candidate_relation_name == relation_name
                                        && !(target_def.name == type_def.name
                                            && candidate.name == parsed_field.name)
                                })
                            });
                        let is_one_to_one = counterpart_field
                            .map(|f| !f.field_type.is_list)
                            .unwrap_or(false);

                        // Track primary FK fields for potential auto-index creation.
                        // We defer coverage checks until all user-defined indexes exist.
                        auto_fk_indexes.push((id_field_name.clone(), is_one_to_one));
                    }
                    fields.push(id_field);
                }
            }

            // Handle field-level @index directive
            if let Some(ref idx_config) = parsed_field.directives.index {
                // Build fields list based on includes
                // For relation fields (non-array), Go DefraDB stores indexes on the FK field
                // (_fieldID) rather than the relation field name.
                let is_relation_field = !parsed_field.field_type.is_list
                    && self
                        .type_defs
                        .contains_key(&parsed_field.field_type.base_type);

                let primary_field_name = if is_relation_field {
                    // Use FK field name for relation fields (e.g., "address" -> "_addressID")
                    format!("_{}ID", parsed_field.name)
                } else {
                    parsed_field.name.clone()
                };
                let primary_descending = matches!(idx_config.direction, IndexDirection::Desc);

                let index_fields: Vec<IndexedFieldDescription> = if idx_config
                    .includes
                    .iter()
                    .any(|(name, _)| name == &parsed_field.name || name == &primary_field_name)
                {
                    // includes explicitly contains the primary field - use includes order
                    // Transform relation field names to FK field names
                    idx_config
                        .includes
                        .iter()
                        .map(|(name, descending)| {
                            let final_name = if *name == parsed_field.name && is_relation_field {
                                format!("_{}ID", parsed_field.name)
                            } else {
                                name.clone()
                            };
                            IndexedFieldDescription {
                                name: final_name,
                                descending: *descending,
                            }
                        })
                        .collect()
                } else if idx_config.includes.is_empty() {
                    // No includes - just the primary field
                    vec![IndexedFieldDescription {
                        name: primary_field_name.clone(),
                        descending: primary_descending,
                    }]
                } else {
                    // includes doesn't contain primary field - prepend it
                    let mut fields = vec![IndexedFieldDescription {
                        name: primary_field_name.clone(),
                        descending: primary_descending,
                    }];
                    for (name, descending) in &idx_config.includes {
                        fields.push(IndexedFieldDescription {
                            name: name.clone(),
                            descending: *descending,
                        });
                    }
                    fields
                };

                // Generate index name based on first field
                let first_field_name = index_fields
                    .first()
                    .map(|f| f.name.as_str())
                    .unwrap_or(&primary_field_name);
                let idx_name = idx_config.name.clone().unwrap_or_else(|| {
                    generate_index_name(&type_def.name, first_field_name, &existing_index_names)
                });
                existing_index_names.push(idx_name.clone());

                index_id_counter += 1;
                indexes.push(IndexDescription {
                    name: idx_name,
                    id: index_id_counter,
                    fields: index_fields,
                    unique: idx_config.unique,
                });
            }

            fields.push(field);
        }

        // Handle type-level @index directives (composite indexes)
        // Build a set of valid field names for validation.
        // Use the `fields` vector (not type_def.fields) because it includes auto-generated
        // FK fields like `_addressID` that may be referenced in type-level indexes.
        let valid_field_names: std::collections::HashSet<_> =
            fields.iter().map(|f| f.name.as_str()).collect();

        for composite_idx in &type_def.directives.indexes {
            // Validate that all referenced fields exist
            for (field_ref, _) in &composite_idx.fields {
                if !valid_field_names.contains(field_ref.as_str()) {
                    return Err(QueryError::parse(format!(
                        "@index on type {} references unknown field '{}'",
                        type_def.name, field_ref
                    )));
                }
            }

            let idx_name = composite_idx.name.clone().unwrap_or_else(|| {
                let first_field = composite_idx
                    .fields
                    .first()
                    .map(|(n, _)| n.as_str())
                    .unwrap_or("unknown");
                generate_index_name(&type_def.name, first_field, &existing_index_names)
            });
            existing_index_names.push(idx_name.clone());

            let indexed_fields: Vec<IndexedFieldDescription> = composite_idx
                .fields
                .iter()
                .map(|(name, descending)| IndexedFieldDescription {
                    name: name.clone(),
                    descending: *descending,
                })
                .collect();

            index_id_counter += 1;
            indexes.push(IndexDescription {
                name: idx_name,
                id: index_id_counter,
                fields: indexed_fields,
                unique: composite_idx.unique,
            });
        }

        // Create auto-indexes for primary FK fields that aren't covered by user indexes.
        // We deferred this until after type-level indexes so we can check coverage.
        // Go's behavior: if user defines ANY index with FK field as first field,
        // that determines uniqueness and suppresses auto-creation.
        // Sort alphabetically to match Go's deterministic index ID assignment.
        auto_fk_indexes.sort_by(|a, b| a.0.cmp(&b.0));
        auto_fk_indexes.dedup_by(|a, b| a.0 == b.0);
        for (fk_field_name, requires_unique) in &auto_fk_indexes {
            // Check if any existing index has this FK field as its first field
            let covering_index = indexes.iter().find(|idx| {
                idx.fields
                    .first()
                    .map(|f| f.name == *fk_field_name)
                    .unwrap_or(false)
            });

            if let Some(existing_index) = covering_index {
                if *requires_unique && !existing_index.unique {
                    return Err(QueryError::parse(
                        "one-to-one relation must have a unique index",
                    ));
                }
                continue;
            }

            // No user index - create auto FK index with the required uniqueness.
            let idx_name =
                generate_index_name(&type_def.name, fk_field_name, &existing_index_names);
            existing_index_names.push(idx_name.clone());
            index_id_counter += 1;
            indexes.push(IndexDescription {
                name: idx_name,
                id: index_id_counter,
                fields: vec![IndexedFieldDescription {
                    name: fk_field_name.clone(),
                    descending: false,
                }],
                unique: *requires_unique,
            });
        }

        // INTEROP CRITICAL: Sort fields alphabetically after _docID (like Go does).
        //
        // Go's collection.go sorts fields so _docID stays at position 0,
        // and remaining fields are sorted alphabetically by name.
        //
        // Field order affects collection CID generation because:
        // 1. Each field gets a priority based on its position (1, 2, 3, ...)
        // 2. Priority is encoded in the field's CRDT delta payload
        // 3. Different priorities = different field CIDs = different collection CID
        //
        // Without this sort, schemas like "type Users { name: String, age: Int }"
        // would have fields [_docID, name, age] in Rust but [_docID, age, name] in Go,
        // causing CID mismatches and P2P topic subscription failures.
        if fields.len() > 1 {
            fields[1..].sort_by(|a, b| a.name.cmp(&b.name));
        }

        // Generate collection ID from type name and fields (like Go, includes field CIDs as links)
        // The headstore simulates Go's prefix collision behavior for deterministic CIDs
        let collection_id = generate_collection_id(&type_def.name, &fields, headstore);

        // Version ID equals collection ID for new schemas (Go behavior)
        let version_id = collection_id.clone();

        // Build encrypted indexes from @encryptedIndex directives
        let encrypted_indexes: Vec<schema::EncryptedIndexDescription> = type_def
            .fields
            .iter()
            .filter(|f| f.directives.encrypted_index)
            .map(|f| schema::EncryptedIndexDescription::new(&f.name))
            .collect();

        // Build full-text indexes from @fulltext directives
        let fulltext_indexes: Vec<schema::FullTextIndexDescription> = type_def
            .fields
            .iter()
            .filter_map(|f| {
                f.directives.fulltext.as_ref().map(|ft| {
                    let mut desc = schema::FullTextIndexDescription::new(&f.name);
                    if let Some(ref lang) = ft.language {
                        desc.language = lang.clone();
                    }
                    if let Some(k1) = ft.k1 {
                        desc.k1 = k1;
                    }
                    if let Some(b) = ft.b {
                        desc.b = b;
                    }
                    desc
                })
            })
            .collect();

        // Build vector embeddings from @embedding directives
        let vector_embeddings: Vec<VectorEmbeddingDescription> = type_def
            .fields
            .iter()
            .filter_map(|f| {
                f.directives.embedding.as_ref().map(|emb| {
                    VectorEmbeddingDescription::new(&f.name, &emb.model, &emb.provider)
                        .with_fields(emb.fields.clone())
                        .with_url(&emb.url)
                        .with_template(&emb.template)
                })
            })
            .collect();

        let mut collection =
            CollectionVersion::new(&type_def.name, &version_id, &collection_id, fields);
        collection.indexes = indexes;
        collection.encrypted_indexes = encrypted_indexes;
        collection.fulltext_indexes = fulltext_indexes;
        collection.vector_embeddings = vector_embeddings;
        collection.is_materialized = type_def.directives.is_materialized;
        collection.downsample_interval = type_def.directives.downsample_interval.clone();
        collection.downsample_time_field = type_def.directives.downsample_time_field.clone();
        collection.downsample_retention = type_def.directives.downsample_retention.clone();
        collection.is_branchable = type_def.directives.is_branchable;
        if let Some(ref policy_config) = type_def.directives.policy {
            collection.policy = Some(schema::PolicyDescription::new(
                &policy_config.id,
                &policy_config.resource,
            ));
        }

        Ok(collection)
    }

    pub(super) fn resolve_field_kind(
        &self,
        parsed_type: &ParsedType,
        field_name: &str,
        type_names: &std::collections::HashSet<String>,
        current_type: &str,
        collection_set: &std::collections::HashMap<String, (i32, usize)>,
        known_collection_ids: &std::collections::HashMap<String, String>,
    ) -> Result<FieldKind> {
        let base = &parsed_type.base_type;

        // Check if it's a scalar type
        if let Some(scalar_kind) = graphql_to_scalar_kind(base) {
            if parsed_type.is_list {
                let array_kind = if parsed_type.element_non_null {
                    scalar_kind.to_array_kind()
                } else {
                    scalar_kind.to_nillable_array_kind()
                };

                return array_kind.map(FieldKind::ScalarArray).ok_or_else(|| {
                    QueryError::parse(format!("scalar type {} cannot be used in arrays", base))
                });
            }
            return Ok(FieldKind::Scalar(scalar_kind));
        }

        // Check if it's a self-reference (same type or "Self" keyword)
        if base == current_type || base == "Self" {
            // For self-references (type pointing to itself), Go ALWAYS uses empty RelativeID
            // The RelativeID indices are only used for cross-type references within a collection set
            return Ok(FieldKind::self_ref("", parsed_type.is_list));
        }

        // Check if it references another type in the schema
        if type_names.contains(base) {
            // This is a relation to another type
            // If both the current type and target type are in the SAME collection set
            // (circular relations), use SelfRef with the target's relative index
            if let (Some(&(target_idx, target_group)), Some(&(_, current_group))) =
                (collection_set.get(base), collection_set.get(current_type))
            {
                if target_group == current_group {
                    return Ok(FieldKind::self_ref(
                        target_idx.to_string(),
                        parsed_type.is_list,
                    ));
                }
            }

            // If target type was already processed (alphabetically earlier),
            // use Relation with the known CollectionID (matches Go behavior)
            if let Some(collection_id) = known_collection_ids.get(base) {
                return Ok(FieldKind::relation(
                    collection_id.clone(),
                    parsed_type.is_list,
                ));
            }

            // For non-circular relations where target not yet processed, use Named
            return Ok(FieldKind::named(base, parsed_type.is_list));
        }

        // Unknown type - error for Go compatibility
        Err(QueryError::parse(format!(
            "no type found for given name. Field: {}, Kind: {}",
            field_name, base
        )))
    }
}

/// Find strongly connected components in a directed graph using Tarjan's algorithm.
/// Returns only SCCs that represent actual cycles (2+ members, or self-referencing singletons).
fn find_sccs(
    graph: &std::collections::HashMap<String, std::collections::HashSet<String>>,
) -> Vec<Vec<String>> {
    struct TarjanState<'a> {
        graph: &'a std::collections::HashMap<String, std::collections::HashSet<String>>,
        type_list: Vec<String>,
        idx_map: std::collections::HashMap<String, usize>,
        index_counter: usize,
        stack: Vec<usize>,
        on_stack: Vec<bool>,
        indices: Vec<usize>,
        lowlinks: Vec<usize>,
        sccs: Vec<Vec<String>>,
    }

    impl TarjanState<'_> {
        fn visit(&mut self, v: usize) {
            self.indices[v] = self.index_counter;
            self.lowlinks[v] = self.index_counter;
            self.index_counter += 1;
            self.stack.push(v);
            self.on_stack[v] = true;

            if let Some(refs) = self.graph.get(&self.type_list[v]) {
                for target in refs {
                    if let Some(&w) = self.idx_map.get(target) {
                        if self.indices[w] == usize::MAX {
                            self.visit(w);
                            self.lowlinks[v] = self.lowlinks[v].min(self.lowlinks[w]);
                        } else if self.on_stack[w] {
                            self.lowlinks[v] = self.lowlinks[v].min(self.indices[w]);
                        }
                    }
                }
            }

            if self.lowlinks[v] == self.indices[v] {
                let mut scc = Vec::new();
                loop {
                    let w = self.stack.pop().expect("Tarjan SCC: v is on stack");
                    self.on_stack[w] = false;
                    scc.push(self.type_list[w].clone());
                    if w == v {
                        break;
                    }
                }
                if scc.len() > 1 {
                    self.sccs.push(scc);
                } else if scc.len() == 1 {
                    if let Some(refs) = self.graph.get(&scc[0]) {
                        if refs.contains(&scc[0]) {
                            self.sccs.push(scc);
                        }
                    }
                }
            }
        }
    }

    let mut type_list: Vec<String> = graph.keys().cloned().collect();
    type_list.sort();
    let n = type_list.len();
    let idx_map: std::collections::HashMap<String, usize> = type_list
        .iter()
        .enumerate()
        .map(|(i, s)| (s.clone(), i))
        .collect();

    let mut state = TarjanState {
        graph,
        type_list,
        idx_map,
        index_counter: 0,
        stack: Vec::new(),
        on_stack: vec![false; n],
        indices: vec![usize::MAX; n],
        lowlinks: vec![0; n],
        sccs: Vec::new(),
    };

    for i in 0..n {
        if state.indices[i] == usize::MAX {
            state.visit(i);
        }
    }

    state.sccs
}
