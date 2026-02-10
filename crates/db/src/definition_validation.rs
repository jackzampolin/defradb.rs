//! Go-compatible validation layer for collection version patches.
//!
//! This module mirrors Go DefraDB's `definition_validation.go`, running the same
//! set of validators after every patch to ensure old vs new state consistency.

use schema::{CType, CollectionVersion, FieldKind, ScalarKind, ORPHAN_COLLECTION_ID};
use std::collections::HashMap;

/// Snapshot of all collection versions for validation.
pub struct DefinitionState {
    pub collections: Vec<CollectionVersion>,
    pub collections_by_id: HashMap<String, CollectionVersion>,
    pub active_by_name: HashMap<String, CollectionVersion>,
    pub active_by_collection_id: HashMap<String, CollectionVersion>,
}

impl DefinitionState {
    pub fn new(collections: &[CollectionVersion]) -> Self {
        let mut by_id = HashMap::new();
        let mut active_by_name = HashMap::new();
        let mut active_by_collection_id = HashMap::new();

        for col in collections {
            by_id.insert(col.version_id.clone(), col.clone());
            if col.is_active {
                active_by_name.insert(col.name.clone(), col.clone());
                active_by_collection_id.insert(col.collection_id.clone(), col.clone());
            }
        }

        Self {
            collections: collections.to_vec(),
            collections_by_id: by_id,
            active_by_name,
            active_by_collection_id,
        }
    }
}

type Validator = fn(new_state: &DefinitionState, old_state: &DefinitionState) -> Vec<String>;

/// Validators that only run during updates (not on initial creation).
const UPDATE_VALIDATORS: &[Validator] = &[
    validate_collection_not_added,
    validate_collection_name_not_mutated,
    validate_version_id_not_mutated,
    validate_collection_id_not_mutated,
    validate_id_not_empty,
    validate_id_unique,
    validate_field_not_moved,
    validate_field_not_mutated,
    validate_policy_not_modified,
    validate_indexes_not_modified,
    validate_sources_not_redefined,
    validate_source_belongs_to_host,
    validate_branchable_not_mutated,
];

/// Validators that run on both create and update.
const GLOBAL_VALIDATORS: &[Validator] = &[
    validate_collection_name_unique,
    validate_collection_name_not_empty,
    validate_type_supported,
    validate_type_and_kind_compatible,
    validate_field_not_duplicated,
    validate_collection_materialized,
    validate_materialized_has_no_policy,
    validate_embedding_and_kind_compatible,
    validate_embedding_fields_for_generation,
    validate_embedding_provider_and_model,
];

/// Validates embedding definitions on newly created collections.
///
/// Only runs embedding-specific validators (type, fields, provider/model).
/// Other global validators are not appropriate at create time.
pub fn validate_new_collections(new_collections: &[CollectionVersion]) -> Result<(), String> {
    let new_state = DefinitionState::new(new_collections);
    let old_state = DefinitionState::new(&[]);

    let validators: &[Validator] = &[
        validate_embedding_and_kind_compatible,
        validate_embedding_fields_for_generation,
        validate_embedding_provider_and_model,
        validate_index_fields_not_counter,
    ];

    let mut errors = Vec::new();
    for validator in validators {
        errors.extend(validator(&new_state, &old_state));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

/// Run all validators comparing old and new collection states.
///
/// Returns Ok(()) if all validators pass, or an error with all validation
/// messages joined by newlines (matching Go's errors.Join behavior).
pub fn validate_collection_changes(
    old_collections: &[CollectionVersion],
    new_collections: &[CollectionVersion],
) -> Result<(), String> {
    let old_state = DefinitionState::new(old_collections);
    let new_state = DefinitionState::new(new_collections);

    let mut errors = Vec::new();
    for validator in UPDATE_VALIDATORS {
        errors.extend(validator(&new_state, &old_state));
    }
    for validator in GLOBAL_VALIDATORS {
        errors.extend(validator(&new_state, &old_state));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

// =============================================================================
// Update-Only Validators
// =============================================================================

/// Matches Go's validateCollectionNotAdded.
/// New collections cannot be added via patch.
fn validate_collection_not_added(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        if col.is_placeholder {
            continue;
        }
        if !old_state.collections_by_id.contains_key(&col.version_id)
            && !old_state
                .active_by_collection_id
                .contains_key(&col.collection_id)
        {
            errs.push(format!(
                "adding collections via patch is not supported. Name: {}",
                col.name
            ));
        }
    }
    errs
}

/// Matches Go's validateCollectionNameNotMutated.
fn validate_collection_name_not_mutated(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        if let Some(old_col) = old_state.collections_by_id.get(&new_col.version_id) {
            if old_col.is_placeholder {
                continue;
            }
            if new_col.name != old_col.name {
                errs.push(format!(
                    "collection name cannot be mutated. NewName: {}, OldName: {}",
                    new_col.name, old_col.name
                ));
            }
        }
    }
    errs
}

/// Matches Go's validateCollectionVersionIDNotMutated.
fn validate_version_id_not_mutated(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        if let Some(old_col) = old_state
            .active_by_collection_id
            .get(&new_col.collection_id)
        {
            // If old collection existed and version IDs match, check that version_id wasn't changed
            if old_col.version_id == new_col.version_id {
                continue;
            }
            // The version_id changed - this is only OK if it's a new version (different version_id
            // with same collection_id), which is the normal patch flow. But if the old version_id
            // was in old_state and now has a DIFFERENT version_id for the same entry, that's a mutation.
            // Go checks: for each new collection, look up by collection_id in old state.
            // If old version_id != new version_id AND the new version_id already existed in old state
            // with the same collection_id, that means someone changed the version_id field directly.
        }
    }
    // Go implementation: for each new collection, find old by collection_id,
    // check if version_id was directly changed (not by creating a new version)
    for new_col in &new_state.collections {
        if let Some(old_col) = old_state.collections_by_id.get(&new_col.version_id) {
            // Same version_id exists in old state - check collection_id wasn't changed
            // This validator specifically catches version_id mutation
            // In Go: it checks if the version_id was changed for an existing collection
            if old_col.collection_id == new_col.collection_id
                && old_col.version_id != new_col.version_id
            {
                errs.push(format!(
                    "collection version ID cannot be mutated. CollectionID: {}",
                    new_col.collection_id
                ));
            }
        }
    }
    errs
}

/// Matches Go's validateCollectionIDNotMutated.
fn validate_collection_id_not_mutated(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        if let Some(old_col) = old_state.collections_by_id.get(&new_col.version_id) {
            if new_col.collection_id != old_col.collection_id {
                errs.push(format!(
                    "collection ID cannot be mutated. CollectionVersionID: {}",
                    new_col.version_id
                ));
            }
        }
    }
    errs
}

/// Matches Go's validateIDNotEmpty.
fn validate_id_not_empty(new_state: &DefinitionState, _old_state: &DefinitionState) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        if col.collection_id.is_empty() {
            errs.push("collection ID cannot be empty".to_string());
        }
    }
    errs
}

/// Matches Go's validateIDUnique.
fn validate_id_unique(new_state: &DefinitionState, _old_state: &DefinitionState) -> Vec<String> {
    let mut errs = Vec::new();
    let mut seen: HashMap<&str, bool> = HashMap::new();
    for col in &new_state.collections {
        if !col.version_id.is_empty() {
            if seen.contains_key(col.version_id.as_str()) {
                errs.push(format!("collection already exists. ID: {}", col.version_id));
            }
            seen.insert(&col.version_id, true);
        }
    }
    errs
}

/// Matches Go's validateSingleVersionActive.
#[allow(dead_code)]
fn validate_single_version_active(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    let mut active_by_collection_id: HashMap<&str, &CollectionVersion> = HashMap::new();
    for col in &new_state.collections {
        if col.is_active {
            if let Some(existing) = active_by_collection_id.get(col.collection_id.as_str()) {
                if existing.version_id != col.version_id {
                    errs.push(format!(
                        "multiple versions of same collection cannot be active. Name: {}, Root: {}",
                        col.name, col.collection_id
                    ));
                }
            }
            active_by_collection_id.insert(&col.collection_id, col);
        }
    }
    errs
}

/// Matches Go's validateFieldNotMoved.
/// Fields cannot be reordered within the collection.
fn validate_field_not_moved(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        let old_col = match old_state.collections_by_id.get(&new_col.version_id) {
            Some(c) => c,
            None => {
                match old_state
                    .active_by_collection_id
                    .get(&new_col.collection_id)
                {
                    Some(c) => c,
                    None => continue,
                }
            }
        };

        // Build old field name→index map
        let old_indices: HashMap<&str, usize> = old_col
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.name.as_str(), i))
            .collect();

        for (new_idx, new_field) in new_col.fields.iter().enumerate() {
            if let Some(&old_idx) = old_indices.get(new_field.name.as_str()) {
                if new_idx != old_idx {
                    errs.push(format!(
                        "moving fields is not currently supported. Name: {}, ProposedIndex: {}, ExistingIndex: {}",
                        new_field.name, new_idx, old_idx
                    ));
                }
            }
        }
    }
    errs
}

/// Matches Go's validateFieldNotMutated.
/// Existing fields cannot have their properties changed.
fn validate_field_not_mutated(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        let old_col = match old_state.collections_by_id.get(&new_col.version_id) {
            Some(c) => c,
            None => {
                // Also try by collection_id for the active version
                match old_state
                    .active_by_collection_id
                    .get(&new_col.collection_id)
                {
                    Some(c) => c,
                    None => continue,
                }
            }
        };

        // Build old field map by FieldID (matching Go's approach)
        let old_fields_by_id: HashMap<&str, &schema::FieldDescription> =
            old_col.fields.iter().map(|f| (f.id.as_str(), f)).collect();

        for new_field in &new_col.fields {
            if new_field.id.is_empty() {
                continue;
            }
            if let Some(old_field) = old_fields_by_id.get(new_field.id.as_str()) {
                if new_field != *old_field {
                    errs.push(format!(
                        "mutating an existing field is not supported. ProposedName: {}",
                        new_field.name
                    ));
                }
            }
        }
    }
    errs
}

/// Matches Go's validatePolicyNotModified.
fn validate_policy_not_modified(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        let old_col = match old_state.collections_by_id.get(&new_col.version_id) {
            Some(c) => c,
            None => {
                match old_state
                    .active_by_collection_id
                    .get(&new_col.collection_id)
                {
                    Some(c) => c,
                    None => continue,
                }
            }
        };
        if old_col.is_placeholder {
            continue;
        }

        if new_col.policy != old_col.policy {
            errs.push("collection policy cannot be mutated.".to_string());
        }
    }
    errs
}

/// Matches Go's validateIndexesNotModified.
fn validate_indexes_not_modified(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        let old_col = match old_state.collections_by_id.get(&new_col.version_id) {
            Some(c) => c,
            None => {
                match old_state
                    .active_by_collection_id
                    .get(&new_col.collection_id)
                {
                    Some(c) => c,
                    None => continue,
                }
            }
        };
        if old_col.is_placeholder {
            continue;
        }

        if new_col.indexes != old_col.indexes {
            errs.push(format!(
                "collection indexes cannot be mutated. CollectionID: {}",
                new_col.version_id
            ));
        }
    }
    errs
}

/// Matches Go's validateSourcesNotRedefined.
/// Checks that PreviousVersion and Query sources are not added/removed/mutated.
fn validate_sources_not_redefined(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        // Go looks up by name in activeCollectionsByName
        let old_col = match old_state.active_by_name.get(&new_col.name) {
            Some(c) => c,
            None => continue,
        };
        if old_col.is_placeholder {
            continue;
        }

        // If version ID is the same (in-place change, not a new version):
        // check PreviousVersion changes
        if old_col.version_id == new_col.version_id {
            let old_has_prev = old_col.previous_version.is_some();
            let new_has_prev = new_col.previous_version.is_some();
            if old_has_prev != new_has_prev {
                errs.push("collection sources cannot be added or removed.".to_string());
            }

            // Check if source collection ID was mutated
            if let (Some(old_prev), Some(new_prev)) =
                (&old_col.previous_version, &new_col.previous_version)
            {
                if old_prev.source_collection_id != new_prev.source_collection_id {
                    errs.push(format!(
                        "collection source ID cannot be mutated. NewSourceID: {}, OldSourceID: {}",
                        new_prev.source_collection_id, old_prev.source_collection_id
                    ));
                }
            }
        }

        // Check if Query source was added or removed (regardless of version ID)
        let old_has_query = old_col.query.is_some();
        let new_has_query = new_col.query.is_some();
        if old_has_query != new_has_query {
            errs.push("collection sources cannot be added or removed.".to_string());
        }
    }
    errs
}

/// Matches Go's validateCollectionSourceFromSameCollection.
/// The PreviousVersion source must point to a version belonging to the same root collection.
/// Skips collections with empty names (orphan placeholders), matching Go's behavior.
fn validate_source_belongs_to_host(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        if col.name.is_empty() {
            continue;
        }
        if let Some(ref prev) = col.previous_version {
            if !prev.source_collection_id.is_empty() {
                // Look up the source version to check its collection_id
                if let Some(source_col) =
                    new_state.collections_by_id.get(&prev.source_collection_id)
                {
                    if source_col.collection_id != col.collection_id {
                        errs.push("collection source must belong to host collection.".to_string());
                    }
                }
            }
        }
    }
    errs
}

/// Matches Go's validateCollectionIsBranchableNotMutated.
fn validate_branchable_not_mutated(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        let old_col = match old_state.collections_by_id.get(&new_col.version_id) {
            Some(c) => c,
            None => {
                match old_state
                    .active_by_collection_id
                    .get(&new_col.collection_id)
                {
                    Some(c) => c,
                    None => continue,
                }
            }
        };

        if new_col.is_branchable != old_col.is_branchable {
            errs.push(format!(
                "mutating IsBranchable is not supported. Collection: {}",
                new_col.name
            ));
        }
    }
    errs
}

// =============================================================================
// Global Validators (run on both create and update)
// =============================================================================

/// Matches Go's validateCollectionNameUnique.
fn validate_collection_name_unique(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for col in &new_state.collections {
        if !col.is_active || col.name.is_empty() {
            continue;
        }
        if seen.contains(col.name.as_str()) {
            errs.push(format!("collection already exists. Name: {}", col.name));
        }
        seen.insert(&col.name);
    }
    errs
}

/// Matches Go's validateCollectionNameNotEmpty.
fn validate_collection_name_not_empty(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        if col.collection_id == ORPHAN_COLLECTION_ID || col.is_placeholder {
            continue;
        }
        if col.name.is_empty() {
            errs.push("collection name can't be empty".to_string());
        }
    }
    errs
}

/// Matches Go's validateTypeSupported.
/// CRDT types must be valid (not arbitrary integers).
fn validate_type_supported(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        for field in &col.fields {
            if !is_crdt_type_supported(field.crdt_type) {
                errs.push(format!(
                    "CRDT type not supported. Name: {}, CRDTType: {}",
                    field.name,
                    format_crdt_type(field.crdt_type)
                ));
            }
        }
    }
    errs
}

/// Matches Go's validateTypeAndKindCompatible.
fn validate_type_and_kind_compatible(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        for field in &col.fields {
            if !field.crdt_type.is_compatible_with(&field.kind) {
                errs.push(format!(
                    "CRDT type {} can't be assigned to field kind {}",
                    format_crdt_type(field.crdt_type),
                    format_field_kind(&field.kind)
                ));
            }
        }
    }
    errs
}

/// Matches Go's validateFieldNotDuplicated.
fn validate_field_not_duplicated(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        let mut seen = std::collections::HashSet::new();
        for field in &col.fields {
            if !seen.insert(&field.name) {
                errs.push(format!("duplicate field. Name: {}", field.name));
            }
        }
    }
    errs
}

/// Matches Go's validateCollectionMaterialized.
///
/// Go only rejects is_materialized=false on regular collections (no query source).
/// Views (collections with a query source) CAN be non-materialized.
fn validate_collection_materialized(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        // Non-materialized is only valid for views (has query source).
        // Regular collections (no query source) must be materialized.
        if !col.is_materialized && col.query.is_none() {
            errs.push(format!(
                "non-materialized collections are not supported. Collection: {}",
                col.name
            ));
        }
    }
    errs
}

/// Matches Go's validateMaterializedHasNoPolicy.
fn validate_materialized_has_no_policy(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        if col.is_materialized && col.policy.is_some() {
            errs.push(format!(
                "materialized views do not support ACP. Collection: {}",
                col.name
            ));
        }
    }
    errs
}

/// Matches Go's validateEmbeddingAndKindCompatible.
fn validate_embedding_and_kind_compatible(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        for embedding in &col.vector_embeddings {
            if embedding.field_name.is_empty() {
                errs.push("embedding FieldName cannot be empty".to_string());
                continue;
            }

            // Check that the target field exists
            let field = col.fields.iter().find(|f| f.name == embedding.field_name);
            match field {
                None => {
                    errs.push(format!(
                        "the given field does not exist. Vector field: {}",
                        embedding.field_name
                    ));
                }
                Some(f) => {
                    // Field must be a scalar array of Float32 or Float64
                    if !is_valid_embedding_kind(&f.kind) {
                        errs.push(format!(
                            "invalid type for vector embedding. Actual: {}",
                            format_field_kind(&f.kind)
                        ));
                    }
                }
            }
        }
    }
    errs
}

/// Matches Go's validateEmbeddingFieldsForGeneration.
fn validate_embedding_fields_for_generation(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        // Build set of embedding field names for cross-reference check
        let embedding_field_names: std::collections::HashSet<&str> = col
            .vector_embeddings
            .iter()
            .map(|e| e.field_name.as_str())
            .collect();

        for embedding in &col.vector_embeddings {
            if embedding.fields.is_empty() {
                errs.push("embedding Fields cannot be empty".to_string());
                continue;
            }

            for field_name in &embedding.fields {
                let is_self_ref = field_name == &embedding.field_name;
                let is_other_embedding_ref =
                    !is_self_ref && embedding_field_names.contains(field_name.as_str());

                if is_self_ref {
                    // Self-reference: report error and skip kind check (Go behavior)
                    errs.push(format!(
                        "embedding fields cannot refer to self or another embedding field. Field: {}",
                        field_name
                    ));
                    continue;
                }

                if is_other_embedding_ref {
                    // Cross-reference to another embedding: report error but also check kind
                    errs.push(format!(
                        "embedding fields cannot refer to self or another embedding field. Field: {}",
                        field_name
                    ));
                }

                // Field must exist on the collection and have valid kind
                let field = col.fields.iter().find(|f| f.name == *field_name);
                match field {
                    None => {
                        if !is_other_embedding_ref {
                            errs.push(format!(
                                "the given field does not exist. Embedding generation field: {}",
                                field_name
                            ));
                        }
                    }
                    Some(f) => {
                        if !is_valid_embedding_generation_kind(&f.kind) {
                            errs.push(format!(
                                "invalid field type for vector embedding generation. Actual: {}",
                                format_field_kind(&f.kind)
                            ));
                        }
                    }
                }
            }
        }
    }
    errs
}

/// Matches Go's validateEmbeddingProviderAndModel.
fn validate_embedding_provider_and_model(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        for embedding in &col.vector_embeddings {
            if embedding.provider.is_empty() {
                errs.push("embedding Provider cannot be empty".to_string());
            }

            if !is_known_embedding_provider(&embedding.provider) {
                errs.push(format!(
                    "unknown embedding provider. Provider: {}",
                    embedding.provider
                ));
            }

            if embedding.model.is_empty() {
                errs.push("embedding Model cannot be empty".to_string());
            }
        }
    }
    errs
}

/// Validates that no index references a CRDT counter field.
/// Matches Go's check in NewCollectionIndex: counter fields cannot be indexed.
fn validate_index_fields_not_counter(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        for index in &col.indexes {
            for idx_field in &index.fields {
                if let Some(field) = col.fields.iter().find(|f| f.name == idx_field.name) {
                    if field.crdt_type.is_counter() {
                        errs.push(format!(
                            "indexing accumulated CRDT fields is not yet supported. Field: {}, CRDTType: {}",
                            field.name,
                            format_crdt_type(field.crdt_type)
                        ));
                    }
                }
            }
        }
    }
    errs
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Check if a CRDT type is supported as a user-specified field type.
/// Matches Go's IsSupportedFieldCType: only None, LwwRegister, PnCounter, PCounter.
/// Object and Composite are internal types, not user-assignable.
fn is_crdt_type_supported(crdt: CType) -> bool {
    matches!(
        crdt,
        CType::None | CType::LwwRegister | CType::PnCounter | CType::PCounter
    )
}

/// Check if a field kind is valid for vector embedding storage.
fn is_valid_embedding_kind(kind: &FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::ScalarArray(schema::ScalarArrayKind::Float32Array)
            | FieldKind::ScalarArray(schema::ScalarArrayKind::Float64Array)
    )
}

/// Check if a field kind is valid for embedding generation input (string-like).
fn is_valid_embedding_generation_kind(kind: &FieldKind) -> bool {
    matches!(
        kind,
        FieldKind::Scalar(ScalarKind::String)
            | FieldKind::Scalar(ScalarKind::Int)
            | FieldKind::Scalar(ScalarKind::Float64)
            | FieldKind::Scalar(ScalarKind::Float32)
            | FieldKind::Scalar(ScalarKind::Bool)
            | FieldKind::Scalar(ScalarKind::DateTime)
            | FieldKind::Scalar(ScalarKind::Blob)
    )
}

/// Known embedding providers (matches Go's supportedEmbeddingProviders).
fn is_known_embedding_provider(provider: &str) -> bool {
    matches!(provider, "openai" | "ollama" | "custom")
}

/// Format a CType for error messages (matches Go's CType.String()).
fn format_crdt_type(crdt: CType) -> String {
    match crdt {
        CType::None => "none".to_string(),
        CType::LwwRegister => "lww".to_string(),
        CType::Object => "object".to_string(),
        CType::Composite => "composite".to_string(),
        CType::PnCounter => "pncounter".to_string(),
        CType::PCounter => "pcounter".to_string(),
        CType::Unknown(_) => "unknown".to_string(),
    }
}

/// Format a FieldKind for error messages (matches Go's Kind.String()).
fn format_field_kind(kind: &FieldKind) -> String {
    match kind {
        FieldKind::Scalar(s) => match s {
            ScalarKind::None => "None".to_string(),
            ScalarKind::DocID => "ID".to_string(),
            ScalarKind::Bool => "Boolean".to_string(),
            ScalarKind::Int => "Int".to_string(),
            ScalarKind::Float64 => "Float64".to_string(),
            ScalarKind::Float32 => "Float32".to_string(),
            ScalarKind::DateTime => "DateTime".to_string(),
            ScalarKind::String => "String".to_string(),
            ScalarKind::Blob => "Blob".to_string(),
            ScalarKind::Json => "JSON".to_string(),
        },
        FieldKind::ScalarArray(a) => match a {
            schema::ScalarArrayKind::BoolArray => "[Boolean!]".to_string(),
            schema::ScalarArrayKind::IntArray => "[Int!]".to_string(),
            schema::ScalarArrayKind::Float64Array => "[Float64!]".to_string(),
            schema::ScalarArrayKind::Float32Array => "[Float32!]".to_string(),
            schema::ScalarArrayKind::StringArray => "[String!]".to_string(),
            schema::ScalarArrayKind::NillableBoolArray => "[Boolean]".to_string(),
            schema::ScalarArrayKind::NillableIntArray => "[Int]".to_string(),
            schema::ScalarArrayKind::NillableFloat64Array => "[Float64]".to_string(),
            schema::ScalarArrayKind::NillableFloat32Array => "[Float32]".to_string(),
            schema::ScalarArrayKind::NillableStringArray => "[String]".to_string(),
        },
        FieldKind::Relation { .. } | FieldKind::SelfRef { .. } | FieldKind::Named { .. } => {
            "Object".to_string()
        }
    }
}
