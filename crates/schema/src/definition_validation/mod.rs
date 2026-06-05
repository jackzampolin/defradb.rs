//! Go-compatible validation layer for collection version patches.
//!
//! This module mirrors Go DefraDB's `definition_validation.go`, running the same
//! set of validators after every patch to ensure old vs new state consistency.

mod global_validators;
mod helpers;
mod update_validators;

use global_validators::*;
use update_validators::*;

use crate::CollectionVersion;
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
    validate_encrypted_indexes_not_modified,
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
    validate_relation_name_unique,
    validate_collection_materialized,
    validate_materialized_has_no_policy,
    validate_embedding_and_kind_compatible,
    validate_embedding_fields_for_generation,
    validate_embedding_provider_and_model,
];

/// Validates embedding definitions on newly created collections.
///
/// Only runs validators that are safe before collection IDs and persisted metadata
/// are assigned.
pub fn validate_new_collections(new_collections: &[CollectionVersion]) -> Result<(), String> {
    let new_state = DefinitionState::new(new_collections);
    let old_state = DefinitionState::new(&[]);

    let validators: &[Validator] = &[
        validate_embedding_and_kind_compatible,
        validate_embedding_fields_for_generation,
        validate_embedding_provider_and_model,
        validate_index_fields_not_counter,
        validate_relation_name_unique,
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
