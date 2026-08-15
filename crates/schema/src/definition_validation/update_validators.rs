//! Validators that only run during updates (not on initial creation).

use std::collections::HashMap;

use crate::CollectionVersion;

use super::DefinitionState;

/// Matches Go's validateCollectionNotAdded.
/// New collections cannot be added via patch.
pub(super) fn validate_collection_not_added(
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
pub(super) fn validate_collection_name_not_mutated(
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
pub(super) fn validate_version_id_not_mutated(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        if let Some(old_col) = old_state
            .active_by_collection_id
            .get(&new_col.collection_id)
        {
            if old_col.version_id == new_col.version_id {
                continue;
            }
        }
    }
    for new_col in &new_state.collections {
        if let Some(old_col) = old_state.collections_by_id.get(&new_col.version_id) {
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
pub(super) fn validate_collection_id_not_mutated(
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
pub(super) fn validate_id_not_empty(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        if col.collection_id.is_empty() {
            errs.push("collection ID cannot be empty".to_string());
        }
    }
    errs
}

/// Matches Go's validateIDUnique.
pub(super) fn validate_id_unique(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
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
pub(super) fn validate_single_version_active(
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
pub(super) fn validate_field_not_moved(
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
pub(super) fn validate_field_not_mutated(
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

        let old_fields_by_id: HashMap<&str, &crate::FieldDescription> =
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
pub(super) fn validate_policy_not_modified(
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
pub(super) fn validate_indexes_not_modified(
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

/// Matches Go's validateEncryptedIndexesNotModified.
pub(super) fn validate_encrypted_indexes_not_modified(
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

        if new_col.encrypted_indexes != old_col.encrypted_indexes {
            errs.push(format!(
                "collection encrypted indexes cannot be mutated. CollectionID: {}",
                new_col.version_id
            ));
        }
    }
    errs
}

/// Matches Go's validateSourcesNotRedefined.
/// Checks that PreviousVersion and Query sources are not added/removed/mutated.
pub(super) fn validate_sources_not_redefined(
    new_state: &DefinitionState,
    old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for new_col in &new_state.collections {
        let old_col = match old_state.active_by_name.get(&new_col.name) {
            Some(c) => c,
            None => continue,
        };
        if old_col.is_placeholder {
            continue;
        }

        if old_col.version_id == new_col.version_id {
            let old_has_prev = old_col.previous_version.is_some();
            let new_has_prev = new_col.previous_version.is_some();
            if old_has_prev != new_has_prev {
                errs.push("collection sources cannot be added or removed.".to_string());
            }

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
pub(super) fn validate_source_belongs_to_host(
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
pub(super) fn validate_branchable_not_mutated(
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
