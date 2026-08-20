//! Validators that run on both create and update.

use super::helpers::*;
use super::DefinitionState;
use crate::{FieldKind, ScalarKind, ORPHAN_COLLECTION_ID};

/// Matches Go's validateCollectionNameUnique.
pub(super) fn validate_collection_name_unique(
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
pub(super) fn validate_collection_name_not_empty(
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
pub(super) fn validate_type_supported(
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
pub(super) fn validate_type_and_kind_compatible(
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
pub(super) fn validate_field_not_duplicated(
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

/// Matches Go's validateRelationNameUnique.
pub(super) fn validate_relation_name_unique(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        let mut relation_fields: std::collections::HashMap<&str, Vec<(&str, bool)>> =
            std::collections::HashMap::new();

        for field in &col.fields {
            let Some(relation_name) = field.relation_name.as_deref() else {
                continue;
            };

            if matches!(field.kind, FieldKind::Scalar(ScalarKind::DocID)) {
                continue;
            }

            relation_fields
                .entry(relation_name)
                .or_default()
                .push((field.name.as_str(), field.is_primary));
        }

        for (relation_name, fields) in relation_fields {
            if fields.len() <= 1 {
                continue;
            }

            let primary_count = fields.iter().filter(|(_, is_primary)| *is_primary).count();
            if primary_count == 1 && fields.len() == 2 {
                continue;
            }

            errs.push(format!(
                "relation name is not unique within collection. Field: {}, RelationName: {}",
                fields[0].0, relation_name
            ));
        }
    }
    errs
}

/// Matches Go's validateCollectionMaterialized.
///
/// Go only rejects is_materialized=false on regular collections (no query source).
/// Views (collections with a query source) CAN be non-materialized.
pub(super) fn validate_collection_materialized(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
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
pub(super) fn validate_materialized_has_no_policy(
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
pub(super) fn validate_embedding_and_kind_compatible(
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

            let field = col.fields.iter().find(|f| f.name == embedding.field_name);
            match field {
                None => {
                    errs.push(format!(
                        "the given field does not exist. Vector field: {}",
                        embedding.field_name
                    ));
                }
                Some(f) => {
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

/// A vector index must be built with a metric its algorithm can rank by.
///
/// Without this the definition is accepted and the failure surfaces on the
/// first document write, as an index-manager construction error naming an
/// engine the schema author never mentioned.
pub(super) fn validate_vector_index_metrics(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        for index in &col.indexes {
            let Some(vector) = index.vector() else {
                continue;
            };
            if !vector.algorithm.supports_metric(vector.metric) {
                errs.push(format!(
                    "index '{}' cannot rank by {}: the {} algorithm does not order it",
                    index.name,
                    vector.metric.as_str(),
                    vector.algorithm.as_str()
                ));
            }
        }
    }
    errs
}

/// Matches Go's validateEmbeddingFieldsForGeneration.
pub(super) fn validate_embedding_fields_for_generation(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
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
                    errs.push(format!(
                        "embedding fields cannot refer to self or another embedding field. Field: {}",
                        field_name
                    ));
                    continue;
                }

                if is_other_embedding_ref {
                    errs.push(format!(
                        "embedding fields cannot refer to self or another embedding field. Field: {}",
                        field_name
                    ));
                }

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

/// Supported embedding providers (matches Go's supportedEmbeddingProviders).
const SUPPORTED_PROVIDERS: &[&str] = &["ollama", "openai"];

/// Validates embedding provider and model fields.
/// Matches Go's validateEmbeddingProviderAndModel.
pub(super) fn validate_embedding_provider_and_model(
    new_state: &DefinitionState,
    _old_state: &DefinitionState,
) -> Vec<String> {
    let mut errs = Vec::new();
    for col in &new_state.collections {
        for embedding in &col.vector_embeddings {
            if embedding.provider.is_empty() {
                errs.push("embedding Provider cannot be empty".to_string());
            }
            if !SUPPORTED_PROVIDERS.contains(&embedding.provider.as_str()) {
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
pub(super) fn validate_index_fields_not_counter(
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
