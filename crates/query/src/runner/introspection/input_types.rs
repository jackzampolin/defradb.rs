use async_graphql::dynamic::*;
use schema::{CollectionVersion, FieldKind, ScalarArrayKind, ScalarKind};
use std::collections::HashMap;

use super::collection::scalar_to_gql_name;

/// Build filter input type for a collection.
pub(super) fn build_filter_input_type(
    collection: &CollectionVersion,
    id_to_name: &HashMap<String, String>,
) -> InputObject {
    let type_name = format!("{}FilterArg", collection.name);
    let mut fields: Vec<(String, InputValue)> = Vec::new();

    // Collect relation backing field names (e.g., `_authorID` for a `author` relation field)
    // These fields store foreign keys and should use IDOperatorBlock in filters
    let relation_backing_fields: std::collections::HashSet<String> = collection
        .fields
        .iter()
        .filter_map(|f| {
            if f.kind.is_relation() {
                Some(format!("_{}ID", f.name))
            } else {
                None
            }
        })
        .collect();

    // Add logical operators and _docID
    fields.push((
        "_alias".to_string(),
        InputValue::new("_alias", TypeRef::named("JSON")),
    ));
    fields.push((
        "_and".to_string(),
        InputValue::new("_and", TypeRef::named_nn_list(&type_name)),
    ));
    fields.push((
        "_docID".to_string(),
        InputValue::new("_docID", TypeRef::named("IDOperatorBlock")),
    ));
    fields.push((
        "_not".to_string(),
        InputValue::new("_not", TypeRef::named(&type_name)),
    ));
    fields.push((
        "_or".to_string(),
        InputValue::new("_or", TypeRef::named_nn_list(&type_name)),
    ));

    // Add filter fields for each collection field
    for field in &collection.fields {
        if field.name == "_docID" {
            continue;
        }
        // Relation backing fields (e.g., _authorID) use IDOperatorBlock
        let filter_type = if relation_backing_fields.contains(&field.name) {
            "IDOperatorBlock".to_string()
        } else {
            get_filter_type_for_field(&field.kind, id_to_name, &collection.name)
        };
        fields.push((
            field.name.clone(),
            InputValue::new(&field.name, TypeRef::named(&filter_type)),
        ));
    }

    // Sort alphabetically to match Go introspection output
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    let mut input = InputObject::new(&type_name);
    for (_, field) in fields {
        input = input.field(field);
    }
    input
}

/// Build order input type for a collection.
pub(super) fn build_order_input_type(
    collection: &CollectionVersion,
    _id_to_name: &HashMap<String, String>,
) -> InputObject {
    let type_name = format!("{}OrderArg", collection.name);
    let mut fields: Vec<(String, InputValue)> = Vec::new();

    // Add _docID order
    fields.push((
        "_docID".to_string(),
        InputValue::new("_docID", TypeRef::named("Ordering")),
    ));

    // Add order fields for each collection field.
    // One-to-one relation fields reference the related collection's OrderArg type to allow
    // nested ordering like `User(order: {device: {model: ASC}})`.
    // One-to-many relations (arrays) are excluded — ordering by an array relation is ambiguous.
    for field in &collection.fields {
        if field.name == "_docID" {
            continue;
        }
        if field.kind.is_relation() && !field.kind.is_array() {
            // One-to-one relation: reference the related collection's OrderArg type
            if let Some(related_collection_id) = field.kind.relation_collection_id() {
                if let Some(related_name) = _id_to_name.get(related_collection_id) {
                    fields.push((
                        field.name.clone(),
                        InputValue::new(
                            &field.name,
                            TypeRef::named(format!("{}OrderArg", related_name)),
                        ),
                    ));
                }
            }
        } else if !field.kind.is_relation() {
            fields.push((
                field.name.clone(),
                InputValue::new(&field.name, TypeRef::named("Ordering")),
            ));
        }
    }

    // Sort alphabetically to match Go introspection output
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    let mut input = InputObject::new(&type_name);
    for (_, field) in fields {
        input = input.field(field);
    }
    input
}

/// Build Field enum for a collection (e.g., UserField).
/// Go includes system fields: _deleted, _docID, _group, _version
pub(super) fn build_field_enum(collection: &CollectionVersion) -> Enum {
    let type_name = format!("{}Field", collection.name);
    let mut items: Vec<String> = vec![
        "_deleted".to_string(),
        "_docID".to_string(),
        "GROUP".to_string(),
        "_version".to_string(),
    ];

    // User-defined fields
    for field in &collection.fields {
        if field.name == "_docID" {
            continue;
        }
        items.push(field.name.clone());
    }

    // Sort alphabetically to match Go
    items.sort();

    let mut enum_type = Enum::new(&type_name);
    for item in items {
        enum_type = enum_type.item(EnumItem::new(&item));
    }
    enum_type
}

/// Build mutation input type for a collection.
pub(super) fn build_mutation_input_type(collection: &CollectionVersion) -> InputObject {
    let type_name = format!("{}MutationInputArg", collection.name);
    let mut input = InputObject::new(&type_name);

    // Add fields from collection definition
    for field in &collection.fields {
        // Skip _docID since it's auto-generated by mutations
        if field.name == "_docID" {
            continue;
        }
        let type_ref = field_kind_to_input_type_ref(&field.kind);
        input = input.field(InputValue::new(&field.name, type_ref));
    }

    input
}

/// Convert field kind to input type ref (for mutation inputs).
pub(super) fn field_kind_to_input_type_ref(kind: &FieldKind) -> TypeRef {
    match kind {
        FieldKind::Scalar(scalar) => TypeRef::named(scalar_to_gql_name(scalar)),
        FieldKind::ScalarArray(array) => {
            let element_type = scalar_to_gql_name(&array.element_kind());
            TypeRef::named_list(element_type)
        }
        FieldKind::Relation { .. } | FieldKind::SelfRef { .. } => {
            // For relations, mutation input takes ID
            TypeRef::named("ID")
        }
        FieldKind::Named { name, is_array } => {
            if *is_array {
                TypeRef::named_list(name)
            } else {
                TypeRef::named(name)
            }
        }
        _ => TypeRef::named("String"),
    }
}

/// Get the filter type name for a field kind.
pub(super) fn get_filter_type_for_field(
    kind: &FieldKind,
    id_to_name: &HashMap<String, String>,
    current_name: &str,
) -> String {
    match kind {
        FieldKind::Scalar(scalar) => match scalar {
            ScalarKind::DocID => "IDOperatorBlock".to_string(),
            ScalarKind::String => "StringOperatorBlock".to_string(),
            ScalarKind::Int => "IntOperatorBlock".to_string(),
            ScalarKind::Float64 => "Float64OperatorBlock".to_string(),
            ScalarKind::Float32 => "Float32OperatorBlock".to_string(),
            ScalarKind::Bool => "BooleanOperatorBlock".to_string(),
            ScalarKind::DateTime => "DateTimeOperatorBlock".to_string(),
            ScalarKind::Json => "JSON".to_string(),
            ScalarKind::Blob | ScalarKind::None => "StringOperatorBlock".to_string(),
            _ => "StringOperatorBlock".to_string(),
        },
        FieldKind::ScalarArray(arr) => match arr {
            ScalarArrayKind::BoolArray => "NotNullBooleanListOperatorBlock".to_string(),
            ScalarArrayKind::IntArray => "NotNullIntListOperatorBlock".to_string(),
            ScalarArrayKind::Float64Array => "NotNullFloat64ListOperatorBlock".to_string(),
            ScalarArrayKind::Float32Array => "NotNullFloat32ListOperatorBlock".to_string(),
            ScalarArrayKind::StringArray => "NotNullStringListOperatorBlock".to_string(),
            ScalarArrayKind::NillableBoolArray => "BooleanListOperatorBlock".to_string(),
            ScalarArrayKind::NillableIntArray => "IntListOperatorBlock".to_string(),
            ScalarArrayKind::NillableFloat64Array => "Float64ListOperatorBlock".to_string(),
            ScalarArrayKind::NillableFloat32Array => "Float32ListOperatorBlock".to_string(),
            ScalarArrayKind::NillableStringArray => "StringListOperatorBlock".to_string(),
            _ => "StringListOperatorBlock".to_string(),
        },
        FieldKind::Relation { collection_id, .. } => {
            let type_name = id_to_name
                .get(collection_id)
                .cloned()
                .unwrap_or_else(|| collection_id.clone());
            format!("{}FilterArg", type_name)
        }
        FieldKind::SelfRef { relative_id, .. } => {
            // Empty relative_id means self-reference within the same collection
            let type_name = if relative_id.is_empty() {
                current_name.to_string()
            } else {
                id_to_name
                    .get(relative_id)
                    .cloned()
                    .unwrap_or_else(|| relative_id.clone())
            };
            format!("{}FilterArg", type_name)
        }
        FieldKind::Named { name, .. } => {
            let type_name = id_to_name
                .get(name)
                .cloned()
                .unwrap_or_else(|| name.clone());
            format!("{}FilterArg", type_name)
        }
        _ => "StringOperatorBlock".to_string(),
    }
}
