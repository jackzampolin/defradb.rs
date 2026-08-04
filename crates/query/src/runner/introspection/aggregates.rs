use async_graphql::dynamic::*;
use schema::{CollectionVersion, FieldKind, ScalarArrayKind};
use std::collections::HashMap;

/// Build aggregate selector input types and register them for a collection.
/// Returns the types to register.
pub(super) fn build_aggregate_types_for_collection(
    collection: &CollectionVersion,
    id_to_name: &HashMap<String, String>,
) -> Vec<InputObject> {
    let coll_name = &collection.name;
    let mut types = Vec::new();

    // {Collection}__CountSelector: filter, groupBy, limit, offset
    let count_selector = InputObject::new(format!("{}__CountSelector", coll_name))
        .field(InputValue::new(
            "filter",
            TypeRef::named(format!("{}FilterArg", coll_name)),
        ))
        .field(InputValue::new(
            "groupBy",
            TypeRef::named_nn_list(format!("{}Field", coll_name)),
        ))
        .field(InputValue::new("limit", TypeRef::named("Int")))
        .field(InputValue::new("offset", TypeRef::named("Int")));
    types.push(count_selector);

    // {Collection}___version__CountSelector: limit, offset
    let version_count_selector =
        InputObject::new(format!("{}___version__CountSelector", coll_name))
            .field(InputValue::new("limit", TypeRef::named("Int")))
            .field(InputValue::new("offset", TypeRef::named("Int")));
    types.push(version_count_selector);

    // {Collection}__NumericSelector: field, filter, limit, offset, order
    // Build numeric fields enum
    let numeric_enum_name = format!("{}NumericFieldsArg", coll_name);
    // (Enum is registered separately)

    let numeric_selector = InputObject::new(format!("{}__NumericSelector", coll_name))
        .field(InputValue::new(
            "field",
            TypeRef::named_nn(&numeric_enum_name),
        ))
        .field(InputValue::new(
            "filter",
            TypeRef::named(format!("{}FilterArg", coll_name)),
        ))
        .field(InputValue::new("limit", TypeRef::named("Int")))
        .field(InputValue::new("offset", TypeRef::named("Int")))
        .field(InputValue::new(
            "order",
            TypeRef::named_list(format!("{}OrderArg", coll_name)),
        ));
    types.push(numeric_selector);

    // Per-field selectors for inline arrays
    for field in &collection.fields {
        if let FieldKind::ScalarArray(arr) = &field.kind {
            let field_name = &field.name;

            // {Collection}__{field}__CountSelector
            let filter_type = match arr {
                // Non-nillable arrays [T!] use NotNull prefix
                ScalarArrayKind::BoolArray => "NotNullBooleanFilterArg",
                ScalarArrayKind::IntArray => "NotNullIntFilterArg",
                ScalarArrayKind::Float64Array => "NotNullFloat64FilterArg",
                ScalarArrayKind::Float32Array => "NotNullFloat32FilterArg",
                ScalarArrayKind::StringArray => "NotNullStringFilterArg",
                // Nillable arrays [T] use plain filter arg
                ScalarArrayKind::NillableBoolArray => "BooleanFilterArg",
                ScalarArrayKind::NillableIntArray => "IntFilterArg",
                ScalarArrayKind::NillableFloat64Array => "Float64FilterArg",
                ScalarArrayKind::NillableFloat32Array => "Float32FilterArg",
                ScalarArrayKind::NillableStringArray => "StringFilterArg",
                _ => "StringFilterArg",
            };

            let inline_count =
                InputObject::new(format!("{}__{}__CountSelector", coll_name, field_name))
                    .field(InputValue::new("filter", TypeRef::named(filter_type)))
                    .field(InputValue::new("limit", TypeRef::named("Int")))
                    .field(InputValue::new("offset", TypeRef::named("Int")));
            types.push(inline_count);

            // Numeric arrays also get NumericSelector
            let is_numeric = matches!(
                arr,
                ScalarArrayKind::IntArray
                    | ScalarArrayKind::Float64Array
                    | ScalarArrayKind::Float32Array
                    | ScalarArrayKind::NillableIntArray
                    | ScalarArrayKind::NillableFloat64Array
                    | ScalarArrayKind::NillableFloat32Array
            );
            if is_numeric {
                let inline_numeric =
                    InputObject::new(format!("{}__{}__NumericSelector", coll_name, field_name))
                        .field(InputValue::new("filter", TypeRef::named(filter_type)))
                        .field(InputValue::new("limit", TypeRef::named("Int")))
                        .field(InputValue::new("offset", TypeRef::named("Int")))
                        .field(InputValue::new("order", TypeRef::named("Ordering")));
                types.push(inline_numeric);
            }
        }
    }

    // Per-relation-field selectors
    for field in &collection.fields {
        match &field.kind {
            FieldKind::Relation {
                collection_id,
                is_array,
            } if *is_array => {
                let related_name = id_to_name
                    .get(collection_id)
                    .cloned()
                    .unwrap_or_else(|| collection_id.clone());
                let field_name = &field.name;

                // {Collection}__{field}__CountSelector
                let rel_count =
                    InputObject::new(format!("{}__{}__CountSelector", coll_name, field_name))
                        .field(InputValue::new(
                            "filter",
                            TypeRef::named(format!("{}FilterArg", related_name)),
                        ))
                        .field(InputValue::new("limit", TypeRef::named("Int")))
                        .field(InputValue::new("offset", TypeRef::named("Int")));
                types.push(rel_count);
            }
            _ => {}
        }
    }

    // Similarity selectors for numeric array fields
    for field in &collection.fields {
        if let FieldKind::ScalarArray(arr) = &field.kind {
            let is_numeric = matches!(
                arr,
                ScalarArrayKind::IntArray
                    | ScalarArrayKind::Float64Array
                    | ScalarArrayKind::Float32Array
                    | ScalarArrayKind::NillableIntArray
                    | ScalarArrayKind::NillableFloat64Array
                    | ScalarArrayKind::NillableFloat32Array
            );
            if is_numeric {
                let vector_type = match arr {
                    ScalarArrayKind::IntArray | ScalarArrayKind::NillableIntArray => "Int",
                    ScalarArrayKind::Float64Array | ScalarArrayKind::NillableFloat64Array => {
                        "Float64"
                    }
                    ScalarArrayKind::Float32Array | ScalarArrayKind::NillableFloat32Array => {
                        "Float32"
                    }
                    _ => unreachable!(),
                };
                let similarity_selector =
                    InputObject::new(format!("{}__{}__SimilaritySelector", coll_name, field.name))
                        .field(InputValue::new(
                            "vector",
                            TypeRef::named_nn_list_nn(vector_type),
                        ));
                types.push(similarity_selector);
            }
        }
    }

    types
}

/// Build the numeric fields enum for a collection (used by NumericSelector).
pub(super) fn build_numeric_fields_enum(collection: &CollectionVersion) -> Enum {
    let type_name = format!("{}NumericFieldsArg", collection.name);
    let mut enum_type = Enum::new(&type_name);

    for field in &collection.fields {
        let is_numeric = field.kind.is_numeric();
        if is_numeric {
            enum_type = enum_type.item(EnumItem::new(&field.name));
        }
    }

    enum_type
}
