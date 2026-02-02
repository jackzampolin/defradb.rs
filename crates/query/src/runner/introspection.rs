//! GraphQL introspection support.
//!
//! This module provides introspection query execution using async-graphql.
//! Introspection queries (__schema, __type) are executed against a dynamically
//! generated schema based on the current collections.

use async_graphql::{dynamic::*, Value as GqlValue};
use schema::{CollectionVersion, FieldKind, ScalarArrayKind, ScalarKind};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::error::{QueryError, Result};

/// Build an async-graphql schema from collections for introspection.
pub fn build_introspection_schema(
    collections: &[CollectionVersion],
) -> std::result::Result<Schema, SchemaError> {
    // Build a mapping from collection ID to collection name for relation resolution
    let id_to_name: HashMap<String, String> = collections
        .iter()
        .map(|c| (c.collection_id.clone(), c.name.clone()))
        .collect();

    // Start with basic scalar types
    // Register Mutation root when collections exist so MutationInputArg types are reachable
    let mutation_name = if collections.is_empty() {
        None
    } else {
        Some("Mutation")
    };
    let mut schema_builder = Schema::build("Query", mutation_name, None);

    // Build a Query type with fields for each collection
    let mut query_type = Object::new("Query").description("Root query type");

    // Create object types for each collection and add query fields
    for collection in collections {
        // Create object type for this collection (always register for type system)
        let obj_type = build_collection_type(collection, &id_to_name);
        schema_builder = schema_builder.register(obj_type);

        // Create filter input type
        let filter_type = build_filter_input_type(collection, &id_to_name);
        schema_builder = schema_builder.register(filter_type);

        // Create order input type
        let order_type = build_order_input_type(collection, &id_to_name);
        schema_builder = schema_builder.register(order_type);

        // Create Field enum for this collection (e.g., UserField)
        let field_enum = build_field_enum(collection);
        schema_builder = schema_builder.register(field_enum);

        // Add mutation input types (needed even for embedded types since
        // non-embedded types may reference them in their mutation inputs)
        let mutation_input = build_mutation_input_type(collection);
        schema_builder = schema_builder.register(mutation_input);

        // Add aggregate selector types
        let agg_types = build_aggregate_types_for_collection(collection, &id_to_name);
        for agg_type in agg_types {
            schema_builder = schema_builder.register(agg_type);
        }

        // Add numeric fields enum
        let numeric_enum = build_numeric_fields_enum(collection);
        schema_builder = schema_builder.register(numeric_enum);

        // Embedded-only types (interface types from view SDL) are registered in the type
        // system but not as root query fields - they can only be accessed via relations.
        if collection.is_embedded_only {
            continue;
        }

        // Add query field for this collection (e.g., User)
        // Args sorted alphabetically to match Go introspection output
        let collection_name = collection.name.clone();
        query_type = query_type.field(
            Field::new(
                &collection.name,
                TypeRef::named_nn_list_nn(&collection.name),
                move |_ctx| {
                    FieldFuture::new(async move { Ok(Some(GqlValue::List(vec![]))) })
                },
            )
            .argument(InputValue::new("cid", TypeRef::named("String")))
            .argument(InputValue::new(
                "docID",
                TypeRef::named_nn_list("ID"),
            ))
            .argument(InputValue::new(
                "filter",
                TypeRef::named(format!("{}FilterArg", collection_name)),
            ))
            .argument(InputValue::new(
                "groupBy",
                TypeRef::named_list(format!("{}Field", collection_name)),
            ))
            .argument(InputValue::new("limit", TypeRef::named("Int")))
            .argument(InputValue::new("offset", TypeRef::named("Int")))
            .argument(InputValue::new(
                "order",
                TypeRef::named_list(format!("{}OrderArg", collection_name)),
            ))
            .argument(InputValue::new("showDeleted", TypeRef::named("Boolean"))),
        );

    }

    // Register Commit type (used by _version virtual field)
    schema_builder = schema_builder.register(build_commit_type());

    // Register standard scalars and filter types
    schema_builder = schema_builder
        .register(Scalar::new("DateTime"))
        .register(Scalar::new("Blob"))
        .register(Scalar::new("JSON"))
        .register(Scalar::new("Float32"))
        .register(Scalar::new("Float64"))
        .register(build_explain_enum())
        .register(build_ordering_enum())
        .register(build_id_operator_block())
        .register(build_string_operator_block())
        .register(build_int_operator_block())
        .register(build_float_operator_block())
        .register(build_float32_operator_block())
        .register(build_float64_operator_block())
        .register(build_bool_operator_block())
        .register(build_datetime_operator_block())
        // List operator blocks for inline array filters
        .register(build_not_null_int_filter_arg())
        .register(build_not_null_float64_filter_arg())
        .register(build_not_null_float32_filter_arg())
        .register(build_not_null_bool_filter_arg())
        .register(build_not_null_string_filter_arg())
        .register(build_int_filter_arg())
        .register(build_float64_filter_arg())
        .register(build_float32_filter_arg())
        .register(build_bool_filter_arg())
        .register(build_string_filter_arg())
        // List operator blocks
        .register(build_int_list_operator_block())
        .register(build_not_null_int_list_operator_block())
        .register(build_float64_list_operator_block())
        .register(build_not_null_float64_list_operator_block())
        .register(build_float32_list_operator_block())
        .register(build_not_null_float32_list_operator_block())
        .register(build_bool_list_operator_block())
        .register(build_not_null_bool_list_operator_block())
        .register(build_string_list_operator_block())
        .register(build_not_null_string_list_operator_block());

    // Add top-level aggregate fields to Query
    if !collections.is_empty() {
        // _count: takes one arg per non-embedded collection
        let mut count_field = Field::new("_count", TypeRef::named("Int"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        });
        for collection in collections {
            if collection.is_embedded_only {
                continue;
            }
            count_field = count_field.argument(InputValue::new(
                &collection.name,
                TypeRef::named(format!("{}__CountSelector", collection.name)),
            ));
        }
        query_type = query_type.field(count_field);

        // _sum, _avg: takes one arg per non-embedded collection
        for agg_name in &["_sum", "_avg"] {
            let mut agg_field = Field::new(*agg_name, TypeRef::named("Float"), |_| {
                FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
            });
            for collection in collections {
                if collection.is_embedded_only {
                    continue;
                }
                agg_field = agg_field.argument(InputValue::new(
                    &collection.name,
                    TypeRef::named(format!("{}__NumericSelector", collection.name)),
                ));
            }
            query_type = query_type.field(agg_field);
        }
    }

    // If no collections, add a placeholder field to Query (required by GraphQL spec)
    if collections.is_empty() {
        query_type = query_type.field(Field::new("_placeholder", TypeRef::named("String"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        }));
    }

    schema_builder = schema_builder.register(query_type);

    // Add mutation type if we have collections
    if !collections.is_empty() {
        let mutation_type = build_mutation_type(collections);
        schema_builder = schema_builder.register(mutation_type);
    }

    schema_builder.finish()
}

/// Build the object type for a collection.
///
/// Fields are added in alphabetical order to match Go's introspection output.
/// Aggregate fields (_count, _sum, _avg, _max, _min) have args referencing
/// selector input types, matching Go's schema generation.
fn build_collection_type(
    collection: &CollectionVersion,
    id_to_name: &HashMap<String, String>,
) -> Object {
    let mut obj = Object::new(&collection.name);
    let coll_name = &collection.name;

    // We'll collect fields as Field objects (not just name+type) so we can add args
    let mut named_fields: Vec<(String, Field)> = Vec::new();

    // Simple scalar virtual fields
    named_fields.push((
        "_docID".to_string(),
        Field::new("_docID", TypeRef::named("ID"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        }),
    ));
    named_fields.push((
        "_deleted".to_string(),
        Field::new("_deleted", TypeRef::named("Boolean"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        }),
    ));

    // _group field (no args in Go)
    named_fields.push((
        "_group".to_string(),
        Field::new("_group", TypeRef::named_list(coll_name), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        }),
    ));

    // _version field
    named_fields.push((
        "_version".to_string(),
        Field::new("_version", TypeRef::named_list("Commit"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        }),
    ));

    // Build aggregate fields with args

    // _count: takes args for _group, _version, and each inline array/relation field
    // Collect args and sort alphabetically
    {
        let mut count_args: Vec<(String, InputValue)> = Vec::new();
        count_args.push((
            "_group".to_string(),
            InputValue::new(
                "_group",
                TypeRef::named(format!("{}__CountSelector", coll_name)),
            ),
        ));
        count_args.push((
            "_version".to_string(),
            InputValue::new(
                "_version",
                TypeRef::named(format!("{}___version__CountSelector", coll_name)),
            ),
        ));
        for field in &collection.fields {
            match &field.kind {
                FieldKind::ScalarArray(_) | FieldKind::Relation { is_array: true, .. } => {
                    count_args.push((
                        field.name.clone(),
                        InputValue::new(
                            &field.name,
                            TypeRef::named(format!(
                                "{}__{}__CountSelector",
                                coll_name, field.name
                            )),
                        ),
                    ));
                }
                _ => {}
            }
        }
        count_args.sort_by(|a, b| a.0.cmp(&b.0));
        let mut count_field = Field::new("_count", TypeRef::named("Int"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        });
        for (_, arg) in count_args {
            count_field = count_field.argument(arg);
        }
        named_fields.push(("_count".to_string(), count_field));
    }

    // _sum, _avg, _max, _min: take args for _group and each numeric inline array field
    for (agg_name, agg_type) in &[
        ("_sum", "Float"),
        ("_avg", "Float"),
        ("_max", "Float"),
        ("_min", "Float"),
    ] {
        let mut agg_args: Vec<(String, InputValue)> = Vec::new();
        agg_args.push((
            "_group".to_string(),
            InputValue::new(
                "_group",
                TypeRef::named(format!("{}__NumericSelector", coll_name)),
            ),
        ));
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
                    agg_args.push((
                        field.name.clone(),
                        InputValue::new(
                            &field.name,
                            TypeRef::named(format!(
                                "{}__{}__NumericSelector",
                                coll_name, field.name
                            )),
                        ),
                    ));
                }
            }
        }
        agg_args.sort_by(|a, b| a.0.cmp(&b.0));
        let mut agg_field = Field::new(*agg_name, TypeRef::named(*agg_type), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        });
        for (_, arg) in agg_args {
            agg_field = agg_field.argument(arg);
        }
        named_fields.push((agg_name.to_string(), agg_field));
    }

    // _similarity: takes args for each numeric array field's similarity selector
    {
        let mut sim_args: Vec<(String, InputValue)> = Vec::new();
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
                    sim_args.push((
                        field.name.clone(),
                        InputValue::new(
                            &field.name,
                            TypeRef::named(format!(
                                "{}__{}__SimilaritySelector",
                                coll_name, field.name
                            )),
                        ),
                    ));
                }
            }
        }
        sim_args.sort_by(|a, b| a.0.cmp(&b.0));
        let mut similarity_field = Field::new("_similarity", TypeRef::named("Float"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        });
        for (_, arg) in sim_args {
            similarity_field = similarity_field.argument(arg);
        }
        named_fields.push(("_similarity".to_string(), similarity_field));
    }

    // Add user-defined fields
    for field in &collection.fields {
        if field.name == "_docID" {
            continue;
        }
        let type_ref = field_kind_to_type_ref(&field.kind, id_to_name, &collection.name);
        named_fields.push((
            field.name.clone(),
            Field::new(&field.name, type_ref, |_| {
                FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
            }),
        ));
    }

    // Sort alphabetically to match Go introspection output
    named_fields.sort_by(|a, b| a.0.cmp(&b.0));

    // Add sorted fields to object
    for (_name, field) in named_fields {
        obj = obj.field(field);
    }

    obj
}

/// Build the Commit object type (used by _version virtual field).
fn build_commit_type() -> Object {
    let mut obj = Object::new("Commit");
    obj = obj.field(Field::new("cid", TypeRef::named("String"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));
    obj = obj.field(Field::new("height", TypeRef::named("Int"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));
    obj = obj.field(Field::new("delta", TypeRef::named("String"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));
    obj = obj.field(Field::new("docID", TypeRef::named("String"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));
    obj = obj.field(Field::new("collectionID", TypeRef::named("Int"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));
    obj = obj.field(Field::new("fieldName", TypeRef::named("String"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));
    obj = obj.field(Field::new("fieldID", TypeRef::named("String"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));
    obj = obj.field(Field::new("links", TypeRef::named_list("Commit"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));
    obj
}

/// Convert field kind to async-graphql TypeRef.
/// `current_name` is the name of the collection being built (for self-reference resolution).
fn field_kind_to_type_ref(
    kind: &FieldKind,
    id_to_name: &HashMap<String, String>,
    current_name: &str,
) -> TypeRef {
    match kind {
        FieldKind::Scalar(scalar) => TypeRef::named(scalar_to_gql_name(scalar)),
        FieldKind::ScalarArray(array) => {
            let element_type = scalar_to_gql_name(&array.element_kind());
            TypeRef::named_list(element_type)
        }
        FieldKind::Relation {
            collection_id,
            is_array,
        } => {
            let type_name = id_to_name
                .get(collection_id)
                .cloned()
                .unwrap_or_else(|| collection_id.clone());
            if *is_array {
                TypeRef::named_list(type_name)
            } else {
                TypeRef::named(type_name)
            }
        }
        FieldKind::SelfRef {
            relative_id,
            is_array,
        } => {
            // Empty relative_id means self-reference within the same collection
            let type_name = if relative_id.is_empty() {
                current_name.to_string()
            } else {
                id_to_name
                    .get(relative_id)
                    .cloned()
                    .unwrap_or_else(|| relative_id.clone())
            };
            if *is_array {
                TypeRef::named_list(type_name)
            } else {
                TypeRef::named(type_name)
            }
        }
        FieldKind::Named { name, is_array } => {
            let type_name = id_to_name
                .get(name)
                .cloned()
                .unwrap_or_else(|| name.clone());
            if *is_array {
                TypeRef::named_list(type_name)
            } else {
                TypeRef::named(type_name)
            }
        }
    }
}

/// Convert scalar kind to GraphQL type name.
/// Go registers Float32 and Float64 as separate scalar types.
/// Go maps unqualified `Float` in SDL to `Float64`.
fn scalar_to_gql_name(scalar: &ScalarKind) -> &'static str {
    match scalar {
        ScalarKind::None => "String",
        ScalarKind::DocID => "ID",
        ScalarKind::Bool => "Boolean",
        ScalarKind::Int => "Int",
        ScalarKind::Float64 => "Float64",
        ScalarKind::Float32 => "Float32",
        ScalarKind::DateTime => "DateTime",
        ScalarKind::String => "String",
        ScalarKind::Blob => "Blob",
        ScalarKind::Json => "JSON",
    }
}

/// Build filter input type for a collection.
fn build_filter_input_type(
    collection: &CollectionVersion,
    id_to_name: &HashMap<String, String>,
) -> InputObject {
    let type_name = format!("{}FilterArg", collection.name);
    let mut fields: Vec<(String, InputValue)> = Vec::new();

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
        let filter_type = get_filter_type_for_field(&field.kind, id_to_name, &collection.name);
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
fn build_order_input_type(
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

    // Add order fields for each collection field
    for field in &collection.fields {
        if field.name == "_docID" {
            continue;
        }
        fields.push((
            field.name.clone(),
            InputValue::new(&field.name, TypeRef::named("Ordering")),
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

/// Build Field enum for a collection (e.g., UserField).
fn build_field_enum(collection: &CollectionVersion) -> Enum {
    let type_name = format!("{}Field", collection.name);
    let mut enum_type = Enum::new(&type_name);

    // Add _docID explicitly
    enum_type = enum_type.item(EnumItem::new("_docID"));

    // Add enum values for each field
    for field in &collection.fields {
        // Skip _docID since we add it explicitly above
        if field.name == "_docID" {
            continue;
        }
        enum_type = enum_type.item(EnumItem::new(&field.name));
    }

    enum_type
}

/// Build mutation input type for a collection.
fn build_mutation_input_type(collection: &CollectionVersion) -> InputObject {
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
fn field_kind_to_input_type_ref(kind: &FieldKind) -> TypeRef {
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
    }
}

/// Build the Mutation type.
fn build_mutation_type(collections: &[CollectionVersion]) -> Object {
    let mut mutation = Object::new("Mutation").description("Root mutation type");

    for collection in collections {
        // Embedded-only types don't have mutation fields
        if collection.is_embedded_only {
            continue;
        }

        let coll_name = collection.name.clone();
        let input_type = format!("{}MutationInputArg", coll_name);

        // create_<Collection>
        mutation = mutation.field(
            Field::new(
                format!("create_{}", coll_name),
                TypeRef::named_nn(&coll_name),
                |_| FieldFuture::new(async { Ok(Some(GqlValue::Null)) }),
            )
            .argument(InputValue::new("input", TypeRef::named_nn(&input_type))),
        );

        // update_<Collection>
        let update_coll_name = coll_name.clone();
        let update_input_type = input_type.clone();
        mutation = mutation.field(
            Field::new(
                format!("update_{}", coll_name),
                TypeRef::named_nn_list_nn(&update_coll_name),
                |_| FieldFuture::new(async { Ok(Some(GqlValue::Null)) }),
            )
            .argument(InputValue::new("docID", TypeRef::named("ID")))
            .argument(InputValue::new("docIDs", TypeRef::named_list("ID")))
            .argument(InputValue::new(
                "input",
                TypeRef::named_nn(&update_input_type),
            )),
        );

        // delete_<Collection>
        let del_coll_name = coll_name.clone();
        mutation = mutation.field(
            Field::new(
                format!("delete_{}", coll_name),
                TypeRef::named_nn_list_nn(&del_coll_name),
                |_| FieldFuture::new(async { Ok(Some(GqlValue::Null)) }),
            )
            .argument(InputValue::new("docID", TypeRef::named("ID")))
            .argument(InputValue::new("docIDs", TypeRef::named_list("ID")))
            .argument(InputValue::new(
                "filter",
                TypeRef::named(format!("{}FilterArg", del_coll_name)),
            )),
        );
    }

    mutation
}

/// Build the ExplainType enum.
fn build_explain_enum() -> Enum {
    Enum::new("ExplainType")
        .description(
            "ExplainType is an enum selecting the type of explanation done by the @explain directive.",
        )
        .item(
            EnumItem::new("simple")
                .description("Simple explanation - dump of the plan graph."),
        )
        .item(EnumItem::new("execute").description(
            "Deeper explanation - insights gathered by executing the plan graph.",
        ))
        .item(
            EnumItem::new("debug")
                .description("Like simple explain, but more verbose nodes (no attributes)."),
        )
}

/// Build the Ordering enum.
fn build_ordering_enum() -> Enum {
    Enum::new("Ordering")
        .item(EnumItem::new("ASC"))
        .item(EnumItem::new("DESC"))
}

/// Build ID operator block input type.
fn build_id_operator_block() -> InputObject {
    InputObject::new("IDOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("ID")))
        .field(InputValue::new("_in", TypeRef::named_list("ID")))
        .field(InputValue::new("_neq", TypeRef::named("ID")))
        .field(InputValue::new("_nin", TypeRef::named_list("ID")))
}

/// Build String operator block input type.
fn build_string_operator_block() -> InputObject {
    InputObject::new("StringOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("String")))
        .field(InputValue::new("_ilike", TypeRef::named("String")))
        .field(InputValue::new("_in", TypeRef::named_list("String")))
        .field(InputValue::new("_like", TypeRef::named("String")))
        .field(InputValue::new("_neq", TypeRef::named("String")))
        .field(InputValue::new("_nilike", TypeRef::named("String")))
        .field(InputValue::new("_nin", TypeRef::named_list("String")))
        .field(InputValue::new("_nlike", TypeRef::named("String")))
}

/// Build Int operator block input type.
fn build_int_operator_block() -> InputObject {
    InputObject::new("IntOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Int")))
        .field(InputValue::new("_geq", TypeRef::named("Int")))
        .field(InputValue::new("_gt", TypeRef::named("Int")))
        .field(InputValue::new("_in", TypeRef::named_list("Int")))
        .field(InputValue::new("_leq", TypeRef::named("Int")))
        .field(InputValue::new("_lt", TypeRef::named("Int")))
        .field(InputValue::new("_neq", TypeRef::named("Int")))
        .field(InputValue::new("_nin", TypeRef::named_list("Int")))
}

/// Build Float operator block input type.
fn build_float_operator_block() -> InputObject {
    InputObject::new("FloatOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Float")))
        .field(InputValue::new("_geq", TypeRef::named("Float")))
        .field(InputValue::new("_gt", TypeRef::named("Float")))
        .field(InputValue::new("_in", TypeRef::named_list("Float")))
        .field(InputValue::new("_leq", TypeRef::named("Float")))
        .field(InputValue::new("_lt", TypeRef::named("Float")))
        .field(InputValue::new("_neq", TypeRef::named("Float")))
        .field(InputValue::new("_nin", TypeRef::named_list("Float")))
}

/// Build Boolean operator block input type.
fn build_bool_operator_block() -> InputObject {
    InputObject::new("BooleanOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Boolean")))
        .field(InputValue::new("_in", TypeRef::named_list("Boolean")))
        .field(InputValue::new("_neq", TypeRef::named("Boolean")))
        .field(InputValue::new("_nin", TypeRef::named_list("Boolean")))
}

/// Build DateTime operator block input type.
fn build_datetime_operator_block() -> InputObject {
    InputObject::new("DateTimeOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("DateTime")))
        .field(InputValue::new("_geq", TypeRef::named("DateTime")))
        .field(InputValue::new("_gt", TypeRef::named("DateTime")))
        .field(InputValue::new("_in", TypeRef::named_list("DateTime")))
        .field(InputValue::new("_leq", TypeRef::named("DateTime")))
        .field(InputValue::new("_lt", TypeRef::named("DateTime")))
        .field(InputValue::new("_neq", TypeRef::named("DateTime")))
        .field(InputValue::new("_nin", TypeRef::named_list("DateTime")))
}

/// Get the filter type name for a field kind.
fn get_filter_type_for_field(
    kind: &FieldKind,
    id_to_name: &HashMap<String, String>,
    current_name: &str,
) -> String {
    match kind {
        FieldKind::Scalar(scalar) => match scalar {
            ScalarKind::String | ScalarKind::DocID => "StringOperatorBlock".to_string(),
            ScalarKind::Int => "IntOperatorBlock".to_string(),
            ScalarKind::Float64 => "Float64OperatorBlock".to_string(),
            ScalarKind::Float32 => "Float32OperatorBlock".to_string(),
            ScalarKind::Bool => "BooleanOperatorBlock".to_string(),
            ScalarKind::DateTime => "DateTimeOperatorBlock".to_string(),
            ScalarKind::Blob | ScalarKind::Json | ScalarKind::None => {
                "StringOperatorBlock".to_string()
            }
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
    }
}

/// Build Float32 operator block input type.
fn build_float32_operator_block() -> InputObject {
    InputObject::new("Float32OperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Float32")))
        .field(InputValue::new("_geq", TypeRef::named("Float32")))
        .field(InputValue::new("_gt", TypeRef::named("Float32")))
        .field(InputValue::new("_in", TypeRef::named_list("Float32")))
        .field(InputValue::new("_leq", TypeRef::named("Float32")))
        .field(InputValue::new("_lt", TypeRef::named("Float32")))
        .field(InputValue::new("_neq", TypeRef::named("Float32")))
        .field(InputValue::new("_nin", TypeRef::named_list("Float32")))
}

/// Build Float64 operator block input type.
fn build_float64_operator_block() -> InputObject {
    InputObject::new("Float64OperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Float64")))
        .field(InputValue::new("_geq", TypeRef::named("Float64")))
        .field(InputValue::new("_gt", TypeRef::named("Float64")))
        .field(InputValue::new("_in", TypeRef::named_list("Float64")))
        .field(InputValue::new("_leq", TypeRef::named("Float64")))
        .field(InputValue::new("_lt", TypeRef::named("Float64")))
        .field(InputValue::new("_neq", TypeRef::named("Float64")))
        .field(InputValue::new("_nin", TypeRef::named_list("Float64")))
}

// --- Inline array filter arg types ---

fn build_not_null_int_filter_arg() -> InputObject {
    InputObject::new("NotNullIntFilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("NotNullIntFilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("Int")))
        .field(InputValue::new("_geq", TypeRef::named("Int")))
        .field(InputValue::new("_gt", TypeRef::named("Int")))
        .field(InputValue::new("_in", TypeRef::named_list("Int")))
        .field(InputValue::new("_leq", TypeRef::named("Int")))
        .field(InputValue::new("_lt", TypeRef::named("Int")))
        .field(InputValue::new("_neq", TypeRef::named("Int")))
        .field(InputValue::new("_nin", TypeRef::named_list("Int")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("NotNullIntFilterArg"),
        ))
}

fn build_int_filter_arg() -> InputObject {
    InputObject::new("IntFilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("IntFilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("Int")))
        .field(InputValue::new("_geq", TypeRef::named("Int")))
        .field(InputValue::new("_gt", TypeRef::named("Int")))
        .field(InputValue::new("_in", TypeRef::named_list("Int")))
        .field(InputValue::new("_leq", TypeRef::named("Int")))
        .field(InputValue::new("_lt", TypeRef::named("Int")))
        .field(InputValue::new("_neq", TypeRef::named("Int")))
        .field(InputValue::new("_nin", TypeRef::named_list("Int")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("IntFilterArg"),
        ))
}

fn build_not_null_float64_filter_arg() -> InputObject {
    InputObject::new("NotNullFloat64FilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("NotNullFloat64FilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("Float64")))
        .field(InputValue::new("_geq", TypeRef::named("Float64")))
        .field(InputValue::new("_gt", TypeRef::named("Float64")))
        .field(InputValue::new("_in", TypeRef::named_list("Float64")))
        .field(InputValue::new("_leq", TypeRef::named("Float64")))
        .field(InputValue::new("_lt", TypeRef::named("Float64")))
        .field(InputValue::new("_neq", TypeRef::named("Float64")))
        .field(InputValue::new("_nin", TypeRef::named_list("Float64")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("NotNullFloat64FilterArg"),
        ))
}

fn build_float64_filter_arg() -> InputObject {
    InputObject::new("Float64FilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("Float64FilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("Float64")))
        .field(InputValue::new("_geq", TypeRef::named("Float64")))
        .field(InputValue::new("_gt", TypeRef::named("Float64")))
        .field(InputValue::new("_in", TypeRef::named_list("Float64")))
        .field(InputValue::new("_leq", TypeRef::named("Float64")))
        .field(InputValue::new("_lt", TypeRef::named("Float64")))
        .field(InputValue::new("_neq", TypeRef::named("Float64")))
        .field(InputValue::new("_nin", TypeRef::named_list("Float64")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("Float64FilterArg"),
        ))
}

fn build_not_null_float32_filter_arg() -> InputObject {
    InputObject::new("NotNullFloat32FilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("NotNullFloat32FilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("Float32")))
        .field(InputValue::new("_geq", TypeRef::named("Float32")))
        .field(InputValue::new("_gt", TypeRef::named("Float32")))
        .field(InputValue::new("_in", TypeRef::named_list("Float32")))
        .field(InputValue::new("_leq", TypeRef::named("Float32")))
        .field(InputValue::new("_lt", TypeRef::named("Float32")))
        .field(InputValue::new("_neq", TypeRef::named("Float32")))
        .field(InputValue::new("_nin", TypeRef::named_list("Float32")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("NotNullFloat32FilterArg"),
        ))
}

fn build_float32_filter_arg() -> InputObject {
    InputObject::new("Float32FilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("Float32FilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("Float32")))
        .field(InputValue::new("_geq", TypeRef::named("Float32")))
        .field(InputValue::new("_gt", TypeRef::named("Float32")))
        .field(InputValue::new("_in", TypeRef::named_list("Float32")))
        .field(InputValue::new("_leq", TypeRef::named("Float32")))
        .field(InputValue::new("_lt", TypeRef::named("Float32")))
        .field(InputValue::new("_neq", TypeRef::named("Float32")))
        .field(InputValue::new("_nin", TypeRef::named_list("Float32")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("Float32FilterArg"),
        ))
}

fn build_not_null_bool_filter_arg() -> InputObject {
    InputObject::new("NotNullBooleanFilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("NotNullBooleanFilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("Boolean")))
        .field(InputValue::new("_in", TypeRef::named_list("Boolean")))
        .field(InputValue::new("_neq", TypeRef::named("Boolean")))
        .field(InputValue::new("_nin", TypeRef::named_list("Boolean")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("NotNullBooleanFilterArg"),
        ))
}

fn build_bool_filter_arg() -> InputObject {
    InputObject::new("BooleanFilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("BooleanFilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("Boolean")))
        .field(InputValue::new("_in", TypeRef::named_list("Boolean")))
        .field(InputValue::new("_neq", TypeRef::named("Boolean")))
        .field(InputValue::new("_nin", TypeRef::named_list("Boolean")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("BooleanFilterArg"),
        ))
}

fn build_not_null_string_filter_arg() -> InputObject {
    InputObject::new("NotNullStringFilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("NotNullStringFilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("String")))
        .field(InputValue::new("_ilike", TypeRef::named("String")))
        .field(InputValue::new("_in", TypeRef::named_list("String")))
        .field(InputValue::new("_like", TypeRef::named("String")))
        .field(InputValue::new("_neq", TypeRef::named("String")))
        .field(InputValue::new("_nilike", TypeRef::named("String")))
        .field(InputValue::new("_nin", TypeRef::named_list("String")))
        .field(InputValue::new("_nlike", TypeRef::named("String")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("NotNullStringFilterArg"),
        ))
}

fn build_string_filter_arg() -> InputObject {
    InputObject::new("StringFilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_nn_list("StringFilterArg"),
        ))
        .field(InputValue::new("_eq", TypeRef::named("String")))
        .field(InputValue::new("_ilike", TypeRef::named("String")))
        .field(InputValue::new("_in", TypeRef::named_list("String")))
        .field(InputValue::new("_like", TypeRef::named("String")))
        .field(InputValue::new("_neq", TypeRef::named("String")))
        .field(InputValue::new("_nilike", TypeRef::named("String")))
        .field(InputValue::new("_nin", TypeRef::named_list("String")))
        .field(InputValue::new("_nlike", TypeRef::named("String")))
        .field(InputValue::new(
            "_or",
            TypeRef::named_nn_list("StringFilterArg"),
        ))
}

// --- List operator blocks ---

fn build_int_list_operator_block() -> InputObject {
    InputObject::new("IntListOperatorBlock")
        .field(InputValue::new("_any", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_all", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_none", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_not_null_int_list_operator_block() -> InputObject {
    InputObject::new("NotNullIntListOperatorBlock")
        .field(InputValue::new("_any", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_all", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_none", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_float64_list_operator_block() -> InputObject {
    InputObject::new("Float64ListOperatorBlock")
        .field(InputValue::new(
            "_any",
            TypeRef::named("Float64OperatorBlock"),
        ))
        .field(InputValue::new(
            "_all",
            TypeRef::named("Float64OperatorBlock"),
        ))
        .field(InputValue::new(
            "_none",
            TypeRef::named("Float64OperatorBlock"),
        ))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_not_null_float64_list_operator_block() -> InputObject {
    InputObject::new("NotNullFloat64ListOperatorBlock")
        .field(InputValue::new(
            "_any",
            TypeRef::named("Float64OperatorBlock"),
        ))
        .field(InputValue::new(
            "_all",
            TypeRef::named("Float64OperatorBlock"),
        ))
        .field(InputValue::new(
            "_none",
            TypeRef::named("Float64OperatorBlock"),
        ))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_float32_list_operator_block() -> InputObject {
    InputObject::new("Float32ListOperatorBlock")
        .field(InputValue::new(
            "_any",
            TypeRef::named("Float32OperatorBlock"),
        ))
        .field(InputValue::new(
            "_all",
            TypeRef::named("Float32OperatorBlock"),
        ))
        .field(InputValue::new(
            "_none",
            TypeRef::named("Float32OperatorBlock"),
        ))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_not_null_float32_list_operator_block() -> InputObject {
    InputObject::new("NotNullFloat32ListOperatorBlock")
        .field(InputValue::new(
            "_any",
            TypeRef::named("Float32OperatorBlock"),
        ))
        .field(InputValue::new(
            "_all",
            TypeRef::named("Float32OperatorBlock"),
        ))
        .field(InputValue::new(
            "_none",
            TypeRef::named("Float32OperatorBlock"),
        ))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_bool_list_operator_block() -> InputObject {
    InputObject::new("BooleanListOperatorBlock")
        .field(InputValue::new(
            "_any",
            TypeRef::named("BooleanOperatorBlock"),
        ))
        .field(InputValue::new(
            "_all",
            TypeRef::named("BooleanOperatorBlock"),
        ))
        .field(InputValue::new(
            "_none",
            TypeRef::named("BooleanOperatorBlock"),
        ))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_not_null_bool_list_operator_block() -> InputObject {
    InputObject::new("NotNullBooleanListOperatorBlock")
        .field(InputValue::new(
            "_any",
            TypeRef::named("BooleanOperatorBlock"),
        ))
        .field(InputValue::new(
            "_all",
            TypeRef::named("BooleanOperatorBlock"),
        ))
        .field(InputValue::new(
            "_none",
            TypeRef::named("BooleanOperatorBlock"),
        ))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_string_list_operator_block() -> InputObject {
    InputObject::new("StringListOperatorBlock")
        .field(InputValue::new(
            "_any",
            TypeRef::named("StringOperatorBlock"),
        ))
        .field(InputValue::new(
            "_all",
            TypeRef::named("StringOperatorBlock"),
        ))
        .field(InputValue::new(
            "_none",
            TypeRef::named("StringOperatorBlock"),
        ))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

fn build_not_null_string_list_operator_block() -> InputObject {
    InputObject::new("NotNullStringListOperatorBlock")
        .field(InputValue::new(
            "_any",
            TypeRef::named("StringOperatorBlock"),
        ))
        .field(InputValue::new(
            "_all",
            TypeRef::named("StringOperatorBlock"),
        ))
        .field(InputValue::new(
            "_none",
            TypeRef::named("StringOperatorBlock"),
        ))
        .field(InputValue::new("_count", TypeRef::named("IntOperatorBlock")))
}

// --- Aggregate selector types ---

/// Build aggregate selector input types and register them for a collection.
/// Returns the types to register.
fn build_aggregate_types_for_collection(
    collection: &CollectionVersion,
    id_to_name: &HashMap<String, String>,
) -> Vec<InputObject> {
    let coll_name = &collection.name;
    let mut types = Vec::new();

    // {Collection}__CountSelector: filter, limit, offset
    let count_selector = InputObject::new(format!("{}__CountSelector", coll_name))
        .field(InputValue::new(
            "filter",
            TypeRef::named(format!("{}FilterArg", coll_name)),
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
                let similarity_selector = InputObject::new(format!(
                    "{}__{}__SimilaritySelector",
                    coll_name, field.name
                ))
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
fn build_numeric_fields_enum(collection: &CollectionVersion) -> Enum {
    let type_name = format!("{}NumericFieldsArg", collection.name);
    let mut enum_type = Enum::new(&type_name);

    for field in &collection.fields {
        let is_numeric = match &field.kind {
            FieldKind::Scalar(ScalarKind::Int)
            | FieldKind::Scalar(ScalarKind::Float32)
            | FieldKind::Scalar(ScalarKind::Float64) => true,
            _ => false,
        };
        if is_numeric {
            enum_type = enum_type.item(EnumItem::new(&field.name));
        }
    }

    enum_type
}

/// Execute an introspection query against the schema.
pub async fn execute_introspection(
    collections: Vec<CollectionVersion>,
    query: &str,
) -> Result<JsonValue> {
    // Build schema from collections
    let schema = build_introspection_schema(&collections)
        .map_err(|e| QueryError::introspection(format!("failed to build schema: {}", e)))?;

    // Execute the query
    let request = async_graphql::Request::new(query);
    let response = schema.execute(request).await;

    // Check for errors
    if !response.errors.is_empty() {
        let error_messages: Vec<String> =
            response.errors.iter().map(|e| e.message.clone()).collect();
        return Err(QueryError::introspection(error_messages.join(", ")));
    }

    // Convert response to JSON
    let json = serde_json::to_value(&response.data)
        .map_err(|e| QueryError::introspection(format!("failed to serialize response: {}", e)))?;

    Ok(json)
}
