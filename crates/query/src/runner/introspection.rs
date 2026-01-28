//! GraphQL introspection support.
//!
//! This module provides introspection query execution using async-graphql.
//! Introspection queries (__schema, __type) are executed against a dynamically
//! generated schema based on the current collections.

use async_graphql::{dynamic::*, Value as GqlValue};
use schema::{CollectionVersion, FieldKind, ScalarKind};
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
    let mut schema_builder = Schema::build("Query", None, None);

    // Build a Query type with fields for each collection
    let mut query_type = Object::new("Query").description("Root query type");

    // Create object types for each collection and add query fields
    for collection in collections {
        // Create object type for this collection
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

        // Add query field for this collection (e.g., User)
        let collection_name = collection.name.clone();
        query_type = query_type.field(
            Field::new(
                &collection.name,
                TypeRef::named_nn_list_nn(&collection.name),
                move |_ctx| {
                    // Introspection doesn't execute actual queries, just returns type info
                    FieldFuture::new(async move { Ok(Some(GqlValue::List(vec![]))) })
                },
            )
            .argument(InputValue::new(
                "filter",
                TypeRef::named(format!("{}FilterArg", collection_name)),
            ))
            .argument(InputValue::new(
                "order",
                TypeRef::named(format!("{}OrderArg", collection_name)),
            ))
            .argument(InputValue::new("limit", TypeRef::named("Int")))
            .argument(InputValue::new("offset", TypeRef::named("Int")))
            .argument(InputValue::new("docID", TypeRef::named("ID")))
            .argument(InputValue::new("docIDs", TypeRef::named_list("ID")))
            .argument(InputValue::new(
                "groupBy",
                TypeRef::named_list(format!("{}Field", collection_name)),
            ))
            .argument(InputValue::new("showDeleted", TypeRef::named("Boolean")))
            .argument(InputValue::new("cid", TypeRef::named("String"))),
        );

        // Add mutation input types
        let mutation_input = build_mutation_input_type(collection);
        schema_builder = schema_builder.register(mutation_input);
    }

    // Register standard scalars and filter types
    schema_builder = schema_builder
        .register(Scalar::new("DateTime"))
        .register(Scalar::new("Blob"))
        .register(Scalar::new("JSON"))
        .register(build_explain_enum())
        .register(build_ordering_enum())
        .register(build_id_operator_block())
        .register(build_string_operator_block())
        .register(build_int_operator_block())
        .register(build_float_operator_block())
        .register(build_bool_operator_block())
        .register(build_datetime_operator_block());

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
fn build_collection_type(
    collection: &CollectionVersion,
    id_to_name: &HashMap<String, String>,
) -> Object {
    let mut obj = Object::new(&collection.name);

    // Add _docID field (always present)
    obj = obj.field(Field::new("_docID", TypeRef::named_nn("ID"), |_| {
        FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
    }));

    // Add fields from collection definition
    for field in &collection.fields {
        // Skip _docID since we add it explicitly above
        if field.name == "_docID" {
            continue;
        }

        let type_ref = field_kind_to_type_ref(&field.kind, id_to_name);

        obj = obj.field(Field::new(&field.name, type_ref, |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        }));
    }

    obj
}

/// Convert field kind to async-graphql TypeRef.
fn field_kind_to_type_ref(kind: &FieldKind, id_to_name: &HashMap<String, String>) -> TypeRef {
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
            // Resolve collection ID to name
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
            // Resolve relative ID to name
            let type_name = id_to_name
                .get(relative_id)
                .cloned()
                .unwrap_or_else(|| relative_id.clone());
            if *is_array {
                TypeRef::named_list(type_name)
            } else {
                TypeRef::named(type_name)
            }
        }
        FieldKind::Named { name, is_array } => {
            // Named references might also be IDs that need resolution
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
fn scalar_to_gql_name(scalar: &ScalarKind) -> &'static str {
    match scalar {
        ScalarKind::None => "String",
        ScalarKind::DocID => "ID",
        ScalarKind::Bool => "Boolean",
        ScalarKind::Int => "Int",
        ScalarKind::Float64 | ScalarKind::Float32 => "Float",
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
    let mut input = InputObject::new(&type_name);

    // Add _and, _or, _not logical operators
    input = input
        .field(InputValue::new("_and", TypeRef::named_list(&type_name)))
        .field(InputValue::new("_or", TypeRef::named_list(&type_name)))
        .field(InputValue::new("_not", TypeRef::named(&type_name)));

    // Add _docID filter
    input = input.field(InputValue::new("_docID", TypeRef::named("IDOperatorBlock")));

    // Add filter fields for each collection field
    for field in &collection.fields {
        // Skip _docID since we add it explicitly above
        if field.name == "_docID" {
            continue;
        }
        let filter_type = get_filter_type_for_field(&field.kind, id_to_name);
        input = input.field(InputValue::new(&field.name, TypeRef::named(&filter_type)));
    }

    input
}

/// Build order input type for a collection.
fn build_order_input_type(
    collection: &CollectionVersion,
    _id_to_name: &HashMap<String, String>,
) -> InputObject {
    let type_name = format!("{}OrderArg", collection.name);
    let mut input = InputObject::new(&type_name);

    // Add _docID order
    input = input.field(InputValue::new("_docID", TypeRef::named("Ordering")));

    // Add order fields for each collection field
    for field in &collection.fields {
        // Skip _docID since we add it explicitly above
        if field.name == "_docID" {
            continue;
        }
        input = input.field(InputValue::new(&field.name, TypeRef::named("Ordering")));
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
        .description("The type of explanation to provide")
        .item(
            EnumItem::new("simple")
                .description("Simple explanation showing query plan structure without execution"),
        )
        .item(EnumItem::new("execute").description(
            "Execute the query and return both the plan structure and execution metrics",
        ))
        .item(
            EnumItem::new("debug")
                .description("Debug mode showing all plan nodes including internal ones"),
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
        .field(InputValue::new("_ne", TypeRef::named("ID")))
        .field(InputValue::new("_in", TypeRef::named_list("ID")))
        .field(InputValue::new("_nin", TypeRef::named_list("ID")))
}

/// Build String operator block input type.
fn build_string_operator_block() -> InputObject {
    InputObject::new("StringOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("String")))
        .field(InputValue::new("_ne", TypeRef::named("String")))
        .field(InputValue::new("_in", TypeRef::named_list("String")))
        .field(InputValue::new("_nin", TypeRef::named_list("String")))
        .field(InputValue::new("_like", TypeRef::named("String")))
        .field(InputValue::new("_nlike", TypeRef::named("String")))
        .field(InputValue::new("_ilike", TypeRef::named("String")))
        .field(InputValue::new("_nilike", TypeRef::named("String")))
}

/// Build Int operator block input type.
fn build_int_operator_block() -> InputObject {
    InputObject::new("IntOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Int")))
        .field(InputValue::new("_ne", TypeRef::named("Int")))
        .field(InputValue::new("_gt", TypeRef::named("Int")))
        .field(InputValue::new("_ge", TypeRef::named("Int")))
        .field(InputValue::new("_lt", TypeRef::named("Int")))
        .field(InputValue::new("_le", TypeRef::named("Int")))
        .field(InputValue::new("_in", TypeRef::named_list("Int")))
        .field(InputValue::new("_nin", TypeRef::named_list("Int")))
}

/// Build Float operator block input type.
fn build_float_operator_block() -> InputObject {
    InputObject::new("FloatOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Float")))
        .field(InputValue::new("_ne", TypeRef::named("Float")))
        .field(InputValue::new("_gt", TypeRef::named("Float")))
        .field(InputValue::new("_ge", TypeRef::named("Float")))
        .field(InputValue::new("_lt", TypeRef::named("Float")))
        .field(InputValue::new("_le", TypeRef::named("Float")))
        .field(InputValue::new("_in", TypeRef::named_list("Float")))
        .field(InputValue::new("_nin", TypeRef::named_list("Float")))
}

/// Build Boolean operator block input type.
fn build_bool_operator_block() -> InputObject {
    InputObject::new("BooleanOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Boolean")))
        .field(InputValue::new("_ne", TypeRef::named("Boolean")))
}

/// Build DateTime operator block input type.
fn build_datetime_operator_block() -> InputObject {
    InputObject::new("DateTimeOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("DateTime")))
        .field(InputValue::new("_ne", TypeRef::named("DateTime")))
        .field(InputValue::new("_gt", TypeRef::named("DateTime")))
        .field(InputValue::new("_ge", TypeRef::named("DateTime")))
        .field(InputValue::new("_lt", TypeRef::named("DateTime")))
        .field(InputValue::new("_le", TypeRef::named("DateTime")))
}

/// Get the filter type name for a field kind.
fn get_filter_type_for_field(kind: &FieldKind, id_to_name: &HashMap<String, String>) -> String {
    match kind {
        FieldKind::Scalar(scalar) => match scalar {
            ScalarKind::String | ScalarKind::DocID => "StringOperatorBlock".to_string(),
            ScalarKind::Int => "IntOperatorBlock".to_string(),
            ScalarKind::Float64 | ScalarKind::Float32 => "FloatOperatorBlock".to_string(),
            ScalarKind::Bool => "BooleanOperatorBlock".to_string(),
            ScalarKind::DateTime => "DateTimeOperatorBlock".to_string(),
            ScalarKind::Blob | ScalarKind::Json | ScalarKind::None => {
                "StringOperatorBlock".to_string()
            }
        },
        FieldKind::ScalarArray(_) => "StringOperatorBlock".to_string(),
        FieldKind::Relation { collection_id, .. } => {
            // For relations, use the related collection's filter type
            let type_name = id_to_name
                .get(collection_id)
                .cloned()
                .unwrap_or_else(|| collection_id.clone());
            format!("{}FilterArg", type_name)
        }
        FieldKind::SelfRef { relative_id, .. } => {
            let type_name = id_to_name
                .get(relative_id)
                .cloned()
                .unwrap_or_else(|| relative_id.clone());
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
