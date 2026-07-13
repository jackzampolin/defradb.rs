//! GraphQL introspection support.
//!
//! This module provides introspection query execution using async-graphql.
//! Introspection queries (__schema, __type) are executed against a dynamically
//! generated schema based on the current collections.

mod aggregates;
mod collection;
mod commits;
mod input_types;
mod mutations;
mod operators;

use async_graphql::{dynamic::*, Value as GqlValue};
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::error::{QueryError, Result};

use aggregates::{build_aggregate_types_for_collection, build_numeric_fields_enum};
use collection::{build_collection_type, build_commit_type};
use commits::{
    build_commit_count_field_arg, build_commit_fields_enum, build_commit_numeric_field_arg,
    build_commits_field_name_filter_arg, build_commits_filter_arg, build_commits_height_filter_arg,
    build_commits_order_arg, build_signature_type,
};
use input_types::{
    build_field_enum, build_filter_input_type, build_mutation_input_type, build_order_input_type,
};
use mutations::{build_explain_enum, build_mutation_type, build_ordering_enum};
use operators::{
    build_bool_filter_arg, build_bool_list_operator_block, build_bool_operator_block,
    build_datetime_operator_block, build_float32_filter_arg, build_float32_list_operator_block,
    build_float32_operator_block, build_float64_filter_arg, build_float64_list_operator_block,
    build_float64_operator_block, build_float_operator_block, build_id_operator_block,
    build_int_filter_arg, build_int_list_operator_block, build_int_operator_block,
    build_not_null_bool_filter_arg, build_not_null_bool_list_operator_block,
    build_not_null_float32_filter_arg, build_not_null_float32_list_operator_block,
    build_not_null_float64_filter_arg, build_not_null_float64_list_operator_block,
    build_not_null_int_filter_arg, build_not_null_int_list_operator_block,
    build_not_null_string_filter_arg, build_not_null_string_list_operator_block,
    build_string_filter_arg, build_string_list_operator_block, build_string_operator_block,
};

/// Build an async-graphql schema from collections for introspection.
pub fn build_introspection_schema(
    collections: &[CollectionVersion],
) -> std::result::Result<Schema, SchemaError> {
    // Build a mapping from collection ID to collection name for relation resolution
    let mut id_to_name: HashMap<String, String> = collections
        .iter()
        .map(|c| (c.collection_id.clone(), c.name.clone()))
        .collect();

    // Add version_id → name entries so that CID-based relation references
    // (common in views with embedded schemas) can resolve to collection names.
    for c in collections {
        if !c.version_id.is_empty() {
            id_to_name
                .entry(c.version_id.clone())
                .or_insert_with(|| c.name.clone());
        }
    }

    // Add relative_id → name entries for collections in collection sets.
    // This allows SelfRef fields (which use relative_id as their identifier)
    // to resolve to the correct collection name during introspection.
    for c in collections {
        if let Some(ref set) = c.collection_set {
            id_to_name.insert(set.relative_id.to_string(), c.name.clone());
        }
    }

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
                move |_ctx| FieldFuture::new(async move { Ok(Some(GqlValue::List(vec![]))) }),
            )
            .argument(InputValue::new("cid", TypeRef::named_nn_list("ID")))
            .argument(InputValue::new("docID", TypeRef::named_nn_list("ID")))
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

    // Register Commit type and supporting types
    schema_builder = schema_builder
        .register(build_commit_type())
        .register(build_signature_type())
        .register(build_commits_filter_arg())
        .register(build_commits_field_name_filter_arg())
        .register(build_commits_height_filter_arg())
        .register(build_commits_order_arg())
        .register(build_commit_fields_enum())
        .register(build_commit_count_field_arg())
        .register(build_commit_numeric_field_arg());

    // Add _commits query field (unconditionally, like Go)
    query_type = query_type.field(
        Field::new("_commits", TypeRef::named_list("Commit"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::List(vec![]))) })
        })
        .argument(InputValue::new("cid", TypeRef::named_nn_list("ID")))
        .argument(InputValue::new("depth", TypeRef::named("Int")))
        .argument(InputValue::new("docID", TypeRef::named_nn_list("ID")))
        .argument(InputValue::new(
            "filter",
            TypeRef::named("CommitsFilterArg"),
        ))
        .argument(InputValue::new(
            "groupBy",
            TypeRef::named_nn_list("commitFields"),
        ))
        .argument(InputValue::new("limit", TypeRef::named("Int")))
        .argument(InputValue::new("offset", TypeRef::named("Int")))
        .argument(InputValue::new(
            "order",
            TypeRef::named_list("commitsOrderArg"),
        )),
    );

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
        let mut count_field = Field::new("COUNT", TypeRef::named("Int"), |_| {
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
        for agg_name in &["SUM", "AVG"] {
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

    // Add a hidden field referencing ExplainType so it appears in introspection.
    // async-graphql only includes registered types that are reachable from the type graph.
    // ExplainType is used as a directive argument in Go but we need it in __schema.types.
    query_type = query_type.field(
        Field::new("_explainType", TypeRef::named("ExplainType"), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        })
        .argument(InputValue::new("type", TypeRef::named("ExplainType"))),
    );

    schema_builder = schema_builder.register(query_type);

    // Add mutation type if we have collections
    if !collections.is_empty() {
        let mutation_type = build_mutation_type(collections);
        schema_builder = schema_builder.register(mutation_type);
    }

    schema_builder.finish()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A bare `{ __typename }` (e.g. a GraphQL health probe) must resolve to
    /// the root query type name via the introspection engine, not fail as an
    /// unknown collection. Regression test for #1124.
    #[tokio::test]
    async fn root_typename_returns_query() {
        let result = execute_introspection(vec![], "{ __typename }")
            .await
            .expect("introspection execution should succeed");
        assert_eq!(result, serde_json::json!({ "__typename": "Query" }));
    }
}
