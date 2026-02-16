use async_graphql::dynamic::*;
use async_graphql::Value as GqlValue;
use schema::{CollectionVersion, FieldKind, ScalarArrayKind, ScalarKind};
use std::collections::HashMap;

/// Build the object type for a collection.
///
/// Fields are added in alphabetical order to match Go's introspection output.
/// Aggregate fields (_count, _sum, _avg, _max, _min) have args referencing
/// selector input types, matching Go's schema generation.
pub(super) fn build_collection_type(
    collection: &CollectionVersion,
    id_to_name: &HashMap<String, String>,
) -> Object {
    let mut obj = Object::new(&collection.name);
    let coll_name = &collection.name;
    let is_view = collection.query.is_some();

    // We'll collect fields as Field objects (not just name+type) so we can add args
    let mut named_fields: Vec<(String, Field)> = Vec::new();

    // Views don't expose _docID, _deleted, or _version virtual fields
    if !is_view {
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
    }

    // _group field with args sorted alphabetically
    let group_filter = format!("{}FilterArg", coll_name);
    let group_order = format!("{}OrderArg", coll_name);
    let group_field_enum = format!("{}Field", coll_name);
    named_fields.push((
        "_group".to_string(),
        Field::new("_group", TypeRef::named_list(coll_name), |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        })
        .argument(InputValue::new("docID", TypeRef::named_nn_list("ID")))
        .argument(InputValue::new("filter", TypeRef::named(&group_filter)))
        .argument(InputValue::new(
            "groupBy",
            TypeRef::named_nn_list(&group_field_enum),
        ))
        .argument(InputValue::new("limit", TypeRef::named("Int")))
        .argument(InputValue::new("offset", TypeRef::named("Int")))
        .argument(InputValue::new("order", TypeRef::named_list(&group_order))),
    ));

    // _version field (not for views)
    if !is_view {
        named_fields.push((
            "_version".to_string(),
            Field::new("_version", TypeRef::named_list("Commit"), |_| {
                FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
            }),
        ));
    }

    // Build aggregate fields with args

    // _count: takes args for _group, _version (non-views), and each inline array/relation field
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
        if !is_view {
            count_args.push((
                "_version".to_string(),
                InputValue::new(
                    "_version",
                    TypeRef::named(format!("{}___version__CountSelector", coll_name)),
                ),
            ));
        }
        for field in &collection.fields {
            match &field.kind {
                FieldKind::ScalarArray(_) | FieldKind::Relation { is_array: true, .. } => {
                    count_args.push((
                        field.name.clone(),
                        InputValue::new(
                            &field.name,
                            TypeRef::named(format!("{}__{}__CountSelector", coll_name, field.name)),
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

/// Build the Commit object type (used by _version and _commits fields).
///
/// Fields match Go's CommitObject() in commits.go, sorted alphabetically.
pub(super) fn build_commit_type() -> Object {
    macro_rules! null_field {
        ($name:expr, $ty:expr) => {
            Field::new($name, $ty, |_| {
                FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
            })
        };
    }

    fn with_commit_link_args(field: Field) -> Field {
        field
            .argument(InputValue::new("cid", TypeRef::named("ID")))
            .argument(InputValue::new("docID", TypeRef::named("ID")))
            .argument(InputValue::new(
                "filter",
                TypeRef::named("CommitsFilterArg"),
            ))
            .argument(InputValue::new(
                "groupBy",
                TypeRef::named_nn_list("commitFields"),
            ))
            .argument(InputValue::new(
                "order",
                TypeRef::named_list("commitsOrderArg"),
            ))
    }

    Object::new("Commit")
        .field(
            null_field!("_count", TypeRef::named("Int")).argument(InputValue::new(
                "field",
                TypeRef::named("commitCountFieldArg"),
            )),
        )
        .field(null_field!("_group", TypeRef::named_list("Commit")))
        .field(null_field!("cid", TypeRef::named("String")))
        .field(null_field!("collectionVersionId", TypeRef::named("String")))
        .field(null_field!("delta", TypeRef::named("String")))
        .field(null_field!("docID", TypeRef::named("String")))
        .field(null_field!("fieldName", TypeRef::named("String")))
        .field(with_commit_link_args(null_field!(
            "heads",
            TypeRef::named_list("Commit")
        )))
        .field(null_field!("height", TypeRef::named("Int")))
        .field(with_commit_link_args(null_field!(
            "links",
            TypeRef::named_list("Commit")
        )))
        .field(null_field!("signature", TypeRef::named("Signature")))
}

/// Convert field kind to async-graphql TypeRef.
/// `current_name` is the name of the collection being built (for self-reference resolution).
pub(super) fn field_kind_to_type_ref(
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
pub(super) fn scalar_to_gql_name(scalar: &ScalarKind) -> &'static str {
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
