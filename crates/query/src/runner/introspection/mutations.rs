use async_graphql::dynamic::*;
use async_graphql::Value as GqlValue;
use schema::CollectionVersion;

/// Build the Mutation type.
pub(super) fn build_mutation_type(collections: &[CollectionVersion]) -> Object {
    let mut mutation = Object::new("Mutation").description("Root mutation type");

    for collection in collections {
        // Embedded-only types don't have mutation fields
        if collection.is_embedded_only {
            continue;
        }

        let coll_name = collection.name.clone();
        let input_type = format!("{}MutationInputArg", coll_name);

        // add_<Collection>
        mutation = mutation.field(
            Field::new(
                format!("add_{}", coll_name),
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
pub(super) fn build_explain_enum() -> Enum {
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
pub(super) fn build_ordering_enum() -> Enum {
    Enum::new("Ordering")
        .item(EnumItem::new("ASC"))
        .item(EnumItem::new("DESC"))
}
