//! Commit-related introspection type builders.
//!
//! Supporting types for the Commit object and _commits query field,
//! matching Go's commits.go type definitions.

use async_graphql::dynamic::*;
use async_graphql::Value as GqlValue;

macro_rules! null_field {
    ($name:expr, $ty:expr) => {
        Field::new($name, $ty, |_| {
            FieldFuture::new(async { Ok(Some(GqlValue::Null)) })
        })
    };
}

/// Build the Signature object type.
pub(super) fn build_signature_type() -> Object {
    Object::new("Signature")
        .field(null_field!("identity", TypeRef::named("String")))
        .field(null_field!("type", TypeRef::named("String")))
        .field(null_field!("value", TypeRef::named("String")))
}

/// Build the CommitsFilterArg input object.
pub(super) fn build_commits_filter_arg() -> InputObject {
    InputObject::new("CommitsFilterArg")
        .field(InputValue::new(
            "_and",
            TypeRef::named_list("CommitsFilterArg"),
        ))
        .field(InputValue::new(
            "_or",
            TypeRef::named_list("CommitsFilterArg"),
        ))
        .field(InputValue::new(
            "fieldName",
            TypeRef::named("CommitsFieldNameFilterArg"),
        ))
}

/// Build the CommitsFieldNameFilterArg input object.
pub(super) fn build_commits_field_name_filter_arg() -> InputObject {
    InputObject::new("CommitsFieldNameFilterArg")
        .field(InputValue::new("_eq", TypeRef::named("String")))
        .field(InputValue::new("_in", TypeRef::named_list("String")))
        .field(InputValue::new("_ne", TypeRef::named("String")))
        .field(InputValue::new("_nin", TypeRef::named_list("String")))
}

/// Build the commitsOrderArg input object.
pub(super) fn build_commits_order_arg() -> InputObject {
    InputObject::new("commitsOrderArg")
        .field(InputValue::new("cid", TypeRef::named("Ordering")))
        .field(InputValue::new("docID", TypeRef::named("Ordering")))
        .field(InputValue::new("height", TypeRef::named("Ordering")))
}

/// Build the commitFields enum.
pub(super) fn build_commit_fields_enum() -> Enum {
    Enum::new("commitFields")
        .item(EnumItem::new("cid"))
        .item(EnumItem::new("docID"))
        .item(EnumItem::new("fieldName"))
        .item(EnumItem::new("height"))
}

/// Build the commitCountFieldArg enum.
pub(super) fn build_commit_count_field_arg() -> Enum {
    Enum::new("commitCountFieldArg")
        .item(EnumItem::new("heads"))
        .item(EnumItem::new("links"))
}
