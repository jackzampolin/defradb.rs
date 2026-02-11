use async_graphql::dynamic::*;

/// Generates a `pub(super) fn build_*() -> InputObject` for scalar operator blocks.
macro_rules! scalar_operator_block {
    ($fn_name:ident, $type_name:literal, $scalar:literal, comparison) => {
        pub(super) fn $fn_name() -> InputObject {
            InputObject::new($type_name)
                .field(InputValue::new("_eq", TypeRef::named($scalar)))
                .field(InputValue::new("_geq", TypeRef::named($scalar)))
                .field(InputValue::new("_gt", TypeRef::named($scalar)))
                .field(InputValue::new("_in", TypeRef::named_list($scalar)))
                .field(InputValue::new("_leq", TypeRef::named($scalar)))
                .field(InputValue::new("_lt", TypeRef::named($scalar)))
                .field(InputValue::new("_neq", TypeRef::named($scalar)))
                .field(InputValue::new("_nin", TypeRef::named_list($scalar)))
        }
    };
    ($fn_name:ident, $type_name:literal, $scalar:literal, equality) => {
        pub(super) fn $fn_name() -> InputObject {
            InputObject::new($type_name)
                .field(InputValue::new("_eq", TypeRef::named($scalar)))
                .field(InputValue::new("_in", TypeRef::named_list($scalar)))
                .field(InputValue::new("_neq", TypeRef::named($scalar)))
                .field(InputValue::new("_nin", TypeRef::named_list($scalar)))
        }
    };
    ($fn_name:ident, $type_name:literal, $scalar:literal, string_ops) => {
        pub(super) fn $fn_name() -> InputObject {
            InputObject::new($type_name)
                .field(InputValue::new("_eq", TypeRef::named($scalar)))
                .field(InputValue::new("_ilike", TypeRef::named($scalar)))
                .field(InputValue::new("_in", TypeRef::named_list($scalar)))
                .field(InputValue::new("_like", TypeRef::named($scalar)))
                .field(InputValue::new("_neq", TypeRef::named($scalar)))
                .field(InputValue::new("_nilike", TypeRef::named($scalar)))
                .field(InputValue::new("_nin", TypeRef::named_list($scalar)))
                .field(InputValue::new("_nlike", TypeRef::named($scalar)))
        }
    };
}

/// Generates a `pub(super) fn build_*() -> InputObject` for filter arg types
/// (same operators as scalar blocks, plus self-recursive `_and`/`_or`).
macro_rules! filter_arg {
    ($fn_name:ident, $type_name:literal, $scalar:literal, comparison) => {
        pub(super) fn $fn_name() -> InputObject {
            InputObject::new($type_name)
                .field(InputValue::new("_and", TypeRef::named_nn_list($type_name)))
                .field(InputValue::new("_eq", TypeRef::named($scalar)))
                .field(InputValue::new("_geq", TypeRef::named($scalar)))
                .field(InputValue::new("_gt", TypeRef::named($scalar)))
                .field(InputValue::new("_in", TypeRef::named_list($scalar)))
                .field(InputValue::new("_leq", TypeRef::named($scalar)))
                .field(InputValue::new("_lt", TypeRef::named($scalar)))
                .field(InputValue::new("_neq", TypeRef::named($scalar)))
                .field(InputValue::new("_nin", TypeRef::named_list($scalar)))
                .field(InputValue::new("_or", TypeRef::named_nn_list($type_name)))
        }
    };
    ($fn_name:ident, $type_name:literal, $scalar:literal, equality) => {
        pub(super) fn $fn_name() -> InputObject {
            InputObject::new($type_name)
                .field(InputValue::new("_and", TypeRef::named_nn_list($type_name)))
                .field(InputValue::new("_eq", TypeRef::named($scalar)))
                .field(InputValue::new("_in", TypeRef::named_list($scalar)))
                .field(InputValue::new("_neq", TypeRef::named($scalar)))
                .field(InputValue::new("_nin", TypeRef::named_list($scalar)))
                .field(InputValue::new("_or", TypeRef::named_nn_list($type_name)))
        }
    };
    ($fn_name:ident, $type_name:literal, $scalar:literal, string_ops) => {
        pub(super) fn $fn_name() -> InputObject {
            InputObject::new($type_name)
                .field(InputValue::new("_and", TypeRef::named_nn_list($type_name)))
                .field(InputValue::new("_eq", TypeRef::named($scalar)))
                .field(InputValue::new("_ilike", TypeRef::named($scalar)))
                .field(InputValue::new("_in", TypeRef::named_list($scalar)))
                .field(InputValue::new("_like", TypeRef::named($scalar)))
                .field(InputValue::new("_neq", TypeRef::named($scalar)))
                .field(InputValue::new("_nilike", TypeRef::named($scalar)))
                .field(InputValue::new("_nin", TypeRef::named_list($scalar)))
                .field(InputValue::new("_nlike", TypeRef::named($scalar)))
                .field(InputValue::new("_or", TypeRef::named_nn_list($type_name)))
        }
    };
}

/// Generates a `pub(super) fn build_*() -> InputObject` for list operator blocks
/// (`_any`/`_all`/`_none` referencing the element block, `_count` always `IntOperatorBlock`).
macro_rules! list_operator_block {
    ($fn_name:ident, $type_name:literal, $element_block:literal) => {
        pub(super) fn $fn_name() -> InputObject {
            InputObject::new($type_name)
                .field(InputValue::new("_any", TypeRef::named($element_block)))
                .field(InputValue::new("_all", TypeRef::named($element_block)))
                .field(InputValue::new("_none", TypeRef::named($element_block)))
                .field(InputValue::new(
                    "_count",
                    TypeRef::named("IntOperatorBlock"),
                ))
        }
    };
}

// --- Scalar operator blocks ---

scalar_operator_block!(build_id_operator_block, "IDOperatorBlock", "ID", equality);
scalar_operator_block!(
    build_string_operator_block,
    "StringOperatorBlock",
    "String",
    string_ops
);
scalar_operator_block!(
    build_int_operator_block,
    "IntOperatorBlock",
    "Int",
    comparison
);
scalar_operator_block!(
    build_float_operator_block,
    "FloatOperatorBlock",
    "Float",
    comparison
);
scalar_operator_block!(
    build_float32_operator_block,
    "Float32OperatorBlock",
    "Float32",
    comparison
);
scalar_operator_block!(
    build_float64_operator_block,
    "Float64OperatorBlock",
    "Float64",
    comparison
);
scalar_operator_block!(
    build_bool_operator_block,
    "BooleanOperatorBlock",
    "Boolean",
    equality
);
scalar_operator_block!(
    build_datetime_operator_block,
    "DateTimeOperatorBlock",
    "DateTime",
    comparison
);

// --- Filter arg types ---

filter_arg!(
    build_not_null_int_filter_arg,
    "NotNullIntFilterArg",
    "Int",
    comparison
);
filter_arg!(build_int_filter_arg, "IntFilterArg", "Int", comparison);
filter_arg!(
    build_not_null_float64_filter_arg,
    "NotNullFloat64FilterArg",
    "Float64",
    comparison
);
filter_arg!(
    build_float64_filter_arg,
    "Float64FilterArg",
    "Float64",
    comparison
);
filter_arg!(
    build_not_null_float32_filter_arg,
    "NotNullFloat32FilterArg",
    "Float32",
    comparison
);
filter_arg!(
    build_float32_filter_arg,
    "Float32FilterArg",
    "Float32",
    comparison
);
filter_arg!(
    build_not_null_bool_filter_arg,
    "NotNullBooleanFilterArg",
    "Boolean",
    equality
);
filter_arg!(
    build_bool_filter_arg,
    "BooleanFilterArg",
    "Boolean",
    equality
);
filter_arg!(
    build_not_null_string_filter_arg,
    "NotNullStringFilterArg",
    "String",
    string_ops
);
filter_arg!(
    build_string_filter_arg,
    "StringFilterArg",
    "String",
    string_ops
);

// --- List operator blocks ---

list_operator_block!(
    build_int_list_operator_block,
    "IntListOperatorBlock",
    "IntOperatorBlock"
);
list_operator_block!(
    build_not_null_int_list_operator_block,
    "NotNullIntListOperatorBlock",
    "IntOperatorBlock"
);
list_operator_block!(
    build_float64_list_operator_block,
    "Float64ListOperatorBlock",
    "Float64OperatorBlock"
);
list_operator_block!(
    build_not_null_float64_list_operator_block,
    "NotNullFloat64ListOperatorBlock",
    "Float64OperatorBlock"
);
list_operator_block!(
    build_float32_list_operator_block,
    "Float32ListOperatorBlock",
    "Float32OperatorBlock"
);
list_operator_block!(
    build_not_null_float32_list_operator_block,
    "NotNullFloat32ListOperatorBlock",
    "Float32OperatorBlock"
);
list_operator_block!(
    build_bool_list_operator_block,
    "BooleanListOperatorBlock",
    "BooleanOperatorBlock"
);
list_operator_block!(
    build_not_null_bool_list_operator_block,
    "NotNullBooleanListOperatorBlock",
    "BooleanOperatorBlock"
);
list_operator_block!(
    build_string_list_operator_block,
    "StringListOperatorBlock",
    "StringOperatorBlock"
);
list_operator_block!(
    build_not_null_string_list_operator_block,
    "NotNullStringListOperatorBlock",
    "StringOperatorBlock"
);
