use async_graphql::dynamic::*;

// --- Scalar operator blocks ---

/// Build ID operator block input type.
pub(super) fn build_id_operator_block() -> InputObject {
    InputObject::new("IDOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("ID")))
        .field(InputValue::new("_in", TypeRef::named_list("ID")))
        .field(InputValue::new("_neq", TypeRef::named("ID")))
        .field(InputValue::new("_nin", TypeRef::named_list("ID")))
}

/// Build String operator block input type.
pub(super) fn build_string_operator_block() -> InputObject {
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
pub(super) fn build_int_operator_block() -> InputObject {
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
pub(super) fn build_float_operator_block() -> InputObject {
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
pub(super) fn build_bool_operator_block() -> InputObject {
    InputObject::new("BooleanOperatorBlock")
        .field(InputValue::new("_eq", TypeRef::named("Boolean")))
        .field(InputValue::new("_in", TypeRef::named_list("Boolean")))
        .field(InputValue::new("_neq", TypeRef::named("Boolean")))
        .field(InputValue::new("_nin", TypeRef::named_list("Boolean")))
}

/// Build DateTime operator block input type.
pub(super) fn build_datetime_operator_block() -> InputObject {
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

/// Build Float32 operator block input type.
pub(super) fn build_float32_operator_block() -> InputObject {
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
pub(super) fn build_float64_operator_block() -> InputObject {
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

pub(super) fn build_not_null_int_filter_arg() -> InputObject {
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

pub(super) fn build_int_filter_arg() -> InputObject {
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

pub(super) fn build_not_null_float64_filter_arg() -> InputObject {
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

pub(super) fn build_float64_filter_arg() -> InputObject {
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

pub(super) fn build_not_null_float32_filter_arg() -> InputObject {
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

pub(super) fn build_float32_filter_arg() -> InputObject {
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

pub(super) fn build_not_null_bool_filter_arg() -> InputObject {
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

pub(super) fn build_bool_filter_arg() -> InputObject {
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

pub(super) fn build_not_null_string_filter_arg() -> InputObject {
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

pub(super) fn build_string_filter_arg() -> InputObject {
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

pub(super) fn build_int_list_operator_block() -> InputObject {
    InputObject::new("IntListOperatorBlock")
        .field(InputValue::new("_any", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_all", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_none", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_not_null_int_list_operator_block() -> InputObject {
    InputObject::new("NotNullIntListOperatorBlock")
        .field(InputValue::new("_any", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_all", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new("_none", TypeRef::named("IntOperatorBlock")))
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_float64_list_operator_block() -> InputObject {
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
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_not_null_float64_list_operator_block() -> InputObject {
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
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_float32_list_operator_block() -> InputObject {
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
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_not_null_float32_list_operator_block() -> InputObject {
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
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_bool_list_operator_block() -> InputObject {
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
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_not_null_bool_list_operator_block() -> InputObject {
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
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_string_list_operator_block() -> InputObject {
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
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}

pub(super) fn build_not_null_string_list_operator_block() -> InputObject {
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
        .field(InputValue::new(
            "_count",
            TypeRef::named("IntOperatorBlock"),
        ))
}
