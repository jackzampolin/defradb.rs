use pgwire::api::Type;
use schema::{FieldKind, ScalarKind};

/// Map a DefraDB FieldKind to a PostgreSQL wire protocol type.
pub fn field_kind_to_pg_type(kind: &FieldKind) -> Type {
    match kind {
        FieldKind::Scalar(scalar) => scalar_to_pg_type(*scalar),
        FieldKind::ScalarArray(_) => Type::JSONB,
        FieldKind::Relation { .. } | FieldKind::SelfRef { .. } | FieldKind::Named { .. } => {
            Type::JSONB
        }
        _ => unreachable!(),
    }
}

fn scalar_to_pg_type(kind: ScalarKind) -> Type {
    match kind {
        ScalarKind::None => Type::TEXT,
        ScalarKind::DocID => Type::TEXT,
        ScalarKind::Bool => Type::BOOL,
        ScalarKind::Int => Type::INT8,
        ScalarKind::Float64 => Type::FLOAT8,
        ScalarKind::Float32 => Type::FLOAT4,
        ScalarKind::DateTime => Type::TIMESTAMPTZ,
        ScalarKind::String => Type::TEXT,
        ScalarKind::Blob => Type::BYTEA,
        ScalarKind::Json => Type::JSONB,
        _ => Type::TEXT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_kinds_map_correctly() {
        assert_eq!(scalar_to_pg_type(ScalarKind::Int), Type::INT8);
        assert_eq!(scalar_to_pg_type(ScalarKind::String), Type::TEXT);
        assert_eq!(scalar_to_pg_type(ScalarKind::Bool), Type::BOOL);
        assert_eq!(scalar_to_pg_type(ScalarKind::Float64), Type::FLOAT8);
        assert_eq!(scalar_to_pg_type(ScalarKind::DocID), Type::TEXT);
    }

    #[test]
    fn relation_kinds_map_to_jsonb() {
        let kind = FieldKind::relation("col1", false);
        assert_eq!(field_kind_to_pg_type(&kind), Type::JSONB);
    }
}
