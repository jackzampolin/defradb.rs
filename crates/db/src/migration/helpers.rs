//! Helper functions for migration placeholder creation and value conversion.

use schema::{CollectionVersion, FieldKind, ORPHAN_COLLECTION_ID};

/// Create an orphan placeholder collection version.
///
/// Used when a migration references a version that doesn't exist yet.
pub(super) fn create_orphan_placeholder(
    version_id: &str,
    name: &str,
    collection_id: &str,
) -> CollectionVersion {
    let mut placeholder = CollectionVersion {
        version_id: version_id.to_string(),
        collection_id: if collection_id.is_empty() {
            ORPHAN_COLLECTION_ID.to_string()
        } else {
            collection_id.to_string()
        },
        name: name.to_string(),
        is_materialized: true,
        is_placeholder: true,
        ..CollectionVersion::new("", "", "", Vec::new())
    };
    placeholder.is_active = false;
    placeholder
}

/// Create a placeholder with source collection info.
pub(super) fn create_placeholder_with_source(
    version_id: &str,
    source_name: &str,
    source_collection_id: &str,
) -> CollectionVersion {
    let mut placeholder = CollectionVersion {
        name: source_name.to_string(),
        version_id: version_id.to_string(),
        collection_id: source_collection_id.to_string(),
        is_materialized: true,
        is_placeholder: true,
        ..CollectionVersion::new("", "", "", Vec::new())
    };
    placeholder.is_active = false;
    placeholder
}

/// Convert a JSON value to a native NormalValue based on the field's schema type.
///
/// When documents are migrated through lens transforms, they come back as JSON values.
/// This function converts them to the appropriate native type (Int, Float, Time, etc.)
/// based on the field's declared type in the schema.
///
/// The scalar coercion delegates to [`document::encoding::json_to_normal_value_for_kind`]
/// — the same converter the mutation-create path uses — so reindexed index entries are
/// byte-identical to freshly-written ones (notably DateTime → `Time`, not a raw string).
/// Values that cannot be coerced to the declared kind fall back to a raw JSON value,
/// preserving prior best-effort behavior.
pub fn json_to_native_value(
    value: &serde_json::Value,
    field_name: &str,
    schema: &CollectionVersion,
) -> document::NormalValue {
    if value.is_null() {
        return document::NormalValue::Null;
    }

    let field_kind = schema
        .fields
        .iter()
        .find(|f| f.name == field_name)
        .map(|f| &f.kind);

    if let Some(FieldKind::Scalar(scalar)) = field_kind {
        if let Some(nv) = document::encoding::json_to_normal_value_for_kind(value, scalar) {
            return nv;
        }
    }

    document::NormalValue::Json(value.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{CollectionVersion, FieldDescription, FieldKind};

    /// A DateTime field reindexed through a lens migration must produce
    /// `NormalValue::Time`, matching the encoding that fresh document indexing
    /// uses (`encode_time_*`, nanoseconds). Returning `NormalValue::String`
    /// here re-encodes the index entries as RFC3339 strings, which diverges
    /// from freshly-built entries and from cursor seek keys (Time-encoded),
    /// breaking index-seek pagination after a reindex.
    #[test]
    fn datetime_field_converts_to_time_not_string() {
        let field = FieldDescription::new("1", "created_at", FieldKind::datetime());
        let coll = CollectionVersion::new("test", "v1", "coll_test_001", vec![field]);

        let json = serde_json::Value::String("2026-05-29T13:06:28Z".to_string());
        let nv = json_to_native_value(&json, "created_at", &coll);

        let expected = chrono::DateTime::parse_from_rfc3339("2026-05-29T13:06:28Z").unwrap();
        assert_eq!(
            nv,
            document::NormalValue::Time(expected),
            "DateTime field must reindex to NormalValue::Time (got {nv:?})"
        );
    }

    /// The actual corruption guard: the index-key bytes a reindexed DateTime
    /// value encodes to must be byte-identical to what the write path produces
    /// for the same instant, in BOTH ascending and descending index directions.
    /// (A regression to String encoding would change the type marker and the
    /// magnitude bytes, landing cursor seeks at the wrong end of the index.)
    #[test]
    fn datetime_reindex_encodes_to_same_index_bytes_as_write_path() {
        use storage::field_value::encode_field_value;

        let field = FieldDescription::new("1", "created_at", FieldKind::datetime());
        let coll = CollectionVersion::new("test", "v1", "coll_test_001", vec![field]);

        let rfc3339 = "2026-05-29T13:06:28Z";
        let reindexed = json_to_native_value(
            &serde_json::Value::String(rfc3339.to_string()),
            "created_at",
            &coll,
        );
        // What a freshly written document holds for this DateTime field.
        let write_path =
            document::NormalValue::Time(chrono::DateTime::parse_from_rfc3339(rfc3339).unwrap());

        for descending in [false, true] {
            let reindexed_bytes = encode_field_value(Vec::new(), &reindexed, descending).unwrap();
            let write_bytes = encode_field_value(Vec::new(), &write_path, descending).unwrap();
            assert_eq!(
                reindexed_bytes, write_bytes,
                "reindexed DateTime index bytes diverge from write path (descending={descending})"
            );
        }
    }

    /// Non-DateTime scalar coercions delegated to the shared converter keep
    /// their prior reindex behavior.
    #[test]
    fn scalar_fields_reindex_to_native_values() {
        let coll = CollectionVersion::new(
            "test",
            "v1",
            "coll_test_001",
            vec![
                FieldDescription::new("1", "count", FieldKind::int()),
                FieldDescription::new("2", "ratio", FieldKind::float32()),
                FieldDescription::new("3", "name", FieldKind::string()),
            ],
        );

        assert_eq!(
            json_to_native_value(&serde_json::json!(7), "count", &coll),
            document::NormalValue::Int(7)
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!(2.5), "ratio", &coll),
            document::NormalValue::Float32(2.5)
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!("hi"), "name", &coll),
            document::NormalValue::String("hi".to_string())
        );
    }
}
