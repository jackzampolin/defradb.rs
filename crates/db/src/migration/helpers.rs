//! Helper functions for migration placeholder creation and document materialization.

use datastore::NamespaceView;
use document::Document;
use lens::{LensDoc, DOC_ID_FIELD};
use schema::{CollectionVersion, FieldKind, ORPHAN_COLLECTION_ID};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::index_manager::IndexManager;

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
/// Scalar and scalar-array coercion delegates to the shared document converters, producing the
/// same native representation as mutation writes. Reindexed entries are therefore byte-identical
/// to freshly-written ones (notably DateTime → `Time`, and arrays → typed array variants rather
/// than a JSON blob). Values that cannot be coerced fall back to raw JSON, preserving prior
/// best-effort behavior.
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

    if let Some(field_kind) = field_kind {
        match field_kind {
            FieldKind::Scalar(scalar) => {
                if let Some(nv) = document::encoding::json_to_normal_value_for_kind(value, scalar) {
                    return nv;
                }
            }
            FieldKind::ScalarArray(array) => {
                if let Some(nv) =
                    document::encoding::json_to_normal_value_for_array_kind(value, array)
                {
                    return nv;
                }
            }
            _ => {}
        }
    }

    document::NormalValue::Json(value.clone())
}

/// Convert a transformed lens document to the active collection's storage representation.
///
/// Lens output is JSON-shaped, while document storage and indexes use schema-aware native
/// values. Unknown output fields are ignored, matching Go's lensed fetcher.
pub(crate) fn lens_doc_to_document(
    mut lens_doc: LensDoc,
    original_doc: &Document,
    collection: &Collection,
) -> Document {
    let mut doc = Document::new();

    if let Some(id) = original_doc.id() {
        doc.set_id(id.clone());
    }

    for field in &collection.schema().fields {
        if field.name == DOC_ID_FIELD {
            continue;
        }
        if let Some(value) = lens_doc.remove(&field.name) {
            doc.set(
                &field.name,
                json_to_native_value(&value, &field.name, collection.schema()),
            );
        } else if original_doc.get(&field.name).is_some() {
            // Go's updateDataStore treats a field removed by a lens as an
            // explicit nil assignment. Rust omits nils from CBOR storage, but
            // retaining Null in the returned in-memory document preserves the
            // same clear semantics for the query that performed the migration.
            doc.set(&field.name, document::NormalValue::Null);
        }
    }

    doc.set_schema_version_id(collection.version_id());
    doc
}

/// Persist a lensed document directly to the datastore without creating CRDT commits.
///
/// Rust stores document fields in one CBOR blob, so this is the current-layout equivalent of
/// Go's per-field `updateDataStore`: replace the blob and update the real version key in the same
/// transaction.
pub(crate) async fn cache_migrated_document(
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
) -> Result<bool> {
    let Some(doc_id) = doc.id() else {
        return Ok(false);
    };
    let Some(doc_short_id) = collection.resolve_doc_short_id(systemstore, doc_id).await? else {
        return Ok(false);
    };

    let data = doc.to_cbor()?;
    datastore
        .set(&collection.doc_key(doc_short_id), &data)
        .await
        .map_err(Error::Storage)?;
    collection.store_version(datastore, doc_short_id).await?;

    Ok(true)
}

/// Persist a lazily migrated document and update its secondary indexes in the
/// same transaction.
///
/// Unlike a user mutation, migration write-back deliberately creates no CRDT
/// blocks or commits. It still has to remove index entries derived from the old
/// stored blob and add entries derived from the migrated representation.
pub(crate) async fn cache_migrated_document_with_indexes(
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
) -> Result<bool> {
    let Some(doc_id) = doc.id() else {
        return Ok(false);
    };
    let Some(doc_short_id) = collection.resolve_doc_short_id(systemstore, doc_id).await? else {
        return Ok(false);
    };

    let key = collection.doc_key(doc_short_id);
    let Some(old_data) = datastore.get(&key).await.map_err(Error::Storage)? else {
        return Ok(false);
    };
    let mut old_doc = Document::from_cbor(&old_data)?;
    old_doc.set_id(doc_id.clone());

    datastore
        .set(&key, &doc.to_cbor()?)
        .await
        .map_err(Error::Storage)?;
    collection.store_version(datastore, doc_short_id).await?;

    // Deleted documents have already been removed from secondary indexes.
    // Materializing their retained blob must not add those entries back.
    if !collection.is_deleted(datastore, doc_short_id).await? {
        let index_manager = IndexManager::from_indexes(
            collection.resolved_root_id(),
            collection.schema(),
            collection.write_indexes(),
        )?;
        index_manager
            .on_document_update(datastore, &old_doc, doc, doc_short_id, collection.schema())
            .await?;
    }

    Ok(true)
}

/// Advance only a document's stored schema version.
pub(crate) async fn cache_document_version(
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    collection: &Collection,
    doc: &Document,
) -> Result<bool> {
    let Some(doc_id) = doc.id() else {
        return Ok(false);
    };
    let Some(doc_short_id) = collection.resolve_doc_short_id(systemstore, doc_id).await? else {
        return Ok(false);
    };

    collection.store_version(datastore, doc_short_id).await?;
    Ok(true)
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

    #[test]
    fn blob_field_remains_hex_text_after_lens_materialization() {
        let collection = Collection::new(CollectionVersion::new(
            "Files",
            "v2",
            "files",
            vec![FieldDescription::new("1", "data", FieldKind::blob())],
        ));
        let mut lens_doc = LensDoc::new();
        lens_doc.insert("data".to_string(), serde_json::json!("00ff"));

        let migrated = lens_doc_to_document(lens_doc, &Document::new(), &collection);
        assert_eq!(
            migrated.get("data"),
            Some(&document::NormalValue::String("00ff".to_string()))
        );

        let persisted = Document::from_cbor(&migrated.to_cbor().unwrap()).unwrap();
        assert_eq!(
            persisted.get("data"),
            Some(&document::NormalValue::String("00ff".to_string()))
        );
    }

    #[test]
    fn lens_removed_field_is_returned_as_explicit_null() {
        let collection = Collection::new(CollectionVersion::new(
            "Users",
            "v2",
            "users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
            ],
        ));
        let mut original = Document::new();
        original.set("name", document::NormalValue::String("Alice".to_string()));

        let migrated = lens_doc_to_document(LensDoc::new(), &original, &collection);

        assert_eq!(
            migrated.get("name"),
            Some(&document::NormalValue::Null),
            "a lens omission must be observable as a clear in the current query"
        );
        assert_eq!(
            Document::from_cbor(&migrated.to_cbor().unwrap())
                .unwrap()
                .get("name"),
            None,
            "nil fields remain omitted from persisted CBOR, matching Go"
        );
    }

    #[test]
    fn scalar_arrays_reindex_to_native_values() {
        let coll = CollectionVersion::new(
            "test",
            "v1",
            "coll_test_001",
            vec![
                FieldDescription::new("1", "bools", FieldKind::bool_array()),
                FieldDescription::new("2", "ints", FieldKind::int_array()),
                FieldDescription::new("3", "float64s", FieldKind::float64_array()),
                FieldDescription::new("4", "float32s", FieldKind::float32_array()),
                FieldDescription::new("5", "strings", FieldKind::string_array()),
                FieldDescription::new("6", "maybe_bools", FieldKind::nillable_bool_array()),
                FieldDescription::new("7", "maybe_ints", FieldKind::nillable_int_array()),
                FieldDescription::new("8", "maybe_float64s", FieldKind::nillable_float64_array()),
                FieldDescription::new("9", "maybe_float32s", FieldKind::nillable_float32_array()),
                FieldDescription::new("10", "maybe_strings", FieldKind::nillable_string_array()),
            ],
        );

        assert_eq!(
            json_to_native_value(&serde_json::json!([true, false]), "bools", &coll),
            document::NormalValue::BoolArray(vec![true, false])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!([1, 2]), "ints", &coll),
            document::NormalValue::IntArray(vec![1, 2])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!([1, 2.5]), "float64s", &coll),
            document::NormalValue::Float64Array(vec![1.0, 2.5])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!([1, 2.5]), "float32s", &coll),
            document::NormalValue::Float32Array(vec![1.0, 2.5])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!(["a", "b"]), "strings", &coll),
            document::NormalValue::StringArray(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!([true, null]), "maybe_bools", &coll),
            document::NormalValue::NillableBoolElementArray(vec![Some(true), None])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!([1, null]), "maybe_ints", &coll),
            document::NormalValue::NillableIntElementArray(vec![Some(1), None])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!([1, null]), "maybe_float64s", &coll),
            document::NormalValue::NillableFloat64ElementArray(vec![Some(1.0), None])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!([1, null]), "maybe_float32s", &coll),
            document::NormalValue::NillableFloat32ElementArray(vec![Some(1.0), None])
        );
        assert_eq!(
            json_to_native_value(&serde_json::json!(["a", null]), "maybe_strings", &coll),
            document::NormalValue::NillableStringElementArray(vec![Some("a".to_string()), None])
        );
    }
}
