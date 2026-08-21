use db::collection::Collection;
use db::definition::migration::helpers::*;
use document::Document;
use lens::LensDoc;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use storage::field_value::encode_field_value;

/// A DateTime field reindexed through a lens migration must produce
/// `NormalValue::Time`, matching the encoding that fresh document indexing
/// uses (`encode_time_*`). Returning `NormalValue::String`
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
