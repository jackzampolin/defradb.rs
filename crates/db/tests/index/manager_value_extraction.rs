use db::index::manager::IndexManager;

use document::Document;
use document::NormalValue;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use schema::IndexDescription;
use schema::IndexedFieldDescription;

fn datetime_index() -> IndexDescription {
    IndexDescription {
        name: "idx_created_at".to_string(),
        id: 1,
        unique: false,
        kind: None,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: "created_at".to_string(),
            descending: true,
        }],
    }
}

fn collection() -> CollectionVersion {
    let mut c = CollectionVersion::new(
        "CodingSession",
        "v1",
        "coll_cs",
        vec![FieldDescription::new(
            "1",
            "created_at",
            FieldKind::datetime(),
        )],
    );
    c.indexes = vec![datetime_index()];
    c
}

/// #72 regression: a DateTime field loaded from CBOR storage comes back as a
/// String. The index builder must coerce it to Time so its entry lands in
/// the same byte range as live-written rows — otherwise it is silently
/// excluded from `order:[{created_at: DESC}]` cursor queries. A plain
/// `maintenance reindex` (pass-through branch) feeds exactly these docs.
#[test]
fn datetime_field_stored_as_string_is_indexed_as_time() {
    let schema = collection();
    let idx = datetime_index();
    let mgr = IndexManager::from_collection(1, &schema).unwrap();

    let mut doc = Document::new();
    doc.set(
        "created_at",
        NormalValue::String("2026-05-29T13:06:28Z".to_string()),
    );

    let rows = mgr.extract_index_values(&doc, &idx, &schema).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        matches!(rows[0][0], NormalValue::Time(_)),
        "String DateTime from storage must index as Time, got {:?}",
        rows[0][0]
    );
}

/// A genuine String field holding a date-like value must NOT be coerced.
#[test]
fn string_field_with_date_like_value_is_left_as_string() {
    let mut schema = CollectionVersion::new(
        "CodingSession",
        "v1",
        "coll_cs",
        vec![FieldDescription::new("1", "label", FieldKind::string())],
    );
    let idx = IndexDescription {
        name: "idx_label".to_string(),
        id: 2,
        unique: false,
        kind: None,
        auto_generated: false,
        fields: vec![IndexedFieldDescription {
            name: "label".to_string(),
            descending: false,
        }],
    };
    schema.indexes = vec![idx.clone()];
    let mgr = IndexManager::from_collection(1, &schema).unwrap();

    let mut doc = Document::new();
    doc.set(
        "label",
        NormalValue::String("2026-05-29T13:06:28Z".to_string()),
    );

    let rows = mgr.extract_index_values(&doc, &idx, &schema).unwrap();
    assert!(
        matches!(rows[0][0], NormalValue::String(_)),
        "String field must stay String, got {:?}",
        rows[0][0]
    );
}
