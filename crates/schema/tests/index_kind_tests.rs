//! The index kind is what a vector index carries inside a collection
//! definition, and a collection definition replicates. These lock the shape.

use schema::{
    DistanceMetric, HnswParams, IndexDescription, IndexKind, OrderedIndexDescription,
    VectorAlgorithm, VectorIndexDescription,
};

fn vector_description() -> VectorIndexDescription {
    VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions: 768,
        hnsw: Some(HnswParams::default()),
        ivfpq: None,
        ssg: None,
    }
}

/// The algorithm and metric are strings on the wire, not numbers, so a stored
/// descriptor stays readable and a new value is additive.
#[test]
fn the_kind_serialises_with_gos_field_names() {
    let json = serde_json::to_value(IndexKind::Vector(vector_description())).unwrap();
    assert_eq!(json["Algorithm"], "HNSW");
    assert_eq!(json["Metric"], "COSINE");
    assert_eq!(json["Dimensions"], 768);
    assert_eq!(json["HNSW"]["M"], 16);
    assert_eq!(json["HNSW"]["EfConstruction"], 128);
    assert_eq!(json["HNSW"]["EfSearch"], 64);
    assert!(json.get("Unique").is_none(), "a vector index is not unique");

    let ordered =
        serde_json::to_value(IndexKind::Ordered(OrderedIndexDescription { unique: true })).unwrap();
    assert_eq!(ordered["Unique"], true);
    assert!(ordered.get("Algorithm").is_none());
}

/// There is no discriminator: the kind is sniffed from a vector-only field
/// being present, matching Go's `parseIndexKind`. A descriptor carrying neither
/// is an ordered index.
#[test]
fn the_kind_is_sniffed_not_tagged() {
    let vector: IndexKind = serde_json::from_str(r#"{"Algorithm":"HNSW"}"#).unwrap();
    assert!(matches!(vector, IndexKind::Vector(_)));

    let by_dimensions: IndexKind = serde_json::from_str(r#"{"Dimensions":4}"#).unwrap();
    assert!(matches!(by_dimensions, IndexKind::Vector(_)));

    let ordered: IndexKind = serde_json::from_str(r#"{"Unique":true}"#).unwrap();
    assert_eq!(
        ordered,
        IndexKind::Ordered(OrderedIndexDescription { unique: true })
    );

    let bare: IndexKind = serde_json::from_str("{}").unwrap();
    assert_eq!(
        bare,
        IndexKind::Ordered(OrderedIndexDescription { unique: false })
    );
}

#[test]
fn a_vector_kind_round_trips() {
    let kind = IndexKind::Vector(vector_description());
    let text = serde_json::to_string(&kind).unwrap();
    assert_eq!(serde_json::from_str::<IndexKind>(&text).unwrap(), kind);
}

/// A description written before kinds existed has no `Kind` field. It must
/// still parse, and it must mean an ordered index carrying its legacy `Unique`.
#[test]
fn a_description_without_a_kind_is_an_ordered_index() {
    let legacy: IndexDescription =
        serde_json::from_str(r#"{"Name":"by_email","ID":3,"Unique":true}"#).unwrap();
    assert!(legacy.kind.is_none());
    assert!(!legacy.is_vector());
    assert!(legacy.resolved_unique());

    let normalized = legacy.normalized();
    assert_eq!(
        normalized.kind,
        Some(IndexKind::Ordered(OrderedIndexDescription { unique: true }))
    );
    assert!(normalized.unique, "the legacy field must stay consistent");
}

/// Two descriptions that mean the same thing but were built in different
/// styles, one setting only `unique` and one setting only `kind`, must compare
/// equal once normalized. Mirrors Go's `Normalize`.
#[test]
fn normalizing_reconciles_the_two_styles() {
    let by_flag = IndexDescription::new("i").as_unique().normalized();
    let mut by_kind = IndexDescription::new("i");
    by_kind.kind = Some(IndexKind::Ordered(OrderedIndexDescription { unique: true }));
    assert_eq!(by_flag, by_kind.normalized());
}

/// A vector index is never unique, whichever way it is asked.
#[test]
fn a_vector_index_is_never_unique() {
    let desc = IndexDescription::new("by_embedding")
        .as_unique()
        .as_vector(vector_description());
    assert!(!desc.resolved_unique());
    assert_eq!(desc.vector().map(|v| v.dimensions), Some(768));
    assert!(!desc.clone().normalized().unique);
}

/// An index description carrying a vector kind must survive a round trip
/// through the form a collection definition is stored and replicated in.
#[test]
fn a_vector_index_description_round_trips() {
    let desc = IndexDescription::new("by_embedding")
        .with_field("embedding", false)
        .as_vector(vector_description())
        .normalized();
    let text = serde_json::to_string(&desc).unwrap();
    let back: IndexDescription = serde_json::from_str(&text).unwrap();
    assert_eq!(back, desc);
    assert!(back.is_vector());
}
