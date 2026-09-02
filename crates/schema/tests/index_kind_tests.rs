//! The index kind is what a vector index carries inside a collection
//! definition, and a collection definition replicates. These lock the shape.

use proptest::prelude::*;
use schema::{
    DistanceMetric, HnswParams, IndexDescription, IndexKind, IvfFlatParams, IvfPqParams,
    OrderedIndexDescription, SsgParams, VectorAlgorithm, VectorIndexDescription,
};

fn vector_description() -> VectorIndexDescription {
    VectorIndexDescription {
        algorithm: VectorAlgorithm::Hnsw,
        metric: DistanceMetric::Cosine,
        dimensions: 768,
        hnsw: Some(HnswParams::default()),
        ivfpq: None,
        ivfflat: None,
        ssg: None,
    }
}

#[test]
fn the_kind_serializes_as_a_discriminated_envelope() {
    let json = serde_json::to_value(IndexKind::Vector(vector_description())).unwrap();
    assert_eq!(json["Kind"], 1);
    assert_eq!(json["KindDescription"]["Algorithm"], "HNSW");
    assert_eq!(json["KindDescription"]["Metric"], "COSINE");
    assert_eq!(json["KindDescription"]["Dimensions"], 768);
    assert_eq!(json["KindDescription"]["HNSW"]["M"], 16);
    assert_eq!(json["KindDescription"]["HNSW"]["EfConstruction"], 128);
    assert_eq!(json["KindDescription"]["HNSW"]["EfSearch"], 64);

    let ordered =
        serde_json::to_value(IndexKind::Ordered(OrderedIndexDescription { unique: true })).unwrap();
    assert_eq!(ordered["Kind"], 0);
    assert_eq!(ordered["KindDescription"]["Unique"], true);
}

#[test]
fn the_kind_tag_controls_deserialization() {
    let vector: IndexKind =
        serde_json::from_str(r#"{"Kind":1,"KindDescription":{"Algorithm":"HNSW"}}"#).unwrap();
    assert!(matches!(vector, IndexKind::Vector(_)));

    let ordered: IndexKind =
        serde_json::from_str(r#"{"Kind":0,"KindDescription":{"Unique":true}}"#).unwrap();
    assert_eq!(
        ordered,
        IndexKind::Ordered(OrderedIndexDescription { unique: true })
    );

    let error = serde_json::from_str::<IndexKind>(r#"{"Kind":42}"#).unwrap_err();
    assert!(error.to_string().contains("unknown index kind: 42"));
}

#[test]
fn hnsw_defaults_and_partial_overrides_parse_from_the_envelope() {
    let implicit: IndexKind =
        serde_json::from_str(r#"{"Kind":1,"KindDescription":{"Algorithm":"HNSW","HNSW":{}}}"#)
            .unwrap();
    let IndexKind::Vector(implicit) = implicit else {
        panic!("expected a vector index");
    };
    assert_eq!(implicit.hnsw, Some(HnswParams::default()));

    let explicit: IndexKind = serde_json::from_str(
        r#"{"Kind":1,"KindDescription":{"Algorithm":"HNSW","HNSW":{"EfConstruction":45,"EfSearch":56}}}"#,
    )
    .unwrap();
    let IndexKind::Vector(explicit) = explicit else {
        panic!("expected a vector index");
    };
    assert_eq!(
        explicit.hnsw,
        Some(HnswParams {
            m: 16,
            ef_construction: 45,
            ef_search: 56,
        })
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
    assert_eq!(
        legacy.kind,
        Some(IndexKind::Ordered(OrderedIndexDescription { unique: true }))
    );
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

fn index_kinds() -> impl Strategy<Value = IndexKind> {
    let algorithms = prop_oneof![
        Just(VectorAlgorithm::Hnsw),
        Just(VectorAlgorithm::Flat),
        Just(VectorAlgorithm::IvfPq),
        Just(VectorAlgorithm::IvfFlat),
        Just(VectorAlgorithm::Ssg),
    ];
    let metrics = prop_oneof![Just(DistanceMetric::Cosine), Just(DistanceMetric::Dot)];
    let hnsw = prop::option::of((any::<u32>(), any::<u32>(), any::<u32>()).prop_map(
        |(m, ef_construction, ef_search)| HnswParams {
            m,
            ef_construction,
            ef_search,
        },
    ));
    let ivfpq = prop::option::of(
        (any::<u32>(), any::<u32>(), any::<u32>(), any::<u64>()).prop_map(
            |(nlist, nprobe, m, sample_bytes)| IvfPqParams {
                nlist,
                nprobe,
                m,
                sample_bytes,
            },
        ),
    );
    let ivfflat = prop::option::of((any::<u32>(), any::<u32>(), any::<u64>()).prop_map(
        |(nlist, nprobe, sample_bytes)| IvfFlatParams {
            nlist,
            nprobe,
            sample_bytes,
        },
    ));
    let ssg = prop::option::of(
        (any::<u32>(), any::<u32>(), any::<u32>()).prop_map(|(r, angle, pool)| SsgParams {
            r,
            angle,
            pool,
        }),
    );

    prop_oneof![
        any::<bool>().prop_map(|unique| IndexKind::Ordered(OrderedIndexDescription { unique })),
        (algorithms, metrics, any::<u32>(), hnsw, ivfpq, ivfflat, ssg).prop_map(
            |(algorithm, metric, dimensions, hnsw, ivfpq, ivfflat, ssg)| {
                IndexKind::Vector(VectorIndexDescription {
                    algorithm,
                    metric,
                    dimensions,
                    hnsw,
                    ivfpq,
                    ivfflat,
                    ssg,
                })
            }
        )
    ]
}

proptest! {
    #[test]
    fn every_index_kind_round_trips_through_the_envelope(kind in index_kinds()) {
        let json = serde_json::to_vec(&kind).unwrap();
        prop_assert_eq!(serde_json::from_slice::<IndexKind>(&json).unwrap(), kind);

        let cbor = serde_ipld_dagcbor::to_vec(&kind).unwrap();
        prop_assert_eq!(serde_ipld_dagcbor::from_slice::<IndexKind>(&cbor).unwrap(), kind);
    }
}
