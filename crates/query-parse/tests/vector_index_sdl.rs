//! `@vectorIndex` in SDL. The argument names are Go's, because the SDL is the
//! surface users and parity tests see.

use query_parse::parse_sdl;
use schema::{DistanceMetric, IndexKind, VectorAlgorithm};

fn collections(sdl: &str) -> Vec<schema::CollectionVersion> {
    parse_sdl(sdl).expect("valid SDL")
}

fn only_vector(sdl: &str) -> schema::VectorIndexDescription {
    let collections = collections(sdl);
    let index = collections[0]
        .indexes
        .iter()
        .find(|i| i.is_vector())
        .expect("a vector index");
    *index.vector().unwrap()
}

/// Omitting the algorithm config still means HNSW, with defaults.
#[test]
fn a_bare_directive_uses_hnsw_defaults() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 768)
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::Hnsw);
    assert_eq!(vector.metric, DistanceMetric::Cosine);
    assert_eq!(vector.dimensions, 768);
    let hnsw = vector.hnsw.expect("HNSW params");
    assert_eq!(
        (hnsw.m, hnsw.ef_construction, hnsw.ef_search),
        (16, 128, 64)
    );
}

#[test]
fn hnsw_parameters_are_read_by_gos_names() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(
                dimensions: 4,
                HNSW: { metric: "COSINE", M: 32, efConstruction: 200, efSearch: 100 }
            )
        }"#,
    );
    let hnsw = vector.hnsw.expect("HNSW params");
    assert_eq!(
        (hnsw.m, hnsw.ef_construction, hnsw.ef_search),
        (32, 200, 100)
    );
    assert_eq!(vector.metric, DistanceMetric::Cosine);
}

/// An omitted member keeps its default rather than zeroing the parameter.
#[test]
fn a_partial_hnsw_config_keeps_the_other_defaults() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 4, HNSW: { M: 48 })
        }"#,
    );
    let hnsw = vector.hnsw.expect("HNSW params");
    assert_eq!(
        (hnsw.m, hnsw.ef_construction, hnsw.ef_search),
        (48, 128, 64)
    );
}

/// Zero dimensions means an `@embedding` on the field fixes the length.
#[test]
fn dimensions_may_be_omitted() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex
        }"#,
    );
    assert_eq!(vector.dimensions, 0);
}

/// The index lands in the collection's one index list, carrying its kind. A
/// parallel list is what full-text did and what #1326 exists to prevent.
#[test]
fn a_vector_index_joins_the_ordinary_index_list() {
    let collections = collections(
        r#"type Doc {
            name: String @index
            embedding: [Float!] @vectorIndex(dimensions: 8)
        }"#,
    );
    let indexes = &collections[0].indexes;
    assert_eq!(indexes.len(), 2, "one ordered and one vector");

    let vector = indexes.iter().find(|i| i.is_vector()).unwrap();
    assert_eq!(vector.fields.len(), 1);
    assert_eq!(vector.fields[0].name, "embedding");
    assert!(!vector.resolved_unique(), "a vector index is never unique");

    let ordered = indexes.iter().find(|i| !i.is_vector()).unwrap();
    assert!(matches!(ordered.kind, None | Some(IndexKind::Ordered(_))));

    assert_ne!(indexes[0].id, indexes[1].id, "ids must not collide");
}

/// A mistyped parameter is a schema error. Silently dropping it would build a
/// differently-shaped index than the schema asks for, and nothing downstream
/// could tell.
#[test]
fn unknown_or_mistyped_arguments_are_refused() {
    for sdl in [
        r#"type Doc { e: [Float!] @vectorIndex(dimensions: 4, HNSW: { efSarch: 10 }) }"#,
        r#"type Doc { e: [Float!] @vectorIndex(dimensions: 4, HNSW: { M: "big" }) }"#,
        r#"type Doc { e: [Float!] @vectorIndex(dimensions: "many") }"#,
        r#"type Doc { e: [Float!] @vectorIndex(dimensions: 4, HNSW: { metric: "EUCLIDEAN" }) }"#,
        r#"type Doc { e: [Float!] @vectorIndex(dimensions: 4, HNSW: 5) }"#,
    ] {
        assert!(parse_sdl(sdl).is_err(), "should have been refused: {sdl}");
    }
}

#[test]
fn the_algorithm_defaults_to_hnsw() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 4)
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::Hnsw);
    assert!(vector.hnsw.is_some(), "HNSW carries build parameters");
}

#[test]
fn the_flat_algorithm_is_selectable() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 4, algorithm: "FLAT")
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::Flat);
    assert_eq!(vector.dimensions, 4);
    assert!(
        vector.hnsw.is_none(),
        "flat has no build parameters, got {:?}",
        vector.hnsw
    );
}

#[test]
fn the_algorithm_may_be_written_as_an_enum() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 4, algorithm: FLAT)
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::Flat);
}

#[test]
fn an_unknown_algorithm_is_refused() {
    let err = parse_sdl(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 4, algorithm: "IVFPQ")
        }"#,
    )
    .expect_err("an unknown algorithm must not parse");
    assert!(
        err.to_string().contains("IVFPQ"),
        "the error must name it, got: {err}"
    );
}

#[test]
fn the_dot_metric_is_selectable() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 4, HNSW: { metric: "DOT" })
        }"#,
    );
    assert_eq!(vector.metric, DistanceMetric::Dot);
}

#[test]
fn the_ivfpq_algorithm_is_selectable() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 8, algorithm: "IVF_PQ")
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::IvfPq);
    assert!(vector.hnsw.is_none(), "IVF-PQ carries no HNSW block");
    let ivfpq = vector.ivfpq.expect("IVF-PQ params");
    assert_eq!(ivfpq.nprobe, 8);
    assert_eq!(ivfpq.nlist, 0, "zero derives nlist from the corpus");
    assert_eq!(ivfpq.m, 0, "zero derives m from the width");
}

#[test]
fn ivfpq_parameters_are_read() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(
                dimensions: 128,
                algorithm: "IVF_PQ",
                IVFPQ: { nlist: 256, nprobe: 16, m: 16, sampleBytes: 1048576 }
            )
        }"#,
    );
    let ivfpq = vector.ivfpq.expect("IVF-PQ params");
    assert_eq!(
        (ivfpq.nlist, ivfpq.nprobe, ivfpq.m, ivfpq.sample_bytes),
        (256, 16, 16, 1_048_576)
    );
}

#[test]
fn an_unknown_ivfpq_argument_is_refused() {
    let err = parse_sdl(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(
                dimensions: 8, algorithm: "IVF_PQ", IVFPQ: { nlists: 4 }
            )
        }"#,
    )
    .expect_err("a misspelled argument must not parse");
    assert!(
        err.to_string().contains("nlists"),
        "the error must name it, got: {err}"
    );
}

/// An IVFPQ block on an HNSW index is inert rather than an error, the same way
/// an HNSW block is inert on a flat index.
#[test]
fn an_ivfpq_block_on_an_hnsw_index_is_inert() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @vectorIndex(dimensions: 8, IVFPQ: { nlist: 4 })
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::Hnsw);
    assert!(vector.hnsw.is_some());
    assert!(vector.ivfpq.is_none());
}
