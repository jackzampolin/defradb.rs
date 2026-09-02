//! `@index(vector: {...})` in SDL. The argument names are Go's, because the SDL
//! is the surface users and parity tests see.
//!
//! Go folded `@vectorIndex` into `@index` in sourcenetwork/defradb#5188 and
//! deleted the old directive outright. Neither spelling had shipped in a
//! release either runtime must honour, so this file tracks the folded grammar
//! only, and `the_old_directive_is_gone` pins that the old one no longer builds
//! an index rather than quietly still working.

use query::parse_sdl;
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

// ---------------------------------------------------------------------------
// The kind selector
// ---------------------------------------------------------------------------

/// `vector:` is what makes an index a vector index, mirroring the wire envelope
/// where `Kind` decides and `KindDescription` configures.
#[test]
fn the_vector_argument_selects_the_kind() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @index(vector: {dimensions: 768})
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

/// No `vector:` means ordered, which is what `@index` has always meant.
#[test]
fn an_index_without_a_vector_argument_is_ordered() {
    let collections = collections(r#"type Doc { name: String @index }"#);
    let index = &collections[0].indexes[0];
    assert!(!index.is_vector());
    assert!(matches!(index.kind, None | Some(IndexKind::Ordered(_))));
}

/// `kind:` names a kind directly, for its default configuration.
#[test]
fn the_ordered_kind_can_be_named() {
    let collections = collections(r#"type Doc { name: String @index(kind: ordered) }"#);
    let index = &collections[0].indexes[0];
    assert!(!index.is_vector());
    assert_eq!(index.fields[0].name, "name");
}

/// `kind: vector` is refused. A vector index needs at least its dimensions, so
/// there is no default configuration for `kind:` to select, and Go's
/// `IndexKind` enum deliberately has no such value.
#[test]
fn the_vector_kind_cannot_be_named_without_a_configuration() {
    let err = parse_sdl(r#"type Doc { e: [Float!] @index(kind: vector) }"#)
        .expect_err("kind: vector must not parse");
    assert!(err.to_string().contains("vector"), "got: {err}");
}

/// A `kind:` that agrees with the configuration block is fine; the two say the
/// same thing.
#[test]
fn a_matching_kind_and_configuration_agree() {
    let collections = collections(
        r#"type Doc { name: String @index(kind: ordered, ordered: {direction: DESC}) }"#,
    );
    assert!(collections[0].indexes[0].fields[0].descending);
}

/// Legacy top-level arguments still select ordered, so a schema written before
/// the fold keeps parsing.
#[test]
fn a_matching_kind_and_legacy_configuration_agree() {
    let collections =
        collections(r#"type Doc { name: String @index(kind: ordered, unique: true) }"#);
    assert!(collections[0].indexes[0].resolved_unique());
}

/// Two selectors that disagree are an error rather than a precedence rule: a
/// precedence rule would build the index the author did not ask for.
#[test]
fn competing_kind_selectors_are_refused() {
    for sdl in [
        r#"type Doc { e: [Float!] @index(kind: ordered, vector: {dimensions: 3}) }"#,
        r#"type Doc { e: [Float!] @index(ordered: {}, vector: {dimensions: 3}) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 3}, unique: false) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 3}, direction: DESC) }"#,
    ] {
        assert!(parse_sdl(sdl).is_err(), "should have been refused: {sdl}");
    }
}

/// A vector index covers exactly one field, so there is no type-level form.
#[test]
fn a_vector_configuration_is_refused_on_a_type() {
    let err = parse_sdl(
        r#"type Doc @index(vector: {dimensions: 3}) {
            embedding: [Float!]
        }"#,
    )
    .expect_err("a type-level vector index must not parse");
    assert!(err.to_string().contains("field"), "got: {err}");
}

/// The directive Go deleted. Keeping it parsing would mean carrying a grammar
/// neither runtime emits.
#[test]
fn the_old_directive_is_gone() {
    let collections = parse_sdl(r#"type Doc { e: [Float!] @vectorIndex(dimensions: 4) }"#)
        .expect("an unknown directive is a warning, not an error");
    assert!(
        !collections[0].indexes.iter().any(|i| i.is_vector()),
        "@vectorIndex must not build an index"
    );
}

// ---------------------------------------------------------------------------
// The ordered configuration
// ---------------------------------------------------------------------------

/// The nested block and the legacy top-level arguments describe the same index,
/// so they merge when they do not overlap.
#[test]
fn nested_and_legacy_ordered_configurations_merge() {
    let collections = collections(
        r#"type Doc { name: String @index(ordered: {unique: true}, direction: DESC) }"#,
    );
    let index = &collections[0].indexes[0];
    assert!(index.resolved_unique());
    assert!(index.fields[0].descending);
}

/// Setting one property from both places is an error: a merge that silently
/// drops one of two explicit values is worse than a parse failure.
#[test]
fn a_property_set_twice_is_refused() {
    for sdl in [
        r#"type Doc { name: String @index(ordered: {unique: true}, unique: false) }"#,
        r#"type Doc { name: String @index(ordered: {direction: ASC}, direction: DESC) }"#,
        r#"type Doc { name: String @index(ordered: {includes: []}, includes: []) }"#,
    ] {
        assert!(parse_sdl(sdl).is_err(), "should have been refused: {sdl}");
    }
}

/// The nested block carries everything the legacy form does, at type level too:
/// the reference parses both levels with one function, and so do we.
#[test]
fn a_nested_ordered_configuration_is_read_on_a_type() {
    let collections = collections(
        r#"type Doc @index(name: "userIndex", ordered: {
            unique: true,
            direction: DESC,
            includes: [{field: "name"}, {field: "age", direction: ASC}]
        }) {
            name: String
            age: Int
        }"#,
    );
    let index = &collections[0].indexes[0];
    assert_eq!(index.name, "userIndex");
    assert!(index.resolved_unique());
    assert_eq!(
        index
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.descending))
            .collect::<Vec<_>>(),
        vec![("name", true), ("age", false)],
        "an entry without its own direction inherits the directive's"
    );
}

/// An unknown argument is reported as one, not as the kind conflict it also
/// causes.
#[test]
fn an_unknown_argument_names_itself() {
    let err =
        parse_sdl(r#"type Doc { e: [Float!] @index(unknown: "x", vector: {dimensions: 3}) }"#)
            .expect_err("an unknown argument must not parse");
    assert!(err.to_string().contains("unknown"), "got: {err}");
}

// ---------------------------------------------------------------------------
// The vector configuration
// ---------------------------------------------------------------------------

/// `@index` names the index, which Go's `@vectorIndex` could not do at all.
#[test]
fn a_vector_index_can_be_named() {
    let collections = collections(
        r#"type Doc {
            embedding: [Float!] @index(name: "by_face", vector: {dimensions: 512})
        }"#,
    );
    assert_eq!(collections[0].indexes[0].name, "by_face");
}

#[test]
fn hnsw_parameters_are_read_by_gos_names() {
    let vector = only_vector(
        r#"type Doc {
            embedding: [Float!] @index(vector: {
                dimensions: 4,
                hnsw: {metric: COSINE, M: 32, efConstruction: 200, efSearch: 100}
            })
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
            embedding: [Float!] @index(vector: {dimensions: 4, hnsw: {M: 48}})
        }"#,
    );
    let hnsw = vector.hnsw.expect("HNSW params");
    assert_eq!(
        (hnsw.m, hnsw.ef_construction, hnsw.ef_search),
        (48, 128, 64)
    );
}

/// Omitted dimensions parse as zero and are never inferred. Go dropped
/// inferring them from an `@embedding` on the same field in
/// sourcenetwork/defradb#5188; definition validation is what rejects the zero,
/// so this only pins that nothing fills it in on the way through.
#[test]
fn dimensions_are_never_inferred() {
    assert_eq!(
        only_vector(r#"type Doc { e: [Float!] @index(vector: {}) }"#).dimensions,
        0
    );
    assert_eq!(
        only_vector(
            r#"type Doc {
                t: String
                e: [Float32!] @index(vector: {})
                    @embedding(provider: "openai", model: "m", fields: ["t"])
            }"#
        )
        .dimensions,
        0,
        "an @embedding on the field must not supply the length"
    );
}

/// The index lands in the collection's one index list, carrying its kind. A
/// parallel list is what full-text did and what #1326 exists to prevent.
#[test]
fn a_vector_index_joins_the_ordinary_index_list() {
    let collections = collections(
        r#"type Doc {
            name: String @index
            embedding: [Float!] @index(vector: {dimensions: 8})
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
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, hnsw: {efSarch: 10}}) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, hnsw: {M: "big"}}) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: "many"}) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, hnsw: {metric: MANHATTAN}}) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, hnsw: 5}) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, unknown: 1}) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, IVFFlat: {}}) }"#,
        r#"type Doc { e: [Float!] @index(vector: 5) }"#,
    ] {
        assert!(parse_sdl(sdl).is_err(), "should have been refused: {sdl}");
    }
}

/// A field carries as many indexes as it has `@index` directives. The reference
/// appends every one it finds rather than keeping the last, and two vector
/// indexes of different metrics on one field is precisely what a query's
/// `metric` argument exists to choose between: without this the argument has
/// nothing to disambiguate.
#[test]
fn a_field_takes_more_than_one_index() {
    let collections = collections(
        r#"type Doc {
            embedding: [Float32!]
                @index(name: "by_cosine", vector: {dimensions: 8, hnsw: {metric: COSINE}})
                @index(name: "by_euclidean", vector: {dimensions: 8, hnsw: {metric: EUCLIDEAN}})
        }"#,
    );
    let indexes = &collections[0].indexes;
    assert_eq!(indexes.len(), 2, "both directives must build an index");
    assert_ne!(indexes[0].id, indexes[1].id, "ids must not collide");

    let by_name = |name: &str| {
        indexes
            .iter()
            .find(|index| index.name == name)
            .and_then(|index| index.vector())
            .unwrap_or_else(|| panic!("no vector index named {name}"))
            .metric
    };
    assert_eq!(by_name("by_cosine"), DistanceMetric::Cosine);
    assert_eq!(by_name("by_euclidean"), DistanceMetric::Euclidean);
}

/// The same, mixing kinds: an ordered index and a vector index on one field.
#[test]
fn a_field_takes_an_ordered_and_a_vector_index() {
    let collections = collections(
        r#"type Doc {
            embedding: [Float32!] @index @index(vector: {dimensions: 8})
        }"#,
    );
    let indexes = &collections[0].indexes;
    assert_eq!(indexes.len(), 2);
    assert_eq!(indexes.iter().filter(|i| i.is_vector()).count(), 1);
    assert_eq!(indexes.iter().filter(|i| !i.is_vector()).count(), 1);
}

// ---------------------------------------------------------------------------
// Algorithm selection
// ---------------------------------------------------------------------------

#[test]
fn the_algorithm_defaults_to_hnsw() {
    let vector = only_vector(r#"type Doc { e: [Float!] @index(vector: {dimensions: 4}) }"#);
    assert_eq!(vector.algorithm, VectorAlgorithm::Hnsw);
    assert!(vector.hnsw.is_some(), "HNSW carries build parameters");
}

/// `alg:` selects an algorithm with its default configuration, which is what
/// Go's `alg` enum does.
#[test]
fn every_algorithm_is_selectable_by_alg() {
    for algorithm in VectorAlgorithm::ALL {
        let vector = only_vector(&format!(
            r#"type Doc {{ e: [Float32!] @index(vector: {{dimensions: 8, alg: {}}}) }}"#,
            algorithm.as_str()
        ));
        assert_eq!(vector.algorithm, *algorithm, "{}", algorithm.as_str());
    }
}

/// The block that configures an algorithm also selects it, so `alg:` is only
/// needed for an algorithm taken with its defaults.
#[test]
fn every_algorithm_is_selectable_by_its_block() {
    for algorithm in VectorAlgorithm::ALL {
        let vector = only_vector(&format!(
            r#"type Doc {{ e: [Float32!] @index(vector: {{dimensions: 8, {}: {{}}}}) }}"#,
            algorithm.sdl_block()
        ));
        assert_eq!(vector.algorithm, *algorithm, "{}", algorithm.sdl_block());
    }
}

/// `alg:` and a block that disagree are refused rather than resolved by
/// precedence, exactly as two kind selectors are.
#[test]
fn an_alg_that_contradicts_its_block_is_refused() {
    for sdl in [
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, alg: SSG, hnsw: {M: 4}}) }"#,
        r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, hnsw: {M: 4}, ivfpq: {nlist: 4}}) }"#,
    ] {
        assert!(parse_sdl(sdl).is_err(), "should have been refused: {sdl}");
    }
}

#[test]
fn an_unknown_algorithm_is_refused() {
    let err =
        parse_sdl(r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, alg: IVFFlat}) }"#)
            .expect_err("an unknown algorithm must not parse");
    assert!(
        err.to_string().contains("IVFFlat"),
        "the error must name it, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// Metrics, which live inside the algorithm's own block
// ---------------------------------------------------------------------------

/// The metric is one of the algorithm's knobs, where the reference puts it, so
/// every algorithm's block declares its own.
///
/// Every pair parses; a pair the algorithm cannot rank by is rejected later, by
/// definition validation, which is where the reference rejects it too.
#[test]
fn every_algorithm_block_carries_every_metric() {
    for algorithm in VectorAlgorithm::ALL {
        for metric in DistanceMetric::ALL {
            let sdl = format!(
                r#"type Doc {{ e: [Float32!] @index(vector: {{dimensions: 8, {}: {{metric: {}}}}}) }}"#,
                algorithm.sdl_block(),
                metric.as_str()
            );
            let vector = only_vector(&sdl);
            assert_eq!(
                (vector.algorithm, vector.metric),
                (*algorithm, *metric),
                "{sdl}"
            );
        }
    }
}

/// The metric has no home outside an algorithm block, so writing it beside
/// `dimensions` is an unknown argument rather than a silently ignored one.
#[test]
fn a_metric_beside_dimensions_is_refused() {
    let err = parse_sdl(r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, metric: DOT}) }"#)
        .expect_err("a vector-level metric must not parse");
    assert!(err.to_string().contains("metric"), "got: {err}");
}

// ---------------------------------------------------------------------------
// The divergent algorithms
// ---------------------------------------------------------------------------

#[test]
fn ivfpq_parameters_are_read() {
    let vector = only_vector(
        r#"type Doc {
            e: [Float!] @index(vector: {
                dimensions: 128,
                ivfpq: {nlist: 256, nprobe: 16, m: 16, sampleBytes: 1048576}
            })
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::IvfPq);
    assert!(vector.hnsw.is_none(), "IVF-PQ carries no HNSW block");
    let ivfpq = vector.ivfpq.expect("IVF-PQ params");
    assert_eq!(
        (ivfpq.nlist, ivfpq.nprobe, ivfpq.m, ivfpq.sample_bytes),
        (256, 16, 16, 1_048_576)
    );
}

#[test]
fn ivfpq_defaults_are_kept() {
    let vector =
        only_vector(r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, alg: IVF_PQ}) }"#);
    let ivfpq = vector.ivfpq.expect("IVF-PQ params");
    assert_eq!(ivfpq.nprobe, 8);
    assert_eq!(ivfpq.nlist, 0, "zero derives nlist from the corpus");
    assert_eq!(ivfpq.m, 0, "zero derives m from the width");
}

#[test]
fn ivfflat_parameters_are_read() {
    let vector = only_vector(
        r#"type Doc {
            e: [Float!] @index(vector: {
                dimensions: 128,
                ivfflat: {nlist: 256, nprobe: 16, sampleBytes: 1048576}
            })
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::IvfFlat);
    assert!(vector.hnsw.is_none(), "IVF_FLAT carries no HNSW block");
    let ivfflat = vector.ivfflat.expect("IVF_FLAT params");
    assert_eq!(
        (ivfflat.nlist, ivfflat.nprobe, ivfflat.sample_bytes),
        (256, 16, 1_048_576)
    );
}

#[test]
fn ivfflat_defaults_are_kept() {
    let vector =
        only_vector(r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, alg: IVF_FLAT}) }"#);
    let ivfflat = vector.ivfflat.expect("IVF_FLAT params");
    assert_eq!(ivfflat.nprobe, 8);
    assert_eq!(ivfflat.nlist, 0, "zero derives nlist from the corpus");
}

#[test]
fn ssg_parameters_are_read() {
    let vector = only_vector(
        r#"type Doc {
            e: [Float!] @index(vector: {dimensions: 128, ssg: {R: 32, angle: 45, pool: 200}})
        }"#,
    );
    assert_eq!(vector.algorithm, VectorAlgorithm::Ssg);
    let ssg = vector.ssg.expect("SSG params");
    assert_eq!((ssg.r, ssg.angle, ssg.pool), (32, 45, 200));
}

#[test]
fn ssg_defaults_are_kept() {
    let vector =
        only_vector(r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, alg: SSG}) }"#);
    let ssg = vector.ssg.expect("SSG params");
    assert_eq!((ssg.r, ssg.angle, ssg.pool), (50, 60, 100));
}

#[test]
fn flat_carries_no_build_parameters() {
    let vector =
        only_vector(r#"type Doc { e: [Float!] @index(vector: {dimensions: 4, alg: FLAT}) }"#);
    assert_eq!(vector.algorithm, VectorAlgorithm::Flat);
    assert!(vector.hnsw.is_none());
    assert!(vector.ivfpq.is_none());
    assert!(vector.ivfflat.is_none());
    assert!(vector.ssg.is_none());
}

#[test]
fn an_unknown_block_argument_is_refused() {
    for (sdl, name) in [
        (
            r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, ivfpq: {nlists: 4}}) }"#,
            "nlists",
        ),
        (
            r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, ssg: {degree: 4}}) }"#,
            "degree",
        ),
        (
            r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, flat: {nlist: 4}}) }"#,
            "nlist",
        ),
        (
            r#"type Doc { e: [Float!] @index(vector: {dimensions: 8, ivfflat: {m: 4}}) }"#,
            "m",
        ),
    ] {
        let err = parse_sdl(sdl).expect_err("a misspelled argument must not parse");
        assert!(err.to_string().contains(name), "got: {err}");
    }
}

/// `[Float32!]` is what an `@embedding` field declares, so it must carry a
/// vector index as readily as `[Float!]`.
#[test]
fn a_float32_field_takes_a_vector_index() {
    let vector = only_vector(r#"type Doc { e: [Float32!] @index(vector: {dimensions: 8}) }"#);
    assert_eq!(vector.dimensions, 8);
    assert_eq!(vector.algorithm, VectorAlgorithm::Hnsw);
}
