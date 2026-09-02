//! `index new --vector` takes the index config as JSON, which is Go's flag.
//!
//! Go's `client index new` grew `--vector '{"Metric":"COSINE","Dimensions":3,
//! "HNSW":{}}'` in sourcenetwork/defradb#5096. Ours spelled the same request as
//! bespoke flags, so a script written against Go did not run here. The JSON goes
//! through the same `schema::VectorIndexDescription` serde the wire uses, so
//! there is one codec and nothing to keep in sync.

use clap::Parser;
use cli::commands::client::index::IndexNewArgs;
use schema::{DistanceMetric, HnswParams, IvfPqParams, VectorAlgorithm};

#[derive(Parser, Debug)]
struct Wrapper {
    #[command(flatten)]
    args: IndexNewArgs,
}

fn parse(extra: &[&str]) -> IndexNewArgs {
    let mut argv = vec!["index-new", "--collection", "Note", "--fields", "embedding"];
    argv.extend_from_slice(extra);
    Wrapper::parse_from(argv).args
}

fn vector(json: &str) -> schema::VectorIndexDescription {
    parse(&["--vector", json])
        .vector_description()
        .unwrap_or_else(|e| panic!("{json}: {e}"))
        .expect("--vector means a vector index")
}

#[test]
fn without_the_flag_there_is_no_vector_config() {
    assert!(parse(&[]).vector_description().unwrap().is_none());
}

/// The example from Go's own `--vector` help text has to work verbatim.
#[test]
fn gos_documented_example_parses() {
    let config = vector(r#"{"Metric":"COSINE","Dimensions":3,"HNSW":{}}"#);
    assert_eq!(config.algorithm, VectorAlgorithm::Hnsw);
    assert_eq!(config.metric, DistanceMetric::Cosine);
    assert_eq!(config.dimensions, 3);
    assert_eq!(config.hnsw, Some(HnswParams::default()));
}

/// Omitted tuning params default rather than zeroing, which is what makes the
/// terse `"HNSW":{}` in Go's example a usable index.
#[test]
fn omitted_hnsw_params_default() {
    let config = vector(r#"{"Dimensions":8,"HNSW":{"M":32}}"#);
    let hnsw = config.hnsw.expect("HNSW params");
    let defaults = HnswParams::default();
    assert_eq!(hnsw.m, 32);
    assert_eq!(hnsw.ef_construction, defaults.ef_construction);
    assert_eq!(hnsw.ef_search, defaults.ef_search);
}

/// An omitted algorithm and metric take their defaults, so the shortest config
/// that says anything is just the length.
#[test]
fn the_shortest_config_is_the_dimensions() {
    let config = vector(r#"{"Dimensions":4}"#);
    assert_eq!(config.algorithm, VectorAlgorithm::default());
    assert_eq!(config.metric, DistanceMetric::default());
    assert_eq!(config.dimensions, 4);
}

/// Every metric is reachable by name, and they are the reference's spellings.
#[test]
fn every_metric_is_reachable() {
    for metric in DistanceMetric::ALL {
        let config = vector(&format!(
            r#"{{"Metric":"{}","Dimensions":8}}"#,
            metric.as_str()
        ));
        assert_eq!(config.metric, *metric);
    }
}

/// Our extra algorithms ride the same JSON rather than a separate flag, so a
/// Rust-only description is a documented divergence in one field, not a
/// different command line.
#[test]
fn every_algorithm_is_reachable() {
    for algorithm in VectorAlgorithm::ALL {
        let config = vector(&format!(
            r#"{{"Algorithm":"{}","Dimensions":8}}"#,
            algorithm.as_str()
        ));
        assert_eq!(config.algorithm, *algorithm, "{}", algorithm.as_str());
    }
}

#[test]
fn a_divergent_algorithm_carries_its_own_params() {
    let config = vector(r#"{"Algorithm":"IVF_PQ","Dimensions":768,"IVFPQ":{"NList":256,"M":16}}"#);
    assert_eq!(config.algorithm, VectorAlgorithm::IvfPq);
    let ivfpq = config.ivfpq.expect("IVF-PQ params");
    let defaults = IvfPqParams::default();
    assert_eq!((ivfpq.nlist, ivfpq.m), (256, 16));
    assert_eq!(
        ivfpq.nprobe, defaults.nprobe,
        "an omitted param keeps its default"
    );
}

/// Malformed JSON is a named error rather than a silently ignored flag: a
/// dropped config would build an ordered index over the vector field.
#[test]
fn malformed_json_is_refused() {
    for json in [
        "not json",
        r#"{"Dimensions":"three"}"#,
        r#"{"Metric":"MANHATTAN","Dimensions":3}"#,
        r#"{"Algorithm":"IVFFLAT","Dimensions":3}"#,
        "[]",
    ] {
        let error = parse(&["--vector", json])
            .vector_description()
            .expect_err(&format!("must be refused: {json}"))
            .to_string();
        assert!(
            error.contains("invalid vector index config"),
            "{json}: {error}"
        );
    }
}

/// The flag now takes a value, so it can no longer be given bare. The bespoke
/// `--vector-algorithm` and friends are gone: one construction path, and one
/// command line that works on either runtime.
#[test]
fn the_flag_requires_its_json_and_the_old_flags_are_gone() {
    for extra in [
        vec!["--vector"],
        vec!["--vector-dimensions", "8"],
        vec!["--vector-algorithm", "HNSW"],
        vec!["--vector-metric", "COSINE"],
    ] {
        let mut argv = vec!["index-new", "--collection", "Note", "--fields", "embedding"];
        argv.extend_from_slice(&extra);
        Wrapper::try_parse_from(argv).expect_err(&format!("{extra:?} must not parse"));
    }
}
