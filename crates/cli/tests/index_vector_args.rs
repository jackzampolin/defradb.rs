//! `index new --vector` must produce the same descriptor SDL does.
//!
//! The CLI could create only ordinary indexes, so a vector index was reachable
//! from SDL and the HTTP API but not from the command line.

use clap::Parser;
use cli::commands::client::index::IndexNewArgs;
use schema::{DistanceMetric, VectorAlgorithm};

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

#[test]
fn without_the_flag_there_is_no_vector_config() {
    assert!(parse(&[]).vector_description().unwrap().is_none());
}

/// Every algorithm and metric the engine supports must be reachable from the
/// command line, and the per-algorithm parameter block must be filled for it.
#[test]
fn every_supported_pair_is_selectable() {
    for algorithm in VectorAlgorithm::ALL {
        for metric in DistanceMetric::ALL {
            if !algorithm.supports_metric(*metric) {
                continue;
            }
            let args = parse(&[
                "--vector",
                "--vector-dimensions",
                "8",
                "--vector-algorithm",
                algorithm.as_str(),
                "--vector-metric",
                metric.as_str(),
            ]);
            let vector = args
                .vector_description()
                .expect("a supported pair must build")
                .expect("--vector means a vector index");

            assert_eq!(vector.algorithm, *algorithm);
            assert_eq!(vector.metric, *metric);
            assert_eq!(vector.dimensions, 8);
            assert_eq!(
                vector.hnsw.is_some(),
                *algorithm == VectorAlgorithm::Hnsw,
                "the HNSW block belongs to HNSW alone"
            );
        }
    }
}

/// Omitted dimensions mean an `@embedding` on the field fixes the length.
#[test]
fn dimensions_may_be_omitted() {
    let args = parse(&["--vector"]);
    let vector = args.vector_description().unwrap().unwrap();
    assert_eq!(vector.dimensions, 0);
    assert_eq!(vector.algorithm, VectorAlgorithm::default());
}

/// The accepted values come from the enums, so an unknown one is refused by the
/// parser with the valid choices rather than reaching the server.
#[test]
fn an_unknown_algorithm_is_refused_by_the_parser() {
    let error = Wrapper::try_parse_from([
        "index-new",
        "--collection",
        "Note",
        "--fields",
        "embedding",
        "--vector",
        "--vector-algorithm",
        "NOT_AN_ENGINE",
    ])
    .expect_err("an unknown algorithm must not parse");

    let rendered = error.to_string();
    assert!(
        VectorAlgorithm::ALL
            .iter()
            .all(|algorithm| rendered.contains(algorithm.as_str())),
        "the error must list every valid algorithm, got: {rendered}"
    );
}

/// The tuning flags are meaningless without `--vector`, so asking for one alone
/// is a usage error rather than a silently ignored argument.
#[test]
fn the_tuning_flags_require_the_vector_flag() {
    Wrapper::try_parse_from([
        "index-new",
        "--collection",
        "Note",
        "--fields",
        "embedding",
        "--vector-dimensions",
        "8",
    ])
    .expect_err("--vector-dimensions alone must not parse");
}
