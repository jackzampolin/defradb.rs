//! Cross-runtime vector index parity, executed against a real Go node.
//!
//! Every vector parity claim before the baseline reached `f73a903f` was read
//! from Go's source, because no Go binary CI could build understood a vector
//! index at all. `GO_COMPAT_COMMIT` now reaches past the merge, so these are the
//! first vector assertions a Go node actually answers.
//!
//! What each one pins, in the words of the parity contract: the same SDL is
//! accepted by both, the same query returns the same documents in the same
//! order, and the shapes only one runtime can express are refused by the other
//! rather than misread.

use integration_test::{DefraClient, TestCluster};
use serde_json::Value;

/// Eight dimensions, which is enough to separate the corpus and short enough to
/// read in a failure message.
const DIMENSIONS: usize = 8;
const CORPUS: usize = 12;

/// The folded directive, in the spelling Go merged in
/// sourcenetwork/defradb#5188. Both runtimes have to accept this verbatim.
const SCHEMA: &str = r#"
    type Note {
        title: String
        embedding: [Float32!] @index(vector: {
            dimensions: 8,
            hnsw: {metric: COSINE, M: 16, efConstruction: 128, efSearch: 64}
        })
    }
"#;

fn vector_for(index: usize) -> Vec<f64> {
    (0..DIMENSIONS)
        .map(|slot| ((index + slot) % 17) as f64)
        .collect()
}

fn render(vector: &[f64]) -> String {
    vector
        .iter()
        .map(|value| format!("{value:?}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Development mode, because the divergence cases purge between schemas and a
/// non-development node refuses that with a 403.
async fn mixed_cluster() -> TestCluster {
    TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_development()
        .build()
        .await
        .expect("mixed rust/go cluster")
}

fn seed(client: &DefraClient, label: &str) {
    for index in 0..CORPUS {
        let document = format!(
            r#"{{"title": "note-{index}", "embedding": [{}]}}"#,
            render(&vector_for(index))
        );
        client
            .collection_create("Note", &document)
            .unwrap_or_else(|e| panic!("{label}: seed note-{index}: {e}"));
    }
}

fn titles(value: &Value, label: &str) -> Vec<String> {
    value["Note"]
        .as_array()
        .unwrap_or_else(|| panic!("{label}: no Note array in {value}"))
        .iter()
        .filter_map(|row| row["title"].as_str().map(str::to_string))
        .collect()
}

/// The folded `@index(vector: {...})` is one grammar, so a schema either
/// runtime accepts is accepted by both. Before the fold each spoke a different
/// directive and neither could read the other's.
#[tokio::test]
async fn go_vector_index_sdl_is_accepted_by_both() {
    let cluster = mixed_cluster().await;
    let rust = cluster.client(0);
    let go = cluster.client(1);

    rust.schema_add(SCHEMA)
        .expect("Rust must accept the schema");
    go.schema_add(SCHEMA).expect("Go must accept the schema");

    // The description each runtime derived from that SDL has to agree, or the
    // same schema means two different indexes.
    let rust_indexes = rust.index_list(Some("Note")).expect("Rust index list");
    let go_indexes = go.index_list(Some("Note")).expect("Go index list");

    let vector_of = |value: &Value, label: &str| -> Value {
        let text = serde_json::to_string(value).unwrap_or_default();
        assert!(
            text.contains("\"Kind\""),
            "{label}: no Kind discriminator in {text}"
        );
        value.clone()
    };
    let rust_shape = vector_of(&rust_indexes, "rust");
    let go_shape = vector_of(&go_indexes, "go");

    for (label, shape) in [("rust", &rust_shape), ("go", &go_shape)] {
        let text = serde_json::to_string(shape).unwrap_or_default();
        assert!(
            text.contains("COSINE"),
            "{label}: the metric did not survive: {text}"
        );
        assert!(
            text.contains("\"Dimensions\":8") || text.contains("\"Dimensions\": 8"),
            "{label}: the dimensions did not survive: {text}"
        );
    }
}

/// The routed shape returns the same documents in the same order on both. This
/// is the parity contract's first clause, and the first time it has been
/// checked against a Go node for a vector query.
#[tokio::test]
async fn go_vector_similarity_ordering_matches() {
    let cluster = mixed_cluster().await;
    let rust = cluster.client(0);
    let go = cluster.client(1);

    rust.schema_add(SCHEMA).expect("Rust schema");
    go.schema_add(SCHEMA).expect("Go schema");
    seed(&rust, "rust");
    seed(&go, "go");

    let query = format!(
        r#"query {{ Note(order: {{_alias: {{sim: DESC}}}}, limit: 5) {{ title sim: SIMILARITY(embedding: {{vector: [{}]}}) }} }}"#,
        render(&vector_for(3))
    );

    let rust_rows = rust.query(&query).expect("Rust query");
    let go_rows = go.query(&query).expect("Go query");

    assert_eq!(
        titles(&rust_rows, "rust"),
        titles(&go_rows, "go"),
        "the routed page differs between runtimes"
    );
}

/// A shape neither runtime routes still has to agree. Ours declines and scans,
/// Go declines and scans, and the answer must be the same one.
#[tokio::test]
async fn go_vector_unrouted_shapes_agree() {
    let cluster = mixed_cluster().await;
    let rust = cluster.client(0);
    let go = cluster.client(1);

    rust.schema_add(SCHEMA).expect("Rust schema");
    go.schema_add(SCHEMA).expect("Go schema");
    seed(&rust, "rust");
    seed(&go, "go");

    let probe = render(&vector_for(3));
    for (label, query) in [
        (
            "no limit",
            format!(
                r#"query {{ Note(order: {{_alias: {{sim: DESC}}}}) {{ title sim: SIMILARITY(embedding: {{vector: [{probe}]}}) }} }}"#
            ),
        ),
        (
            "ascending",
            format!(
                r#"query {{ Note(order: {{_alias: {{sim: ASC}}}}, limit: 5) {{ title sim: SIMILARITY(embedding: {{vector: [{probe}]}}) }} }}"#
            ),
        ),
    ] {
        let rust_rows = rust
            .query(&query)
            .unwrap_or_else(|e| panic!("{label}: rust: {e}"));
        let go_rows = go
            .query(&query)
            .unwrap_or_else(|e| panic!("{label}: go: {e}"));
        assert_eq!(
            titles(&rust_rows, "rust"),
            titles(&go_rows, "go"),
            "{label}: an unrouted query differs between runtimes"
        );
    }
}

/// Our extra algorithms are a stated divergence, so a Go node must refuse a
/// schema carrying one rather than read it as something else. A silent
/// misparse is what #1518 existed to close, and this is the executable form of
/// that fence.
#[tokio::test]
async fn go_refuses_our_divergent_algorithms() {
    let cluster = mixed_cluster().await;
    let rust = cluster.client(0);
    let go = cluster.client(1);

    for (label, sdl) in [
        (
            "ivfpq",
            "type Diverged { e: [Float32!] @index(vector: {dimensions: 8, ivfpq: {nlist: 4}}) }",
        ),
        (
            "ssg",
            "type Diverged { e: [Float32!] @index(vector: {dimensions: 8, ssg: {R: 32}}) }",
        ),
        (
            "flat",
            "type Diverged { e: [Float32!] @index(vector: {dimensions: 8, flat: {}}) }",
        ),
    ] {
        rust.schema_add(sdl)
            .unwrap_or_else(|e| panic!("{label}: ours must accept its own algorithm: {e}"));
        assert!(
            go.schema_add(sdl).is_err(),
            "{label}: Go accepted an algorithm it does not implement, which means it read it as something else"
        );
        rust.purge().expect("purge Rust between cases");
        go.purge().expect("purge Go between cases");
    }
}

/// Both runtimes share the HNSW parameter ceilings, so a schema one refuses the
/// other must refuse too. A parameter accepted by only one is a schema that
/// replicates into a node that cannot build it.
#[tokio::test]
async fn go_and_rust_refuse_the_same_out_of_range_params() {
    let cluster = mixed_cluster().await;
    let rust = cluster.client(0);
    let go = cluster.client(1);

    for (label, sdl) in [
        (
            "zero dimensions",
            "type Bad { e: [Float32!] @index(vector: {dimensions: 0}) }",
        ),
        (
            "absent dimensions",
            "type Bad { e: [Float32!] @index(vector: {}) }",
        ),
        (
            "unknown metric",
            "type Bad { e: [Float32!] @index(vector: {dimensions: 8, hnsw: {metric: MANHATTAN}}) }",
        ),
    ] {
        assert!(
            rust.schema_add(sdl).is_err(),
            "{label}: ours accepted a schema it should refuse"
        );
        assert!(
            go.schema_add(sdl).is_err(),
            "{label}: Go accepted a schema ours refuses"
        );
    }
}
