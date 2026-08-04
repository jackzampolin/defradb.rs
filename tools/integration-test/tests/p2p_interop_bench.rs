//! Cross-runtime peer-to-peer replication cost.
//!
//! The same replication workload is run over three topologies - Rust to Rust, Go to Go,
//! and Go to Rust - and the resulting latencies are printed side by side. The amount by
//! which the mixed topology exceeds the two homogeneous ones is the cost of crossing the
//! runtime boundary.
//!
//! All three topologies are measured inside a single test so that the comparison is made
//! on one machine in one run. Comparing numbers from separately scheduled tests would fold
//! machine noise into the delta being measured.
//!
//! The Go topologies require a `defradb` binary on `PATH`; the test panics during cluster
//! construction if one is not found.
//!
//! Run with:
//!
//! ```text
//! cargo test -p integration-test --test p2p_interop_bench -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use integration_test::TestCluster;

const SCHEMA: &str = "type User { name: String  age: Int }";

/// Document-set sizes measured against every topology.
///
/// These are deliberately smaller than the sizes used by the in-process benchmarks: every
/// node here is a separate operating system process driven over HTTP, so each document
/// costs considerably more to write and to observe.
const DOC_COUNTS: &[usize] = &[1, 50];

/// How often to re-query the receiving node while waiting for convergence.
///
/// This bounds the resolution of every latency reported here - a measured latency can
/// overshoot the true one by up to this much.
const POLL_INTERVAL: Duration = Duration::from_millis(5);

const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// One topology's measurement for one document-set size.
struct Measurement {
    doc_count: usize,
    /// Time from the first create mutation being issued on node 0 to a query against
    /// node 1 returning every document.
    visible: Duration,
    /// Time from the last create mutation returning on node 0 to node 1 holding every
    /// document. This isolates the replication tail from the cost of issuing the writes.
    tail: Duration,
}

/// Brings both nodes to the point where node 0 replicates `User` to node 1.
///
/// Returns once the replicator is configured. None of this is timed - the benchmarks
/// measure the replication of writes, not the cost of establishing the relationship.
async fn establish_replication(cluster: &TestCluster) {
    cluster
        .wait_for_log(0, "p2p_listening", STARTUP_TIMEOUT)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", STARTUP_TIMEOUT)
        .await
        .expect("node1 P2P listener did not start");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address")
        .to_string();

    node0.schema_add(SCHEMA).expect("add schema on node0");
    node1.schema_add(SCHEMA).expect("add schema on node1");

    node0
        .p2p_connect(&[&addr1])
        .expect("connect node0 to node1");
    node0
        .p2p_collection_add(&["User"])
        .expect("subscribe node0 to User");
    node1
        .p2p_collection_add(&["User"])
        .expect("subscribe node1 to User");
    node0
        .p2p_replicator_set(&["User"], &addr1)
        .expect("set replicator from node0 to node1");
}

/// Returns the number of `User` documents currently readable on the node.
///
/// A failed query panics rather than counting as zero: absorbing a node failure into
/// the convergence poll would turn it into a [`CONVERGENCE_TIMEOUT`] wait reported as a
/// slow topology.
fn user_count(cluster: &TestCluster, node_index: usize) -> usize {
    let result = cluster
        .client(node_index)
        .query("query { User { _docID } }")
        .unwrap_or_else(|e| panic!("query node{node_index} for User documents: {e}"));

    result["User"]
        .as_array()
        .map(|users| users.len())
        .unwrap_or(0)
}

/// Creates `doc_count` further documents on node 0 and times how long node 1 takes to
/// hold all of them.
///
/// `baseline` is the number of documents node 1 already held, so that successive
/// measurements can share one cluster - building a cluster launches real processes and is
/// far more expensive than the measurement itself.
async fn measure(cluster: &TestCluster, doc_count: usize, baseline: usize) -> Measurement {
    let node0 = cluster.client(0);
    let expected = baseline + doc_count;

    let start = Instant::now();
    for i in baseline..expected {
        node0
            .query(&format!(
                r#"mutation {{ add_User(input: {{name: "User-{i}", age: {}}}) {{ _docID }} }}"#,
                i % 100
            ))
            .expect("create document on node0");
    }
    let local_write = start.elapsed();

    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let count = user_count(cluster, 1);
        if count >= expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node1 reached only {count} of {expected} documents within {CONVERGENCE_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
    let visible = start.elapsed();

    Measurement {
        doc_count,
        visible,
        tail: visible.saturating_sub(local_write),
    }
}

/// Runs every size in [`DOC_COUNTS`] against one already-replicating cluster.
async fn measure_topology(cluster: TestCluster) -> Vec<Measurement> {
    establish_replication(&cluster).await;

    let mut measurements = Vec::with_capacity(DOC_COUNTS.len());
    let mut baseline = 0;
    for &doc_count in DOC_COUNTS {
        measurements.push(measure(&cluster, doc_count, baseline).await);
        baseline += doc_count;
    }

    measurements
}

/// Returns how much slower the mixed topology was than the homogeneous baseline, in
/// milliseconds and as a percentage.
///
/// The baseline is the mean of the two homogeneous topologies, since a mixed pair is half
/// of each. Both figures are signed: a mixed pair can legitimately land between the two
/// homogeneous pairs, and clamping that to zero would hide it.
fn interop_delta(rust_rust: Duration, go_go: Duration, go_rust: Duration) -> (f64, f64) {
    let baseline = (rust_rust.as_secs_f64() + go_go.as_secs_f64()) / 2.0;
    let delta_ms = (go_rust.as_secs_f64() - baseline) * 1e3;
    let percent = if baseline == 0.0 {
        f64::INFINITY
    } else {
        (go_rust.as_secs_f64() / baseline - 1.0) * 100.0
    };

    (delta_ms, percent)
}

/// Measures replication latency across all three topologies and reports the cost of the
/// mixed one.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: launches six node processes and requires a `defradb` binary on PATH"]
async fn bench_p2p_interop_cost() {
    let rust_rust = measure_topology(
        TestCluster::builder()
            .rust_nodes(2)
            .with_p2p()
            .build()
            .await
            .expect("build rust/rust cluster"),
    )
    .await;

    let go_go = measure_topology(
        TestCluster::builder()
            .go_nodes(2)
            .with_p2p()
            .build()
            .await
            .expect("build go/go cluster"),
    )
    .await;

    let go_rust = measure_topology(
        TestCluster::builder()
            .rust_nodes(1)
            .go_nodes(1)
            .with_p2p()
            .build()
            .await
            .expect("build go/rust cluster"),
    )
    .await;

    println!("\ncross-runtime replication cost (poll resolution {POLL_INTERVAL:?})");
    println!(
        "{:<6} {:>12} {:>12} {:>12} {:>12} {:>9}",
        "docs", "rust->rust", "go->go", "go->rust", "delta", "delta %"
    );

    for i in 0..DOC_COUNTS.len() {
        let (rr, gg, gr) = (&rust_rust[i], &go_go[i], &go_rust[i]);
        let (delta_ms, percent) = interop_delta(rr.visible, gg.visible, gr.visible);
        println!(
            "{:<6} {:>12.2?} {:>12.2?} {:>12.2?} {delta_ms:>+9.2} ms {percent:>+8.1}%",
            rr.doc_count, rr.visible, gg.visible, gr.visible
        );
    }

    println!("\nreplication tail only (excludes the cost of issuing the writes)");
    println!(
        "{:<6} {:>12} {:>12} {:>12} {:>12} {:>9}",
        "docs", "rust->rust", "go->go", "go->rust", "delta", "delta %"
    );

    for i in 0..DOC_COUNTS.len() {
        let (rr, gg, gr) = (&rust_rust[i], &go_go[i], &go_rust[i]);
        let (delta_ms, percent) = interop_delta(rr.tail, gg.tail, gr.tail);
        println!(
            "{:<6} {:>12.2?} {:>12.2?} {:>12.2?} {delta_ms:>+9.2} ms {percent:>+8.1}%",
            rr.doc_count, rr.tail, gg.tail, gr.tail
        );
    }
}
