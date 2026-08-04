//! Cross-runtime peer-to-peer replication cost.
//!
//! The same replication workload is run over four topologies - Rust to Rust, Go to Go,
//! Rust to Go and Go to Rust - and the resulting latencies are printed side by side. The
//! amount by which a mixed topology exceeds the two homogeneous ones is the cost of
//! crossing the runtime boundary.
//!
//! Both mixed directions are measured because they are not the same experiment: a Rust
//! sender pushing to a Go receiver exercises Rust's replicator against Go's block handler,
//! and the reverse pairing exercises the opposite halves. Reporting one of them as "the"
//! interop cost would hide whichever half is slower.
//!
//! All four topologies are measured inside a single test so that the comparison is made
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
    /// Time from the first create mutation being issued on the sender to a query against
    /// the receiver returning every document.
    visible: Duration,
    /// Time from the last create mutation returning on the sender to the receiver holding
    /// every document. This isolates the replication tail from the cost of issuing the
    /// writes.
    tail: Duration,
}

/// One topology's label and its measurements, in [`DOC_COUNTS`] order.
struct Topology {
    label: &'static str,
    measurements: Vec<Measurement>,
}

/// Brings both nodes to the point where `sender` replicates `User` to `receiver`.
///
/// The indices are explicit because the cluster builder places Rust nodes at the low
/// indices and Go nodes after them, so which runtime sits at which index depends on how
/// the cluster was configured.
///
/// Returns once the replicator is configured. None of this is timed - the benchmarks
/// measure the replication of writes, not the cost of establishing the relationship.
async fn establish_replication(cluster: &TestCluster, sender: usize, receiver: usize) {
    for index in [sender, receiver] {
        cluster
            .wait_for_log(index, "p2p_listening", STARTUP_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("node{index} P2P listener did not start: {e}"));
    }

    let sender_client = cluster.client(sender);
    let receiver_client = cluster.client(receiver);

    let receiver_info = receiver_client
        .p2p_info()
        .unwrap_or_else(|e| panic!("failed to get node{receiver} p2p info: {e}"));
    let receiver_addr = receiver_info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("node{receiver} has no P2P address"))
        .to_string();

    sender_client
        .schema_add(SCHEMA)
        .unwrap_or_else(|e| panic!("add schema on node{sender}: {e}"));
    receiver_client
        .schema_add(SCHEMA)
        .unwrap_or_else(|e| panic!("add schema on node{receiver}: {e}"));

    sender_client
        .p2p_connect(&[&receiver_addr])
        .unwrap_or_else(|e| panic!("connect node{sender} to node{receiver}: {e}"));
    sender_client
        .p2p_collection_add(&["User"])
        .unwrap_or_else(|e| panic!("subscribe node{sender} to User: {e}"));
    receiver_client
        .p2p_collection_add(&["User"])
        .unwrap_or_else(|e| panic!("subscribe node{receiver} to User: {e}"));
    sender_client
        .p2p_replicator_set(&["User"], &receiver_addr)
        .unwrap_or_else(|e| panic!("set replicator from node{sender} to node{receiver}: {e}"));
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

/// Creates `doc_count` further documents on `sender` and times how long `receiver` takes
/// to hold all of them.
///
/// `baseline` is the number of documents the receiver already held, so that successive
/// measurements can share one cluster - building a cluster launches real processes and is
/// far more expensive than the measurement itself.
async fn measure(
    cluster: &TestCluster,
    sender: usize,
    receiver: usize,
    doc_count: usize,
    baseline: usize,
) -> Measurement {
    let sender_client = cluster.client(sender);
    let expected = baseline + doc_count;

    let start = Instant::now();
    for i in baseline..expected {
        sender_client
            .query(&format!(
                r#"mutation {{ add_User(input: {{name: "User-{i}", age: {}}}) {{ _docID }} }}"#,
                i % 100
            ))
            .unwrap_or_else(|e| panic!("create document on node{sender}: {e}"));
    }
    let local_write = start.elapsed();

    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let count = user_count(cluster, receiver);
        if count >= expected {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "node{receiver} reached only {count} of {expected} documents \
             within {CONVERGENCE_TIMEOUT:?}"
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

/// Runs every size in [`DOC_COUNTS`] against one cluster, replicating `sender` to
/// `receiver`.
async fn measure_topology(
    label: &'static str,
    cluster: TestCluster,
    sender: usize,
    receiver: usize,
) -> Topology {
    establish_replication(&cluster, sender, receiver).await;

    let mut measurements = Vec::with_capacity(DOC_COUNTS.len());
    let mut baseline = 0;
    for &doc_count in DOC_COUNTS {
        measurements.push(measure(&cluster, sender, receiver, doc_count, baseline).await);
        baseline += doc_count;
    }

    Topology {
        label,
        measurements,
    }
}

/// Returns how much slower a mixed topology was than the homogeneous baseline, in
/// milliseconds and as a percentage.
///
/// The baseline is the mean of the two homogeneous topologies at the same document-set
/// size, since a mixed pair is half of each. Both mixed directions are compared against
/// that same baseline. Both figures are signed: a mixed pair can legitimately land between
/// the two homogeneous pairs, and clamping that to zero would hide it.
fn interop_delta(rust_rust: Duration, go_go: Duration, mixed: Duration) -> (f64, f64) {
    let baseline = (rust_rust.as_secs_f64() + go_go.as_secs_f64()) / 2.0;
    let delta_ms = (mixed.as_secs_f64() - baseline) * 1e3;
    let percent = if baseline == 0.0 {
        f64::INFINITY
    } else {
        (mixed.as_secs_f64() / baseline - 1.0) * 100.0
    };

    (delta_ms, percent)
}

/// Prints one metric for every topology and document-set size.
///
/// Each mixed row carries the baseline it was compared against, so the delta columns are
/// never ambiguous about which homogeneous pair they refer to.
fn print_table(
    title: &str,
    rust_rust: &Topology,
    go_go: &Topology,
    mixed: [&Topology; 2],
    metric: fn(&Measurement) -> Duration,
) {
    println!("\n{title}");
    println!(
        "{:<6} {:<11} {:>12} {:>12} {:>12} {:>9}",
        "docs", "topology", "latency", "baseline", "delta", "delta %"
    );

    for i in 0..DOC_COUNTS.len() {
        let rr = metric(&rust_rust.measurements[i]);
        let gg = metric(&go_go.measurements[i]);
        let doc_count = rust_rust.measurements[i].doc_count;

        for topology in [rust_rust, go_go] {
            println!(
                "{doc_count:<6} {:<11} {:>12.2?} {:>12} {:>12} {:>9}",
                topology.label,
                metric(&topology.measurements[i]),
                "-",
                "-",
                "-"
            );
        }

        for topology in mixed {
            let value = metric(&topology.measurements[i]);
            let (delta_ms, percent) = interop_delta(rr, gg, value);
            println!(
                "{doc_count:<6} {:<11} {:>12.2?} {:>12.2?} {delta_ms:>+9.2} ms {percent:>+8.1}%",
                topology.label,
                value,
                (rr + gg) / 2
            );
        }
    }
}

/// Measures replication latency across all four topologies and reports the cost of each
/// mixed direction.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: launches eight node processes and requires a `defradb` binary on PATH"]
async fn bench_p2p_interop_cost() {
    let rust_rust = measure_topology(
        "rust->rust",
        TestCluster::builder()
            .rust_nodes(2)
            .with_p2p()
            .build()
            .await
            .expect("build rust/rust cluster"),
        0,
        1,
    )
    .await;

    let go_go = measure_topology(
        "go->go",
        TestCluster::builder()
            .go_nodes(2)
            .with_p2p()
            .build()
            .await
            .expect("build go/go cluster"),
        0,
        1,
    )
    .await;

    // The builder spawns Rust nodes first, so in a one-Rust/one-Go cluster node 0 is the
    // Rust node and node 1 is the Go node. Each direction gets its own cluster: a
    // replicator is one-way, and reusing a cluster would leave the documents from the
    // first direction already present for the second.
    let rust_go = measure_topology(
        "rust->go",
        TestCluster::builder()
            .rust_nodes(1)
            .go_nodes(1)
            .with_p2p()
            .build()
            .await
            .expect("build rust/go cluster"),
        0,
        1,
    )
    .await;

    let go_rust = measure_topology(
        "go->rust",
        TestCluster::builder()
            .rust_nodes(1)
            .go_nodes(1)
            .with_p2p()
            .build()
            .await
            .expect("build go/rust cluster"),
        1,
        0,
    )
    .await;

    print_table(
        &format!("cross-runtime replication cost (poll resolution {POLL_INTERVAL:?})"),
        &rust_rust,
        &go_go,
        [&rust_go, &go_rust],
        |m| m.visible,
    );

    print_table(
        "replication tail only (excludes the cost of issuing the writes)",
        &rust_rust,
        &go_go,
        [&rust_go, &go_rust],
        |m| m.tail,
    );
}
