//! Two-node peer-to-peer replication benchmarks.
//!
//! These measure how long it takes a write on one [`EmbeddedNode`] to become readable on
//! a second [`EmbeddedNode`] in the same process, connected over Iroh with relays and
//! discovery disabled.
//!
//! They are written as `#[ignore]`d tests rather than as criterion benchmarks. Criterion
//! is built around many short, repeatable iterations of a pure function, whereas a single
//! iteration here has to build two nodes, establish a QUIC connection and wait for
//! network convergence. Criterion's sampling would spend most of its time on node
//! construction, and its statistics would describe that rather than replication. A plain
//! test that runs each size once and prints the spans it measured is both cheaper and
//! more honest.
//!
//! Run with:
//!
//! ```text
//! cargo test -p defra-node --features p2p p2p_bench -- --ignored --nocapture
//! ```

use std::net::{IpAddr, Ipv4Addr};
use std::time::{Duration, Instant};

use super::{EmbeddedNode, P2PConfig, QueryResponse};

/// Document-set sizes each benchmark is run against.
const DOC_COUNTS: &[usize] = &[1, 50, 500];

/// History depths [`bench_catch_up_by_history_depth`] is run against.
///
/// A depth of one thousand cannot currently be measured: catching up on a DAG that deep
/// overflows the stack of a tokio worker thread and aborts the process, which points at an
/// unbounded recursive walk somewhere in the fetch-and-merge path. This should be extended
/// once that walk is bounded or made iterative.
const CATCH_UP_DEPTHS: &[usize] = &[10, 100, 500];

/// Document-set sizes [`bench_pull_by_document_count`] is run against.
///
/// These mirror the Go `Sync_Pull/docs=1` and `Sync_Pull/docs=50` benchmarks so the two
/// sets of rows can be read side by side.
const PULL_DOC_COUNTS: &[usize] = &[1, 50];

/// How long to keep polling the receiving node after `sync_documents` has returned.
///
/// The call is supposed to have waited for the documents itself, so anything that only
/// shows up during this window arrived after the call claimed to be finished.
const PULL_SETTLE_WINDOW: Duration = Duration::from_secs(5);

/// How long to wait for a node to converge before giving up.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(120);

/// How often to re-query the receiving node while waiting for convergence.
///
/// This is deliberately far tighter than the interval used by the correctness tests in
/// `p2p_tests`, because it bounds the resolution of every latency reported here: a
/// measured latency can overshoot the true one by up to this much.
const POLL_INTERVAL: Duration = Duration::from_millis(1);

const BENCH_SDL: &str = "type User { name: String age: Int }";

fn bench_p2p_config() -> P2PConfig {
    P2PConfig {
        port: 0,
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
        discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
        max_concurrent_multipath_paths: None,
        secret_key_path: None,
        load_persisted_collections: false,
        max_concurrent_dag_fetches: p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
        max_concurrent_push_tasks: p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
        max_doc_sync_request_doc_ids: p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
        rate_limit_burst: p2p::sync::DEFAULT_RATE_LIMIT_BURST,
        rate_limit_rate: p2p::sync::DEFAULT_RATE_LIMIT_RATE,
        max_pending_dags: p2p::sync::DEFAULT_MAX_PENDING_DAGS,
    }
}

/// The wall-clock spans captured by one benchmark run.
struct ReplicationSpans {
    /// Time spent issuing the create mutations on the sending node, measured from the
    /// first mutation being issued to the last one returning.
    ///
    /// On the push path this overlaps with replication - the replicator starts pushing
    /// the earlier documents while the later ones are still being written.
    local_write: Duration,

    /// Time from the first create mutation being issued on the sending node to a query
    /// against the receiving node returning every document.
    ///
    /// This is the write-to-remote-visible latency. It is bounded by an actual read of
    /// the receiving node's data, not by an internal event, so it includes everything
    /// between the two: local commit, push, block transfer, merge, and the receiving
    /// node's read path.
    visible: Duration,

    /// Time from the last create mutation returning on the sending node to the receiving
    /// node having every document.
    ///
    /// This isolates the replication tail from the cost of producing the writes, and is
    /// the more meaningful figure at larger document counts, where issuing the mutations
    /// dominates [`ReplicationSpans::visible`].
    tail: Duration,
}

/// Builds a node with peer-to-peer enabled and the benchmark schema installed.
async fn build_bench_node() -> EmbeddedNode {
    let node = EmbeddedNode::builder()
        .with_p2p(bench_p2p_config())
        .build()
        .await
        .expect("build P2P node");
    node.add_schema(BENCH_SDL).await.expect("add bench schema");

    node
}

async fn listen_addr(node: &EmbeddedNode) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = node
            .p2p()
            .expect("P2P should be enabled")
            .listen_addresses()
            .await
            .expect("listen_addresses should succeed");
        if let Some(addr) = addrs.first() {
            return addr.clone();
        }
        assert!(
            Instant::now() < deadline,
            "node never exposed a P2P listen address"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn await_connected_peer(node: &EmbeddedNode) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let peers = node
            .p2p()
            .expect("P2P should be enabled")
            .connected_peers()
            .await
            .expect("connected_peers should succeed");
        if !peers.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node never reported a connected peer"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Connects the two nodes and configures `sender` to replicate `User` to `receiver`.
///
/// All of this is done before any timing starts, so that the benchmarks measure the
/// replication of writes rather than the cost of establishing the relationship.
async fn connect_and_replicate(sender: &EmbeddedNode, receiver: &EmbeddedNode) {
    let sender_addr = listen_addr(sender).await;
    let receiver_addr = listen_addr(receiver).await;

    let sender_p2p = sender.p2p().expect("sender P2P should be enabled");
    let receiver_p2p = receiver.p2p().expect("receiver P2P should be enabled");

    sender_p2p
        .connect_peer(&receiver_addr)
        .await
        .expect("connect sender to receiver");
    await_connected_peer(sender).await;
    await_connected_peer(receiver).await;

    let collections = vec!["User".to_string()];
    sender_p2p
        .add_collections(collections.clone())
        .await
        .expect("subscribe sender to User");
    receiver_p2p
        .add_collections(collections.clone())
        .await
        .expect("subscribe receiver to User");

    receiver_p2p
        .add_replicator(
            collections.clone(),
            Some(&sender_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("authorize sender on receiver");
    sender_p2p
        .add_replicator(
            collections,
            Some(&receiver_addr),
            Default::default(),
            Vec::new(),
            None,
        )
        .await
        .expect("set replicator from sender to receiver");
}

/// Connects `receiver` to `sender` without configuring any replicator.
///
/// [`connect_and_replicate`] cannot be used for the pull benchmark: the replicator it
/// installs backfills documents that already exist on the sender - that is exactly what
/// [`measure_catch_up`] relies on - which would deliver the documents before
/// `sync_documents` is ever called. A bare connection is also all the serving side needs,
/// since it authorizes DocSync heads for any connected peer.
async fn connect_only(sender: &EmbeddedNode, receiver: &EmbeddedNode) {
    let sender_addr = listen_addr(sender).await;

    let sender_p2p = sender.p2p().expect("sender P2P should be enabled");
    let receiver_p2p = receiver.p2p().expect("receiver P2P should be enabled");

    receiver_p2p
        .connect_peer(&sender_addr)
        .await
        .expect("connect receiver to sender");
    await_connected_peer(receiver).await;
    await_connected_peer(sender).await;

    let collections = vec!["User".to_string()];
    sender_p2p
        .add_collections(collections.clone())
        .await
        .expect("subscribe sender to User");
    receiver_p2p
        .add_collections(collections)
        .await
        .expect("subscribe receiver to User");
}

/// Returns the number of `User` documents currently readable on the node.
async fn user_count(node: &EmbeddedNode) -> usize {
    let response = node.execute("query { User { _docID } }").await;
    assert!(
        response.errors.is_empty(),
        "query returned errors: {:?}",
        response.errors
    );

    response
        .data
        .as_ref()
        .and_then(|data| data.get("User"))
        .and_then(|users| users.as_array())
        .map(|users| users.len())
        .unwrap_or(0)
}

/// Polls the node until it holds `expected` documents, or panics on timeout.
async fn await_user_count(node: &EmbeddedNode, expected: usize) {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let count = user_count(node).await;
        if count >= expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "receiving node reached only {count} of {expected} documents within {CONVERGENCE_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Stops both nodes before the next measurement builds more.
///
/// Neither `EmbeddedNode` nor the `P2PLifecycle` it owns has a `Drop` impl, so the Iroh
/// endpoint and its background tasks survive the nodes going out of scope. Without this,
/// every size in a benchmark would run alongside the leftovers of all the earlier ones.
async fn shutdown_nodes(sender: &EmbeddedNode, receiver: &EmbeddedNode) {
    sender.shutdown().await;
    receiver.shutdown().await;
}

/// Returns the `_docID` of the single document a create mutation reported.
fn created_doc_id(response: &QueryResponse) -> String {
    response
        .data
        .as_ref()
        .and_then(|data| data.get("add_User"))
        .and_then(|users| users.as_array())
        .and_then(|users| users.first())
        .and_then(|user| user.get("_docID"))
        .and_then(|id| id.as_str())
        .expect("created document should have a _docID")
        .to_string()
}

/// Runs one replication measurement for `doc_count` documents.
async fn measure_replication(doc_count: usize) -> ReplicationSpans {
    let sender = build_bench_node().await;
    let receiver = build_bench_node().await;
    connect_and_replicate(&sender, &receiver).await;

    let start = Instant::now();
    for i in 0..doc_count {
        let response = sender
            .execute(&format!(
                r#"mutation {{ add_User(input: {{name: "User-{i}", age: {}}}) {{ _docID }} }}"#,
                i % 100
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "create mutation returned errors: {:?}",
            response.errors
        );
    }
    let local_write = start.elapsed();

    await_user_count(&receiver, doc_count).await;
    let visible = start.elapsed();

    shutdown_nodes(&sender, &receiver).await;

    ReplicationSpans {
        local_write,
        visible,
        tail: visible.saturating_sub(local_write),
    }
}

fn report(doc_count: usize, spans: &ReplicationSpans) {
    let docs = doc_count as f64;
    let visible_secs = spans.visible.as_secs_f64();
    let throughput = if visible_secs > 0.0 {
        docs / visible_secs
    } else {
        f64::INFINITY
    };

    println!(
        "docs={doc_count:<4} visible={:>9.2?}  local_write={:>9.2?}  tail={:>9.2?}  {throughput:>9.1} docs/s",
        spans.visible, spans.local_write, spans.tail
    );
}

/// Measures write-to-remote-visible latency and replication throughput for the live push
/// path, across [`DOC_COUNTS`].
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: spins up two networked nodes per document count"]
async fn bench_two_node_push_replication() {
    println!("two-node push replication (poll resolution {POLL_INTERVAL:?})");
    for &doc_count in DOC_COUNTS {
        let spans = measure_replication(doc_count).await;
        report(doc_count, &spans);
    }
}

/// Measures how long a receiving node takes to catch up on a document whose history was
/// built while it was not yet replicating, across increasing history depths.
///
/// The updates are applied before the receiving node exists, so it has to fetch the whole
/// DAG in one go once replication begins. The reported latency is the time from the
/// receiving node starting to connect through to it reading the final value, and so
/// includes connection establishment - unlike the push benchmark, that setup is part of
/// what a late-joining node has to pay.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: spins up two networked nodes per history depth"]
async fn bench_catch_up_by_history_depth() {
    println!("catch-up by history depth (poll resolution {POLL_INTERVAL:?})");
    for &depth in CATCH_UP_DEPTHS {
        let latency = measure_catch_up(depth).await;
        let updates_per_sec = depth as f64 / latency.as_secs_f64();
        println!("updates={depth:<5} catch_up={latency:>9.2?}  {updates_per_sec:>9.1} updates/s");
    }
}

/// Returns the time taken for a receiving node to converge on a document that already has
/// `update_count` updates in its history when replication is configured.
async fn measure_catch_up(update_count: usize) -> Duration {
    let sender = build_bench_node().await;

    let response = sender
        .execute(r#"mutation { add_User(input: {name: "Historic", age: 0}) { _docID } }"#)
        .await;
    assert!(
        response.errors.is_empty(),
        "create mutation returned errors: {:?}",
        response.errors
    );

    let doc_id = created_doc_id(&response);

    for age in 1..=update_count {
        let response = sender
            .execute(&format!(
                r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: {age}}}) {{ _docID }} }}"#
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "update mutation returned errors: {:?}",
            response.errors
        );
    }

    // The receiving node is built only once the history exists, both because that is the
    // scenario being measured - a node joining a document that already has a deep DAG -
    // and because an idle node left waiting through a long write phase eventually becomes
    // undialable.
    let receiver = build_bench_node().await;

    let start = Instant::now();
    connect_and_replicate(&sender, &receiver).await;
    await_user_age(&receiver, update_count as i64).await;
    let elapsed = start.elapsed();

    shutdown_nodes(&sender, &receiver).await;

    elapsed
}

/// Polls the node until its single `User` document reports `expected` as its age.
async fn await_user_age(node: &EmbeddedNode, expected: i64) {
    let deadline = Instant::now() + CONVERGENCE_TIMEOUT;
    loop {
        let response = node.execute("query { User { age } }").await;
        assert!(
            response.errors.is_empty(),
            "query returned errors: {:?}",
            response.errors
        );

        let age = response
            .data
            .as_ref()
            .and_then(|data| data.get("User"))
            .and_then(|users| users.as_array())
            .and_then(|users| users.first())
            .and_then(|user| user.get("age"))
            .and_then(|age| age.as_i64());
        if age == Some(expected) {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "receiving node reached age {age:?} rather than {expected} within {CONVERGENCE_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The outcome of one `sync_documents` measurement.
struct PullOutcome {
    /// Wall-clock duration of the `sync_documents` call itself.
    call: Duration,

    /// Whether `sync_documents` returned `Ok`.
    ///
    /// The Iroh implementation ends in an unconditional `Ok(())` after its timeouts
    /// expire, so this is recorded separately from what actually arrived.
    returned_ok: bool,

    /// Documents readable on the receiving node the moment `sync_documents` returned.
    arrived: usize,

    /// Documents readable on the receiving node after a further
    /// [`PULL_SETTLE_WINDOW`] of polling.
    ///
    /// Anything beyond [`PullOutcome::arrived`] landed after the call had already
    /// reported completion.
    settled: usize,
}

/// Pulls `doc_count` documents that the receiving node has never replicated.
///
/// The sender writes the documents before the two nodes are connected, and no replicator
/// is configured in either direction, so the only thing that can move them is the
/// receiver's own `sync_documents` call.
async fn measure_pull(doc_count: usize) -> PullOutcome {
    let sender = build_bench_node().await;

    let mut doc_ids = Vec::with_capacity(doc_count);
    for i in 0..doc_count {
        let response = sender
            .execute(&format!(
                r#"mutation {{ add_User(input: {{name: "User-{i}", age: {}}}) {{ _docID }} }}"#,
                i % 100
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "create mutation returned errors: {:?}",
            response.errors
        );

        doc_ids.push(created_doc_id(&response));
    }

    // Built after the writes for the same reason as in `measure_catch_up`: an idle node
    // left waiting through a long write phase eventually becomes undialable.
    let receiver = build_bench_node().await;
    connect_only(&sender, &receiver).await;
    assert_eq!(
        user_count(&receiver).await,
        0,
        "receiving node must not hold any documents before the pull"
    );

    let start = Instant::now();
    let result = receiver
        .p2p()
        .expect("receiver P2P should be enabled")
        .sync_documents("User", doc_ids)
        .await;
    let call = start.elapsed();

    let arrived = user_count(&receiver).await;

    let settle_deadline = Instant::now() + PULL_SETTLE_WINDOW;
    let mut settled = arrived;
    while settled < doc_count && Instant::now() < settle_deadline {
        tokio::time::sleep(POLL_INTERVAL).await;
        settled = user_count(&receiver).await;
    }

    shutdown_nodes(&sender, &receiver).await;

    PullOutcome {
        call,
        returned_ok: result.is_ok(),
        arrived,
        settled,
    }
}

/// Measures how long `sync_documents` takes to pull documents the receiving node has
/// never seen, across [`PULL_DOC_COUNTS`].
///
/// The duration is only half the result. `sync_documents` returns `P2PResult<()>` but its
/// Iroh implementation ends in an unconditional `Ok(())` once its timeouts expire, so the
/// document count on the receiving node - not the return value - is what says whether the
/// pull worked.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: spins up two networked nodes per document count"]
async fn bench_pull_by_document_count() {
    println!("document pull via sync_documents (poll resolution {POLL_INTERVAL:?})");
    for &doc_count in PULL_DOC_COUNTS {
        let outcome = measure_pull(doc_count).await;
        let returned = if outcome.returned_ok { "Ok" } else { "Err" };
        let honest = if outcome.returned_ok && outcome.arrived < doc_count {
            "  RETURNED Ok WITH DOCUMENTS MISSING"
        } else {
            ""
        };

        println!(
            "docs={doc_count:<4} call={:>9.2?}  returned={returned:<3}  arrived={:>4}/{doc_count:<4} settled={:>4}/{doc_count:<4}{honest}",
            outcome.call, outcome.arrived, outcome.settled
        );
    }
}
