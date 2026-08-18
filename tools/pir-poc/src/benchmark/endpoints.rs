use std::collections::BTreeMap;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::{micros, millis, percentile, Profile, SERVER_WORKER_THREADS};
use crate::dense;
use crate::http::{self, PirClient};
use crate::snapshot::{bucket_for_key, Record, Snapshot, SnapshotCatalog, SnapshotConfig};

const TAG: &[u8] = b"needle";
const PUBLIC_WINDOW_COUNT: usize = 64;
const ROW_SIZE: usize = 64;
const QUICK_SAMPLES: usize = 5;
const FULL_SAMPLES: usize = 15;
const SELECTED_WINDOW_COUNTS: [usize; 5] = [1, 4, 16, 32, 64];
const TARGET_SERVER_COUNTS: [usize; 2] = [2, 3];

#[derive(Debug, Serialize)]
pub struct EndpointBenchmarkReport {
    pub protocol: &'static str,
    pub profile: String,
    pub generated_at_unix_seconds: u64,
    pub methodology: &'static str,
    pub global_bucket_count: usize,
    pub public_window_count: usize,
    pub buckets_per_window: usize,
    pub row_size: usize,
    pub global_snapshot_bytes_per_replica: usize,
    pub all_window_snapshots_bytes_per_replica: usize,
    pub combined_catalog_bytes_per_replica: usize,
    pub catalog_build_ms: f64,
    pub results: Vec<EndpointScenarioResult>,
}

#[derive(Debug, Serialize)]
pub struct EndpointScenarioResult {
    pub mode: &'static str,
    pub selected_window_count: usize,
    pub server_count: usize,
    pub samples: usize,
    pub tables_scanned_per_server: usize,
    pub table_buckets_scanned_per_server: usize,
    pub expected_rows_xored_per_server: usize,
    pub expected_data_bytes_xored_per_server: usize,
    pub query_bytes_per_server: usize,
    pub total_upload_bytes: usize,
    pub response_bytes_per_server: usize,
    pub total_download_bytes: usize,
    pub client_share_generation_p50_us: f64,
    pub co_located_server_wall_p50_ms: f64,
    pub co_located_server_wall_p95_ms: f64,
    pub sum_server_elapsed_p50_ms: f64,
    pub http_end_to_end_p50_ms: f64,
    pub http_end_to_end_p95_ms: f64,
    pub expected_server_row_reduction_vs_global_percent: f64,
    pub measured_server_wall_reduction_vs_global_percent: f64,
    pub measured_http_wall_reduction_vs_global_percent: f64,
}

struct RawScenarioResult {
    mode: &'static str,
    selected_window_count: usize,
    server_count: usize,
    samples: usize,
    tables_scanned_per_server: usize,
    table_buckets_scanned_per_server: usize,
    expected_rows_xored_per_server: usize,
    expected_data_bytes_xored_per_server: usize,
    query_bytes_per_server: usize,
    total_upload_bytes: usize,
    response_bytes_per_server: usize,
    total_download_bytes: usize,
    client_share_generation_p50_us: f64,
    co_located_server_wall_p50_ms: f64,
    co_located_server_wall_p95_ms: f64,
    sum_server_elapsed_p50_ms: f64,
    http_end_to_end_p50_ms: f64,
    http_end_to_end_p95_ms: f64,
}

pub async fn run(profile: Profile) -> Result<EndpointBenchmarkReport> {
    let global_bucket_count = match profile {
        Profile::Quick => 1 << 20,
        Profile::Full => 1 << 22,
    };
    let samples = match profile {
        Profile::Quick => QUICK_SAMPLES,
        Profile::Full => FULL_SAMPLES,
    };
    let buckets_per_window = global_bucket_count / PUBLIC_WINDOW_COUNT;

    let build_started = Instant::now();
    let global = Arc::new(build_snapshot(
        global_bucket_count,
        "global",
        b"global-page",
    )?);
    let mut windows = BTreeMap::new();
    for index in 0..PUBLIC_WINDOW_COUNT {
        let window_id = format!("window-{index:02}");
        let value = format!("{window_id}-page");
        let snapshot = Arc::new(build_snapshot(
            buckets_per_window,
            &window_id,
            value.as_bytes(),
        )?);
        windows.insert(window_id, snapshot);
    }
    let catalog = Arc::new(SnapshotCatalog::new(Arc::clone(&global), windows)?);
    let catalog_build_ms = millis(build_started.elapsed());
    let all_window_snapshots_bytes_per_replica = catalog
        .windows()
        .values()
        .map(|snapshot| snapshot.rows().len())
        .sum::<usize>();

    let mut servers = Vec::new();
    for _ in 0..*TARGET_SERVER_COUNTS.iter().max().expect("non-empty counts") {
        servers.push(http::spawn_catalog(Arc::clone(&catalog), "127.0.0.1:0").await?);
    }
    let urls = servers
        .iter()
        .map(|server| format!("http://{}", server.address))
        .collect::<Vec<_>>();

    let mut raw_results = Vec::new();
    for server_count in TARGET_SERVER_COUNTS {
        let client = PirClient::connect(&urls[..server_count]).await?;
        raw_results.push(
            benchmark_scenario(
                profile,
                samples,
                server_count,
                &client,
                vec![Arc::clone(&global)],
                &[],
            )
            .await?,
        );
        for selected_window_count in SELECTED_WINDOW_COUNTS {
            let window_ids = catalog
                .windows()
                .keys()
                .take(selected_window_count)
                .cloned()
                .collect::<Vec<_>>();
            let snapshots = window_ids
                .iter()
                .map(|window_id| {
                    catalog
                        .window(window_id)
                        .cloned()
                        .with_context(|| format!("missing benchmark window {window_id}"))
                })
                .collect::<Result<Vec<_>>>()?;
            raw_results.push(
                benchmark_scenario(
                    profile,
                    samples,
                    server_count,
                    &client,
                    snapshots,
                    &window_ids,
                )
                .await?,
            );
        }
    }

    let mut results = Vec::with_capacity(raw_results.len());
    for raw in raw_results {
        let global_reference = results
            .iter()
            .find(|result: &&EndpointScenarioResult| {
                result.server_count == raw.server_count && result.mode == "global"
            })
            .map(|result| {
                (
                    result.expected_rows_xored_per_server as f64,
                    result.co_located_server_wall_p50_ms,
                    result.http_end_to_end_p50_ms,
                )
            })
            .unwrap_or((
                raw.expected_rows_xored_per_server as f64,
                raw.co_located_server_wall_p50_ms,
                raw.http_end_to_end_p50_ms,
            ));
        results.push(EndpointScenarioResult {
            mode: raw.mode,
            selected_window_count: raw.selected_window_count,
            server_count: raw.server_count,
            samples: raw.samples,
            tables_scanned_per_server: raw.tables_scanned_per_server,
            table_buckets_scanned_per_server: raw.table_buckets_scanned_per_server,
            expected_rows_xored_per_server: raw.expected_rows_xored_per_server,
            expected_data_bytes_xored_per_server: raw.expected_data_bytes_xored_per_server,
            query_bytes_per_server: raw.query_bytes_per_server,
            total_upload_bytes: raw.total_upload_bytes,
            response_bytes_per_server: raw.response_bytes_per_server,
            total_download_bytes: raw.total_download_bytes,
            client_share_generation_p50_us: raw.client_share_generation_p50_us,
            co_located_server_wall_p50_ms: raw.co_located_server_wall_p50_ms,
            co_located_server_wall_p95_ms: raw.co_located_server_wall_p95_ms,
            sum_server_elapsed_p50_ms: raw.sum_server_elapsed_p50_ms,
            http_end_to_end_p50_ms: raw.http_end_to_end_p50_ms,
            http_end_to_end_p95_ms: raw.http_end_to_end_p95_ms,
            expected_server_row_reduction_vs_global_percent: reduction_percent(
                global_reference.0,
                raw.expected_rows_xored_per_server as f64,
            ),
            measured_server_wall_reduction_vs_global_percent: reduction_percent(
                global_reference.1,
                raw.co_located_server_wall_p50_ms,
            ),
            measured_http_wall_reduction_vs_global_percent: reduction_percent(
                global_reference.2,
                raw.http_end_to_end_p50_ms,
            ),
        });
    }

    Ok(EndpointBenchmarkReport {
        protocol: "dense-xor-global-vs-public-window-http",
        profile: format!("{profile:?}").to_lowercase(),
        generated_at_unix_seconds: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        methodology: "Synthetic equal-sized immutable tables with one valid tag page each. Global covers the full bucket domain; 64 public windows partition the same capacity. Local measurements use persistent two-worker evaluators on co-located server threads. HTTP measurements use the real loopback endpoints and reusable clients, including fresh share generation, base64/JSON, server evaluation, response decoding, and reconstruction; TLS and real network latency are excluded. All replicas share snapshot allocations in this process, but storage fields report logical bytes per replica.",
        global_bucket_count,
        public_window_count: PUBLIC_WINDOW_COUNT,
        buckets_per_window,
        row_size: ROW_SIZE,
        global_snapshot_bytes_per_replica: global.rows().len(),
        all_window_snapshots_bytes_per_replica,
        combined_catalog_bytes_per_replica: global
            .rows()
            .len()
            .saturating_add(all_window_snapshots_bytes_per_replica),
        catalog_build_ms,
        results,
    })
}

async fn benchmark_scenario(
    profile: Profile,
    samples: usize,
    server_count: usize,
    client: &PirClient,
    snapshots: Vec<Arc<Snapshot>>,
    window_ids: &[String],
) -> Result<RawScenarioResult> {
    let mode = if window_ids.is_empty() {
        "global"
    } else {
        "public-window"
    };
    let pool = CatalogServerPool::new(server_count, SERVER_WORKER_THREADS)?;
    let mut rng = StdRng::seed_from_u64(
        snapshots
            .iter()
            .map(|snapshot| snapshot.manifest.bucket_count as u64)
            .sum::<u64>()
            ^ server_count as u64,
    );

    let warm_work = generate_work(&snapshots, server_count, &mut rng)?;
    verify_evaluation(&snapshots, pool.evaluate(warm_work)?)?;
    verify_http(client, window_ids).await?;

    let mut query_generation = Vec::with_capacity(samples);
    let mut local_wall = Vec::with_capacity(samples);
    let mut server_elapsed = Vec::with_capacity(samples);
    let mut http_wall = Vec::with_capacity(samples);
    for _ in 0..samples {
        let query_started = Instant::now();
        let work = generate_work(&snapshots, server_count, &mut rng)?;
        query_generation.push(query_started.elapsed());
        let evaluation = pool.evaluate(work)?;
        local_wall.push(evaluation.wall);
        server_elapsed.push(evaluation.sum_server_elapsed);
        verify_evaluation(&snapshots, evaluation)?;

        let http_started = Instant::now();
        verify_http(client, window_ids).await?;
        http_wall.push(http_started.elapsed());
    }
    query_generation.sort_unstable();
    local_wall.sort_unstable();
    server_elapsed.sort_unstable();
    http_wall.sort_unstable();

    let table_buckets_scanned_per_server = snapshots
        .iter()
        .map(|snapshot| snapshot.manifest.bucket_count)
        .sum::<usize>();
    let expected_rows_xored_per_server = table_buckets_scanned_per_server / 2;
    let query_bytes_per_server = snapshots
        .iter()
        .map(|snapshot| dense::query_size(snapshot.manifest.bucket_count))
        .sum::<usize>();
    let response_bytes_per_server = snapshots
        .iter()
        .map(|snapshot| snapshot.manifest.row_size)
        .sum::<usize>();

    Ok(RawScenarioResult {
        mode,
        selected_window_count: window_ids.len(),
        server_count,
        samples: match profile {
            Profile::Quick => QUICK_SAMPLES,
            Profile::Full => FULL_SAMPLES,
        },
        tables_scanned_per_server: snapshots.len(),
        table_buckets_scanned_per_server,
        expected_rows_xored_per_server,
        expected_data_bytes_xored_per_server: expected_rows_xored_per_server * ROW_SIZE,
        query_bytes_per_server,
        total_upload_bytes: query_bytes_per_server * server_count,
        response_bytes_per_server,
        total_download_bytes: response_bytes_per_server * server_count,
        client_share_generation_p50_us: micros(percentile(&query_generation, 50)),
        co_located_server_wall_p50_ms: millis(percentile(&local_wall, 50)),
        co_located_server_wall_p95_ms: millis(percentile(&local_wall, 95)),
        sum_server_elapsed_p50_ms: millis(percentile(&server_elapsed, 50)),
        http_end_to_end_p50_ms: millis(percentile(&http_wall, 50)),
        http_end_to_end_p95_ms: millis(percentile(&http_wall, 95)),
    })
}

fn build_snapshot(bucket_count: usize, cutoff: &str, value: &[u8]) -> Result<Snapshot> {
    Snapshot::build(
        vec![Record::new(TAG, value)],
        SnapshotConfig {
            bucket_count,
            bucket_capacity: 1,
            values_per_page: 1,
            max_key_bytes: 8,
            max_value_bytes: 50,
            source: "endpoint-benchmark".into(),
            source_cutoff: cutoff.into(),
        },
    )
}

async fn verify_http(client: &PirClient, window_ids: &[String]) -> Result<()> {
    if window_ids.is_empty() {
        let values = client.private_lookup_global(TAG).await?;
        if values != vec![b"global-page".to_vec()] {
            bail!("global endpoint recovered the wrong value");
        }
    } else {
        let results = client.private_lookup_windows(TAG, window_ids).await?;
        if results.len() != window_ids.len()
            || results.iter().zip(window_ids).any(|(result, window_id)| {
                result.window_id != *window_id
                    || result.values != vec![format!("{window_id}-page").into_bytes()]
            })
        {
            bail!("public-window endpoint recovered the wrong values");
        }
    }
    Ok(())
}

fn generate_work(
    snapshots: &[Arc<Snapshot>],
    server_count: usize,
    rng: &mut StdRng,
) -> Result<Vec<Vec<WorkItem>>> {
    let mut per_server = (0..server_count)
        .map(|_| Vec::with_capacity(snapshots.len()))
        .collect::<Vec<_>>();
    for snapshot in snapshots {
        let bucket = bucket_for_key(TAG, snapshot.manifest.bucket_count);
        let shares =
            dense::query_shares(bucket, snapshot.manifest.bucket_count, server_count, rng)?;
        for (work, query_share) in per_server.iter_mut().zip(shares) {
            work.push(WorkItem {
                snapshot: Arc::clone(snapshot),
                query_share,
            });
        }
    }
    Ok(per_server)
}

fn verify_evaluation(snapshots: &[Arc<Snapshot>], evaluation: CatalogEvaluation) -> Result<()> {
    for (item_index, snapshot) in snapshots.iter().enumerate() {
        let shares = evaluation
            .answers
            .iter()
            .map(|answers| answers[item_index].as_slice())
            .collect::<Vec<_>>();
        let row = dense::combine(&shares)?;
        let bucket = bucket_for_key(TAG, snapshot.manifest.bucket_count);
        if row != snapshot.row(bucket)? {
            bail!("endpoint benchmark recovered the wrong row");
        }
    }
    Ok(())
}

fn reduction_percent(reference: f64, candidate: f64) -> f64 {
    if reference == 0.0 {
        return 0.0;
    }
    (1.0 - candidate / reference) * 100.0
}

struct WorkItem {
    snapshot: Arc<Snapshot>,
    query_share: Vec<u8>,
}

struct CatalogJob {
    work: Vec<WorkItem>,
    response: mpsc::Sender<CatalogResponse>,
}

struct CatalogResponse {
    server_index: usize,
    answers: std::result::Result<Vec<Vec<u8>>, String>,
    elapsed: Duration,
}

struct CatalogEvaluation {
    answers: Vec<Vec<Vec<u8>>>,
    wall: Duration,
    sum_server_elapsed: Duration,
}

struct CatalogServerPool {
    senders: Vec<mpsc::Sender<CatalogJob>>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl CatalogServerPool {
    fn new(server_count: usize, worker_threads: usize) -> Result<Self> {
        let mut senders = Vec::with_capacity(server_count);
        let mut workers = Vec::with_capacity(server_count);
        for server_index in 0..server_count {
            let (sender, receiver) = mpsc::channel::<CatalogJob>();
            let evaluator = dense::ParallelEvaluator::new(worker_threads)?;
            workers.push(std::thread::spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let started = Instant::now();
                    let answers = job
                        .work
                        .into_iter()
                        .map(|item| evaluator.answer(item.snapshot.view(), &item.query_share))
                        .collect::<Result<Vec<_>>>()
                        .map_err(|error| error.to_string());
                    let _ = job.response.send(CatalogResponse {
                        server_index,
                        answers,
                        elapsed: started.elapsed(),
                    });
                }
            }));
            senders.push(sender);
        }
        Ok(Self { senders, workers })
    }

    fn evaluate(&self, per_server_work: Vec<Vec<WorkItem>>) -> Result<CatalogEvaluation> {
        if per_server_work.len() != self.senders.len() {
            bail!("server and endpoint benchmark work counts differ");
        }
        let (response_sender, response_receiver) = mpsc::channel();
        let wall_started = Instant::now();
        for (sender, work) in self.senders.iter().zip(per_server_work) {
            sender
                .send(CatalogJob {
                    work,
                    response: response_sender.clone(),
                })
                .context("send endpoint benchmark work")?;
        }
        drop(response_sender);

        let mut answers = (0..self.senders.len()).map(|_| None).collect::<Vec<_>>();
        let mut sum_server_elapsed = Duration::ZERO;
        for _ in 0..self.senders.len() {
            let response = response_receiver
                .recv()
                .context("receive endpoint benchmark answer")?;
            sum_server_elapsed += response.elapsed;
            answers[response.server_index] = Some(response.answers.map_err(anyhow::Error::msg)?);
        }
        Ok(CatalogEvaluation {
            answers: answers
                .into_iter()
                .map(|answer| answer.context("endpoint benchmark server returned no answer"))
                .collect::<Result<Vec<_>>>()?,
            wall: wall_started.elapsed(),
            sum_server_elapsed,
        })
    }
}

impl Drop for CatalogServerPool {
    fn drop(&mut self) {
        self.senders.clear();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}
