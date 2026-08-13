use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use chalametpir_client::Client;
use chalametpir_server::Server;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::Serialize;

use crate::benchmark::Profile;
use crate::dense;
use crate::snapshot::{Record, Snapshot, SnapshotConfig};

#[derive(Debug, Serialize)]
pub struct ComparisonReport {
    pub profile: String,
    pub chalamet: Vec<ChalametResult>,
    pub baselines: Vec<BaselineResult>,
}

#[derive(Debug, Serialize)]
pub struct ChalametResult {
    pub records: usize,
    pub value_bytes: usize,
    pub server_setup_ms: f64,
    pub client_setup_ms: f64,
    pub hint_bytes: usize,
    pub filter_parameter_bytes: usize,
    pub query_generation_p50_ms: f64,
    pub query_bytes: usize,
    pub server_response_p50_ms: f64,
    pub response_bytes: usize,
    pub client_recovery_p50_ms: f64,
    pub dense_snapshot_build_ms: f64,
    pub dense_query_bytes_per_server: usize,
    pub dense_response_bytes_per_server: usize,
    pub dense_server_p50_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct BaselineResult {
    pub name: String,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub privacy: String,
    pub server_work: String,
}

pub fn run(profile: Profile) -> Result<ComparisonReport> {
    let sizes = match profile {
        Profile::Quick => vec![256, 1024],
        Profile::Full => vec![256, 1024, 4096],
    };
    let chalamet = sizes
        .into_iter()
        .map(|records| compare_chalamet(records, 64))
        .collect::<Result<Vec<_>>>()?;
    let baselines = vec![
        BaselineResult {
            name: "direct-key-lookup".into(),
            request_bytes: 16,
            response_bytes: 64,
            privacy: "none".into(),
            server_work: "one indexed lookup".into(),
        },
        BaselineResult {
            name: "eight-decoy-lookups".into(),
            request_bytes: 16 * 8,
            response_bytes: 64 * 8,
            privacy: "heuristic only; server sees all candidates".into(),
            server_work: "eight indexed lookups".into(),
        },
    ];
    Ok(ComparisonReport {
        profile: format!("{profile:?}").to_lowercase(),
        chalamet,
        baselines,
    })
}

fn compare_chalamet(record_count: usize, value_bytes: usize) -> Result<ChalametResult> {
    let records = synthetic_records(record_count, value_bytes);
    let db: HashMap<&[u8], &[u8]> = records
        .iter()
        .map(|record| (record.key.as_slice(), record.value.as_slice()))
        .collect();
    let seed = [0x42u8; chalametpir_server::SEED_BYTE_LEN];
    let setup_started = Instant::now();
    let (server, hint, filter_parameters) =
        Server::setup::<3>(&seed, db).map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let server_setup_ms = millis(setup_started.elapsed());

    let client_started = Instant::now();
    let mut client = Client::setup(&seed, &hint, &filter_parameters)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let client_setup_ms = millis(client_started.elapsed());
    let target = &records[record_count / 3];
    let mut query_times = Vec::with_capacity(11);
    let mut response_times = Vec::with_capacity(11);
    let mut recovery_times = Vec::with_capacity(11);
    let mut query_bytes = 0;
    let mut response_bytes = 0;
    for _ in 0..11 {
        let query_started = Instant::now();
        let query = client
            .query(&target.key)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        query_times.push(query_started.elapsed());
        query_bytes = query.len();

        let response_started = Instant::now();
        let response = server
            .respond(&query)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        response_times.push(response_started.elapsed());
        response_bytes = response.len();

        let recovery_started = Instant::now();
        let recovered = client
            .process_response(&target.key, &response)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        recovery_times.push(recovery_started.elapsed());
        if recovered != target.value {
            bail!("ChalametPIR recovered a different value");
        }
    }

    let bucket_count = record_count.next_power_of_two();
    let dense_started = Instant::now();
    let snapshot = Snapshot::build(
        records.clone(),
        SnapshotConfig {
            bucket_count,
            bucket_capacity: 8,
            max_key_bytes: 24,
            max_value_bytes: value_bytes,
            source: "comparison".into(),
            source_cutoff: record_count.to_string(),
        },
    )?;
    let dense_snapshot_build_ms = millis(dense_started.elapsed());
    let bucket = crate::snapshot::bucket_for_key(&target.key, bucket_count);
    let mut rng = StdRng::seed_from_u64(55);
    let dense_query = dense::query_shares(bucket, bucket_count, &mut rng)?.0;
    let mut dense_times = Vec::with_capacity(11);
    for _ in 0..11 {
        let dense_server_started = Instant::now();
        std::hint::black_box(dense::answer(&snapshot, &dense_query)?);
        dense_times.push(dense_server_started.elapsed());
    }

    Ok(ChalametResult {
        records: record_count,
        value_bytes,
        server_setup_ms,
        client_setup_ms,
        hint_bytes: hint.len(),
        filter_parameter_bytes: filter_parameters.len(),
        query_generation_p50_ms: millis(median(&mut query_times)),
        query_bytes,
        server_response_p50_ms: millis(median(&mut response_times)),
        response_bytes,
        client_recovery_p50_ms: millis(median(&mut recovery_times)),
        dense_snapshot_build_ms,
        dense_query_bytes_per_server: dense::query_size(bucket_count),
        dense_response_bytes_per_server: snapshot.manifest.row_size,
        dense_server_p50_ms: millis(median(&mut dense_times)),
    })
}

fn synthetic_records(count: usize, value_bytes: usize) -> Vec<Record> {
    let mut rng = StdRng::seed_from_u64(count as u64 ^ value_bytes as u64);
    (0..count)
        .map(|index| {
            let key = format!("key-{index:016x}").into_bytes();
            let mut value = vec![0u8; value_bytes];
            rng.fill_bytes(&mut value);
            Record::new(key, value)
        })
        .collect()
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn median(values: &mut [Duration]) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

#[cfg(feature = "dpf-compare")]
#[derive(Debug, Serialize)]
pub struct DpfReport {
    pub warning: String,
    pub dimensions: Vec<DpfResult>,
}

#[cfg(feature = "dpf-compare")]
#[derive(Debug, Serialize)]
pub struct DpfResult {
    pub bucket_count: usize,
    pub row_size: usize,
    pub dpf_query_bytes_per_server: usize,
    pub dense_query_bytes_per_server: usize,
    pub dpf_query_generation_us: f64,
    pub dpf_server_a_ms: f64,
    pub dpf_server_b_ms: f64,
    pub dense_server_ms: f64,
}

#[cfg(feature = "dpf-compare")]
pub fn run_dpf() -> Result<DpfReport> {
    let dimensions = vec![
        compare_dpf_depth::<10>()?,
        compare_dpf_depth::<14>()?,
        compare_dpf_depth::<18>()?,
    ];
    Ok(DpfReport {
        warning: "dpf 0.2.0 is an early-stage nightly-only research crate".into(),
        dimensions,
    })
}

#[cfg(feature = "dpf-compare")]
fn compare_dpf_depth<const DEPTH: usize>() -> Result<DpfResult> {
    use dpf::{DpfKey, DPF_KEY_SIZE};

    let bucket_count = 1 << DEPTH;
    let row_size = 64;
    let snapshot = Snapshot::benchmark(bucket_count, row_size, DEPTH as u64)?;
    let bucket = bucket_count / 3;
    let mut rng = StdRng::seed_from_u64(DEPTH as u64);
    let mut root_a = [0u8; DPF_KEY_SIZE];
    let mut root_b = [0u8; DPF_KEY_SIZE];
    rng.fill_bytes(&mut root_a);
    rng.fill_bytes(&mut root_b);
    let mut point = [0u8; DPF_KEY_SIZE];
    point[0] = 1;
    let query_started = Instant::now();
    let (key_a, key_b) = DpfKey::<DEPTH>::gen(bucket, &point, root_a, root_b);
    let dpf_query_generation_us = query_started.elapsed().as_secs_f64() * 1_000_000.0;
    let server_a_started = Instant::now();
    let answer_a = dpf_answer(&snapshot, &key_a)?;
    let dpf_server_a_ms = millis(server_a_started.elapsed());
    let server_b_started = Instant::now();
    let answer_b = dpf_answer(&snapshot, &key_b)?;
    let dpf_server_b_ms = millis(server_b_started.elapsed());
    let recovered = dense::combine(&answer_a, &answer_b)?;
    if recovered != snapshot.row(bucket)? {
        bail!("DPF answers failed to recover the selected row");
    }

    let dense_query = dense::query_shares(bucket, bucket_count, &mut rng)?.0;
    let dense_started = Instant::now();
    std::hint::black_box(dense::answer(&snapshot, &dense_query)?);
    Ok(DpfResult {
        bucket_count,
        row_size,
        dpf_query_bytes_per_server: std::mem::size_of::<DpfKey<DEPTH>>(),
        dense_query_bytes_per_server: dense_query.len(),
        dpf_query_generation_us,
        dpf_server_a_ms,
        dpf_server_b_ms,
        dense_server_ms: millis(dense_started.elapsed()),
    })
}

#[cfg(feature = "dpf-compare")]
fn dpf_answer<const DEPTH: usize>(
    snapshot: &Snapshot,
    key: &dpf::DpfKey<DEPTH>,
) -> Result<Vec<u8>> {
    let selectors = key.eval_all();
    let mut answer = vec![0u8; snapshot.manifest.row_size];
    for (bucket, selector) in selectors.iter().enumerate() {
        let mask = 0u8.wrapping_sub(selector[0] & 1);
        for (output, input) in answer.iter_mut().zip(snapshot.row(bucket)?) {
            *output ^= *input & mask;
        }
    }
    Ok(answer)
}
