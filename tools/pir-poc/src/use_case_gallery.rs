//! Small executable PIR scenarios grounded in Mizu/Shieldd, Shinzo and generic
//! DefraDB query shapes.
//!
//! These fixtures intentionally reuse the selected POC's two production-shaped
//! primitives instead of adding another protocol: immutable [`PrivateTable`]
//! snapshots use Dense XOR, while live equality notifications use Compact DPF.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rand::rngs::OsRng;
use serde::Serialize;

use crate::dense;
use crate::selected::{PocLimits, PrivateTable, NULLIFIER_WITNESS_BYTES};
use crate::shinzo::{
    ethereum_log_selector_bucket, DEFAULT_BUCKET_COUNT as SHINZO_BUCKET_COUNT, LOG_ADDRESS_FIELD,
};
use crate::subscription::{combine_compact, compact_registration, CompactSubscriptionServer};

const SNAPSHOT_ROWS: usize = 256;
const LIVE_BUCKETS: usize = 1 << 16;
const SERVERS: usize = 2;
const BENCH_SAMPLES: usize = 31;
const DECOY_CANDIDATES: usize = 100;
const LIVE_OPS_PER_SAMPLE: usize = 1_000;

const USDC_ADDRESS: &str = "0xA0b86991c6218b36c1d19D4a2E9Eb0cE3606eB48";
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum UseCaseOwner {
    Mizu,
    Shinzo,
    Defra,
}

impl UseCaseOwner {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "mizu" => Ok(Self::Mizu),
            "shinzo" => Ok(Self::Shinzo),
            "defra" => Ok(Self::Defra),
            _ => bail!("use-case owner must be mizu, shinzo, or defra"),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UseCaseGalleryReport {
    pub purpose: &'static str,
    pub timing_note: &'static str,
    pub scale_comparison: Vec<ScaleComparison>,
    pub snapshot_cases: Vec<SnapshotCaseReport>,
    pub live_cases: Vec<LiveCaseReport>,
}

#[derive(Debug, Serialize)]
pub struct ScaleComparison {
    pub rows: u64,
    pub evidence: &'static str,
    pub dense_physical_row_positions_visited: u64,
    pub dense_expected_aggregate_source_rows: u64,
    pub dense_total_upload_bytes: u64,
    pub dense_response_rows: u64,
    pub decoy_source_rows: u64,
    pub decoy_response_rows: u64,
    pub dense_logical_row_work_over_decoy: f64,
    pub blind_index_source_rows: u64,
    pub blind_index_token_upload_bytes: u64,
    pub blind_index_minimum_raw_locator_bytes: u64,
    pub current_json_directory_estimate_bytes: u64,
    pub exact_mphf_2_4_bits_estimate_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct SnapshotCaseReport {
    pub owner: UseCaseOwner,
    pub name: &'static str,
    pub value: &'static str,
    pub source_shape: &'static str,
    pub query_shape: &'static str,
    pub public_metadata: &'static str,
    pub protocol: &'static str,
    pub server_flexibility: &'static str,
    pub production_projection: &'static str,
    pub rows: usize,
    pub fixed_values_per_row: usize,
    pub fixed_value_bytes: usize,
    pub client_metadata_bytes: usize,
    pub total_client_upload_bytes: usize,
    pub total_server_response_bytes: usize,
    pub recovered_values: usize,
    pub missing_key_rejected: bool,
    pub result_verified: bool,
    pub client_query_us: f64,
    pub aggregate_server_us: f64,
    pub client_finish_us: f64,
    pub decoy_candidates: usize,
    pub decoy_client_upload_bytes: usize,
    pub decoy_server_response_bytes: usize,
    pub decoy_ignored_rows: usize,
    pub decoy_result_verified: bool,
    pub decoy_client_query_us: f64,
    pub decoy_server_us: f64,
    pub decoy_client_finish_us: f64,
    pub private_server_over_decoy: f64,
    pub decoy_download_over_private: f64,
    pub limitation: &'static str,
}

#[derive(Debug, Serialize)]
pub struct LiveCaseReport {
    pub owner: UseCaseOwner,
    pub name: &'static str,
    pub value: &'static str,
    pub event_shape: &'static str,
    pub protocol: &'static str,
    pub production_direction: &'static str,
    pub server_flexibility: &'static str,
    pub bucket_count: usize,
    pub subscriptions_in_poc: usize,
    pub client_registration_upload_bytes: usize,
    pub response_bytes_per_event: usize,
    pub miss_detected_as_match: bool,
    pub match_detected: bool,
    pub client_registration_us: f64,
    pub aggregate_server_us_per_event: f64,
    pub client_finish_us_per_event: f64,
    pub decoy_candidates: usize,
    pub decoy_registration_upload_bytes: usize,
    pub decoy_response_bytes_per_event: usize,
    pub decoy_client_registration_us: f64,
    pub decoy_server_us_per_event: f64,
    pub decoy_client_finish_us_per_event: f64,
    pub private_server_over_decoy: f64,
    pub limitation: &'static str,
}

struct SnapshotSpec {
    owner: UseCaseOwner,
    name: &'static str,
    value: &'static str,
    source_shape: &'static str,
    query_shape: &'static str,
    public_metadata: &'static str,
    production_projection: &'static str,
    target_key: Vec<u8>,
    target_values: Vec<Vec<u8>>,
    values_per_row: usize,
    max_value_bytes: usize,
    limitation: &'static str,
}

struct LiveSpec {
    owner: UseCaseOwner,
    name: &'static str,
    value: &'static str,
    event_shape: &'static str,
    target_bucket: usize,
    limitation: &'static str,
}

struct LookupResult {
    values: Option<Vec<Vec<u8>>>,
    total_upload_bytes: usize,
    total_response_bytes: usize,
    client_query_us: f64,
    aggregate_server_us: f64,
    client_finish_us: f64,
}

struct DecoyLookupResult {
    values: Option<Vec<Vec<u8>>>,
    client_upload_bytes: usize,
    server_response_bytes: usize,
    client_query_us: f64,
    server_us: f64,
    client_finish_us: f64,
}

/// Runs all scenarios, or only one owner when `filter` is supplied.
pub fn run(filter: Option<&str>) -> Result<UseCaseGalleryReport> {
    let filter = filter.map(UseCaseOwner::parse).transpose()?;
    let snapshot_cases = snapshot_specs()
        .into_iter()
        .filter(|spec| filter.is_none_or(|owner| owner == spec.owner))
        .map(run_snapshot)
        .collect::<Result<Vec<_>>>()?;
    let live_cases = live_specs()?
        .into_iter()
        .filter(|spec| filter.is_none_or(|owner| owner == spec.owner))
        .map(run_live)
        .collect::<Result<Vec<_>>>()?;
    Ok(UseCaseGalleryReport {
        purpose: "executable correctness POCs over the selected PIR primitives",
        timing_note: "median of 31 in-process release-mode operations; excludes HTTP, OHTTP, queues and artifact build",
        scale_comparison: scale_comparison(),
        snapshot_cases,
        live_cases,
    })
}

fn scale_comparison() -> Vec<ScaleComparison> {
    [1_000u64, 1_000_000, 1_000_000_000]
        .into_iter()
        .map(|rows| ScaleComparison {
            rows,
            evidence: "exact wire/logical geometry; no elapsed time is extrapolated",
            dense_physical_row_positions_visited: 2 * rows,
            dense_expected_aggregate_source_rows: rows,
            dense_total_upload_bytes: 2 * rows.div_ceil(8),
            dense_response_rows: 2,
            decoy_source_rows: DECOY_CANDIDATES as u64,
            decoy_response_rows: DECOY_CANDIDATES as u64,
            dense_logical_row_work_over_decoy: rows as f64 / DECOY_CANDIDATES as f64,
            blind_index_source_rows: 1,
            blind_index_token_upload_bytes: 32,
            // 32-byte blind token + 37-byte AES-GCM envelope around an 8-byte ordinal.
            blind_index_minimum_raw_locator_bytes: rows * 69,
            // Measured canonical JSON directories in the 256-row gallery are about 145 B/row.
            current_json_directory_estimate_bytes: rows * 145,
            // The research MPHF assumption used elsewhere in this POC is 2.4 bits/key.
            exact_mphf_2_4_bits_estimate_bytes: (rows * 3).div_ceil(10),
        })
        .collect()
}

fn snapshot_specs() -> Vec<SnapshotSpec> {
    let nullifier = vec![0x42; 32];
    let routing_key = b"generation:42|route-prefix:0x0abc1".to_vec();
    let history_key = format!(
        "blocks:20000000-20000999|address:{}|topic0:{}",
        USDC_ADDRESS.to_ascii_lowercase(),
        TRANSFER_TOPIC
    )
    .into_bytes();
    vec![
        SnapshotSpec {
            owner: UseCaseOwner::Mizu,
            name: "routing-tag-retrieval",
            value: "Retrieve only encrypted action payloads after a private routing-tag alert or catch-up query",
            source_shape: "Shieldd RoutingRecord -> encrypted note payloads for one public generation/window",
            query_shape: "generation/window + low-bit routing prefix -> padded encrypted actions",
            public_metadata: "generation or height window, table generation and fixed result class",
            production_projection: "immutable routing-prefix pages exported from compact blocks",
            target_key: routing_key,
            target_values: vec![
                fixture_bytes("mizu-encrypted-note-a", 192),
                fixture_bytes("mizu-encrypted-note-b", 192),
            ],
            values_per_row: 4,
            max_value_bytes: 192,
            limitation: "the compact ordinal directory leaks populated routing prefixes to dictionary attacks; a full prefix domain avoids that leakage at higher table cost",
        },
        SnapshotSpec {
            owner: UseCaseOwner::Mizu,
            name: "nullifier-nonmembership-witness",
            value: "Fetch the fixed Merkle witness needed to prove an unspent note without revealing its nullifier",
            source_shape: "Shieldd active or immutable nullifier generation -> 2,008-byte witness",
            query_shape: "32-byte nullifier -> one fixed quaternary Merkle witness",
            public_metadata: "generation index, root and witness format",
            production_projection: "authenticated nullifier-to-witness table or radix/delta generation image",
            target_key: nullifier,
            target_values: vec![fixture_bytes("mizu-nullifier-witness", NULLIFIER_WITNESS_BYTES)],
            values_per_row: 1,
            max_value_bytes: NULLIFIER_WITNESS_BYTES,
            limitation: "this gallery checks byte-exact retrieval; the selected nullifier endpoint additionally verifies the Poseidon path against the authenticated root",
        },
        SnapshotSpec {
            owner: UseCaseOwner::Shinzo,
            name: "historical-contract-logs",
            value: "Query an address/topic pair without telling the indexer which contract or event is being investigated",
            source_shape: "Ethereum__Mainnet__Log projected into immutable block-window pages",
            query_shape: "public block window + private address/topic0 -> padded log projections",
            public_metadata: "chain, block window, table generation and maximum page fanout",
            production_projection: "address/topic/window pages containing transaction hash, block, log index and data",
            target_key: history_key,
            target_values: vec![
                br#"{"tx":"0x01","block":20000012,"log":3,"data":"0xaa"}"#.to_vec(),
                br#"{"tx":"0x02","block":20000451,"log":7,"data":"0xbb"}"#.to_vec(),
                br#"{"tx":"0x03","block":20000888,"log":1,"data":"0xcc"}"#.to_vec(),
            ],
            values_per_row: 4,
            max_value_bytes: 128,
            limitation: "high-fanout selectors require deterministic continuation pages; the public window intentionally leaks coarse query time",
        },
        SnapshotSpec {
            owner: UseCaseOwner::Shinzo,
            name: "private-transaction-receipt",
            value: "Fetch one transaction and its receipt/provenance projection without exposing the transaction hash",
            source_shape: "Ethereum__Mainnet__Transaction + BlockSignature attestation projection",
            query_shape: "transaction hash -> fixed transaction, receipt and attestation fields",
            public_metadata: "chain and immutable snapshot generation",
            production_projection: "hash-keyed fixed receipt rows sealed by Shinzo block attestations",
            target_key: vec![0x91; 32],
            target_values: vec![br#"{"block":20000888,"status":true,"gasUsed":"21000","logs":2,"attested":true}"#.to_vec()],
            values_per_row: 1,
            max_value_bytes: 160,
            limitation: "the result is public chain data; PIR protects the wallet's interest, not data confidentiality",
        },
        SnapshotSpec {
            owner: UseCaseOwner::Defra,
            name: "private-document-by-id",
            value: "Read a fixed projection of one document without revealing its high-entropy document ID",
            source_shape: "authorized collection snapshot -> document ID keyed projection",
            query_shape: "collection generation + document ID -> fixed encrypted projection",
            public_metadata: "collection, snapshot generation and projection schema",
            production_projection: "ACP-filtered immutable rows containing only fields authorized for the client class",
            target_key: vec![0xd0; 32],
            target_values: vec![fixture_bytes("defra-private-document-projection", 256)],
            values_per_row: 1,
            max_value_bytes: 256,
            limitation: "PIR does not replace ACP; artifacts must be built from an authorization-equivalent view and must not mix incompatible readers",
        },
        SnapshotSpec {
            owner: UseCaseOwner::Defra,
            name: "private-secondary-index-page",
            value: "Retrieve all matches for an equality predicate as fixed continuation pages",
            source_shape: "ordinary @index equality scan -> immutable key/page projection",
            query_shape: "collection + field + value + page -> padded document projections",
            public_metadata: "collection, optional coarse window and fixed page class",
            production_projection: "index-key pages carrying compact ordinals, CIDs or encrypted field projections",
            target_key: b"PrivateMessage.ownerTag=0x48a1|page=0".to_vec(),
            target_values: vec![
                fixture_bytes("defra-index-result-a", 128),
                fixture_bytes("defra-index-result-b", 128),
                fixture_bytes("defra-index-result-c", 128),
            ],
            values_per_row: 4,
            max_value_bytes: 128,
            limitation: "fanout is hidden only up to the fixed page schedule; repeated continuation requests can leak a lower bound unless clients pad page counts",
        },
    ]
}

fn live_specs() -> Result<Vec<LiveSpec>> {
    Ok(vec![
        LiveSpec {
            owner: UseCaseOwner::Mizu,
            name: "routing-tag-alert",
            value: "Learn that an encrypted action for the wallet's routing tag appeared without revealing that tag",
            event_shape: "committed Shieldd encrypted-action routing prefix -> domain-separated event bucket",
            target_bucket: bucket(b"mizu-routing-tag", b"route-prefix:0x0abc1", LIVE_BUCKETS)?,
            limitation: "every server evaluates every active subscription; durable expiry and fixed-cadence delivery are required in production",
        },
        LiveSpec {
            owner: UseCaseOwner::Shinzo,
            name: "contract-event-alert",
            value: "Receive a private hint when a watched Ethereum address emits a log",
            event_shape: "Ethereum__Mainnet__Log.address -> canonical Shinzo event bucket",
            target_bucket: ethereum_log_selector_bucket(
                LOG_ADDRESS_FIELD,
                USDC_ADDRESS,
                SHINZO_BUCKET_COUNT,
            )?,
            limitation: "a 65,536-bucket domain can create false positives, so the wallet must verify the fetched log; Compact DPF is exactly two-party",
        },
        LiveSpec {
            owner: UseCaseOwner::Defra,
            name: "private-change-feed",
            value: "Subscribe to equality-filtered document changes without exposing the filter value",
            event_shape: "committed collection/field/value update -> domain-separated event bucket",
            target_bucket: bucket(
                b"defra-change-feed",
                b"PrivateMessage.ownerTag=0x48a1",
                LIVE_BUCKETS,
            )?,
            limitation: "the notification is only a private hint; clients still need a padded snapshot retrieval for the changed document projection",
        },
    ])
}

fn run_snapshot(spec: SnapshotSpec) -> Result<SnapshotCaseReport> {
    let limits = PocLimits::default();
    let mut records = Vec::with_capacity(SNAPSHOT_ROWS);
    let mut cover_keys = Vec::with_capacity(SNAPSHOT_ROWS - 1);
    for ordinal in 0..SNAPSHOT_ROWS - 1 {
        let key = format!("{}:cover:{ordinal:04}", spec.name).into_bytes();
        cover_keys.push(key.clone());
        records.push((
            key,
            vec![fixture_bytes(&format!("{}-cover-{ordinal}", spec.name), 32)],
        ));
    }
    records.push((spec.target_key.clone(), spec.target_values.clone()));
    let table = PrivateTable::build(
        spec.name,
        records,
        spec.values_per_row,
        spec.max_value_bytes,
        &limits,
    )?;

    let lookups = (0..BENCH_SAMPLES)
        .map(|_| private_lookup(&table, &spec.target_key, &limits))
        .collect::<Result<Vec<_>>>()?;
    let lookup = &lookups[0];
    let recovered = lookup
        .values
        .as_ref()
        .context("gallery target key did not decode")?;
    let missing_key = format!("{}:definitely-absent", spec.name).into_bytes();
    let missing_key_rejected = private_lookup(&table, &missing_key, &limits)?
        .values
        .is_none();
    let result_verified = recovered == &spec.target_values;
    if !result_verified || !missing_key_rejected {
        bail!("{} gallery lookup failed verification", spec.name);
    }
    let client_query_us = median(lookups.iter().map(|sample| sample.client_query_us));
    let aggregate_server_us = median(lookups.iter().map(|sample| sample.aggregate_server_us));
    let client_finish_us = median(lookups.iter().map(|sample| sample.client_finish_us));

    let target_decoy_index = 37;
    let mut decoy_keys = cover_keys
        .into_iter()
        .take(DECOY_CANDIDATES - 1)
        .collect::<Vec<_>>();
    decoy_keys.insert(target_decoy_index, spec.target_key.clone());
    let decoy_samples = (0..BENCH_SAMPLES)
        .map(|_| {
            decoy_lookup(
                &table,
                &decoy_keys,
                target_decoy_index,
                &spec.target_key,
                &limits,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let decoy = &decoy_samples[0];
    let decoy_result_verified = decoy.values.as_ref() == Some(&spec.target_values);
    if !decoy_result_verified {
        bail!("{} decoy gallery lookup failed verification", spec.name);
    }
    let decoy_client_query_us = median(decoy_samples.iter().map(|sample| sample.client_query_us));
    let decoy_server_us = median(decoy_samples.iter().map(|sample| sample.server_us));
    let decoy_client_finish_us = median(decoy_samples.iter().map(|sample| sample.client_finish_us));

    Ok(SnapshotCaseReport {
        owner: spec.owner,
        name: spec.name,
        value: spec.value,
        source_shape: spec.source_shape,
        query_shape: spec.query_shape,
        public_metadata: spec.public_metadata,
        protocol: "replicated Dense XOR over one immutable fixed-row table",
        server_flexibility: "two or more replicas; privacy holds if at least one required replica does not collude, with linear aggregate server work",
        production_projection: spec.production_projection,
        rows: table.manifest.row_count,
        fixed_values_per_row: table.manifest.values_per_row,
        fixed_value_bytes: table.manifest.max_value_bytes,
        client_metadata_bytes: table.manifest.client_metadata_bytes,
        total_client_upload_bytes: lookup.total_upload_bytes,
        total_server_response_bytes: lookup.total_response_bytes,
        recovered_values: recovered.len(),
        missing_key_rejected,
        result_verified,
        client_query_us,
        aggregate_server_us,
        client_finish_us,
        decoy_candidates: DECOY_CANDIDATES,
        decoy_client_upload_bytes: decoy.client_upload_bytes,
        decoy_server_response_bytes: decoy.server_response_bytes,
        decoy_ignored_rows: DECOY_CANDIDATES - 1,
        decoy_result_verified,
        decoy_client_query_us,
        decoy_server_us,
        decoy_client_finish_us,
        private_server_over_decoy: aggregate_server_us / decoy_server_us.max(f64::EPSILON),
        decoy_download_over_private: decoy.server_response_bytes as f64
            / lookup.total_response_bytes as f64,
        limitation: spec.limitation,
    })
}

fn decoy_lookup(
    table: &PrivateTable,
    candidates: &[Vec<u8>],
    target_index: usize,
    target_key: &[u8],
    limits: &PocLimits,
) -> Result<DecoyLookupResult> {
    let client_started = Instant::now();
    let request = candidates.to_vec();
    let client_upload_bytes = request.iter().map(Vec::len).sum();
    let client_query_us = elapsed_us(client_started);

    let server_started = Instant::now();
    let rows = table.direct_rows(&request, limits)?;
    let server_us = elapsed_us(server_started);
    let server_response_bytes = rows.iter().map(Vec::len).sum();

    let finish_started = Instant::now();
    let values = table.decode(
        rows.get(target_index)
            .context("decoy response omitted the target slot")?,
        target_key,
    )?;
    let client_finish_us = elapsed_us(finish_started);
    Ok(DecoyLookupResult {
        values,
        client_upload_bytes,
        server_response_bytes,
        client_query_us,
        server_us,
        client_finish_us,
    })
}

fn private_lookup(table: &PrivateTable, key: &[u8], limits: &PocLimits) -> Result<LookupResult> {
    let client_started = Instant::now();
    let (ordinal, _) = table.strict_ordinal(key);
    let query_shares = dense::query_shares(ordinal, table.manifest.row_count, SERVERS, &mut OsRng)?;
    let client_query_us = elapsed_us(client_started);
    let total_upload_bytes = query_shares.iter().map(Vec::len).sum();

    let server_started = Instant::now();
    let answers = query_shares
        .iter()
        .map(|share| table.evaluate_share(share, limits))
        .collect::<Result<Vec<_>>>()?;
    let aggregate_server_us = elapsed_us(server_started);
    let total_response_bytes = answers.iter().map(Vec::len).sum();

    let finish_started = Instant::now();
    let row = dense::combine(&answers)?;
    let values = table.decode(&row, key)?;
    let client_finish_us = elapsed_us(finish_started);
    Ok(LookupResult {
        values,
        total_upload_bytes,
        total_response_bytes,
        client_query_us,
        aggregate_server_us,
        client_finish_us,
    })
}

fn run_live(spec: LiveSpec) -> Result<LiveCaseReport> {
    let mut registration_samples = Vec::with_capacity(BENCH_SAMPLES);
    let mut registration = None;
    for _ in 0..BENCH_SAMPLES {
        let registration_started = Instant::now();
        let generated = compact_registration(spec.target_bucket, LIVE_BUCKETS, &mut OsRng)?;
        registration_samples.push(elapsed_us(registration_started));
        registration = Some(generated);
    }
    let registration = registration.expect("the benchmark sample count is non-zero");
    let client_registration_us = median(registration_samples);
    let client_registration_upload_bytes = registration.server_keys.iter().map(Vec::len).sum();
    let mut servers = [
        CompactSubscriptionServer::new(0, LIVE_BUCKETS)?,
        CompactSubscriptionServer::new(1, LIVE_BUCKETS)?,
    ];
    for (server, key) in servers.iter_mut().zip(&registration.server_keys) {
        server.register(registration.id, key)?;
    }

    let miss_bucket = (spec.target_bucket + 1) % LIVE_BUCKETS;
    let miss = servers
        .iter()
        .map(|server| server.evaluate_one(registration.id, miss_bucket))
        .collect::<Result<Vec<_>>>()?;
    let miss_detected_as_match = combine_compact(&miss)?;

    let mut server_samples = Vec::with_capacity(BENCH_SAMPLES);
    let mut matched = None;
    for _ in 0..BENCH_SAMPLES {
        let server_started = Instant::now();
        for _ in 0..LIVE_OPS_PER_SAMPLE {
            let shares = servers
                .iter()
                .map(|server| server.evaluate_one(registration.id, spec.target_bucket))
                .collect::<Result<Vec<_>>>()?;
            black_box(&shares);
            matched = Some(shares);
        }
        server_samples.push(elapsed_us(server_started) / LIVE_OPS_PER_SAMPLE as f64);
    }
    let aggregate_server_us_per_event = median(server_samples);
    let matched = matched.context("live benchmark produced no result shares")?;
    let mut client_samples = Vec::with_capacity(BENCH_SAMPLES);
    let mut match_detected = false;
    for _ in 0..BENCH_SAMPLES {
        let started = Instant::now();
        for _ in 0..LIVE_OPS_PER_SAMPLE {
            match_detected = combine_compact(black_box(&matched))?;
        }
        client_samples.push(elapsed_us(started) / LIVE_OPS_PER_SAMPLE as f64);
    }
    let client_finish_us_per_event = median(client_samples);
    if miss_detected_as_match || !match_detected {
        bail!("{} Compact DPF gallery check failed", spec.name);
    }

    let mut decoy_registration_samples = Vec::with_capacity(BENCH_SAMPLES);
    let mut decoy_candidates = Vec::new();
    for _ in 0..BENCH_SAMPLES {
        let started = Instant::now();
        decoy_candidates = (0..DECOY_CANDIDATES)
            .map(|candidate| (spec.target_bucket + candidate * 613) % LIVE_BUCKETS)
            .collect();
        black_box(&decoy_candidates);
        decoy_registration_samples.push(elapsed_us(started));
    }
    let mut decoy_index = BTreeMap::<usize, bool>::new();
    for candidate in &decoy_candidates {
        decoy_index.insert(*candidate, true);
    }
    let mut decoy_server_samples = Vec::with_capacity(BENCH_SAMPLES);
    let mut decoy_match = false;
    for _ in 0..BENCH_SAMPLES {
        let started = Instant::now();
        for _ in 0..LIVE_OPS_PER_SAMPLE {
            decoy_match = black_box(
                decoy_index
                    .get(&spec.target_bucket)
                    .copied()
                    .unwrap_or(false),
            );
        }
        decoy_server_samples.push(elapsed_us(started) / LIVE_OPS_PER_SAMPLE as f64);
    }
    if !decoy_match {
        bail!("{} indexed decoy gallery check failed", spec.name);
    }
    let decoy_client_registration_us = median(decoy_registration_samples);
    let decoy_server_us_per_event = median(decoy_server_samples);
    let mut decoy_client_samples = Vec::with_capacity(BENCH_SAMPLES);
    let mut decoy_client_match = false;
    for _ in 0..BENCH_SAMPLES {
        let started = Instant::now();
        for _ in 0..LIVE_OPS_PER_SAMPLE {
            decoy_client_match = black_box(decoy_match);
        }
        decoy_client_samples.push(elapsed_us(started) / LIVE_OPS_PER_SAMPLE as f64);
    }
    if !decoy_client_match {
        bail!("{} indexed decoy client check failed", spec.name);
    }
    let decoy_client_finish_us_per_event = median(decoy_client_samples);

    Ok(LiveCaseReport {
        owner: spec.owner,
        name: spec.name,
        value: spec.value,
        event_shape: spec.event_shape,
        protocol: "two-party Compact DPF equality subscription",
        production_direction: "registered packed-presence Dense over a fixed public epoch; use this immediate Compact-DPF path only when epoch delay is unacceptable",
        server_flexibility: "exactly two non-colluding parties in the current implementation",
        bucket_count: LIVE_BUCKETS,
        subscriptions_in_poc: 1,
        client_registration_upload_bytes,
        response_bytes_per_event: 32,
        miss_detected_as_match,
        match_detected,
        client_registration_us,
        aggregate_server_us_per_event,
        client_finish_us_per_event,
        decoy_candidates: DECOY_CANDIDATES,
        decoy_registration_upload_bytes: DECOY_CANDIDATES * std::mem::size_of::<u32>(),
        decoy_response_bytes_per_event: 1,
        decoy_client_registration_us,
        decoy_server_us_per_event,
        decoy_client_finish_us_per_event,
        private_server_over_decoy: aggregate_server_us_per_event
            / decoy_server_us_per_event.max(f64::EPSILON),
        limitation: spec.limitation,
    })
}

fn bucket(domain: &[u8], value: &[u8], bucket_count: usize) -> Result<usize> {
    if bucket_count < 2 || !bucket_count.is_power_of_two() {
        bail!("gallery bucket count must be a power of two greater than one");
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"defradb-pir-use-case-gallery-v1");
    hasher.update(&(domain.len() as u32).to_le_bytes());
    hasher.update(domain);
    hasher.update(&(value.len() as u32).to_le_bytes());
    hasher.update(value);
    let digest = hasher.finalize();
    let prefix = u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("fixed digest"));
    Ok(prefix as usize & (bucket_count - 1))
}

fn fixture_bytes(label: &str, length: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(length);
    let mut counter = 0u64;
    while output.len() < length {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"defradb-pir-use-case-fixture-v1");
        hasher.update(label.as_bytes());
        hasher.update(&counter.to_le_bytes());
        output.extend_from_slice(hasher.finalize().as_bytes());
        counter += 1;
    }
    output.truncate(length);
    output
}

fn elapsed_us(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000_000.0
}

fn median(samples: impl IntoIterator<Item = f64>) -> f64 {
    let mut samples = samples.into_iter().collect::<Vec<_>>();
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_gallery_case_recovers_or_matches() {
        let report = run(None).unwrap();
        assert_eq!(report.snapshot_cases.len(), 6);
        assert_eq!(report.live_cases.len(), 3);
        assert!(report.snapshot_cases.iter().all(|case| case.result_verified
            && case.decoy_result_verified
            && case.missing_key_rejected));
        assert!(report
            .live_cases
            .iter()
            .all(|case| case.match_detected && !case.miss_detected_as_match));
    }

    #[test]
    fn owner_filter_returns_three_cases() {
        for owner in ["mizu", "shinzo", "defra"] {
            let report = run(Some(owner)).unwrap();
            assert_eq!(report.snapshot_cases.len(), 2);
            assert_eq!(report.live_cases.len(), 1);
        }
        assert!(run(Some("unknown")).is_err());
    }
}
