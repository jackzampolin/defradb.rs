//! Common-corpus adapter for the pinned YPIR artifact.
//!
//! This is copied into an ignored upstream checkout as a new binary.  It uses
//! only the artifact's public client/server APIs and does not patch its
//! protocol implementation.

use clap::Parser;
use serde::Serialize;
use std::fs;
use std::io::Cursor;
use std::time::Instant;
use ypir::bits::u64s_to_contiguous_bytes;
use ypir::client::YPIRClient;
use ypir::params::{params_for_scenario_simplepir, DbRowsCols, PtModulusBits};
use ypir::serialize::{FilePtIter, ToBytes};
use ypir::server::{ToU64, YServer};

const ARTIFACT_REVISION: &str = "b9801521301f34502496d694b2ac034857104ebc";
const MIN_SIMPLEPIR_ITEM_BITS: usize = 2048 * 14;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    corpus: String,
    #[arg(long, default_value_t = 262_144)]
    pages: usize,
    #[arg(long, default_value_t = 96)]
    page_bytes: usize,
    #[arg(long, default_value_t = 3)]
    samples: usize,
    #[arg(long)]
    output: String,
}

#[derive(Serialize)]
struct Metric<T> {
    value: T,
    evidence: &'static str,
    qualification: &'static str,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    protocol: &'static str,
    artifact: Artifact,
    comparison_scope: ComparisonScope,
    security: Security,
    corpus: Corpus,
    global_build: GlobalBuild,
    online: Online,
    client: ClientMetrics,
    persisted_storage: Storage,
    amortization: Amortization,
    runner_diagnostics: Diagnostics,
}

#[derive(Serialize)]
struct Artifact {
    repository: &'static str,
    revision: &'static str,
    upstream_modifications: &'static str,
    adapter_qualification: &'static str,
}

#[derive(Serialize)]
struct ComparisonScope {
    workload: String,
    result: &'static str,
    physical_result: String,
    public_partition: &'static str,
    leakage_class: &'static str,
}

#[derive(Serialize)]
struct Security {
    privacy: &'static str,
    server_count: usize,
    collusion_tolerance: usize,
    required_answers: usize,
    assumptions: &'static str,
    integrity: &'static str,
}

#[derive(Serialize)]
struct Corpus {
    page_count: usize,
    page_bytes: usize,
    useful_bytes: usize,
    pages_per_physical_row: usize,
    physical_rows: usize,
    padded_physical_rows: usize,
    useful_bytes_per_physical_row: usize,
    returned_bytes_per_physical_row: usize,
    row_padding_bytes: usize,
    database_encoding_padding_bytes: usize,
}

#[derive(Serialize)]
struct GlobalBuild {
    corpus_load_and_server_build_ms: Metric<f64>,
    server_preprocessing_ms: Metric<f64>,
    client_download_bytes: Metric<usize>,
}

#[derive(Serialize)]
struct Online {
    unit: &'static str,
    server_time_p50_ms: Metric<f64>,
    server_time_samples_ms: Vec<f64>,
    logical_selected_bytes: Metric<usize>,
    allocated_database_bytes: Metric<usize>,
    scans: Metric<usize>,
    useful_result_bytes: Metric<usize>,
    physical_result_bytes: Metric<usize>,
    network_rounds: Metric<usize>,
}

#[derive(Serialize)]
struct ClientMetrics {
    query_cpu_p50_ms: Metric<f64>,
    query_cpu_samples_ms: Vec<f64>,
    recover_cpu_p50_ms: Metric<f64>,
    recover_cpu_samples_ms: Vec<f64>,
    upload_bytes: Metric<usize>,
    download_bytes: Metric<usize>,
    persistent_state_bytes: Metric<usize>,
}

#[derive(Serialize)]
struct Storage {
    allocated_database_bytes: Metric<usize>,
    offline_server_state_serialized_bytes: Metric<usize>,
}

#[derive(Serialize)]
struct Amortization {
    global_build: &'static str,
    per_client_setup: &'static str,
    note: &'static str,
}

#[derive(Serialize)]
struct Diagnostics {
    correctness_checked_samples: usize,
    cpu_feature_mode: &'static str,
    hardware_counters: &'static str,
    warning: &'static str,
}

fn measured<T>(value: T, qualification: &'static str) -> Metric<T> {
    Metric {
        value,
        evidence: "measured",
        qualification,
    }
}

fn deterministic<T>(value: T, qualification: &'static str) -> Metric<T> {
    Metric {
        value,
        evidence: "deterministic",
        qualification,
    }
}

fn estimated<T>(value: T, qualification: &'static str) -> Metric<T> {
    Metric {
        value,
        evidence: "estimated",
        qualification,
    }
}

fn p50(xs: &mut [f64]) -> f64 {
    xs.sort_by(f64::total_cmp);
    let middle = xs.len() / 2;
    if xs.len() % 2 == 0 {
        (xs[middle - 1] + xs[middle]) / 2.0
    } else {
        xs[middle]
    }
}

fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000.0
}

fn main() {
    let args = Args::parse();
    assert!(args.samples > 0, "samples must be non-zero");
    assert!(args.page_bytes > 0, "page_bytes must be non-zero");

    let raw = fs::read(&args.corpus).expect("read corpus");
    assert_eq!(raw.len(), args.pages * args.page_bytes, "corpus geometry");

    // The paper artifact's YPIR+SP parameter picker rejects items below
    // 2048*14 bits. Search exact 14-bit-aligned page groups and minimize the
    // artifact's encoded plaintext table. For this corpus the winner is 70
    // pages: 3,745 useful rows round to 4,096 and fit in two 2,048-column
    // instances. The arithmetic minimum of 38 pages would round to 8,192 rows.
    let minimum_pages = MIN_SIMPLEPIR_ITEM_BITS.div_ceil(args.page_bytes * 8);
    let maximum_pages = args.pages / 2048;
    let (pages_per_row, params) = (minimum_pages..=maximum_pages)
        .filter(|pages| (pages * args.page_bytes * 8) % 14 == 0)
        .map(|pages| {
            let rows = args.pages.div_ceil(pages);
            let item_bits = pages * args.page_bytes * 8;
            let params = params_for_scenario_simplepir(rows as u64, item_bits as u64);
            let encoded_bytes =
                params.db_rows() * params.db_cols_simplepir() * params.pt_modulus_bits() / 8;
            let returned_bytes = params.db_cols_simplepir() * params.pt_modulus_bits() / 8;
            ((encoded_bytes, returned_bytes, pages), params)
        })
        .min_by_key(|(cost, _)| *cost)
        .map(|((_, _, pages), params)| (pages, params))
        .expect("an artifact-compatible 14-bit page grouping");
    let useful_row_bytes = pages_per_row * args.page_bytes;
    let physical_rows = args.pages.div_ceil(pages_per_row);
    let item_bits = useful_row_bytes * 8;
    let returned_row_bytes = params.db_cols_simplepir() * params.pt_modulus_bits() / 8;
    assert!(returned_row_bytes >= useful_row_bytes);

    let build_start = Instant::now();
    // FilePtIter reads fixed-width physical rows. Pad the final incomplete row
    // in memory so read_exact cannot discard a trailing partial 14-byte chunk.
    let mut physical_raw = raw.clone();
    physical_raw.resize(physical_rows * useful_row_bytes, 0);
    let iterator = FilePtIter::new(
        Cursor::new(physical_raw),
        useful_row_bytes,
        params.db_cols_simplepir(),
        params.pt_modulus_bits(),
    );
    let server = YServer::<u16>::new(&params, iterator, true, false, true);
    let server_build_ms = elapsed_ms(build_start);
    let allocated_database_bytes = server.db().len() * std::mem::size_of::<u16>();

    let preprocessing_start = Instant::now();
    let mut offline = server.perform_offline_precomputation_simplepir(None, None, None);
    let preprocessing_ms = elapsed_ms(preprocessing_start);
    let offline_state_bytes = offline.to_bytes().len();

    let mut query_ms = Vec::with_capacity(args.samples);
    let mut server_ms = Vec::with_capacity(args.samples);
    let mut recover_ms = Vec::with_capacity(args.samples);
    let mut upload_bytes = None;
    let mut download_bytes = None;

    // Match the paper artifact's timing discipline: one correctness-checked
    // warmup followed by the requested number of measured iterations.
    for iteration in 0..=args.samples {
        let measured = iteration > 0;
        let sample = iteration.saturating_sub(1);
        let target_page = match (measured, sample) {
            (false, _) => 17 % args.pages,
            (true, 0) => args.pages - 1,
            _ => (sample.wrapping_mul(104_729).wrapping_add(17)) % args.pages,
        };
        let target_row = target_page / pages_per_row;
        let page_in_row = target_page % pages_per_row;

        let query_start = Instant::now();
        let client = YPIRClient::from_db_sz(physical_rows as u64, item_bits as u64, true);
        let (query, client_seed) = client.generate_query_simplepir(target_row);
        let query_bytes = query.to_bytes();
        let this_query_ms = elapsed_ms(query_start);

        let server_start = Instant::now();
        let response = server.perform_full_online_computation_simplepir(&mut offline, &query_bytes);
        let this_server_ms = elapsed_ms(server_start);

        let recover_start = Instant::now();
        let decoded = client.decode_response_simplepir(client_seed, &response);
        let this_recover_ms = elapsed_ms(recover_start);

        let page_start = page_in_row * args.page_bytes;
        let expected_start = target_page * args.page_bytes;
        assert_eq!(
            &decoded[page_start..page_start + args.page_bytes],
            &raw[expected_start..expected_start + args.page_bytes],
            "full useful page mismatch for iteration {iteration}"
        );

        let server_row = server
            .get_row(target_row)
            .iter()
            .map(|x| x.to_u64())
            .collect::<Vec<_>>();
        let server_row_bytes = u64s_to_contiguous_bytes(&server_row, params.pt_modulus_bits());
        assert_eq!(
            &decoded[..useful_row_bytes],
            &server_row_bytes[..useful_row_bytes]
        );

        match upload_bytes {
            Some(previous) => assert_eq!(previous, query_bytes.len()),
            None => upload_bytes = Some(query_bytes.len()),
        }
        match download_bytes {
            Some(previous) => assert_eq!(previous, response.len()),
            None => download_bytes = Some(response.len()),
        }
        if measured {
            query_ms.push(this_query_ms);
            server_ms.push(this_server_ms);
            recover_ms.push(this_recover_ms);
        }
    }

    let mut query_for_median = query_ms.clone();
    let mut server_for_median = server_ms.clone();
    let mut recover_for_median = recover_ms.clone();
    let report = Report {
        schema: "pir-aggregate-work-v1",
        protocol: "YPIR+SP",
        artifact: Artifact {
            repository: "https://github.com/menonsamir/ypir",
            revision: ARTIFACT_REVISION,
            upstream_modifications: "none; this out-of-tree binary is copied into the pinned checkout",
            adapter_qualification: "the artifact requires records of at least 2048*14 bits; the minimum-table 14-bit-aligned mapping packs 70 exact 96-byte pages into each physical row, one row is privately retrieved, and the client locally selects one page",
        },
        comparison_scope: ComparisonScope {
            workload: format!("{} populated immutable 96-byte Defra tag pages", args.pages),
            result: "one exact 96-byte useful page",
            physical_result: format!("one {}-byte YPIR+SP row containing {} pages plus padding", returned_row_bytes, pages_per_row),
            public_partition: "global snapshot",
            leakage_class: "exact query privacy",
        },
        security: Security {
            privacy: "single-server computational query privacy under the pinned artifact's LWE/RLWE parameters",
            server_count: 1,
            collusion_tolerance: 0,
            required_answers: 1,
            assumptions: "computational lattice assumptions; semi-honest research artifact; no offline client hint",
            integrity: "every selected 96-byte page is checked byte-for-byte; the protocol has no malicious-server integrity proof",
        },
        corpus: Corpus {
            page_count: args.pages,
            page_bytes: args.page_bytes,
            useful_bytes: raw.len(),
            pages_per_physical_row: pages_per_row,
            physical_rows,
            padded_physical_rows: params.db_rows(),
            useful_bytes_per_physical_row: useful_row_bytes,
            returned_bytes_per_physical_row: returned_row_bytes,
            row_padding_bytes: returned_row_bytes - useful_row_bytes,
            database_encoding_padding_bytes: params.db_rows() * returned_row_bytes - raw.len(),
        },
        global_build: GlobalBuild {
            corpus_load_and_server_build_ms: measured(
                server_build_ms,
                "includes FilePtIter ingestion and the upstream in-memory server layout",
            ),
            server_preprocessing_ms: measured(
                preprocessing_ms,
                "database-dependent silent preprocessing, kept outside online time",
            ),
            client_download_bytes: deterministic(0, "YPIR has no offline client hint"),
        },
        online: Online {
            unit: "one exact 96-byte useful page selected locally from one private physical-row result",
            server_time_p50_ms: measured(
                p50(&mut server_for_median),
                "median of correctness-checked in-process server calls",
            ),
            server_time_samples_ms: server_ms,
            logical_selected_bytes: estimated(
                params.db_rows() * returned_row_bytes,
                "encoded plaintext table size; hardware memory traffic was not counted",
            ),
            allocated_database_bytes: estimated(
                allocated_database_bytes,
                "upstream u16 database allocation, not physical DRAM traffic",
            ),
            scans: deterministic(1, "one artifact YPIR+SP online evaluation"),
            useful_result_bytes: deterministic(args.page_bytes, "one complete Defra page"),
            physical_result_bytes: deterministic(
                returned_row_bytes,
                "decoded physical row before local page selection",
            ),
            network_rounds: deterministic(1, "one query and one answer after server preprocessing"),
        },
        client: ClientMetrics {
            query_cpu_p50_ms: measured(
                p50(&mut query_for_median),
                "includes the artifact's per-query client setup and serialized query generation",
            ),
            query_cpu_samples_ms: query_ms,
            recover_cpu_p50_ms: measured(
                p50(&mut recover_for_median),
                "response decoding only; local 96-byte slice selection is negligible and included in correctness checks",
            ),
            recover_cpu_samples_ms: recover_ms,
            upload_bytes: deterministic(upload_bytes.unwrap(), "serialized artifact query"),
            download_bytes: deterministic(download_bytes.unwrap(), "serialized artifact answer"),
            persistent_state_bytes: deterministic(0, "client state is generated per query; YPIR has no database hint"),
        },
        persisted_storage: Storage {
            allocated_database_bytes: deterministic(
                allocated_database_bytes,
                "u16 allocation held by the server; allocator overhead excluded",
            ),
            offline_server_state_serialized_bytes: deterministic(
                offline_state_bytes,
                "serialized upstream database-dependent offline values",
            ),
        },
        amortization: Amortization {
            global_build: "all queries served by one immutable snapshot",
            per_client_setup: "none retained; query key material is charged online",
            note: "server build, server preprocessing, client query, server answer, and client recovery remain separate",
        },
        runner_diagnostics: Diagnostics {
            correctness_checked_samples: args.samples + 1,
            cpu_feature_mode: if cfg!(target_feature = "avx512f") {
                "native AVX-512"
            } else {
                "artifact scalar/non-explicit fallback; eligible as a same-host AVX2 measurement, not as an AVX-512 paper reproduction"
            },
            hardware_counters: "not collected",
            warning: "single-server computational PIR is a separate security lane from replicated information-theoretic PIR",
        },
    };

    let json = serde_json::to_vec_pretty(&report).expect("serialize report");
    fs::write(&args.output, &json).expect("write report");
    println!("{}", String::from_utf8(json).unwrap());
}
