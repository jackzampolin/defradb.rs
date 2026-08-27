//! Warm 100-decoy point-retrieval benchmark matched to the Ethereum Reads
//! `inspire-gpu` database geometries.
//!
//! InsPIRe's published server benchmark is index-based and warm: keyword to
//! ordinal mapping and storage page faults are outside its timed server kernel.
//! This comparator therefore receives 100 public ordinals and measures 100
//! fixed-width random row copies.  A file-backed mapping spans the exact logical
//! address space at every scale.  Only the deterministic query schedule is
//! populated and resident-touched, which keeps the experiment runnable without
//! claiming that this host holds the complete 16 GB database in RAM.

use std::{
    collections::HashSet,
    hint::black_box,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use memmap2::{Mmap, MmapMut, MmapOptions};
use serde::Serialize;

use super::Profile;

const ENTRY_BYTES: usize = 120;
const CANDIDATES: usize = 100;
const ORDINAL_BYTES: usize = 8;
const HOST_PAGE_BYTES: usize = 4_096;
const QUICK_QUERY_SETS: usize = 256;
const FULL_QUERY_SETS: usize = 1_024;
const QUICK_SAMPLES: usize = 7;
const FULL_SAMPLES: usize = 11;
const MIN_SAMPLE_TIME: Duration = Duration::from_millis(100);
const PERMUTATION_MULTIPLIER: u64 = 0x9e37_79b9_7f4a_7c15;
const PERMUTATION_OFFSET: u64 = 0xd1b5_4a32_d192_ed03;
const REFERENCE_URL: &str = "https://github.com/keewoolee/inspire-gpu";

#[derive(Clone, Debug, Serialize)]
pub struct GpuReferenceDecoyReport {
    pub schema: &'static str,
    pub profile: &'static str,
    pub methodology: &'static str,
    pub ethereum_reference: EthereumReference,
    pub scales: Vec<ScaleResult>,
    pub conclusion: &'static str,
    pub caveats: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EthereumReference {
    pub source: &'static str,
    pub protocol: &'static str,
    pub server_hardware: &'static str,
    pub client_hardware: &'static str,
    pub entry_bytes: usize,
    pub round_trip_bytes_per_query: usize,
    pub client_query_ms: f64,
    pub privacy: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScaleResult {
    pub published_label: &'static str,
    pub entries: usize,
    pub logical_database_bytes: usize,
    pub mapped_address_space_bytes: usize,
    pub resident_schedule_rows: usize,
    pub estimated_resident_schedule_page_bytes: usize,
    pub process_rss_after_prefault_bytes: Option<usize>,
    pub queries_per_sample: usize,
    pub samples: usize,
    pub decoy_candidates: usize,
    pub decoy_request_bytes: usize,
    pub decoy_response_bytes: usize,
    pub decoy_source_row_bytes: usize,
    pub decoy_server_p50_us: f64,
    pub decoy_server_p95_us: f64,
    pub decoy_throughput_queries_per_second: f64,
    pub response_checksum: u64,
    pub inspire_single_query_ms: f64,
    pub inspire_single_query_over_decoy: f64,
    pub inspire_batched_per_query_ms: f64,
    pub inspire_batched_throughput_queries_per_second: f64,
    pub inspire_batched_per_query_over_decoy: f64,
    pub inspire_round_trip_over_decoy: f64,
}

#[derive(Clone, Copy)]
struct ReferenceScale {
    label: &'static str,
    entries: usize,
    single_query_ms: f64,
    batched_per_query_ms: f64,
    batched_qps: f64,
}

const REFERENCE_SCALES: [ReferenceScale; 3] = [
    ReferenceScale {
        label: "1 GB",
        entries: 1 << 23,
        single_query_ms: 2.6,
        batched_per_query_ms: 1_000.0 / 579.0,
        batched_qps: 579.0,
    },
    ReferenceScale {
        label: "4 GB",
        entries: 1 << 25,
        single_query_ms: 7.9,
        batched_per_query_ms: 1_000.0 / 258.0,
        batched_qps: 258.0,
    },
    ReferenceScale {
        label: "16 GB",
        entries: 1 << 27,
        single_query_ms: 31.1,
        batched_per_query_ms: 8.7,
        batched_qps: 115.0,
    },
];

pub fn run(profile: Profile) -> Result<GpuReferenceDecoyReport> {
    let query_sets = match profile {
        Profile::Quick => QUICK_QUERY_SETS,
        Profile::Full => FULL_QUERY_SETS,
    };
    let samples = match profile {
        Profile::Quick => QUICK_SAMPLES,
        Profile::Full => FULL_SAMPLES,
    };
    let mut scales = Vec::with_capacity(REFERENCE_SCALES.len());
    for reference in REFERENCE_SCALES {
        scales.push(benchmark_scale(reference, query_sets, samples)?);
    }
    Ok(GpuReferenceDecoyReport {
        schema: "defradb-pir-gpu-reference-decoy-v1",
        profile: match profile {
            Profile::Quick => "quick",
            Profile::Full => "full",
        },
        methodology: "Warm index-based server-kernel comparison using the exact inspire-gpu 120-byte entry counts. Each request supplies 100 visible present ordinals and receives all 100 fixed-width rows. A temporary file-backed mapping spans the exact logical database, while only a deterministic schedule larger than last-level cache is populated, flushed, prefaulted, and measured. Timings exclude keyword-to-ordinal lookup, HTTP, TLS, storage page faults, client filtering, and network transfer, matching the published PIR kernel's warm/index-based scope.",
        ethereum_reference: EthereumReference {
            source: REFERENCE_URL,
            protocol: "InsPIRe^1 single-server doubly-stateless computational PIR",
            server_hardware: "NVIDIA RTX 5090 32 GB plus Xeon Gold 6530 host",
            client_hardware: "portable CPU-only C++ client; no CUDA and no database-dependent hint",
            entry_bytes: ENTRY_BYTES,
            round_trip_bytes_per_query: 383 * 1_024,
            client_query_ms: 31.0,
            privacy: "strict computational index privacy from the single server; network origin and timing remain visible without a separate anonymous transport",
        },
        scales,
        conclusion: "For candidate-set privacy, 100 indexed row reads remain substantially cheaper server work and traffic than strict GPU PIR even at huge N because their work is O(100), not O(N). GPU InsPIRe makes strict privacy operationally plausible at multi-gigabyte scale; it does not make it server-work-equivalent to visible decoys.",
        caveats: vec![
            "100 visible candidates and strict PIR do not provide equivalent privacy; repeated or biased candidate sets can reveal the target.",
            "The mapped address space is exact but the entire database is not resident: this measures a warm, cache-defeating scheduled working set, not cold SSD lookup or full-RAM capacity.",
            "The benchmark starts from public ordinals because inspire-gpu is also index-based; a production keyword index adds work to both designs and must be measured separately.",
            "Published InsPIRe timings were measured on an RTX 5090, while the decoy path is measured on this host CPU; ratios describe deployed alternatives, not cryptographic primitive efficiency on identical hardware.",
            "Server time excludes request parsing, transport, scheduling, rate limiting, and response transmission; byte counts expose those separate costs.",
        ],
    })
}

fn benchmark_scale(
    reference: ReferenceScale,
    query_sets: usize,
    samples: usize,
) -> Result<ScaleResult> {
    let logical_bytes = reference
        .entries
        .checked_mul(ENTRY_BYTES)
        .context("GPU-reference logical database size overflow")?;
    let schedule = make_schedule(reference.entries, query_sets)?;
    let resident_pages = scheduled_pages(&schedule, logical_bytes)?;

    let file = tempfile::tempfile().context("create GPU-reference temporary table")?;
    file.set_len(u64::try_from(logical_bytes)?)
        .context("size GPU-reference temporary table")?;
    // SAFETY: this benchmark exclusively owns `file`, fixes its length before
    // mapping, never truncates it, and keeps the file alive for the map's full
    // lifetime. Every later row range is bounds-checked against `logical_bytes`.
    let mut writable = unsafe { MmapOptions::new().len(logical_bytes).map_mut(&file) }
        .context("map GPU-reference temporary table")?;
    populate_schedule(&mut writable, &schedule)?;
    writable
        .flush()
        .context("flush GPU-reference scheduled rows")?;
    let mapped = writable
        .make_read_only()
        .context("make GPU-reference mapping read-only")?;

    let mut response = vec![0u8; CANDIDATES * ENTRY_BYTES];
    let warm_checksum = run_schedule_pass(&mapped, &schedule, &mut response)?;
    verify_response(
        &mapped,
        schedule.first().context("empty schedule")?,
        &mut response,
    )?;
    let process_rss = memory_stats::memory_stats().map(|stats| stats.physical_mem);

    let repeats = calibrated_repeats(&mapped, &schedule, &mut response)?;
    let operations = repeats
        .checked_mul(schedule.len())
        .context("GPU-reference operation count overflow")?;
    let mut per_query = Vec::with_capacity(samples);
    let mut checksum = warm_checksum;
    for _ in 0..samples {
        let started = Instant::now();
        for _ in 0..repeats {
            checksum ^= run_schedule_pass(&mapped, &schedule, &mut response)?;
        }
        per_query.push(started.elapsed().as_secs_f64() / operations as f64);
    }
    per_query.sort_by(f64::total_cmp);
    black_box(checksum);

    let p50_seconds = percentile_f64(&per_query, 50);
    let p95_seconds = percentile_f64(&per_query, 95);
    let decoy_ms = p50_seconds * 1_000.0;
    let decoy_round_trip = CANDIDATES
        .checked_mul(ORDINAL_BYTES + ENTRY_BYTES)
        .context("GPU-reference decoy round trip overflow")?;

    Ok(ScaleResult {
        published_label: reference.label,
        entries: reference.entries,
        logical_database_bytes: logical_bytes,
        mapped_address_space_bytes: mapped.len(),
        resident_schedule_rows: schedule
            .len()
            .checked_mul(CANDIDATES)
            .context("GPU-reference resident row count overflow")?,
        estimated_resident_schedule_page_bytes: resident_pages
            .checked_mul(HOST_PAGE_BYTES)
            .context("GPU-reference resident page byte count overflow")?,
        process_rss_after_prefault_bytes: process_rss,
        queries_per_sample: operations,
        samples,
        decoy_candidates: CANDIDATES,
        decoy_request_bytes: CANDIDATES * ORDINAL_BYTES,
        decoy_response_bytes: CANDIDATES * ENTRY_BYTES,
        decoy_source_row_bytes: CANDIDATES * ENTRY_BYTES,
        decoy_server_p50_us: p50_seconds * 1_000_000.0,
        decoy_server_p95_us: p95_seconds * 1_000_000.0,
        decoy_throughput_queries_per_second: 1.0 / p50_seconds,
        response_checksum: checksum,
        inspire_single_query_ms: reference.single_query_ms,
        inspire_single_query_over_decoy: reference.single_query_ms / decoy_ms,
        inspire_batched_per_query_ms: reference.batched_per_query_ms,
        inspire_batched_throughput_queries_per_second: reference.batched_qps,
        inspire_batched_per_query_over_decoy: reference.batched_per_query_ms / decoy_ms,
        inspire_round_trip_over_decoy: (383 * 1_024) as f64 / decoy_round_trip as f64,
    })
}

fn make_schedule(entries: usize, query_sets: usize) -> Result<Vec<[usize; CANDIDATES]>> {
    if !entries.is_power_of_two() {
        bail!("GPU-reference entry count must be a power of two");
    }
    let required = query_sets
        .checked_mul(CANDIDATES)
        .context("GPU-reference schedule size overflow")?;
    if required > entries {
        bail!("GPU-reference schedule exceeds the available entries");
    }
    let mask = u64::try_from(entries - 1)?;
    let mut schedule = Vec::with_capacity(query_sets);
    for query in 0..query_sets {
        let mut ordinals = [0usize; CANDIDATES];
        for (candidate, ordinal) in ordinals.iter_mut().enumerate() {
            let counter = query
                .checked_mul(CANDIDATES)
                .and_then(|value| value.checked_add(candidate))
                .context("GPU-reference schedule counter overflow")?;
            let permuted = (u64::try_from(counter)?
                .wrapping_mul(PERMUTATION_MULTIPLIER)
                .wrapping_add(PERMUTATION_OFFSET))
                & mask;
            *ordinal = usize::try_from(permuted)?;
        }
        schedule.push(ordinals);
    }
    Ok(schedule)
}

fn scheduled_pages(schedule: &[[usize; CANDIDATES]], logical_bytes: usize) -> Result<usize> {
    let mut pages = HashSet::new();
    for ordinals in schedule {
        for &ordinal in ordinals {
            let start = row_offset(ordinal, logical_bytes)?;
            let end = start
                .checked_add(ENTRY_BYTES - 1)
                .context("GPU-reference row end overflow")?;
            pages.insert(start / HOST_PAGE_BYTES);
            pages.insert(end / HOST_PAGE_BYTES);
        }
    }
    Ok(pages.len())
}

fn populate_schedule(mapped: &mut MmapMut, schedule: &[[usize; CANDIDATES]]) -> Result<()> {
    for ordinals in schedule {
        for &ordinal in ordinals {
            let start = row_offset(ordinal, mapped.len())?;
            fill_row(ordinal, &mut mapped[start..start + ENTRY_BYTES]);
        }
    }
    Ok(())
}

fn calibrated_repeats(
    mapped: &Mmap,
    schedule: &[[usize; CANDIDATES]],
    response: &mut [u8],
) -> Result<usize> {
    let mut repeats = 1usize;
    loop {
        let started = Instant::now();
        let mut checksum = 0u64;
        for _ in 0..repeats {
            checksum ^= run_schedule_pass(mapped, schedule, response)?;
        }
        black_box(checksum);
        if started.elapsed() >= MIN_SAMPLE_TIME || repeats >= 1 << 16 {
            return Ok(repeats);
        }
        repeats = repeats
            .checked_mul(2)
            .context("GPU-reference calibration repeat overflow")?;
    }
}

fn run_schedule_pass(
    mapped: &Mmap,
    schedule: &[[usize; CANDIDATES]],
    response: &mut [u8],
) -> Result<u64> {
    let mut checksum = 0u64;
    for ordinals in schedule {
        lookup(mapped, ordinals, response)?;
        checksum = checksum.rotate_left(7)
            ^ u64::from_le_bytes(response[..8].try_into().expect("120-byte response"));
        black_box(&*response);
    }
    Ok(checksum)
}

fn lookup(mapped: &Mmap, ordinals: &[usize; CANDIDATES], response: &mut [u8]) -> Result<()> {
    if response.len() != CANDIDATES * ENTRY_BYTES {
        bail!("GPU-reference response buffer has the wrong fixed size");
    }
    for (candidate, &ordinal) in ordinals.iter().enumerate() {
        let source = row_offset(ordinal, mapped.len())?;
        let destination = candidate * ENTRY_BYTES;
        response[destination..destination + ENTRY_BYTES]
            .copy_from_slice(&mapped[source..source + ENTRY_BYTES]);
    }
    Ok(())
}

fn verify_response(
    mapped: &Mmap,
    ordinals: &[usize; CANDIDATES],
    response: &mut [u8],
) -> Result<()> {
    lookup(mapped, ordinals, response)?;
    let mut expected = [0u8; ENTRY_BYTES];
    for (candidate, &ordinal) in ordinals.iter().enumerate() {
        fill_row(ordinal, &mut expected);
        let start = candidate * ENTRY_BYTES;
        if response[start..start + ENTRY_BYTES] != expected {
            bail!("GPU-reference candidate {candidate} returned the wrong row");
        }
    }
    Ok(())
}

fn row_offset(ordinal: usize, logical_bytes: usize) -> Result<usize> {
    let start = ordinal
        .checked_mul(ENTRY_BYTES)
        .context("GPU-reference row offset overflow")?;
    let end = start
        .checked_add(ENTRY_BYTES)
        .context("GPU-reference row range overflow")?;
    if end > logical_bytes {
        bail!("GPU-reference ordinal is outside the logical database");
    }
    Ok(start)
}

fn fill_row(ordinal: usize, row: &mut [u8]) {
    debug_assert_eq!(row.len(), ENTRY_BYTES);
    row[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
    let mut state = (ordinal as u64)
        .wrapping_mul(0xa076_1d64_78bd_642f)
        .wrapping_add(0xe703_7ed1_a0b4_28db);
    for byte in &mut row[8..] {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        *byte = state.wrapping_mul(0x2545_f491_4f6c_dd1d) as u8;
    }
}

fn percentile_f64(values: &[f64], percentile: usize) -> f64 {
    values[((values.len() - 1) * percentile).div_ceil(100)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_geometries_match_the_published_entry_width() {
        assert_eq!(REFERENCE_SCALES[0].entries * ENTRY_BYTES, 1_006_632_960);
        assert_eq!(REFERENCE_SCALES[1].entries * ENTRY_BYTES, 4_026_531_840);
        assert_eq!(REFERENCE_SCALES[2].entries * ENTRY_BYTES, 16_106_127_360);
    }

    #[test]
    fn schedule_has_distinct_present_ordinals() {
        let schedule = make_schedule(1 << 20, 32).unwrap();
        let flattened = schedule.iter().flatten().copied().collect::<HashSet<_>>();
        assert_eq!(flattened.len(), 32 * CANDIDATES);
        assert!(flattened.iter().all(|ordinal| *ordinal < 1 << 20));
    }
}
