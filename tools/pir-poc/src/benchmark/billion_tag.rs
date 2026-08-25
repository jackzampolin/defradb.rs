//! One-billion-document, 0.01%-selectivity encrypted-tag benchmark.
//!
//! The full 194 GB immutable projection is represented as 391 equal planes.
//! A full run executes every plane over one resident 497 MB representative
//! plane, preserving the exact selector, XOR, response, and logical-byte work
//! while avoiding a 194 GB allocation on the benchmark host.

use std::hint::black_box;
use std::time::{Duration, Instant};

use aes_gcm::{
    aead::{AeadInPlace, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::Profile;
use crate::{
    dense,
    dense_batch::{BatchEvaluator, BatchKernel},
    snapshot::SnapshotView,
    tag_pages::TagPageConfig,
};

const DOCUMENTS: u64 = 1_000_000_000;
const DISTINCT_TAGS: usize = 10_000;
const MATCHES: usize = 100_000;
const DECOYS: usize = 100;
const VALUES_PER_STRIPE: usize = 256;
const ENCRYPTED_VALUE_BYTES: usize = 188;
const PADDED_VALUE_BYTES: usize = 192;
const TARGET_TAG_ORDINAL: usize = 42;
const SERVER_COUNT: usize = 2;

#[derive(Clone, Debug, Serialize)]
pub struct BillionTagReport {
    pub schema: &'static str,
    pub profile: &'static str,
    pub evidence: &'static str,
    pub workload: Workload,
    pub representation: Representation,
    pub strict_pir: StrictMeasurement,
    pub decoy_100: DecoyMeasurement,
    pub comparison: Comparison,
    pub caveats: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct Workload {
    pub documents: u64,
    pub distinct_tags: usize,
    pub average_tags_per_document: usize,
    pub selectivity_percent: f64,
    pub matches: usize,
    pub encrypted_fields: usize,
    pub encrypted_value_bytes: usize,
    pub useful_target_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct Representation {
    pub layout: &'static str,
    pub values_per_stripe: usize,
    pub stripes_per_tag: usize,
    pub stripe_row_bytes: usize,
    pub rows_per_plane: usize,
    pub representative_plane_bytes: usize,
    pub full_table_bytes_per_replica: u64,
    pub deployed_two_replica_bytes: u64,
    pub selector_bytes_per_server: usize,
    pub estimated_mphf_metadata_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrictMeasurement {
    pub protocol: &'static str,
    pub privacy: &'static str,
    pub measured_stripes: usize,
    pub workload_stripes: usize,
    pub exact_full_workload_timing: bool,
    pub measured_aggregate_server_ms: f64,
    pub full_workload_aggregate_server_ms: f64,
    pub measured_client_combine_ms: f64,
    pub full_workload_client_combine_ms: f64,
    pub query_generation_ms: f64,
    pub measured_client_aead_decryptions: usize,
    pub full_workload_client_aead_ms: f64,
    pub upload_bytes: usize,
    pub download_bytes: usize,
    pub expected_aggregate_source_operand_bytes: u64,
    pub measured_aggregate_source_operand_bytes: u64,
    pub result_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct DecoyMeasurement {
    pub protocol: &'static str,
    pub privacy: &'static str,
    pub measured_stripes: usize,
    pub workload_stripes: usize,
    pub exact_full_workload_timing: bool,
    pub measured_server_copy_ms: f64,
    pub full_workload_server_copy_ms: f64,
    pub measured_client_aead_decryptions: usize,
    pub full_workload_client_aead_ms: f64,
    pub upload_bytes: usize,
    pub download_bytes: u64,
    pub aggregate_source_read_bytes: u64,
    pub returned_encrypted_values: usize,
    pub ignored_without_decryption: usize,
    pub target_result_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct Comparison {
    pub strict_server_over_decoy: f64,
    pub decoy_download_over_strict: f64,
    pub decoy_aead_over_strict: f64,
    pub strict_scans_percent_of_projection_table: f64,
    pub decoy_reads_percent_of_projection_table: f64,
    pub conclusion: &'static str,
}

pub fn run(profile: Profile) -> Result<BillionTagReport> {
    let config = TagPageConfig {
        bucket_capacity: 1,
        target_load_percent: 95,
        values_per_page: VALUES_PER_STRIPE,
        max_value_bytes: PADDED_VALUE_BYTES,
    };
    let row_bytes = config.page_size()?;
    let stripes = MATCHES.div_ceil(VALUES_PER_STRIPE);
    let plane_bytes = DISTINCT_TAGS
        .checked_mul(row_bytes)
        .context("billion-tag representative plane size overflow")?;
    let table_bytes = u64::try_from(plane_bytes)?
        .checked_mul(u64::try_from(stripes)?)
        .context("billion-tag full table size overflow")?;
    let measured_stripes = match profile {
        Profile::Quick => 8,
        Profile::Full => stripes,
    };
    let exact_full_workload_timing = measured_stripes == stripes;

    let mut rows = vec![0u8; plane_bytes];
    // Force every host page to be resident without spending the benchmark on
    // synthetic corpus generation. Row prefixes remain distinct so the PIR
    // reconstruction check cannot pass by returning an arbitrary row.
    for (page_index, page) in rows.chunks_mut(4_096).enumerate() {
        page[0] = (page_index as u8).wrapping_mul(131).wrapping_add(17);
    }
    for (row_index, row) in rows.chunks_exact_mut(row_bytes).enumerate() {
        let digest = blake3::hash(&(row_index as u64).to_le_bytes());
        row[..32].copy_from_slice(digest.as_bytes());
    }
    let view = SnapshotView::new(&rows, DISTINCT_TAGS, row_bytes);

    let decoy_server = measure_decoy_server(view, measured_stripes)?;

    let query_started = Instant::now();
    let mut rng = StdRng::seed_from_u64(0x4249_4c4c_494f_4e01);
    let shares = dense::query_shares(TARGET_TAG_ORDINAL, DISTINCT_TAGS, SERVER_COUNT, &mut rng)?;
    let query_generation = query_started.elapsed();
    let evaluator = BatchEvaluator::new(1, row_bytes)?;
    let mut server_elapsed = Duration::ZERO;
    let mut client_combine_elapsed = Duration::ZERO;
    let mut measured_source_bytes = 0u64;
    for _ in 0..measured_stripes {
        let mut answers = Vec::with_capacity(SERVER_COUNT);
        for share in &shares {
            let started = Instant::now();
            let evaluation = evaluator.evaluate(
                view,
                std::slice::from_ref(share),
                BatchKernel::SharedRowMajor,
            )?;
            server_elapsed += started.elapsed();
            measured_source_bytes = measured_source_bytes
                .checked_add(u64::try_from(
                    evaluation.metrics.immutable_source_operand_bytes,
                )?)
                .context("billion-tag measured Dense work overflow")?;
            answers.push(
                evaluation
                    .answers
                    .into_iter()
                    .next()
                    .context("Dense server omitted its answer")?,
            );
        }
        let combine_started = Instant::now();
        let recovered = dense::combine(&answers.iter().map(Vec::as_slice).collect::<Vec<_>>())?;
        client_combine_elapsed += combine_started.elapsed();
        if recovered != view.row(TARGET_TAG_ORDINAL)? {
            bail!("striped billion-tag Dense retrieval reconstructed the wrong row");
        }
        black_box(recovered);
    }

    let strict_crypto_iterations = match profile {
        Profile::Quick => 10_000,
        Profile::Full => MATCHES,
    };
    let decoy_returned_values = MATCHES
        .checked_mul(DECOYS)
        .context("billion-tag decoy value count overflow")?;
    let decoy_crypto_iterations = match profile {
        Profile::Quick => 10_000,
        Profile::Full => MATCHES,
    };
    let strict_crypto_elapsed = measure_aead_decryptions(strict_crypto_iterations)?;
    let decoy_crypto_elapsed = measure_aead_decryptions(decoy_crypto_iterations)?;

    let strict_full_server = scale_duration(server_elapsed, stripes, measured_stripes);
    let strict_full_combine = scale_duration(client_combine_elapsed, stripes, measured_stripes);
    let decoy_full_server = scale_duration(decoy_server, stripes, measured_stripes);
    let strict_full_crypto =
        scale_duration(strict_crypto_elapsed, MATCHES, strict_crypto_iterations);
    let decoy_full_crypto = scale_duration(decoy_crypto_elapsed, MATCHES, decoy_crypto_iterations);
    let expected_source_bytes = table_bytes;
    let full_measured_source_bytes = scale_u64(
        measured_source_bytes,
        u64::try_from(stripes)?,
        u64::try_from(measured_stripes)?,
    )?;
    let strict_download = row_bytes
        .checked_mul(stripes)
        .and_then(|bytes| bytes.checked_mul(SERVER_COUNT))
        .context("billion-tag strict response size overflow")?;
    let decoy_download = u64::try_from(row_bytes)?
        .checked_mul(u64::try_from(stripes)?)
        .and_then(|bytes| bytes.checked_mul(DECOYS as u64))
        .context("billion-tag decoy response size overflow")?;
    let strict_upload = dense::query_size(DISTINCT_TAGS)
        .checked_mul(SERVER_COUNT)
        .context("billion-tag strict upload size overflow")?;
    let result_bytes = MATCHES
        .checked_mul(ENCRYPTED_VALUE_BYTES)
        .context("billion-tag useful result size overflow")?;

    let strict_server_ms = millis(strict_full_server);
    let decoy_server_ms = millis(decoy_full_server);
    let strict_aead_ms = millis(strict_full_crypto);
    let decoy_aead_ms = millis(decoy_full_crypto);

    Ok(BillionTagReport {
        schema: "defradb-pir-billion-tag-v1",
        profile: match profile {
            Profile::Quick => "quick",
            Profile::Full => "full",
        },
        evidence: if exact_full_workload_timing {
            "measured full logical server traversal and full AEAD count over one resident representative stripe plane"
        } else {
            "bounded measured stripe/AEAD samples linearly projected to the full workload"
        },
        workload: Workload {
            documents: DOCUMENTS,
            distinct_tags: DISTINCT_TAGS,
            average_tags_per_document: 1,
            selectivity_percent: 0.01,
            matches: MATCHES,
            encrypted_fields: 5,
            encrypted_value_bytes: ENCRYPTED_VALUE_BYTES,
            useful_target_bytes: result_bytes,
        },
        representation: Representation {
            layout: "one exact-MPHF tag ordinal; one shared Dense selector reused over fixed continuation-stripe planes",
            values_per_stripe: VALUES_PER_STRIPE,
            stripes_per_tag: stripes,
            stripe_row_bytes: row_bytes,
            rows_per_plane: DISTINCT_TAGS,
            representative_plane_bytes: plane_bytes,
            full_table_bytes_per_replica: table_bytes,
            deployed_two_replica_bytes: table_bytes
                .checked_mul(SERVER_COUNT as u64)
                .context("billion-tag deployed storage overflow")?,
            selector_bytes_per_server: dense::query_size(DISTINCT_TAGS),
            estimated_mphf_metadata_bytes: DISTINCT_TAGS
                .checked_mul(24)
                .context("billion-tag MPHF estimate overflow")?
                .div_ceil(80),
        },
        strict_pir: StrictMeasurement {
            protocol: "two-server exact-MPHF striped Dense XOR/shared-row",
            privacy: "information-theoretic tag privacy if one replica does not collude; public result-size class",
            measured_stripes,
            workload_stripes: stripes,
            exact_full_workload_timing,
            measured_aggregate_server_ms: millis(server_elapsed),
            full_workload_aggregate_server_ms: strict_server_ms,
            measured_client_combine_ms: millis(client_combine_elapsed),
            full_workload_client_combine_ms: millis(strict_full_combine),
            query_generation_ms: millis(query_generation),
            measured_client_aead_decryptions: strict_crypto_iterations,
            full_workload_client_aead_ms: strict_aead_ms,
            upload_bytes: strict_upload,
            download_bytes: strict_download,
            expected_aggregate_source_operand_bytes: expected_source_bytes,
            measured_aggregate_source_operand_bytes: full_measured_source_bytes,
            result_bytes,
        },
        decoy_100: DecoyMeasurement {
            protocol: "one-server public exact index with 100 present decoy tags",
            privacy: "candidate-set privacy only; equality, popularity, cardinality, and longitudinal intersections leak",
            measured_stripes,
            workload_stripes: stripes,
            exact_full_workload_timing,
            measured_server_copy_ms: millis(decoy_server),
            full_workload_server_copy_ms: decoy_server_ms,
            measured_client_aead_decryptions: decoy_crypto_iterations,
            full_workload_client_aead_ms: decoy_aead_ms,
            upload_bytes: DECOYS * 16,
            download_bytes: decoy_download,
            aggregate_source_read_bytes: decoy_download,
            returned_encrypted_values: decoy_returned_values,
            ignored_without_decryption: decoy_returned_values - MATCHES,
            target_result_bytes: result_bytes,
        },
        comparison: Comparison {
            strict_server_over_decoy: strict_server_ms / decoy_server_ms,
            decoy_download_over_strict: decoy_download as f64 / strict_download as f64,
            decoy_aead_over_strict: decoy_aead_ms / strict_aead_ms,
            strict_scans_percent_of_projection_table: 100.0,
            decoy_reads_percent_of_projection_table: 100.0 * DECOYS as f64 / DISTINCT_TAGS as f64,
            conclusion: "at 0.01% selectivity, strict PIR trades about one full-table traversal for 50x less response data; the decoy client decrypts only its known target slot, while 100 decoys read 1% of this equal-cardinality table and win raw server work",
        },
        caveats: vec![
            "the full run executes all 391 traversals but reuses one 497 MB resident representative plane instead of allocating 194 GB of distinct projection bytes",
            "the representative plane exceeds ordinary last-level cache, but repeated bytes can still differ from a deployed sharded or memory-mapped 194 GB table",
            "server timings are sequential in-process replica elapsed time, not cycles, energy, HTTP, TLS, or queue time",
            "the exact-MPHF metadata estimate uses 2.4 bits per populated tag and is analytical",
            "the model assumes exactly one indexed tag per document; storage and server work scale with average tag multiplicity",
            "strict PIR and 100 decoys do not provide equivalent privacy",
        ],
    })
}

fn measure_decoy_server(view: SnapshotView<'_>, stripes: usize) -> Result<Duration> {
    let mut candidates = (0..DECOYS).collect::<Vec<_>>();
    candidates.rotate_left(37);
    if !candidates.contains(&TARGET_TAG_ORDINAL) {
        bail!("billion-tag decoy schedule omitted the target");
    }
    let response_bytes = view
        .row_size
        .checked_mul(DECOYS)
        .context("billion-tag decoy scratch size overflow")?;
    let mut response = vec![0u8; response_bytes];
    let started = Instant::now();
    for _ in 0..stripes {
        for (candidate_index, &ordinal) in candidates.iter().enumerate() {
            let start = candidate_index * view.row_size;
            response[start..start + view.row_size].copy_from_slice(view.row(ordinal)?);
        }
        black_box(&response);
    }
    Ok(started.elapsed())
}

fn measure_aead_decryptions(iterations: usize) -> Result<Duration> {
    let cipher = Aes256Gcm::new_from_slice(&[0x5au8; 32]).expect("fixed AES-256 key");
    let nonce = Nonce::from_slice(&[0x3cu8; 12]);
    let aad = b"defradb-pir-billion-tag-aead-benchmark";
    let plaintext = [0xa5u8; 160];
    let mut encrypted = plaintext.to_vec();
    let tag = cipher
        .encrypt_in_place_detached(nonce, aad, &mut encrypted)
        .map_err(|_| anyhow::anyhow!("encrypt billion-tag AEAD fixture"))?;
    let mut buffer = vec![0u8; plaintext.len()];
    // Remove first-use dispatch and page-fault effects from both measurements.
    buffer.copy_from_slice(&encrypted);
    cipher
        .decrypt_in_place_detached(nonce, aad, &mut buffer, &tag)
        .map_err(|_| anyhow::anyhow!("warm billion-tag AEAD fixture"))?;

    let started = Instant::now();
    for _ in 0..iterations {
        buffer.copy_from_slice(&encrypted);
        cipher
            .decrypt_in_place_detached(nonce, aad, &mut buffer, &tag)
            .map_err(|_| anyhow::anyhow!("decrypt billion-tag AEAD fixture"))?;
        black_box(&buffer);
    }
    if buffer != plaintext {
        bail!("billion-tag AEAD fixture reconstructed the wrong plaintext");
    }
    Ok(started.elapsed())
}

fn scale_duration(measured: Duration, total: usize, measured_units: usize) -> Duration {
    Duration::from_secs_f64(measured.as_secs_f64() * total as f64 / measured_units as f64)
}

fn scale_u64(measured: u64, total: u64, measured_units: u64) -> Result<u64> {
    measured
        .checked_mul(total)
        .context("billion-tag scaled work overflow")
        .map(|value| value / measured_units)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn billion_tag_geometry_is_exact() {
        let config = TagPageConfig {
            bucket_capacity: 1,
            target_load_percent: 95,
            values_per_page: VALUES_PER_STRIPE,
            max_value_bytes: PADDED_VALUE_BYTES,
        };
        let row_bytes = config.page_size().unwrap();
        let stripes = MATCHES.div_ceil(VALUES_PER_STRIPE);
        assert_eq!(row_bytes, 49_688);
        assert_eq!(stripes, 391);
        assert_eq!(row_bytes * DISTINCT_TAGS, 496_880_000);
        assert_eq!(
            row_bytes as u64 * DISTINCT_TAGS as u64 * stripes as u64,
            194_280_080_000
        );
        assert_eq!(dense::query_size(DISTINCT_TAGS) * 2, 2_500);
        assert_eq!(row_bytes * stripes * 2, 38_856_016);
        assert_eq!(
            row_bytes as u64 * stripes as u64 * DECOYS as u64,
            1_942_800_800
        );
        assert_eq!(MATCHES * ENCRYPTED_VALUE_BYTES, 18_800_000);
    }
}
