use std::{env, fs, path::PathBuf};

use anyhow::{bail, Context, Result};
use pir_poc::tag_pages::{benchmark_raw_pages, benchmark_tag, TagPageConfig};
use serde::Serialize;
use sha2::{Digest, Sha256};

const SCHEMA: &str = "defra-pir-raw-page-corpus-v1";

#[derive(Serialize)]
struct Manifest {
    schema: &'static str,
    document_count: usize,
    distinct_tag_count: usize,
    page_count: usize,
    page_bytes: usize,
    values_per_page: usize,
    locator_bytes: usize,
    query_index: usize,
    query_tag_hex: String,
    expected_page_hex: String,
    corpus_blake3: String,
    corpus_sha256: String,
    generation: &'static str,
}

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    let output_dir = args
        .next()
        .map(PathBuf::from)
        .context("usage: export-simplepir-corpus OUTPUT_DIR [DOCUMENTS] [TAGS]")?;
    let document_count = parse_or_default(args.next(), 1 << 20, "document count")?;
    let distinct_tag_count = parse_or_default(args.next(), 1 << 18, "tag count")?;
    if args.next().is_some() {
        bail!("usage: export-simplepir-corpus OUTPUT_DIR [DOCUMENTS] [TAGS]");
    }

    let config = TagPageConfig {
        bucket_capacity: 4,
        target_load_percent: 90,
        values_per_page: 4,
        max_value_bytes: 16,
    };
    let page_bytes = config.page_size()?;
    let raw = benchmark_raw_pages(document_count, distinct_tag_count, &config)?;
    let page_count = raw.len() / page_bytes;
    if page_count == 0 {
        bail!("benchmark corpus produced no pages");
    }
    // This matches the query selected by the common Fuse comparison whenever
    // the standard workload has exactly one page per tag.
    let query_index = (distinct_tag_count / 3).min(page_count - 1);
    let selected = &raw[query_index * page_bytes..(query_index + 1) * page_bytes];
    let manifest = Manifest {
        schema: SCHEMA,
        document_count,
        distinct_tag_count,
        page_count,
        page_bytes,
        values_per_page: config.values_per_page,
        locator_bytes: config.max_value_bytes,
        query_index,
        query_tag_hex: hex::encode(benchmark_tag(distinct_tag_count / 3)),
        expected_page_hex: hex::encode(selected),
        corpus_blake3: blake3::hash(&raw).to_hex().to_string(),
        corpus_sha256: hex::encode(Sha256::digest(&raw)),
        generation: "pir_poc::tag_pages::benchmark_raw_pages; layout-neutral pages before cuckoo, Fuse, MPHF, or PIR padding",
    };

    fs::create_dir_all(&output_dir).with_context(|| format!("create {}", output_dir.display()))?;
    fs::write(output_dir.join("pages.bin"), &raw)
        .with_context(|| format!("write {}/pages.bin", output_dir.display()))?;
    fs::write(
        output_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )
    .with_context(|| format!("write {}/manifest.json", output_dir.display()))?;

    println!(
        "exported {page_count} x {page_bytes}-byte pages ({} bytes) to {}",
        raw.len(),
        output_dir.display()
    );
    Ok(())
}

fn parse_or_default(value: Option<String>, default: usize, label: &str) -> Result<usize> {
    value.map_or(Ok(default), |value| {
        value
            .parse::<usize>()
            .with_context(|| format!("invalid {label}: {value}"))
    })
}
