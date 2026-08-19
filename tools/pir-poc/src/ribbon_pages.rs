//! Standard Ribbon static retrieval over replicated Dense XOR PIR.
//!
//! This is the retrieval construction from Algorithms 1--3 of
//! "Ribbon: Fast Succinct Static Retrieval and Approximate Membership".
//! Each encoded page contributes one random, contiguous GF(2) equation.  The
//! immutable server table is the solution of that banded system.  A client can
//! derive the equation from the page key and the authenticated manifest, then
//! privately retrieve its XOR with one multi-hot Dense selector.
//!
//! This is deliberately Standard Ribbon, not the homogeneous membership
//! filter and not BuRR.  BuRR needs public bump-routing metadata and recursive
//! overloaded layers; silently treating failed equations as absent would not
//! be a faithful retrieval implementation.

use std::mem::size_of;

use anyhow::{bail, Context, Result};

use crate::{
    snapshot::{page_key, Record, SnapshotView},
    tag_pages::{
        benchmark_page_set, decode_page, encode_records, fingerprint, DecodedPage, EncodedPageSet,
        TagPageConfig,
    },
};

const HASH_DOMAIN: &[u8] = b"defradb-pir-standard-ribbon-v1";
const GENERATION_DOMAIN: &[u8] = b"defradb-pir-standard-ribbon-generation-v1";
const LAYOUT_VERSION: u32 = 1;
const MAX_BUILD_ATTEMPTS: usize = 64;
const ABSENT_KEY_VERIFICATION_BITS: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RibbonConfig {
    /// Width of each contiguous Boolean equation.  The POC uses one `u64`.
    pub width: usize,
    /// Epsilon in the paper, expressed as an integer percentage.
    pub overhead_percent: usize,
}

impl Default for RibbonConfig {
    fn default() -> Self {
        Self {
            width: 64,
            overhead_percent: 10,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RibbonPageManifest {
    pub layout_version: u32,
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub page_count: usize,
    pub maximum_pages_per_tag: usize,
    pub values_per_page: usize,
    pub max_value_bytes: usize,
    pub page_size: usize,
    pub width: usize,
    pub overhead_percent: usize,
    pub cell_count: usize,
    pub table_seed: u64,
    /// BLAKE3 commitment to all layout dimensions and ordered solution rows.
    pub generation: [u8; 32],
}

impl RibbonPageManifest {
    pub fn expansion_factor(&self) -> f64 {
        self.cell_count as f64 / self.page_count as f64
    }

    pub fn generation_hex(&self) -> String {
        hex::encode(self.generation)
    }

    pub fn absent_key_verification_bits(&self) -> usize {
        ABSENT_KEY_VERIFICATION_BITS
    }

    /// Exact fixed-size public fields a cold client needs for selector
    /// derivation.  Production would encode these in the signed manifest.
    pub fn client_metadata_bytes(&self) -> usize {
        size_of::<u32>() + 11 * size_of::<u64>() + self.generation.len() + HASH_DOMAIN.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RibbonBuildMetrics {
    pub attempts: usize,
    /// Peak explicitly-owned corpus, equation system, output, and scratch.
    pub peak_tracked_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct RibbonPageSnapshot {
    pub manifest: RibbonPageManifest,
    pub build_metrics: RibbonBuildMetrics,
    rows: Box<[u8]>,
}

impl RibbonPageSnapshot {
    pub fn build(
        records: Vec<Record>,
        page_config: TagPageConfig,
        ribbon_config: RibbonConfig,
    ) -> Result<Self> {
        let page_set = encode_records(records, &page_config)?;
        Self::from_page_set(&page_set, page_config, ribbon_config)
    }

    pub fn benchmark(
        document_count: usize,
        distinct_tag_count: usize,
        page_config: TagPageConfig,
        ribbon_config: RibbonConfig,
    ) -> Result<Self> {
        let page_set = benchmark_page_set(document_count, distinct_tag_count, &page_config)?;
        Self::from_page_set(&page_set, page_config, ribbon_config)
    }

    pub(crate) fn from_page_set(
        page_set: &EncodedPageSet,
        page_config: TagPageConfig,
        ribbon_config: RibbonConfig,
    ) -> Result<Self> {
        validate_config(page_set, ribbon_config)?;
        let page_size = page_config.page_size()?;
        let cell_count = table_cells(page_set.pages.len(), ribbon_config)?;
        let corpus_bytes = page_set.tracked_bytes();
        let (table_seed, rows, attempts, peak_tracked_bytes) = build_solution(
            page_set,
            page_size,
            cell_count,
            ribbon_config.width,
            corpus_bytes,
        )?;
        let generation = generation_digest(
            page_set,
            &page_config,
            ribbon_config,
            cell_count,
            table_seed,
            &rows,
        );

        Ok(Self {
            manifest: RibbonPageManifest {
                layout_version: LAYOUT_VERSION,
                document_count: page_set.document_count,
                distinct_tag_count: page_set.distinct_tag_count,
                page_count: page_set.pages.len(),
                maximum_pages_per_tag: page_set.maximum_pages_per_tag,
                values_per_page: page_config.values_per_page,
                max_value_bytes: page_config.max_value_bytes,
                page_size,
                width: ribbon_config.width,
                overhead_percent: ribbon_config.overhead_percent,
                cell_count,
                table_seed,
                generation,
            },
            build_metrics: RibbonBuildMetrics {
                attempts,
                peak_tracked_bytes,
            },
            rows: rows.into_boxed_slice(),
        })
    }

    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    pub fn view(&self) -> SnapshotView<'_> {
        SnapshotView::new(
            &self.rows,
            self.manifest.cell_count,
            self.manifest.page_size,
        )
    }

    pub fn cells(&self, tag: &[u8], page: usize) -> Result<Vec<usize>> {
        ribbon_cells(&self.manifest, tag, page)
    }

    pub fn decode_retrieved_page(
        &self,
        retrieved: &[u8],
        tag: &[u8],
        page: usize,
    ) -> Result<Option<DecodedPage>> {
        if retrieved.len() != self.manifest.page_size {
            bail!("Standard Ribbon answer page has the wrong size");
        }
        let key = page_key(tag, page)?;
        if retrieved[..16] != fingerprint(&key) {
            return Ok(None);
        }
        decode_page(
            retrieved,
            self.manifest.values_per_page,
            self.manifest.max_value_bytes,
        )
        .map(Some)
    }

    pub fn public_lookup(&self, tag: &[u8]) -> Result<Vec<Vec<u8>>> {
        let first = self
            .lookup_page(tag, 0)?
            .context("tag is not present in the Standard Ribbon snapshot")?;
        let mut values = first.values;
        for page in 1..first.total_pages {
            values.extend(
                self.lookup_page(tag, page)?
                    .context("Standard Ribbon continuation page is missing")?
                    .values,
            );
        }
        Ok(values)
    }

    fn lookup_page(&self, tag: &[u8], page: usize) -> Result<Option<DecodedPage>> {
        let mut retrieved = vec![0u8; self.manifest.page_size];
        for cell in self.cells(tag, page)? {
            xor_in_place(&mut retrieved, self.view().row(cell)?);
        }
        self.decode_retrieved_page(&retrieved, tag, page)
    }
}

/// Derives the Standard Ribbon query only from the requested public key and an
/// authenticated immutable-generation manifest.
pub fn ribbon_cells(manifest: &RibbonPageManifest, tag: &[u8], page: usize) -> Result<Vec<usize>> {
    if manifest.layout_version != LAYOUT_VERSION || manifest.width == 0 || manifest.width > 64 {
        bail!("unsupported Standard Ribbon manifest");
    }
    if manifest.cell_count < manifest.width {
        bail!("invalid Standard Ribbon dimensions");
    }
    let key = page_key(tag, page)?;
    let equation = equation(
        &key,
        manifest.table_seed,
        manifest.cell_count,
        manifest.width,
    );
    Ok(equation_cells(equation))
}

#[derive(Clone, Copy, Debug)]
struct Equation {
    start: usize,
    coefficients: u64,
}

fn validate_config(page_set: &EncodedPageSet, config: RibbonConfig) -> Result<()> {
    if page_set.pages.is_empty() {
        bail!("Standard Ribbon retrieval needs at least one encoded page");
    }
    if !(2..=64).contains(&config.width) {
        bail!("Standard Ribbon width must be between 2 and 64");
    }
    if !(1..=50).contains(&config.overhead_percent) {
        bail!("Standard Ribbon overhead must be between 1 and 50 percent");
    }
    Ok(())
}

fn table_cells(page_count: usize, config: RibbonConfig) -> Result<usize> {
    let denominator = 100usize - config.overhead_percent;
    page_count
        .checked_mul(100)
        .and_then(|value| value.checked_add(denominator - 1))
        .map(|value| value / denominator)
        .and_then(|value| value.checked_add(config.width - 1))
        .context("Standard Ribbon table size overflow")
}

fn build_solution(
    page_set: &EncodedPageSet,
    page_size: usize,
    cell_count: usize,
    width: usize,
    corpus_bytes: usize,
) -> Result<(u64, Vec<u8>, usize, usize)> {
    let system_bytes = cell_count
        .checked_mul(size_of::<u64>() + page_size)
        .context("Standard Ribbon system size overflow")?;
    let peak_tracked_bytes = corpus_bytes
        .checked_add(system_bytes)
        .and_then(|value| value.checked_add(page_size))
        .context("Standard Ribbon peak-memory accounting overflow")?;

    for attempt in 0..MAX_BUILD_ATTEMPTS {
        let table_seed = attempt as u64;
        let mut coefficients = vec![0u64; cell_count];
        let mut right_hand_sides = vec![0u8; cell_count * page_size];
        let mut failed = false;

        for page in &page_set.pages {
            let mut equation = equation(&page.key, table_seed, cell_count, width);
            let mut value = page.bytes.clone();
            loop {
                let slot_coefficients = coefficients[equation.start];
                if slot_coefficients == 0 {
                    coefficients[equation.start] = equation.coefficients;
                    row_mut(&mut right_hand_sides, equation.start, page_size)
                        .copy_from_slice(&value);
                    break;
                }

                equation.coefficients ^= slot_coefficients;
                xor_in_place(
                    &mut value,
                    row(&right_hand_sides, equation.start, page_size),
                );
                if equation.coefficients == 0 {
                    if value.iter().any(|byte| *byte != 0) {
                        failed = true;
                    }
                    break;
                }
                let shift = equation.coefficients.trailing_zeros() as usize;
                equation.start += shift;
                equation.coefficients >>= shift;
            }
            if failed {
                break;
            }
        }
        if failed {
            continue;
        }

        // Algorithm 2 back substitution, in place.  At this point rows above
        // `i` already hold Z; the old RHS at `i` can therefore become Z_i.
        for cell in (0..cell_count).rev() {
            let mut tail = coefficients[cell] >> 1;
            let mut relative = 1usize;
            while tail != 0 {
                let zeroes = tail.trailing_zeros() as usize;
                relative += zeroes;
                xor_rows_in_place(&mut right_hand_sides, cell, cell + relative, page_size);
                tail >>= zeroes + 1;
                relative += 1;
            }
        }
        return Ok((
            table_seed,
            right_hand_sides,
            attempt + 1,
            peak_tracked_bytes,
        ));
    }
    bail!(
        "could not build Standard Ribbon after {MAX_BUILD_ATTEMPTS} deterministic attempts; increase width or overhead"
    )
}

fn equation(key: &[u8], table_seed: u64, cell_count: usize, width: usize) -> Equation {
    let mut hasher = blake3::Hasher::new();
    hasher.update(HASH_DOMAIN);
    hasher.update(&table_seed.to_le_bytes());
    hasher.update(key);
    let digest = hasher.finalize();
    let start_hash = u64::from_le_bytes(
        digest.as_bytes()[..8]
            .try_into()
            .expect("fixed Standard Ribbon hash"),
    );
    let coefficient_hash = u64::from_le_bytes(
        digest.as_bytes()[8..16]
            .try_into()
            .expect("fixed Standard Ribbon hash"),
    );
    let start_range = cell_count - width + 1;
    let start = ((start_hash as u128 * start_range as u128) >> 64) as usize;
    let width_mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    Equation {
        start,
        // Algorithm 1 requires the first coefficient to be one.
        coefficients: (coefficient_hash & width_mask) | 1,
    }
}

fn equation_cells(equation: Equation) -> Vec<usize> {
    let mut coefficients = equation.coefficients;
    let mut cells = Vec::with_capacity(coefficients.count_ones() as usize);
    while coefficients != 0 {
        let bit = coefficients.trailing_zeros() as usize;
        cells.push(equation.start + bit);
        coefficients &= coefficients - 1;
    }
    cells
}

fn generation_digest(
    page_set: &EncodedPageSet,
    page_config: &TagPageConfig,
    ribbon_config: RibbonConfig,
    cell_count: usize,
    table_seed: u64,
    rows: &[u8],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(GENERATION_DOMAIN);
    hasher.update(&LAYOUT_VERSION.to_le_bytes());
    for value in [
        page_set.document_count,
        page_set.distinct_tag_count,
        page_set.pages.len(),
        page_set.maximum_pages_per_tag,
        page_config.values_per_page,
        page_config.max_value_bytes,
        ribbon_config.width,
        ribbon_config.overhead_percent,
        cell_count,
    ] {
        hasher.update(&(value as u64).to_le_bytes());
    }
    hasher.update(&table_seed.to_le_bytes());
    hasher.update(&(rows.len() as u64).to_le_bytes());
    hasher.update(rows);
    *hasher.finalize().as_bytes()
}

fn row(rows: &[u8], cell: usize, page_size: usize) -> &[u8] {
    let start = cell * page_size;
    &rows[start..start + page_size]
}

fn row_mut(rows: &mut [u8], cell: usize, page_size: usize) -> &mut [u8] {
    let start = cell * page_size;
    &mut rows[start..start + page_size]
}

fn xor_rows_in_place(rows: &mut [u8], target: usize, source: usize, page_size: usize) {
    debug_assert!(source > target);
    let target_start = target * page_size;
    let source_start = source * page_size;
    let (before_source, source_and_after) = rows.split_at_mut(source_start);
    xor_in_place(
        &mut before_source[target_start..target_start + page_size],
        &source_and_after[..page_size],
    );
}

fn xor_in_place(target: &mut [u8], source: &[u8]) {
    for (target, source) in target.iter_mut().zip(source) {
        *target ^= source;
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;
    use crate::dense;

    fn page_config() -> TagPageConfig {
        TagPageConfig {
            bucket_capacity: 4,
            target_load_percent: 90,
            values_per_page: 3,
            max_value_bytes: 16,
        }
    }

    fn records() -> Vec<Record> {
        (0..100)
            .flat_map(|tag| {
                (0..5).map(move |value| Record::new(format!("tag-{tag}"), format!("value-{value}")))
            })
            .collect()
    }

    #[test]
    fn recovers_exact_multi_page_values_and_rejects_absent_keys() {
        let snapshot = RibbonPageSnapshot::build(
            records(),
            page_config(),
            RibbonConfig {
                width: 32,
                overhead_percent: 25,
            },
        )
        .unwrap();
        assert_eq!(
            snapshot.public_lookup(b"tag-37").unwrap(),
            (0..5)
                .map(|value| format!("value-{value}").into_bytes())
                .collect::<Vec<_>>()
        );
        assert!(snapshot.public_lookup(b"missing").is_err());
        assert_eq!(snapshot.manifest.absent_key_verification_bits(), 128);
    }

    #[test]
    fn one_multi_hot_dense_query_works_for_any_server_count() {
        let snapshot = RibbonPageSnapshot::build(
            records(),
            page_config(),
            RibbonConfig {
                width: 32,
                overhead_percent: 25,
            },
        )
        .unwrap();
        for server_count in [2, 3, 5] {
            let cells = snapshot.cells(b"tag-37", 0).unwrap();
            let shares = dense::query_shares_for_buckets(
                &cells,
                snapshot.manifest.cell_count,
                server_count,
                &mut StdRng::seed_from_u64(server_count as u64),
            )
            .unwrap();
            let answers = shares
                .iter()
                .map(|share| dense::answer(snapshot.view(), share).unwrap())
                .collect::<Vec<_>>();
            let page = dense::combine(&answers).unwrap();
            assert_eq!(
                snapshot
                    .decode_retrieved_page(&page, b"tag-37", 0)
                    .unwrap()
                    .unwrap()
                    .values,
                ["value-0", "value-1", "value-2"].map(str::as_bytes)
            );
        }
    }

    #[test]
    fn manifest_alone_derives_the_same_selector() {
        let snapshot = RibbonPageSnapshot::build(
            records(),
            page_config(),
            RibbonConfig {
                width: 32,
                overhead_percent: 25,
            },
        )
        .unwrap();
        assert_eq!(
            snapshot.cells(b"tag-37", 1).unwrap(),
            ribbon_cells(&snapshot.manifest, b"tag-37", 1).unwrap()
        );
    }

    #[test]
    fn build_and_generation_are_deterministic() {
        let first = RibbonPageSnapshot::build(
            records(),
            page_config(),
            RibbonConfig {
                width: 32,
                overhead_percent: 25,
            },
        )
        .unwrap();
        let second = RibbonPageSnapshot::build(
            records().into_iter().rev().collect(),
            page_config(),
            RibbonConfig {
                width: 32,
                overhead_percent: 25,
            },
        )
        .unwrap();
        assert_eq!(first.rows(), second.rows());
        assert_eq!(first.manifest, second.manifest);
    }

    #[test]
    fn generation_commits_to_values() {
        let first = RibbonPageSnapshot::build(
            vec![Record::new("tag", "a")],
            page_config(),
            RibbonConfig {
                width: 8,
                overhead_percent: 50,
            },
        )
        .unwrap();
        let second = RibbonPageSnapshot::build(
            vec![Record::new("tag", "b")],
            page_config(),
            RibbonConfig {
                width: 8,
                overhead_percent: 50,
            },
        )
        .unwrap();
        assert_ne!(first.manifest.generation, second.manifest.generation);
    }

    #[test]
    fn populated_corpora_round_trip() {
        for distinct_tags in [1, 2, 7, 31, 127] {
            let snapshot = RibbonPageSnapshot::benchmark(
                distinct_tags * 7,
                distinct_tags,
                page_config(),
                RibbonConfig {
                    width: 32,
                    overhead_percent: 25,
                },
            )
            .unwrap();
            for tag_index in 0..distinct_tags {
                let tag = crate::tag_pages::benchmark_tag(tag_index);
                assert_eq!(snapshot.public_lookup(&tag).unwrap().len(), 7);
            }
        }
    }
}
