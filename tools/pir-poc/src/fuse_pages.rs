//! Exact 3-wise and 4-wise Fuse retrieval over replicated Dense XOR PIR.
//!
//! A Fuse filter normally stores short membership fingerprints. This layout
//! uses the same spatially-coupled, peelable hypergraph as a static function:
//! every key maps to three or four cells whose XOR is the complete encoded tag
//! page. A Dense XOR multi-row selector privately reconstructs that page in one
//! request, with no per-client index or preload.

use std::mem::size_of;

use anyhow::{bail, Context, Result};

use crate::{
    snapshot::{page_key, Record, SnapshotView},
    tag_pages::{
        benchmark_page_set, decode_page, encode_records, fingerprint, DecodedPage, EncodedPage,
        EncodedPageSet, TagPageConfig,
    },
};

const HASH_DOMAIN: &[u8] = b"defradb-pir-fuse-retrieval-v1";
const MAX_BUILD_ATTEMPTS: usize = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FuseArity {
    Three,
    Four,
}

impl FuseArity {
    pub fn value(self) -> usize {
        match self {
            Self::Three => 3,
            Self::Four => 4,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Three => "fuse-3",
            Self::Four => "fuse-4",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FusePageManifest {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub page_count: usize,
    pub maximum_pages_per_tag: usize,
    pub arity: usize,
    pub segment_length: usize,
    pub segment_count_length: usize,
    pub cell_count: usize,
    pub values_per_page: usize,
    pub max_value_bytes: usize,
    pub page_size: usize,
    pub table_seed: u64,
}

impl FusePageManifest {
    pub fn expansion_factor(&self) -> f64 {
        self.cell_count as f64 / self.page_count as f64
    }

    pub fn client_metadata_bytes(&self) -> usize {
        // Version, seed, seven dimensions, and the fixed hash-domain identifier.
        4 + 8 + 7 * 8 + HASH_DOMAIN.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FuseBuildMetrics {
    pub attempts: usize,
    /// Peak bytes owned by the corpus and layout builder. Allocator and runtime
    /// overhead are intentionally excluded so runs remain comparable.
    pub peak_tracked_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct FusePageSnapshot {
    pub manifest: FusePageManifest,
    pub build_metrics: FuseBuildMetrics,
    rows: Box<[u8]>,
}

#[derive(Clone, Copy, Debug)]
struct Dimensions {
    segment_length: usize,
    segment_count_length: usize,
    cell_count: usize,
}

#[derive(Clone, Copy, Debug)]
struct Peel {
    edge: usize,
    cell: usize,
}

impl FusePageSnapshot {
    pub fn build(records: Vec<Record>, config: TagPageConfig, arity: FuseArity) -> Result<Self> {
        let page_set = encode_records(records, &config)?;
        Self::from_page_set(&page_set, config, arity)
    }

    pub fn benchmark(
        document_count: usize,
        distinct_tag_count: usize,
        config: TagPageConfig,
        arity: FuseArity,
    ) -> Result<Self> {
        let page_set = benchmark_page_set(document_count, distinct_tag_count, &config)?;
        Self::from_page_set(&page_set, config, arity)
    }

    pub(crate) fn from_page_set(
        page_set: &EncodedPageSet,
        config: TagPageConfig,
        arity: FuseArity,
    ) -> Result<Self> {
        if page_set.pages.is_empty() {
            bail!("Fuse retrieval needs at least one encoded page");
        }
        let page_size = config.page_size()?;
        let dimensions = dimensions(page_set.pages.len(), arity);
        let corpus_bytes = page_set.tracked_bytes();
        let (table_seed, peel, attempts, peel_peak_bytes) =
            build_peel(&page_set.pages, dimensions, arity, corpus_bytes)?;

        let table_bytes = dimensions
            .cell_count
            .checked_mul(page_size)
            .context("Fuse table size overflow")?;
        let mut rows = vec![0u8; table_bytes];
        for peeled in peel.iter().rev() {
            let page = &page_set.pages[peeled.edge];
            let mut value = page.bytes.clone();
            for cell in positions(&page.key, table_seed, dimensions, arity)
                .into_iter()
                .take(arity.value())
            {
                if cell != peeled.cell {
                    xor_in_place(&mut value, row(&rows, cell, page_size));
                }
            }
            row_mut(&mut rows, peeled.cell, page_size).copy_from_slice(&value);
        }
        let materialization_bytes =
            corpus_bytes + peel.capacity() * size_of::<Peel>() + rows.capacity() + page_size;

        Ok(Self {
            manifest: FusePageManifest {
                document_count: page_set.document_count,
                distinct_tag_count: page_set.distinct_tag_count,
                page_count: page_set.pages.len(),
                maximum_pages_per_tag: page_set.maximum_pages_per_tag,
                arity: arity.value(),
                segment_length: dimensions.segment_length,
                segment_count_length: dimensions.segment_count_length,
                cell_count: dimensions.cell_count,
                values_per_page: config.values_per_page,
                max_value_bytes: config.max_value_bytes,
                page_size,
                table_seed,
            },
            build_metrics: FuseBuildMetrics {
                attempts,
                peak_tracked_bytes: peel_peak_bytes.max(materialization_bytes),
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
        let key = page_key(tag, page)?;
        let dimensions = Dimensions {
            segment_length: self.manifest.segment_length,
            segment_count_length: self.manifest.segment_count_length,
            cell_count: self.manifest.cell_count,
        };
        let arity = match self.manifest.arity {
            3 => FuseArity::Three,
            4 => FuseArity::Four,
            _ => bail!("unsupported Fuse arity in manifest"),
        };
        Ok(positions(&key, self.manifest.table_seed, dimensions, arity)
            .into_iter()
            .take(arity.value())
            .collect())
    }

    pub fn decode_retrieved_page(
        &self,
        retrieved: &[u8],
        tag: &[u8],
        page: usize,
    ) -> Result<Option<DecodedPage>> {
        if retrieved.len() != self.manifest.page_size {
            bail!("Fuse answer page has the wrong size");
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
            .context("tag is not present in the Fuse snapshot")?;
        let mut values = first.values;
        for page in 1..first.total_pages {
            values.extend(
                self.lookup_page(tag, page)?
                    .context("Fuse continuation page is missing")?
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

fn dimensions(size: usize, arity: FuseArity) -> Dimensions {
    let size_f64 = size as f64;
    let exponent = match arity {
        FuseArity::Three => (size_f64.ln() / 3.33_f64.ln() + 2.25).floor(),
        FuseArity::Four => (size_f64.ln() / 2.91_f64.ln() - 0.5).floor(),
    }
    .clamp(2.0, 18.0) as u32;
    let segment_length = 1usize << exponent;
    let size_factor = if size <= 1 {
        2.0
    } else {
        match arity {
            FuseArity::Three => 1.125_f64.max(0.875 + 0.25 * 1e6_f64.ln() / size_f64.ln()),
            FuseArity::Four => 1.075_f64.max(0.77 + 0.305 * 6e5_f64.ln() / size_f64.ln()),
        }
    };
    let capacity = (size_f64 * size_factor).round() as usize;
    let initial_segments = capacity.div_ceil(segment_length).max(1);
    let segment_count = if initial_segments < arity.value() {
        1
    } else {
        initial_segments - (arity.value() - 1)
    };
    Dimensions {
        segment_length,
        segment_count_length: segment_count * segment_length,
        cell_count: (segment_count + arity.value() - 1) * segment_length,
    }
}

fn build_peel(
    pages: &[EncodedPage],
    dimensions: Dimensions,
    arity: FuseArity,
    corpus_bytes: usize,
) -> Result<(u64, Vec<Peel>, usize, usize)> {
    let workspace_bytes = corpus_bytes
        + dimensions.cell_count * (size_of::<u32>() + size_of::<usize>() * 2)
        + pages.len() * (size_of::<bool>() + size_of::<Peel>());

    for attempt in 0..MAX_BUILD_ATTEMPTS {
        let table_seed = attempt as u64;
        let mut degree = vec![0u32; dimensions.cell_count];
        let mut edge_xor = vec![0usize; dimensions.cell_count];
        for (edge, page) in pages.iter().enumerate() {
            for cell in positions(&page.key, table_seed, dimensions, arity)
                .into_iter()
                .take(arity.value())
            {
                degree[cell] = degree[cell]
                    .checked_add(1)
                    .context("Fuse cell degree overflow")?;
                edge_xor[cell] ^= edge;
            }
        }

        let mut queue = Vec::with_capacity(dimensions.cell_count);
        queue.extend(
            degree
                .iter()
                .enumerate()
                .filter_map(|(cell, &degree)| (degree == 1).then_some(cell)),
        );
        let mut removed = vec![false; pages.len()];
        let mut peel = Vec::with_capacity(pages.len());
        while let Some(cell) = queue.pop() {
            if degree[cell] != 1 {
                continue;
            }
            let edge = edge_xor[cell];
            if removed[edge] {
                continue;
            }
            removed[edge] = true;
            peel.push(Peel { edge, cell });
            for incident in positions(&pages[edge].key, table_seed, dimensions, arity)
                .into_iter()
                .take(arity.value())
            {
                if degree[incident] == 0 {
                    continue;
                }
                degree[incident] -= 1;
                edge_xor[incident] ^= edge;
                if degree[incident] == 1 {
                    queue.push(incident);
                }
            }
        }
        if peel.len() == pages.len() {
            return Ok((table_seed, peel, attempt + 1, workspace_bytes));
        }
    }
    bail!("could not build the Fuse retrieval table after {MAX_BUILD_ATTEMPTS} attempts")
}

fn positions(key: &[u8], table_seed: u64, dimensions: Dimensions, arity: FuseArity) -> [usize; 4] {
    let hash = hash_key(key, table_seed);
    let base = ((hash as u128 * dimensions.segment_count_length as u128) >> 64) as usize;
    let mask = dimensions.segment_length - 1;
    match arity {
        FuseArity::Three => [
            base,
            (base + dimensions.segment_length) ^ ((hash >> 18) as usize & mask),
            (base + 2 * dimensions.segment_length) ^ (hash as usize & mask),
            0,
        ],
        FuseArity::Four => [
            base,
            (base + dimensions.segment_length) ^ (hash as usize & mask),
            (base + 2 * dimensions.segment_length) ^ ((hash >> 16) as usize & mask),
            (base + 3 * dimensions.segment_length) ^ ((hash >> 32) as usize & mask),
        ],
    }
}

fn hash_key(key: &[u8], table_seed: u64) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(HASH_DOMAIN);
    hasher.update(&table_seed.to_le_bytes());
    hasher.update(key);
    u64::from_le_bytes(
        hasher.finalize().as_bytes()[..8]
            .try_into()
            .expect("fixed hash"),
    )
}

fn row(rows: &[u8], cell: usize, page_size: usize) -> &[u8] {
    let start = cell * page_size;
    &rows[start..start + page_size]
}

fn row_mut(rows: &mut [u8], cell: usize, page_size: usize) -> &mut [u8] {
    let start = cell * page_size;
    &mut rows[start..start + page_size]
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

    fn config() -> TagPageConfig {
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
    fn both_arities_recover_exact_multi_page_values() {
        for arity in [FuseArity::Three, FuseArity::Four] {
            let snapshot = FusePageSnapshot::build(records(), config(), arity).unwrap();
            assert_eq!(
                snapshot.public_lookup(b"tag-37").unwrap(),
                (0..5)
                    .map(|value| format!("value-{value}").into_bytes())
                    .collect::<Vec<_>>()
            );
            assert!(snapshot.public_lookup(b"missing").is_err());
        }
    }

    #[test]
    fn one_multi_hot_dense_query_recovers_a_fuse_page() {
        for arity in [FuseArity::Three, FuseArity::Four] {
            let snapshot = FusePageSnapshot::build(records(), config(), arity).unwrap();
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
    }

    #[test]
    fn construction_is_deterministic() {
        for arity in [FuseArity::Three, FuseArity::Four] {
            let mut reversed = records();
            reversed.reverse();
            let first = FusePageSnapshot::build(records(), config(), arity).unwrap();
            let second = FusePageSnapshot::build(reversed, config(), arity).unwrap();
            assert_eq!(first.manifest, second.manifest);
            assert_eq!(first.rows(), second.rows());
        }
    }
}
