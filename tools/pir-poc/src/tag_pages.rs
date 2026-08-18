//! Compact, stateless keyword-to-page layout for snapshot PIR.
//!
//! Dense PIR hides an array index, not a keyword. This module turns a tag into
//! two public cuckoo-hash bucket candidates. A client privately retrieves both
//! fixed-size buckets and keeps the page whose 128-bit fingerprint matches.
//! The two choices avoid a linear client-side minimal-perfect-hash map while
//! allowing a much denser table than the original one-hash snapshot layout.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};

use crate::snapshot::{page_key, Record, SnapshotView};

const HASH_DOMAIN: &[u8] = b"defradb-pir-cuckoo-page-v1";
const FINGERPRINT_DOMAIN: &[u8] = b"defradb-pir-page-fingerprint-v1";
const PAGE_HEADER_BYTES: usize = 24;
const VALUE_LENGTH_BYTES: usize = 2;
const MAX_BUILD_ATTEMPTS: u64 = 64;
const MAX_CUCKOO_KICKS: usize = 1_024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagPageConfig {
    pub bucket_capacity: usize,
    pub target_load_percent: usize,
    pub values_per_page: usize,
    pub max_value_bytes: usize,
}

impl TagPageConfig {
    pub fn page_size(&self) -> Result<usize> {
        validate_config(self)?;
        PAGE_HEADER_BYTES
            .checked_add(
                self.values_per_page
                    .checked_mul(VALUE_LENGTH_BYTES + self.max_value_bytes)
                    .context("tag page value area overflow")?,
            )
            .context("tag page size overflow")
    }

    pub fn row_size(&self) -> Result<usize> {
        self.page_size()?
            .checked_mul(self.bucket_capacity)
            .context("tag bucket row size overflow")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagPageManifest {
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub page_count: usize,
    pub maximum_pages_per_tag: usize,
    pub bucket_count: usize,
    pub bucket_capacity: usize,
    pub values_per_page: usize,
    pub max_value_bytes: usize,
    pub page_size: usize,
    pub row_size: usize,
    pub table_seed: u64,
}

impl TagPageManifest {
    pub fn table_slots(&self) -> usize {
        self.bucket_count * self.bucket_capacity
    }

    pub fn load_factor(&self) -> f64 {
        self.page_count as f64 / self.table_slots() as f64
    }

    /// Tiny public metadata needed by a cold client.
    pub fn client_metadata_bytes(&self) -> usize {
        // Version, seed, six dimensions, and a hash-domain identifier.
        4 + 8 + 6 * 8 + HASH_DOMAIN.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedPage {
    pub total_pages: usize,
    pub values: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct TagPageSnapshot {
    pub manifest: TagPageManifest,
    rows: Box<[u8]>,
}

#[derive(Debug)]
struct EncodedPage {
    key: Vec<u8>,
    bytes: Vec<u8>,
}

impl TagPageSnapshot {
    pub fn build(records: Vec<Record>, config: TagPageConfig) -> Result<Self> {
        validate_config(&config)?;
        let document_count = records.len();
        let mut by_tag = BTreeMap::<Vec<u8>, Vec<Vec<u8>>>::new();
        for record in records {
            if record.key.is_empty() {
                bail!("empty tags are not supported");
            }
            if record.value.len() > config.max_value_bytes {
                bail!(
                    "tag value is {} bytes, limit is {}",
                    record.value.len(),
                    config.max_value_bytes
                );
            }
            by_tag.entry(record.key).or_default().push(record.value);
        }
        for values in by_tag.values_mut() {
            values.sort();
        }

        let distinct_tag_count = by_tag.len();
        let maximum_pages_per_tag = by_tag
            .values()
            .map(|values| values.len().div_ceil(config.values_per_page))
            .max()
            .unwrap_or_default();
        let mut pages = Vec::new();
        for (tag, values) in by_tag {
            let total_pages = values.len().div_ceil(config.values_per_page);
            for (page_index, chunk) in values.chunks(config.values_per_page).enumerate() {
                let key = page_key(&tag, page_index)?;
                pages.push(EncodedPage {
                    bytes: encode_page(&key, total_pages, chunk, &config)?,
                    key,
                });
            }
        }

        Self::from_pages(
            pages,
            document_count,
            distinct_tag_count,
            maximum_pages_per_tag,
            config,
        )
    }

    pub fn benchmark(
        document_count: usize,
        distinct_tag_count: usize,
        config: TagPageConfig,
    ) -> Result<Self> {
        validate_config(&config)?;
        if distinct_tag_count == 0 || document_count < distinct_tag_count {
            bail!("benchmark needs at least one document per distinct tag");
        }
        let base_values = document_count / distinct_tag_count;
        let extra_values = document_count % distinct_tag_count;
        let maximum_values = base_values + usize::from(extra_values != 0);
        let maximum_pages_per_tag = maximum_values.div_ceil(config.values_per_page);
        let page_count = (0..distinct_tag_count).try_fold(0usize, |total, tag_index| {
            let values = base_values + usize::from(tag_index < extra_values);
            total
                .checked_add(values.div_ceil(config.values_per_page))
                .context("benchmark tag page count overflow")
        })?;
        let mut pages = Vec::with_capacity(page_count);
        for tag_index in 0..distinct_tag_count {
            let tag = benchmark_tag(tag_index);
            let value_count = base_values + usize::from(tag_index < extra_values);
            let total_pages = value_count.div_ceil(config.values_per_page);
            for page_index in 0..total_pages {
                let first_value = page_index * config.values_per_page;
                let values_on_page = (value_count - first_value).min(config.values_per_page);
                let values = (0..values_on_page)
                    .map(|offset| {
                        benchmark_value(tag_index, first_value + offset, config.max_value_bytes)
                    })
                    .collect::<Vec<_>>();
                let key = page_key(&tag, page_index)?;
                pages.push(EncodedPage {
                    bytes: encode_page(&key, total_pages, &values, &config)?,
                    key,
                });
            }
        }
        Self::from_pages(
            pages,
            document_count,
            distinct_tag_count,
            maximum_pages_per_tag,
            config,
        )
    }

    fn from_pages(
        pages: Vec<EncodedPage>,
        document_count: usize,
        distinct_tag_count: usize,
        maximum_pages_per_tag: usize,
        config: TagPageConfig,
    ) -> Result<Self> {
        let page_size = config.page_size()?;
        let page_count = pages.len();
        let minimum_slots = page_count
            .checked_mul(100)
            .and_then(|value| value.checked_add(config.target_load_percent - 1))
            .context("tag page table size overflow")?
            / config.target_load_percent;
        let bucket_count = minimum_slots.div_ceil(config.bucket_capacity).max(1);
        let (table_seed, placements) = build_cuckoo_table(&pages, bucket_count, &config)?;
        let row_size = config.row_size()?;
        let mut rows = vec![0u8; bucket_count * row_size];
        for (slot, page_index) in placements.into_iter().enumerate() {
            if let Some(page_index) = page_index {
                let start = slot * page_size;
                rows[start..start + page_size].copy_from_slice(&pages[page_index].bytes);
            }
        }

        Ok(Self {
            manifest: TagPageManifest {
                document_count,
                distinct_tag_count,
                page_count,
                maximum_pages_per_tag,
                bucket_count,
                bucket_capacity: config.bucket_capacity,
                values_per_page: config.values_per_page,
                max_value_bytes: config.max_value_bytes,
                page_size,
                row_size,
                table_seed,
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
            self.manifest.bucket_count,
            self.manifest.row_size,
        )
    }

    pub fn candidate_buckets(&self, tag: &[u8], page: usize) -> Result<[usize; 2]> {
        let key = page_key(tag, page)?;
        Ok(candidate_buckets(
            &key,
            self.manifest.table_seed,
            self.manifest.bucket_count,
        ))
    }

    pub fn decode_bucket_row(
        &self,
        row: &[u8],
        tag: &[u8],
        page: usize,
    ) -> Result<Option<DecodedPage>> {
        if row.len() != self.manifest.row_size {
            bail!("tag-page answer row has the wrong size");
        }
        let key = page_key(tag, page)?;
        let expected_fingerprint = fingerprint(&key);
        for slot in row.chunks_exact(self.manifest.page_size) {
            if slot[..16] == expected_fingerprint {
                return decode_page(slot, &self.manifest).map(Some);
            }
        }
        Ok(None)
    }

    pub fn public_lookup(&self, tag: &[u8]) -> Result<Vec<Vec<u8>>> {
        let first = self
            .lookup_page(tag, 0)?
            .context("tag is not present in the page snapshot")?;
        let mut values = first.values;
        for page in 1..first.total_pages {
            let decoded = self
                .lookup_page(tag, page)?
                .context("tag continuation page is missing")?;
            values.extend(decoded.values);
        }
        Ok(values)
    }

    fn lookup_page(&self, tag: &[u8], page: usize) -> Result<Option<DecodedPage>> {
        for bucket in self.candidate_buckets(tag, page)? {
            if let Some(decoded) = self.decode_bucket_row(self.view().row(bucket)?, tag, page)? {
                return Ok(Some(decoded));
            }
        }
        Ok(None)
    }
}

pub fn benchmark_tag(index: usize) -> [u8; 8] {
    (index as u64).to_le_bytes()
}

pub fn benchmark_value(tag_index: usize, value_index: usize, size: usize) -> Vec<u8> {
    let mut value = vec![0u8; size];
    let mut state = (tag_index as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(value_index as u64)
        .wrapping_add(1);
    for byte in &mut value {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    value
}

fn validate_config(config: &TagPageConfig) -> Result<()> {
    if config.bucket_capacity == 0 || config.values_per_page == 0 || config.max_value_bytes == 0 {
        bail!("tag page capacities and value size must be non-zero");
    }
    if !(50..=95).contains(&config.target_load_percent) {
        bail!("tag page target load must be between 50 and 95 percent");
    }
    if config.max_value_bytes > u16::MAX as usize {
        bail!("tag page values must fit a 16-bit length");
    }
    Ok(())
}

fn encode_page(
    key: &[u8],
    total_pages: usize,
    values: &[Vec<u8>],
    config: &TagPageConfig,
) -> Result<Vec<u8>> {
    let total_pages = u32::try_from(total_pages).context("too many pages for one tag")?;
    let value_count = u16::try_from(values.len()).context("too many values in one tag page")?;
    let mut page = vec![0u8; config.page_size()?];
    page[..16].copy_from_slice(&fingerprint(key));
    page[16..20].copy_from_slice(&total_pages.to_le_bytes());
    page[20..22].copy_from_slice(&value_count.to_le_bytes());
    let value_slot_size = VALUE_LENGTH_BYTES + config.max_value_bytes;
    for (index, value) in values.iter().enumerate() {
        let start = PAGE_HEADER_BYTES + index * value_slot_size;
        page[start..start + 2].copy_from_slice(&(value.len() as u16).to_le_bytes());
        page[start + 2..start + 2 + value.len()].copy_from_slice(value);
    }
    Ok(page)
}

fn decode_page(page: &[u8], manifest: &TagPageManifest) -> Result<DecodedPage> {
    let total_pages = u32::from_le_bytes(page[16..20].try_into().expect("fixed header")) as usize;
    let value_count = u16::from_le_bytes(page[20..22].try_into().expect("fixed header")) as usize;
    if total_pages == 0 || value_count == 0 || value_count > manifest.values_per_page {
        bail!("tag page contains invalid counts");
    }
    let value_slot_size = VALUE_LENGTH_BYTES + manifest.max_value_bytes;
    let mut values = Vec::with_capacity(value_count);
    for index in 0..value_count {
        let start = PAGE_HEADER_BYTES + index * value_slot_size;
        let value_len =
            u16::from_le_bytes(page[start..start + 2].try_into().expect("fixed length")) as usize;
        if value_len > manifest.max_value_bytes {
            bail!("tag page contains an invalid value length");
        }
        values.push(page[start + 2..start + 2 + value_len].to_vec());
    }
    Ok(DecodedPage {
        total_pages,
        values,
    })
}

fn build_cuckoo_table(
    pages: &[EncodedPage],
    bucket_count: usize,
    config: &TagPageConfig,
) -> Result<(u64, Vec<Option<usize>>)> {
    let slot_count = bucket_count * config.bucket_capacity;
    for table_seed in 0..MAX_BUILD_ATTEMPTS {
        let candidates = pages
            .iter()
            .map(|page| candidate_buckets(&page.key, table_seed, bucket_count))
            .collect::<Vec<_>>();
        let mut slots = vec![None; slot_count];
        let mut succeeded = true;
        for page_index in 0..pages.len() {
            if !insert_page(
                page_index,
                table_seed,
                &candidates,
                &mut slots,
                config.bucket_capacity,
            ) {
                succeeded = false;
                break;
            }
        }
        if succeeded {
            return Ok((table_seed, slots));
        }
    }
    bail!(
        "could not build the tag-page cuckoo table after {MAX_BUILD_ATTEMPTS} deterministic attempts"
    )
}

fn insert_page(
    mut page_index: usize,
    table_seed: u64,
    candidates: &[[usize; 2]],
    slots: &mut [Option<usize>],
    bucket_capacity: usize,
) -> bool {
    let mut bucket = candidates[page_index][mix64(table_seed ^ page_index as u64) as usize & 1];
    for kick in 0..MAX_CUCKOO_KICKS {
        let start = bucket * bucket_capacity;
        if let Some(empty) = slots[start..start + bucket_capacity]
            .iter()
            .position(Option::is_none)
        {
            slots[start + empty] = Some(page_index);
            return true;
        }

        let victim = mix64(
            table_seed ^ page_index as u64 ^ (kick as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
        ) as usize
            % bucket_capacity;
        let displaced = slots[start + victim].replace(page_index);
        page_index = displaced.expect("full cuckoo bucket has a victim");
        let [first, second] = candidates[page_index];
        bucket = if bucket == first { second } else { first };
    }
    false
}

fn candidate_buckets(key: &[u8], table_seed: u64, bucket_count: usize) -> [usize; 2] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(HASH_DOMAIN);
    hasher.update(&table_seed.to_le_bytes());
    hasher.update(key);
    let digest = hasher.finalize();
    let first_hash = u64::from_le_bytes(digest.as_bytes()[..8].try_into().expect("fixed hash"));
    let second_hash = u64::from_le_bytes(digest.as_bytes()[8..16].try_into().expect("fixed hash"));
    let first = first_hash as usize % bucket_count;
    let mut second = second_hash as usize % bucket_count;
    if second == first && bucket_count > 1 {
        second = (second + 1) % bucket_count;
    }
    [first, second]
}

fn fingerprint(key: &[u8]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(FINGERPRINT_DOMAIN);
    hasher.update(key);
    hasher.finalize().as_bytes()[..16]
        .try_into()
        .expect("fixed fingerprint")
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
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

    #[test]
    fn packed_pages_are_deterministic_and_recover_all_values() {
        let records = vec![
            Record::new("music", "c"),
            Record::new("sports", "one"),
            Record::new("music", "a"),
            Record::new("music", "d"),
            Record::new("music", "b"),
        ];
        let first = TagPageSnapshot::build(records.clone(), config()).unwrap();
        let second = TagPageSnapshot::build(records.into_iter().rev().collect(), config()).unwrap();
        assert_eq!(first.manifest, second.manifest);
        assert_eq!(first.rows(), second.rows());
        assert_eq!(
            first.public_lookup(b"music").unwrap(),
            ["a", "b", "c", "d"].map(str::as_bytes)
        );
        assert!(first.public_lookup(b"missing").is_err());
    }

    #[test]
    fn two_server_dense_lookup_recovers_the_matching_candidate() {
        let records = (0..20)
            .flat_map(|tag| {
                (0..5).map(move |value| Record::new(format!("tag-{tag}"), format!("value-{value}")))
            })
            .collect();
        let snapshot = TagPageSnapshot::build(records, config()).unwrap();
        let mut rng = StdRng::seed_from_u64(7);
        let mut recovered = Vec::new();
        let mut page = 0;
        loop {
            let mut decoded = None;
            for bucket in snapshot.candidate_buckets(b"tag-7", page).unwrap() {
                let shares =
                    dense::query_shares(bucket, snapshot.manifest.bucket_count, 2, &mut rng)
                        .unwrap();
                let answers = shares
                    .iter()
                    .map(|share| dense::answer(snapshot.view(), share).unwrap())
                    .collect::<Vec<_>>();
                let row = dense::combine(&answers).unwrap();
                if let Some(found) = snapshot.decode_bucket_row(&row, b"tag-7", page).unwrap() {
                    decoded = Some(found);
                }
            }
            let decoded = decoded.expect("one candidate contains the page");
            let total_pages = decoded.total_pages;
            recovered.extend(decoded.values);
            page += 1;
            if page == total_pages {
                break;
            }
        }
        assert_eq!(
            recovered,
            (0..5)
                .map(|value| format!("value-{value}").into_bytes())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cuckoo_layout_reaches_the_requested_load() {
        let records = (0u32..10_000)
            .map(|index| Record::new(format!("tag-{index}"), index.to_le_bytes()))
            .collect();
        let snapshot = TagPageSnapshot::build(records, config()).unwrap();
        assert!(snapshot.manifest.load_factor() >= 0.89);
        for index in [0u32, 1, 99, 9_999] {
            assert_eq!(
                snapshot
                    .public_lookup(format!("tag-{index}").as_bytes())
                    .unwrap(),
                vec![index.to_le_bytes().to_vec()]
            );
        }
    }
}
