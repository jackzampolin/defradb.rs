//! Server-side subset-XOR preprocessing for replicated Dense XOR PIR.
//!
//! Rows are partitioned into small groups.  For each group the server stores
//! the XOR of every non-empty subset, so answering a Dense query needs at most
//! one indexed row read per group instead of one source-row read per set bit.
//! The all-zero subset is implicit and costs neither storage nor an XOR.
//!
//! The index is immutable. Replacing one source row changes
//! `2^(group_size - 1)` combinations in its group, but this POC intentionally
//! uses full rebuilds so readers never observe a partially updated index. The
//! raw serialization below is crate-private benchmark/test machinery: it is
//! not generation-bound and must not be used as a production persistence API.

#[cfg(test)]
use std::io::Read;
use std::io::Write;

use anyhow::{bail, Context, Result};

use crate::{dense, snapshot::SnapshotView};

const MAGIC: &[u8; 8] = b"PIRSUBX1";
const FORMAT_VERSION: u32 = 1;
const HEADER_BYTES: usize = 32;
const MIN_GROUP_SIZE: usize = 2;
const MAX_GROUP_SIZE: usize = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubsetXorEstimate {
    pub bucket_count: usize,
    pub row_size: usize,
    pub group_size: usize,
    pub group_count: usize,
    pub stored_combination_rows: usize,
    pub index_data_bytes: usize,
    pub persisted_bytes: usize,
    /// Analytical source-row bytes plus the final index allocation.  Allocator
    /// metadata, runtime RSS, temporary persistence buffers, and the caller's
    /// corpus-building buffers are excluded.
    pub peak_tracked_bytes: usize,
}

impl SubsetXorEstimate {
    pub fn storage_amplification(self) -> f64 {
        self.persisted_bytes as f64 / (self.bucket_count * self.row_size) as f64
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubsetXorAnswerMetrics {
    pub logical_row_reads: usize,
    pub logical_row_xors: usize,
    pub logical_data_bytes_read: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubsetXorAnswer {
    pub bytes: Vec<u8>,
    pub metrics: SubsetXorAnswerMetrics,
}

#[derive(Debug)]
pub struct SubsetXorIndex {
    bucket_count: usize,
    row_size: usize,
    group_size: usize,
    group_count: usize,
    stored_combination_rows: usize,
    rows: Box<[u8]>,
}

impl SubsetXorIndex {
    pub fn estimate(snapshot: SnapshotView<'_>, group_size: usize) -> Result<SubsetXorEstimate> {
        estimate_dimensions(snapshot.bucket_count, snapshot.row_size, group_size)
    }

    pub fn build(snapshot: SnapshotView<'_>, group_size: usize) -> Result<Self> {
        Self::build_with_limit(snapshot, group_size, usize::MAX)
    }

    pub fn build_with_limit(
        snapshot: SnapshotView<'_>,
        group_size: usize,
        maximum_index_bytes: usize,
    ) -> Result<Self> {
        let estimate = Self::estimate(snapshot, group_size)?;
        if estimate.index_data_bytes > maximum_index_bytes {
            bail!(
                "subset-XOR group {} needs {} index bytes, above the {}-byte limit",
                group_size,
                estimate.index_data_bytes,
                maximum_index_bytes
            );
        }

        let mut rows = allocate_zeroed(estimate.index_data_bytes, "subset-XOR index")?;
        let mut output_row = 0usize;
        for group in 0..estimate.group_count {
            let first_bucket = group * group_size;
            let group_bits = (snapshot.bucket_count - first_bucket).min(group_size);
            let group_rows = combinations(group_bits)? - 1;
            build_group(
                snapshot,
                first_bucket,
                group_bits,
                &mut rows
                    [output_row * snapshot.row_size..(output_row + group_rows) * snapshot.row_size],
            );
            output_row += group_rows;
        }
        debug_assert_eq!(output_row, estimate.stored_combination_rows);

        Ok(Self {
            bucket_count: snapshot.bucket_count,
            row_size: snapshot.row_size,
            group_size,
            group_count: estimate.group_count,
            stored_combination_rows: estimate.stored_combination_rows,
            rows: rows.into_boxed_slice(),
        })
    }

    pub fn answer(&self, query_share: &[u8]) -> Result<Vec<u8>> {
        self.answer_with_metrics(query_share)
            .map(|answer| answer.bytes)
    }

    pub fn answer_with_metrics(&self, query_share: &[u8]) -> Result<SubsetXorAnswer> {
        let expected = dense::query_size(self.bucket_count);
        if query_share.len() != expected {
            bail!(
                "query share has {} bytes, expected {expected}",
                query_share.len()
            );
        }

        let mut answer = vec![0u8; self.row_size];
        let mut metrics = SubsetXorAnswerMetrics::default();
        let full_group_rows = combinations(self.group_size)? - 1;
        for group in 0..self.group_count {
            let first_bucket = group * self.group_size;
            let group_bits = (self.bucket_count - first_bucket).min(self.group_size);
            let selection = read_query_bits(query_share, first_bucket, group_bits);
            if selection == 0 {
                continue;
            }
            let first_combination_row = group * full_group_rows;
            let stored_row = first_combination_row + selection - 1;
            let start = stored_row * self.row_size;
            xor_row(&mut answer, &self.rows[start..start + self.row_size]);
            metrics.logical_row_reads += 1;
            metrics.logical_row_xors += 1;
            metrics.logical_data_bytes_read += self.row_size;
        }
        Ok(SubsetXorAnswer {
            bytes: answer,
            metrics,
        })
    }

    pub fn bucket_count(&self) -> usize {
        self.bucket_count
    }

    pub fn row_size(&self) -> usize {
        self.row_size
    }

    pub fn group_size(&self) -> usize {
        self.group_size
    }

    pub fn group_count(&self) -> usize {
        self.group_count
    }

    pub fn stored_combination_rows(&self) -> usize {
        self.stored_combination_rows
    }

    pub fn index_data_bytes(&self) -> usize {
        self.rows.len()
    }

    pub fn persisted_bytes(&self) -> usize {
        HEADER_BYTES + self.rows.len()
    }

    pub fn storage_amplification(&self) -> f64 {
        self.persisted_bytes() as f64 / (self.bucket_count * self.row_size) as f64
    }

    pub(crate) fn write_to(&self, mut writer: impl Write) -> Result<()> {
        writer.write_all(MAGIC).context("write subset-XOR magic")?;
        writer
            .write_all(&FORMAT_VERSION.to_le_bytes())
            .context("write subset-XOR version")?;
        writer
            .write_all(
                &u32::try_from(self.group_size)
                    .context("subset-XOR group size is not persistable")?
                    .to_le_bytes(),
            )
            .context("write subset-XOR group size")?;
        writer
            .write_all(
                &u64::try_from(self.bucket_count)
                    .context("subset-XOR bucket count is not persistable")?
                    .to_le_bytes(),
            )
            .context("write subset-XOR bucket count")?;
        writer
            .write_all(
                &u64::try_from(self.row_size)
                    .context("subset-XOR row size is not persistable")?
                    .to_le_bytes(),
            )
            .context("write subset-XOR row size")?;
        writer
            .write_all(&self.rows)
            .context("write subset-XOR combinations")?;
        Ok(())
    }

    #[cfg(test)]
    fn read_from(mut reader: impl Read) -> Result<Self> {
        Self::read_from_with_limit(&mut reader, usize::MAX)
    }

    /// Reads a persisted index while refusing an allocation above `maximum_index_bytes`.
    ///
    /// Test/benchmark callers derive the limit from their configured index
    /// budget before reading the untrusted benchmark artifact.
    #[cfg(test)]
    fn read_from_with_limit(mut reader: impl Read, maximum_index_bytes: usize) -> Result<Self> {
        let mut magic = [0u8; 8];
        reader
            .read_exact(&mut magic)
            .context("read subset-XOR magic")?;
        if &magic != MAGIC {
            bail!("invalid subset-XOR index magic");
        }
        let version = read_u32(&mut reader, "version")?;
        if version != FORMAT_VERSION {
            bail!("unsupported subset-XOR index version {version}");
        }
        let group_size = read_u32(&mut reader, "group size")? as usize;
        let bucket_count = usize::try_from(read_u64(&mut reader, "bucket count")?)
            .context("subset-XOR bucket count does not fit this platform")?;
        let row_size = usize::try_from(read_u64(&mut reader, "row size")?)
            .context("subset-XOR row size does not fit this platform")?;

        validate_dimensions(bucket_count, row_size, group_size)?;
        let estimate = estimate_dimensions(bucket_count, row_size, group_size)?;
        if estimate.index_data_bytes > maximum_index_bytes {
            bail!(
                "persisted subset-XOR group {} needs {} index bytes, above the {}-byte limit",
                group_size,
                estimate.index_data_bytes,
                maximum_index_bytes
            );
        }
        let mut rows = allocate_zeroed(estimate.index_data_bytes, "persisted subset-XOR index")?;
        reader
            .read_exact(&mut rows)
            .context("read subset-XOR combinations")?;
        let mut trailing = [0u8; 1];
        if reader
            .read(&mut trailing)
            .context("read subset-XOR trailer")?
            != 0
        {
            bail!("subset-XOR index contains trailing bytes");
        }
        Ok(Self {
            bucket_count,
            row_size,
            group_size,
            group_count: estimate.group_count,
            stored_combination_rows: estimate.stored_combination_rows,
            rows: rows.into_boxed_slice(),
        })
    }
}

fn allocate_zeroed(bytes: usize, allocation: &str) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(bytes)
        .with_context(|| format!("allocate {bytes} bytes for {allocation}"))?;
    output.resize(bytes, 0);
    Ok(output)
}

fn estimate_dimensions(
    bucket_count: usize,
    row_size: usize,
    group_size: usize,
) -> Result<SubsetXorEstimate> {
    validate_dimensions(bucket_count, row_size, group_size)?;
    let full_groups = bucket_count / group_size;
    let trailing_bits = bucket_count % group_size;
    let full_combinations = combinations(group_size)? - 1;
    let trailing_combinations = if trailing_bits == 0 {
        0
    } else {
        combinations(trailing_bits)? - 1
    };
    let stored_combination_rows = full_groups
        .checked_mul(full_combinations)
        .and_then(|value| value.checked_add(trailing_combinations))
        .context("subset-XOR combination count overflow")?;
    let index_data_bytes = stored_combination_rows
        .checked_mul(row_size)
        .context("subset-XOR index size overflow")?;
    let source_bytes = bucket_count
        .checked_mul(row_size)
        .context("snapshot size overflow")?;
    Ok(SubsetXorEstimate {
        bucket_count,
        row_size,
        group_size,
        group_count: bucket_count.div_ceil(group_size),
        stored_combination_rows,
        index_data_bytes,
        persisted_bytes: HEADER_BYTES
            .checked_add(index_data_bytes)
            .context("persisted subset-XOR size overflow")?,
        peak_tracked_bytes: source_bytes
            .checked_add(index_data_bytes)
            .context("tracked subset-XOR memory overflow")?,
    })
}

fn validate_dimensions(bucket_count: usize, row_size: usize, group_size: usize) -> Result<()> {
    if bucket_count == 0 {
        bail!("subset-XOR index requires at least one bucket");
    }
    if row_size == 0 {
        bail!("subset-XOR index requires non-empty rows");
    }
    if !(MIN_GROUP_SIZE..=MAX_GROUP_SIZE).contains(&group_size) {
        bail!("subset-XOR group size must be between {MIN_GROUP_SIZE} and {MAX_GROUP_SIZE}");
    }
    Ok(())
}

fn combinations(group_bits: usize) -> Result<usize> {
    1usize
        .checked_shl(u32::try_from(group_bits).context("group size does not fit u32")?)
        .context("subset-XOR combination count overflow")
}

fn build_group(
    snapshot: SnapshotView<'_>,
    first_bucket: usize,
    group_bits: usize,
    output: &mut [u8],
) {
    let row_size = snapshot.row_size;
    debug_assert_eq!(output.len(), ((1usize << group_bits) - 1) * row_size);
    for selection in 1..(1usize << group_bits) {
        let previous = selection & (selection - 1);
        let output_start = (selection - 1) * row_size;
        if previous != 0 {
            let previous_start = (previous - 1) * row_size;
            output.copy_within(previous_start..previous_start + row_size, output_start);
        }
        let selected_bit = selection.trailing_zeros() as usize;
        let source_start = (first_bucket + selected_bit) * row_size;
        xor_row(
            &mut output[output_start..output_start + row_size],
            &snapshot.rows()[source_start..source_start + row_size],
        );
    }
}

fn read_query_bits(query: &[u8], bit_offset: usize, bit_count: usize) -> usize {
    debug_assert!((1..=MAX_GROUP_SIZE).contains(&bit_count));
    let byte_index = bit_offset / 8;
    let shift = bit_offset % 8;
    let mut word = 0u32;
    for byte_offset in 0..3 {
        word |= u32::from(
            query
                .get(byte_index + byte_offset)
                .copied()
                .unwrap_or_default(),
        ) << (byte_offset * 8);
    }
    ((word >> shift) as usize) & ((1usize << bit_count) - 1)
}

#[inline(always)]
fn xor_row(output: &mut [u8], row: &[u8]) {
    const WORD_BYTES: usize = std::mem::size_of::<u64>();
    let word_bytes = output.len() / WORD_BYTES * WORD_BYTES;
    let mut offset = 0;
    while offset < word_bytes {
        let left = u64::from_ne_bytes(
            output[offset..offset + WORD_BYTES]
                .try_into()
                .expect("fixed word"),
        );
        let right = u64::from_ne_bytes(
            row[offset..offset + WORD_BYTES]
                .try_into()
                .expect("fixed word"),
        );
        output[offset..offset + WORD_BYTES].copy_from_slice(&(left ^ right).to_ne_bytes());
        offset += WORD_BYTES;
    }
    for (left, right) in output[word_bytes..].iter_mut().zip(&row[word_bytes..]) {
        *left ^= *right;
    }
}

#[cfg(test)]
fn read_u32(reader: &mut impl Read, field: &str) -> Result<u32> {
    let mut bytes = [0u8; 4];
    reader
        .read_exact(&mut bytes)
        .with_context(|| format!("read subset-XOR {field}"))?;
    Ok(u32::from_le_bytes(bytes))
}

#[cfg(test)]
fn read_u64(reader: &mut impl Read, field: &str) -> Result<u64> {
    let mut bytes = [0u8; 8];
    reader
        .read_exact(&mut bytes)
        .with_context(|| format!("read subset-XOR {field}"))?;
    Ok(u64::from_le_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use rand::{rngs::StdRng, RngCore, SeedableRng};

    use super::*;
    use crate::snapshot::SnapshotView;

    fn test_rows(bucket_count: usize, row_size: usize, seed: u64) -> Vec<u8> {
        let mut rows = vec![0u8; bucket_count * row_size];
        StdRng::seed_from_u64(seed).fill_bytes(&mut rows);
        rows
    }

    #[test]
    fn every_supported_group_and_generic_row_width_matches_dense() {
        for bucket_count in [3, 8, 17, 33] {
            for row_size in [1, 3, 8, 13, 65] {
                let rows = test_rows(bucket_count, row_size, 0x51b5e7);
                let snapshot = SnapshotView::new(&rows, bucket_count, row_size);
                let mut rng = StdRng::seed_from_u64(bucket_count as u64 ^ row_size as u64);
                for group_size in [2, 4, 6, 8, 10] {
                    let index = SubsetXorIndex::build(snapshot, group_size).unwrap();
                    for _ in 0..16 {
                        let mut query = vec![0u8; dense::query_size(bucket_count)];
                        rng.fill_bytes(&mut query);
                        assert_eq!(
                            index.answer(&query).unwrap(),
                            dense::answer(snapshot, &query).unwrap()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn any_server_count_recovers_single_and_multi_hot_queries() {
        let rows = test_rows(67, 37, 9);
        let snapshot = SnapshotView::new(&rows, 67, 37);
        for group_size in [2, 4, 6, 8] {
            let index = SubsetXorIndex::build(snapshot, group_size).unwrap();
            for server_count in 2..=6 {
                let mut rng = StdRng::seed_from_u64((group_size * 10 + server_count) as u64);
                for buckets in [&[19usize][..], &[2usize, 17, 41, 63][..]] {
                    let queries = dense::query_shares_for_buckets(
                        buckets,
                        snapshot.bucket_count,
                        server_count,
                        &mut rng,
                    )
                    .unwrap();
                    let answers = queries
                        .iter()
                        .map(|query| index.answer(query).unwrap())
                        .collect::<Vec<_>>();
                    let recovered = dense::combine(&answers).unwrap();
                    let mut expected = vec![0u8; snapshot.row_size];
                    for bucket in buckets {
                        xor_row(&mut expected, snapshot.row(*bucket).unwrap());
                    }
                    assert_eq!(recovered, expected);
                }
            }
        }
    }

    #[test]
    fn persisted_round_trip_preserves_answers_and_exact_size() {
        let rows = test_rows(19, 11, 4);
        let snapshot = SnapshotView::new(&rows, 19, 11);
        let index = SubsetXorIndex::build(snapshot, 6).unwrap();
        let mut encoded = Vec::new();
        index.write_to(&mut encoded).unwrap();
        assert_eq!(encoded.len(), index.persisted_bytes());

        let restored = SubsetXorIndex::read_from(Cursor::new(encoded)).unwrap();
        assert_eq!(restored.bucket_count(), index.bucket_count());
        assert_eq!(restored.row_size(), index.row_size());
        assert_eq!(restored.group_size(), index.group_size());
        let query = vec![0b1010_0110, 0b0000_0101, 0b0000_0011];
        assert_eq!(
            restored.answer(&query).unwrap(),
            index.answer(&query).unwrap()
        );
    }

    #[test]
    fn build_limit_prevents_large_allocation() {
        let rows = test_rows(64, 32, 1);
        let snapshot = SnapshotView::new(&rows, 64, 32);
        let estimate = SubsetXorIndex::estimate(snapshot, 8).unwrap();
        let error = SubsetXorIndex::build_with_limit(snapshot, 8, estimate.index_data_bytes - 1)
            .unwrap_err();
        assert!(error.to_string().contains("above the"));
    }

    #[test]
    fn metrics_skip_implicit_zero_combinations() {
        let rows = test_rows(9, 7, 1);
        let snapshot = SnapshotView::new(&rows, 9, 7);
        let index = SubsetXorIndex::build(snapshot, 4).unwrap();
        let answer = index
            .answer_with_metrics(&[0b0000_0011, 0b0000_0001])
            .unwrap();
        assert_eq!(answer.metrics.logical_row_reads, 2);
        assert_eq!(answer.metrics.logical_row_xors, 2);
        assert_eq!(answer.metrics.logical_data_bytes_read, 14);
        assert_eq!(answer.bytes, dense::answer(snapshot, &[3, 1]).unwrap());
    }

    #[test]
    fn estimates_omit_zero_subset_and_trim_final_group() {
        let rows = test_rows(10, 5, 1);
        let snapshot = SnapshotView::new(&rows, 10, 5);
        let estimate = SubsetXorIndex::estimate(snapshot, 4).unwrap();
        assert_eq!(estimate.group_count, 3);
        assert_eq!(estimate.stored_combination_rows, 15 + 15 + 3);
        assert_eq!(estimate.index_data_bytes, 33 * 5);
        assert_eq!(estimate.persisted_bytes, HEADER_BYTES + 33 * 5);
    }

    #[test]
    fn persistence_rejects_trailing_or_invalid_data() {
        let rows = test_rows(8, 3, 1);
        let snapshot = SnapshotView::new(&rows, 8, 3);
        let index = SubsetXorIndex::build(snapshot, 2).unwrap();
        let mut encoded = Vec::new();
        index.write_to(&mut encoded).unwrap();
        encoded.push(0);
        assert!(SubsetXorIndex::read_from(Cursor::new(encoded)).is_err());
        assert!(SubsetXorIndex::read_from(Cursor::new(vec![0; HEADER_BYTES])).is_err());
    }

    #[test]
    fn persistence_allocation_limit_is_checked_before_reading_rows() {
        let rows = test_rows(16, 7, 1);
        let snapshot = SnapshotView::new(&rows, 16, 7);
        let index = SubsetXorIndex::build(snapshot, 4).unwrap();
        let mut encoded = Vec::new();
        index.write_to(&mut encoded).unwrap();
        let error = SubsetXorIndex::read_from_with_limit(
            Cursor::new(encoded),
            index.index_data_bytes() - 1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("above the"));
    }
}
