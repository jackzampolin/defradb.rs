use anyhow::{bail, Context, Result};

use crate::dense;
use crate::snapshot::SnapshotView;

const FOUR_RUSSIANS_GROUP_BITS: usize = 4;
const FOUR_RUSSIANS_COMBINATIONS: usize = 1 << FOUR_RUSSIANS_GROUP_BITS;

pub(super) fn masked<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
) -> Result<Vec<Vec<u8>>> {
    validate_queries(snapshot, query_shares)?;
    let rows = snapshot.rows();
    let mut answers = vec![vec![0u8; snapshot.row_size]; query_shares.len()];
    for bucket in 0..snapshot.bucket_count {
        let row_start = bucket * snapshot.row_size;
        let row = &rows[row_start..row_start + snapshot.row_size];
        for (query, output) in query_shares.iter().zip(&mut answers) {
            let bit = (query.as_ref()[bucket / 8] >> (bucket % 8)) & 1;
            let mask = 0u8.wrapping_sub(bit);
            for (answer_byte, input) in output.iter_mut().zip(row) {
                *answer_byte ^= *input & mask;
            }
        }
    }
    Ok(answers)
}

pub(super) fn words<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
) -> Result<Vec<Vec<u8>>> {
    validate_queries(snapshot, query_shares)?;
    query_shares
        .iter()
        .map(|query| answer_set_bits(snapshot, query.as_ref(), xor_row_words))
        .collect()
}

pub(super) fn simd<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
) -> Result<Vec<Vec<u8>>> {
    validate_queries(snapshot, query_shares)?;

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: AVX2 support was checked immediately above.
        return Ok(query_shares
            .iter()
            .map(|query| unsafe { answer_set_bits_avx2(snapshot, query.as_ref()) })
            .collect());
    }

    dense::answer_batch(snapshot, query_shares)
}

pub(super) fn four_russians<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
) -> Result<Vec<Vec<u8>>> {
    validate_queries(snapshot, query_shares)?;
    let mut answers = vec![vec![0u8; snapshot.row_size]; query_shares.len()];
    let mut table = vec![0u8; FOUR_RUSSIANS_COMBINATIONS * snapshot.row_size];
    let group_count = snapshot.bucket_count.div_ceil(FOUR_RUSSIANS_GROUP_BITS);

    for group in 0..group_count {
        build_group_table(
            snapshot,
            group * FOUR_RUSSIANS_GROUP_BITS,
            FOUR_RUSSIANS_GROUP_BITS,
            &mut table,
        );
        let bit_offset = group * FOUR_RUSSIANS_GROUP_BITS;
        for (query, output) in query_shares.iter().zip(&mut answers) {
            let selection = read_query_bits(query.as_ref(), bit_offset, FOUR_RUSSIANS_GROUP_BITS);
            if selection != 0 {
                let start = selection * snapshot.row_size;
                xor_row_words(output, &table[start..start + snapshot.row_size]);
            }
        }
    }
    Ok(answers)
}

fn validate_queries<Q: AsRef<[u8]>>(snapshot: SnapshotView<'_>, query_shares: &[Q]) -> Result<()> {
    let expected = dense::query_size(snapshot.bucket_count);
    for query in query_shares {
        if query.as_ref().len() != expected {
            bail!(
                "query share has {} bytes, expected {expected}",
                query.as_ref().len()
            );
        }
    }
    Ok(())
}

fn answer_set_bits<F>(
    snapshot: SnapshotView<'_>,
    query_share: &[u8],
    mut xor_row: F,
) -> Result<Vec<u8>>
where
    F: FnMut(&mut [u8], &[u8]),
{
    let rows = snapshot.rows();
    let mut answer = vec![0u8; snapshot.row_size];
    for (byte_index, query_byte) in query_share.iter().copied().enumerate() {
        let mut selected = query_byte;
        while selected != 0 {
            let bit_index = selected.trailing_zeros() as usize;
            let bucket = byte_index * 8 + bit_index;
            if bucket < snapshot.bucket_count {
                let row_start = bucket * snapshot.row_size;
                xor_row(&mut answer, &rows[row_start..row_start + snapshot.row_size]);
            }
            selected &= selected - 1;
        }
    }
    Ok(answer)
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn answer_set_bits_avx2(snapshot: SnapshotView<'_>, query_share: &[u8]) -> Vec<u8> {
    let rows = snapshot.rows();
    let mut answer = vec![0u8; snapshot.row_size];
    for (byte_index, query_byte) in query_share.iter().copied().enumerate() {
        let mut selected = query_byte;
        while selected != 0 {
            let bit_index = selected.trailing_zeros() as usize;
            let bucket = byte_index * 8 + bit_index;
            if bucket < snapshot.bucket_count {
                let row_start = bucket * snapshot.row_size;
                // SAFETY: the caller checked AVX2 support. Both slices are valid for
                // `row_size` bytes and the helper uses unaligned operations.
                unsafe {
                    xor_row_avx2(&mut answer, &rows[row_start..row_start + snapshot.row_size]);
                }
            }
            selected &= selected - 1;
        }
    }
    answer
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[target_feature(enable = "avx2")]
unsafe fn xor_row_avx2(output: &mut [u8], row: &[u8]) {
    #[cfg(target_arch = "x86")]
    use std::arch::x86::{__m256i, _mm256_loadu_si256, _mm256_storeu_si256, _mm256_xor_si256};
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{__m256i, _mm256_loadu_si256, _mm256_storeu_si256, _mm256_xor_si256};

    const VECTOR_BYTES: usize = 32;
    let vector_bytes = output.len() / VECTOR_BYTES * VECTOR_BYTES;
    let mut offset = 0;
    while offset < vector_bytes {
        // SAFETY: offset plus one vector is inside both slices. The intrinsics use
        // unaligned operations and AVX2 is enabled on this function.
        unsafe {
            let output_vector = _mm256_loadu_si256(output.as_ptr().add(offset).cast::<__m256i>());
            let input_vector = _mm256_loadu_si256(row.as_ptr().add(offset).cast::<__m256i>());
            _mm256_storeu_si256(
                output.as_mut_ptr().add(offset).cast::<__m256i>(),
                _mm256_xor_si256(output_vector, input_vector),
            );
        }
        offset += VECTOR_BYTES;
    }
    xor_row_bytes(&mut output[vector_bytes..], &row[vector_bytes..]);
}

#[inline(always)]
fn xor_row_bytes(output: &mut [u8], row: &[u8]) {
    for (output, input) in output.iter_mut().zip(row) {
        *output ^= *input;
    }
}

#[inline(always)]
fn xor_row_words(output: &mut [u8], row: &[u8]) {
    const WORD_BYTES: usize = std::mem::size_of::<u64>();
    let word_bytes = output.len() / WORD_BYTES * WORD_BYTES;
    let mut offset = 0;
    while offset < word_bytes {
        let output_word = u64::from_ne_bytes(
            output[offset..offset + WORD_BYTES]
                .try_into()
                .expect("fixed word"),
        );
        let input_word = u64::from_ne_bytes(
            row[offset..offset + WORD_BYTES]
                .try_into()
                .expect("fixed word"),
        );
        output[offset..offset + WORD_BYTES]
            .copy_from_slice(&(output_word ^ input_word).to_ne_bytes());
        offset += WORD_BYTES;
    }
    xor_row_bytes(&mut output[word_bytes..], &row[word_bytes..]);
}

fn build_group_table(
    snapshot: SnapshotView<'_>,
    first_bucket: usize,
    group_bits: usize,
    table: &mut [u8],
) {
    let combinations = 1usize << group_bits;
    debug_assert_eq!(table.len(), combinations * snapshot.row_size);
    table[..snapshot.row_size].fill(0);
    for selection in 1..combinations {
        let previous = selection & (selection - 1);
        let selected_bit = selection.trailing_zeros() as usize;
        let previous_start = previous * snapshot.row_size;
        let output_start = selection * snapshot.row_size;
        table.copy_within(
            previous_start..previous_start + snapshot.row_size,
            output_start,
        );
        let bucket = first_bucket + selected_bit;
        if bucket < snapshot.bucket_count {
            let row_start = bucket * snapshot.row_size;
            xor_row_words(
                &mut table[output_start..output_start + snapshot.row_size],
                &snapshot.rows()[row_start..row_start + snapshot.row_size],
            );
        }
    }
}

fn read_query_bits(query: &[u8], bit_offset: usize, bit_count: usize) -> usize {
    debug_assert!((1..=8).contains(&bit_count));
    let byte_index = bit_offset / 8;
    let shift = bit_offset % 8;
    let low = query.get(byte_index).copied().unwrap_or_default() as u16;
    let high = query.get(byte_index + 1).copied().unwrap_or_default() as u16;
    (((low | high << 8) >> shift) as usize) & ((1usize << bit_count) - 1)
}

#[derive(Debug)]
pub(super) struct XorIndex {
    group_bits: usize,
    combinations: usize,
    group_count: usize,
    bucket_count: usize,
    row_size: usize,
    rows: Box<[u8]>,
}

impl XorIndex {
    pub(super) fn build(snapshot: SnapshotView<'_>, group_bits: usize) -> Result<Self> {
        if !(1..=8).contains(&group_bits) {
            bail!("XOR index group bits must be between 1 and 8");
        }
        let combinations = 1usize << group_bits;
        let group_count = snapshot.bucket_count.div_ceil(group_bits);
        let index_bytes = group_count
            .checked_mul(combinations)
            .and_then(|size| size.checked_mul(snapshot.row_size))
            .context("XOR index size overflow")?;
        let mut rows = vec![0u8; index_bytes];
        let table_size = combinations * snapshot.row_size;
        for group in 0..group_count {
            let start = group * table_size;
            build_group_table(
                snapshot,
                group * group_bits,
                group_bits,
                &mut rows[start..start + table_size],
            );
        }
        Ok(Self {
            group_bits,
            combinations,
            group_count,
            bucket_count: snapshot.bucket_count,
            row_size: snapshot.row_size,
            rows: rows.into_boxed_slice(),
        })
    }

    pub(super) fn storage_bytes(&self) -> usize {
        self.rows.len()
    }

    pub(super) fn storage_amplification(&self, snapshot_bytes: usize) -> f64 {
        self.storage_bytes() as f64 / snapshot_bytes as f64
    }

    pub(super) fn answer(&self, query_share: &[u8]) -> Result<Vec<u8>> {
        if query_share.len() != dense::query_size(self.bucket_count) {
            bail!(
                "query share has {} bytes, expected {}",
                query_share.len(),
                dense::query_size(self.bucket_count)
            );
        }
        let mut answer = vec![0u8; self.row_size];
        let table_size = self.combinations * self.row_size;
        for group in 0..self.group_count {
            let selection = read_query_bits(query_share, group * self.group_bits, self.group_bits);
            if selection != 0 {
                let start = group * table_size + selection * self.row_size;
                xor_row_words(&mut answer, &self.rows[start..start + self.row_size]);
            }
        }
        Ok(answer)
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, RngCore, SeedableRng};

    use super::*;
    use crate::snapshot::Snapshot;

    #[test]
    fn candidate_kernels_match_the_selected_kernel() {
        for (bucket_count, row_size) in [(4, 13), (32, 32), (64, 65)] {
            let snapshot = Snapshot::benchmark(bucket_count, row_size, 2).unwrap();
            let mut rng = StdRng::seed_from_u64(bucket_count as u64);
            let queries = (0..32)
                .map(|_| {
                    let mut query = vec![0u8; dense::query_size(bucket_count)];
                    rng.fill_bytes(&mut query);
                    query
                })
                .collect::<Vec<_>>();
            let reference = masked(snapshot.view(), &queries).unwrap();
            assert_eq!(
                dense::answer_batch(snapshot.view(), &queries).unwrap(),
                reference
            );
            assert_eq!(words(snapshot.view(), &queries).unwrap(), reference);
            assert_eq!(simd(snapshot.view(), &queries).unwrap(), reference);
            assert_eq!(four_russians(snapshot.view(), &queries).unwrap(), reference);
            for group_bits in 1..=4 {
                let index = XorIndex::build(snapshot.view(), group_bits).unwrap();
                let answers = queries
                    .iter()
                    .map(|query| index.answer(query).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(answers, reference);
            }
        }
    }
}
