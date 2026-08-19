//! Batched evaluation for replicated Dense XOR PIR.
//!
//! Every input is one ordinary, independently generated query share for this
//! server. Reordering or grouping the local GF(2) matrix multiplication does
//! not change the n-out-of-n privacy proof: combining the answer in batch slot
//! `i` still reconstructs exactly the selector in query slot `i`.

use anyhow::{bail, Context, Result};

use crate::{dense, snapshot::SnapshotView};

/// Server-side evaluation order for a batch of independent Dense query shares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchKernel {
    /// The existing query-major evaluator. This is the compatibility baseline.
    Independent,
    /// Visit one eight-row selector byte, then every query in the batch.
    SharedRowMajor,
    /// Keep a source-table block cache-resident while evaluating every query.
    CacheBlocked { rows_per_block: usize },
    /// Transpose each eight-row selector stripe into query masks before XORing.
    SelectorTransposed,
    /// Build an ephemeral table of every subset of a small source-row group.
    ///
    /// The temporary table is rebuilt during every evaluation; it is not a
    /// persisted index and does not change the snapshot or client query.
    GroupedFourRussians { group_bits: usize },
}

impl BatchKernel {
    fn validate(self) -> Result<()> {
        match self {
            Self::CacheBlocked { rows_per_block: 0 } => {
                bail!("cache-blocked Dense batch rows_per_block must be non-zero")
            }
            Self::GroupedFourRussians { group_bits } if !(2..=8).contains(&group_bits) => {
                bail!("Dense batch Four-Russians group_bits must be between 2 and 8")
            }
            _ => Ok(()),
        }
    }

    fn scratch_bytes(self, query_count: usize, row_size: usize) -> Result<usize> {
        match self {
            Self::SelectorTransposed => query_count
                .div_ceil(64)
                .checked_mul(size_of::<[u64; 8]>())
                .context("Dense batch transpose scratch size overflow"),
            Self::GroupedFourRussians { group_bits } => (1usize << group_bits)
                .checked_mul(row_size)
                .context("Dense batch group table size overflow"),
            _ => Ok(0),
        }
    }
}

/// Logical work counters for one server evaluation.
///
/// These counters deliberately do not claim physical DRAM traffic. In
/// particular, a shared traversal can reuse a source row from cache/registers,
/// but every destination XOR is still counted as an answer-row operation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchMetrics {
    pub query_count: usize,
    pub query_share_bytes: usize,
    pub table_ordering_passes: usize,
    /// Unique uploaded selector material addressed by this evaluation.
    ///
    /// This is not an instruction-load count: grouped kernels may reread an
    /// overlapping one/two-byte window for adjacent source-row groups.
    pub unique_selector_bytes_addressed: usize,
    pub immutable_source_row_operand_reads: usize,
    pub immutable_source_operand_bytes: usize,
    pub scratch_row_copies: usize,
    pub scratch_row_xors: usize,
    pub scratch_write_bytes: usize,
    pub answer_row_xors: usize,
    pub answer_xor_write_bytes: usize,
    pub materialized_answer_bytes: usize,
    pub peak_transient_working_bytes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchEvaluation {
    pub answers: Vec<Vec<u8>>,
    pub metrics: BatchMetrics,
}

/// A bounded, allocation-checked server-side batch evaluator.
///
/// The caller chooses limits appropriate for its admission-control policy.
/// Dense XOR cannot prove that an individual share is random, so production
/// still needs authenticated clients, rate limits, and a maximum queue dwell
/// time. The grouped kernel has query-content-independent answer work and is
/// useful when CPU denial-of-service resistance matters.
#[derive(Clone, Debug)]
pub struct BatchEvaluator {
    max_queries: usize,
    max_transient_working_bytes: usize,
}

impl BatchEvaluator {
    pub fn new(max_queries: usize, max_transient_working_bytes: usize) -> Result<Self> {
        if max_queries == 0 {
            bail!("Dense batch max_queries must be non-zero");
        }
        if max_transient_working_bytes == 0 {
            bail!("Dense batch working-memory limit must be non-zero");
        }
        Ok(Self {
            max_queries,
            max_transient_working_bytes,
        })
    }

    pub fn evaluate<Q: AsRef<[u8]>>(
        &self,
        snapshot: SnapshotView<'_>,
        query_shares: &[Q],
        kernel: BatchKernel,
    ) -> Result<BatchEvaluation> {
        kernel.validate()?;
        if query_shares.len() > self.max_queries {
            bail!(
                "Dense batch contains {} queries, limit is {}",
                query_shares.len(),
                self.max_queries
            );
        }
        let expected_query_bytes = dense::query_size(snapshot.bucket_count);
        for query in query_shares {
            if query.as_ref().len() != expected_query_bytes {
                bail!(
                    "query share has {} bytes, expected {expected_query_bytes}",
                    query.as_ref().len()
                );
            }
        }

        let materialized_answer_bytes = query_shares
            .len()
            .checked_mul(snapshot.row_size)
            .context("Dense batch answer allocation size overflow")?;
        let scratch_bytes = kernel.scratch_bytes(query_shares.len(), snapshot.row_size)?;
        let working_bytes = materialized_answer_bytes
            .checked_add(scratch_bytes)
            .context("Dense batch working-memory size overflow")?;
        if working_bytes > self.max_transient_working_bytes {
            bail!(
                "Dense batch needs {working_bytes} transient bytes, limit is {}",
                self.max_transient_working_bytes
            );
        }

        if query_shares.is_empty() {
            return Ok(BatchEvaluation {
                answers: Vec::new(),
                metrics: BatchMetrics::default(),
            });
        }

        let selected_rows = count_selected_rows(query_shares, snapshot.bucket_count);
        let (answers, mut metrics) = match kernel {
            BatchKernel::Independent => {
                let answers = dense::answer_batch(snapshot, query_shares)?;
                let metrics = direct_metrics(
                    snapshot,
                    query_shares.len(),
                    expected_query_bytes,
                    selected_rows,
                    query_shares.len(),
                    working_bytes,
                )?;
                (answers, metrics)
            }
            BatchKernel::SharedRowMajor => {
                let answers = shared_row_major(snapshot, query_shares)?;
                let metrics = direct_metrics(
                    snapshot,
                    query_shares.len(),
                    expected_query_bytes,
                    selected_rows,
                    1,
                    working_bytes,
                )?;
                (answers, metrics)
            }
            BatchKernel::CacheBlocked { rows_per_block } => {
                let answers = cache_blocked(snapshot, query_shares, rows_per_block)?;
                let metrics = direct_metrics(
                    snapshot,
                    query_shares.len(),
                    expected_query_bytes,
                    selected_rows,
                    1,
                    working_bytes,
                )?;
                (answers, metrics)
            }
            BatchKernel::SelectorTransposed => {
                let answers = selector_transposed(snapshot, query_shares)?;
                let metrics = direct_metrics(
                    snapshot,
                    query_shares.len(),
                    expected_query_bytes,
                    selected_rows,
                    1,
                    working_bytes,
                )?;
                (answers, metrics)
            }
            BatchKernel::GroupedFourRussians { group_bits } => grouped_four_russians(
                snapshot,
                query_shares,
                group_bits,
                expected_query_bytes,
                working_bytes,
            )?,
        };
        metrics.materialized_answer_bytes = materialized_answer_bytes;
        Ok(BatchEvaluation { answers, metrics })
    }
}

fn direct_metrics(
    snapshot: SnapshotView<'_>,
    query_count: usize,
    query_share_bytes: usize,
    selected_rows: usize,
    table_ordering_passes: usize,
    working_bytes: usize,
) -> Result<BatchMetrics> {
    let selected_bytes = selected_rows
        .checked_mul(snapshot.row_size)
        .context("Dense batch selected byte count overflow")?;
    Ok(BatchMetrics {
        query_count,
        query_share_bytes,
        table_ordering_passes,
        unique_selector_bytes_addressed: query_count
            .checked_mul(query_share_bytes)
            .context("Dense batch selector byte count overflow")?,
        immutable_source_row_operand_reads: selected_rows,
        immutable_source_operand_bytes: selected_bytes,
        scratch_row_copies: 0,
        scratch_row_xors: 0,
        scratch_write_bytes: 0,
        answer_row_xors: selected_rows,
        answer_xor_write_bytes: selected_bytes,
        materialized_answer_bytes: 0,
        peak_transient_working_bytes: working_bytes,
    })
}

fn allocate_answers(query_count: usize, row_size: usize) -> Result<Vec<Vec<u8>>> {
    let mut answers = Vec::new();
    answers
        .try_reserve_exact(query_count)
        .context("failed to reserve Dense batch answer list")?;
    for _ in 0..query_count {
        answers.push(allocate_zeroed(row_size, "Dense batch answer")?);
    }
    Ok(answers)
}

fn allocate_zeroed(size: usize, label: &'static str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .with_context(|| format!("failed to reserve {label}"))?;
    bytes.resize(size, 0);
    Ok(bytes)
}

fn shared_row_major<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
) -> Result<Vec<Vec<u8>>> {
    let mut answers = allocate_answers(query_shares.len(), snapshot.row_size)?;
    for byte_index in 0..dense::query_size(snapshot.bucket_count) {
        let first_row = byte_index * 8;
        for (query, answer) in query_shares.iter().zip(&mut answers) {
            let mut selected = query.as_ref()[byte_index];
            while selected != 0 {
                let bit = selected.trailing_zeros() as usize;
                let row_index = first_row + bit;
                if row_index < snapshot.bucket_count {
                    xor_row(answer, snapshot.row(row_index)?);
                }
                selected &= selected - 1;
            }
        }
    }
    Ok(answers)
}

fn cache_blocked<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
    rows_per_block: usize,
) -> Result<Vec<Vec<u8>>> {
    let mut answers = allocate_answers(query_shares.len(), snapshot.row_size)?;
    let query_bytes = dense::query_size(snapshot.bucket_count);
    let bytes_per_block = rows_per_block.div_ceil(8).max(1);
    for first_byte in (0..query_bytes).step_by(bytes_per_block) {
        let past_last_byte = first_byte.saturating_add(bytes_per_block).min(query_bytes);
        for (query, answer) in query_shares.iter().zip(&mut answers) {
            for byte_index in first_byte..past_last_byte {
                let mut selected = query.as_ref()[byte_index];
                while selected != 0 {
                    let bit = selected.trailing_zeros() as usize;
                    let row_index = byte_index * 8 + bit;
                    if row_index < snapshot.bucket_count {
                        xor_row(answer, snapshot.row(row_index)?);
                    }
                    selected &= selected - 1;
                }
            }
        }
    }
    Ok(answers)
}

fn selector_transposed<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
) -> Result<Vec<Vec<u8>>> {
    let mut answers = allocate_answers(query_shares.len(), snapshot.row_size)?;
    let mask_words = query_shares.len().div_ceil(64);
    let mask_len = mask_words
        .checked_mul(8)
        .context("Dense batch transpose mask size overflow")?;
    let mut masks = allocate_zeroed(
        mask_len
            .checked_mul(size_of::<u64>())
            .context("Dense batch transpose mask byte size overflow")?,
        "Dense batch transpose masks",
    )?;
    // The byte allocation above makes failure fallible. Reuse it as aligned-
    // independent storage via explicit byte encoding to avoid assuming allocator
    // alignment for a `u64` cast.
    for byte_index in 0..dense::query_size(snapshot.bucket_count) {
        masks.fill(0);
        for (query_index, query) in query_shares.iter().enumerate() {
            let mut selected = query.as_ref()[byte_index];
            while selected != 0 {
                let bit = selected.trailing_zeros() as usize;
                set_mask_bit(&mut masks, mask_words, bit, query_index);
                selected &= selected - 1;
            }
        }
        for bit in 0..8 {
            let row_index = byte_index * 8 + bit;
            if row_index >= snapshot.bucket_count {
                break;
            }
            let row = snapshot.row(row_index)?;
            for word_index in 0..mask_words {
                let mut selected_queries = read_mask_word(&masks, mask_words, bit, word_index);
                while selected_queries != 0 {
                    let query_bit = selected_queries.trailing_zeros() as usize;
                    let query_index = word_index * 64 + query_bit;
                    xor_row(&mut answers[query_index], row);
                    selected_queries &= selected_queries - 1;
                }
            }
        }
    }
    Ok(answers)
}

fn set_mask_bit(masks: &mut [u8], words_per_row: usize, row_bit: usize, query_index: usize) {
    let word_index = row_bit * words_per_row + query_index / 64;
    let offset = word_index * size_of::<u64>();
    let mut word = u64::from_ne_bytes(
        masks[offset..offset + size_of::<u64>()]
            .try_into()
            .expect("fixed mask word"),
    );
    word |= 1u64 << (query_index % 64);
    masks[offset..offset + size_of::<u64>()].copy_from_slice(&word.to_ne_bytes());
}

fn read_mask_word(masks: &[u8], words_per_row: usize, row_bit: usize, word_index: usize) -> u64 {
    let offset = (row_bit * words_per_row + word_index) * size_of::<u64>();
    u64::from_ne_bytes(
        masks[offset..offset + size_of::<u64>()]
            .try_into()
            .expect("fixed mask word"),
    )
}

fn grouped_four_russians<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
    group_bits: usize,
    query_share_bytes: usize,
    working_bytes: usize,
) -> Result<(Vec<Vec<u8>>, BatchMetrics)> {
    let mut answers = allocate_answers(query_shares.len(), snapshot.row_size)?;
    let combinations = 1usize << group_bits;
    let table_bytes = combinations
        .checked_mul(snapshot.row_size)
        .context("Dense batch group table size overflow")?;
    let mut table = allocate_zeroed(table_bytes, "Dense batch group table")?;
    let group_count = snapshot.bucket_count.div_ceil(group_bits);
    let mut source_reads = 0usize;
    let mut scratch_copies = 0usize;
    let mut scratch_xors = 0usize;

    for group in 0..group_count {
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
            scratch_copies += 1;
            let row_index = group * group_bits + selected_bit;
            if row_index < snapshot.bucket_count {
                xor_row(
                    &mut table[output_start..output_start + snapshot.row_size],
                    snapshot.row(row_index)?,
                );
                source_reads += 1;
                scratch_xors += 1;
            }
        }
        for (query, answer) in query_shares.iter().zip(&mut answers) {
            let selection = read_query_bits(query.as_ref(), group * group_bits, group_bits);
            let start = selection * snapshot.row_size;
            // Include selection zero. This makes answer work independent of a
            // potentially malicious client's share Hamming weight.
            xor_row(answer, &table[start..start + snapshot.row_size]);
        }
    }

    let answer_row_xors = group_count
        .checked_mul(query_shares.len())
        .context("Dense batch grouped answer row count overflow")?;
    let answer_bytes = answer_row_xors
        .checked_mul(snapshot.row_size)
        .context("Dense batch grouped answer byte count overflow")?;
    let source_bytes = source_reads
        .checked_mul(snapshot.row_size)
        .context("Dense batch grouped source byte count overflow")?;
    let scratch_writes = scratch_copies
        .checked_add(scratch_xors)
        .and_then(|rows| rows.checked_add(group_count))
        .and_then(|rows| rows.checked_mul(snapshot.row_size))
        .context("Dense batch grouped scratch byte count overflow")?;
    Ok((
        answers,
        BatchMetrics {
            query_count: query_shares.len(),
            query_share_bytes,
            table_ordering_passes: 1,
            unique_selector_bytes_addressed: query_shares
                .len()
                .checked_mul(query_share_bytes)
                .context("Dense batch grouped selector byte count overflow")?,
            immutable_source_row_operand_reads: source_reads,
            immutable_source_operand_bytes: source_bytes,
            scratch_row_copies: scratch_copies,
            scratch_row_xors: scratch_xors,
            scratch_write_bytes: scratch_writes,
            answer_row_xors,
            answer_xor_write_bytes: answer_bytes,
            materialized_answer_bytes: 0,
            peak_transient_working_bytes: working_bytes,
        },
    ))
}

fn read_query_bits(query: &[u8], bit_offset: usize, bit_count: usize) -> usize {
    debug_assert!((1..=8).contains(&bit_count));
    let byte_index = bit_offset / 8;
    let shift = bit_offset % 8;
    let low = query.get(byte_index).copied().unwrap_or_default() as u16;
    let high = query.get(byte_index + 1).copied().unwrap_or_default() as u16;
    (((low | high << 8) >> shift) as usize) & ((1usize << bit_count) - 1)
}

fn count_selected_rows<Q: AsRef<[u8]>>(queries: &[Q], bucket_count: usize) -> usize {
    let full_bytes = bucket_count / 8;
    let remaining_bits = bucket_count % 8;
    queries
        .iter()
        .map(|query| {
            let query = query.as_ref();
            let mut selected = query[..full_bytes]
                .iter()
                .map(|byte| byte.count_ones() as usize)
                .sum::<usize>();
            if remaining_bits != 0 {
                let mask = (1u8 << remaining_bits) - 1;
                selected += (query[full_bytes] & mask).count_ones() as usize;
            }
            selected
        })
        .sum()
}

#[inline(always)]
fn xor_row(output: &mut [u8], row: &[u8]) {
    debug_assert_eq!(output.len(), row.len());
    const WORD_BYTES: usize = size_of::<u64>();
    let word_bytes = output.len() / WORD_BYTES * WORD_BYTES;
    let mut offset = 0;
    while offset < word_bytes {
        // SAFETY: `offset + WORD_BYTES` is within both slices. Unaligned reads
        // and writes are used explicitly, and the regions do not overlap.
        unsafe {
            let output_word = output.as_ptr().add(offset).cast::<u64>().read_unaligned();
            let row_word = row.as_ptr().add(offset).cast::<u64>().read_unaligned();
            output
                .as_mut_ptr()
                .add(offset)
                .cast::<u64>()
                .write_unaligned(output_word ^ row_word);
        }
        offset += WORD_BYTES;
    }
    for (output, row) in output[word_bytes..].iter_mut().zip(&row[word_bytes..]) {
        *output ^= *row;
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, RngCore, SeedableRng};

    use super::*;
    use crate::snapshot::Snapshot;

    const KERNELS: [BatchKernel; 6] = [
        BatchKernel::Independent,
        BatchKernel::SharedRowMajor,
        BatchKernel::CacheBlocked { rows_per_block: 17 },
        BatchKernel::SelectorTransposed,
        BatchKernel::GroupedFourRussians { group_bits: 3 },
        BatchKernel::GroupedFourRussians { group_bits: 6 },
    ];

    fn evaluator() -> BatchEvaluator {
        BatchEvaluator::new(256, 16 * 1024 * 1024).unwrap()
    }

    #[test]
    fn kernels_match_current_dense_for_arbitrary_rows_and_batches() {
        for row_size in [1, 7, 8, 13, 31, 32, 33, 65, 96, 127] {
            let rows = (0..64 * row_size)
                .map(|index| (index as u8).wrapping_mul(31).wrapping_add(row_size as u8))
                .collect::<Vec<_>>();
            let snapshot = SnapshotView::new(&rows, 64, row_size);
            for batch_size in [0, 1, 2, 7, 16, 65] {
                let mut rng = StdRng::seed_from_u64((row_size * 1000 + batch_size) as u64);
                let queries = (0..batch_size)
                    .map(|_| {
                        let mut query = vec![0u8; dense::query_size(64)];
                        rng.fill_bytes(&mut query);
                        query
                    })
                    .collect::<Vec<_>>();
                let expected = dense::answer_batch(snapshot, &queries).unwrap();
                for kernel in KERNELS {
                    assert_eq!(
                        evaluator()
                            .evaluate(snapshot, &queries, kernel)
                            .unwrap()
                            .answers,
                        expected,
                        "row_size={row_size}, batch={batch_size}, kernel={kernel:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn two_through_six_servers_recover_present_zero_and_multi_hot_queries() {
        let snapshot = Snapshot::benchmark(128, 37, 91).unwrap();
        for server_count in 2..=6 {
            let mut rng = StdRng::seed_from_u64(server_count as u64);
            let logical_selectors = [vec![9], vec![], vec![2, 17, 91]];
            let mut per_server = vec![Vec::new(); server_count];
            for selector in &logical_selectors {
                for (server, share) in per_server.iter_mut().zip(
                    dense::query_shares_for_buckets(selector, 128, server_count, &mut rng).unwrap(),
                ) {
                    server.push(share);
                }
            }
            for kernel in KERNELS {
                let server_answers = per_server
                    .iter()
                    .map(|queries| {
                        evaluator()
                            .evaluate(snapshot.view(), queries, kernel)
                            .unwrap()
                            .answers
                    })
                    .collect::<Vec<_>>();
                for (query_index, selector) in logical_selectors.iter().enumerate() {
                    let shares = server_answers
                        .iter()
                        .map(|server| server[query_index].as_slice())
                        .collect::<Vec<_>>();
                    let recovered = dense::combine(&shares).unwrap();
                    let mut expected = vec![0u8; snapshot.manifest.row_size];
                    for &row in selector {
                        xor_row(&mut expected, snapshot.row(row).unwrap());
                    }
                    assert_eq!(
                        recovered, expected,
                        "servers={server_count}, kernel={kernel:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn padding_bits_are_ignored() {
        let snapshot = Snapshot::benchmark(32, 11, 4).unwrap();
        let short_view = SnapshotView::new(snapshot.rows(), 29, 11);
        let mut query = vec![0u8; dense::query_size(29)];
        query[3] = 0b1110_0000;
        for kernel in KERNELS {
            assert_eq!(
                evaluator()
                    .evaluate(short_view, &[query.clone()], kernel)
                    .unwrap()
                    .answers[0],
                vec![0u8; 11]
            );
        }
    }

    #[test]
    fn admission_limits_and_shapes_are_enforced() {
        let snapshot = Snapshot::benchmark(64, 96, 5).unwrap();
        let tiny = BatchEvaluator::new(1, 96).unwrap();
        assert!(tiny
            .evaluate(
                snapshot.view(),
                &[vec![0u8; 8], vec![0u8; 8]],
                BatchKernel::Independent
            )
            .is_err());
        assert!(tiny
            .evaluate(snapshot.view(), &[vec![0u8; 7]], BatchKernel::Independent)
            .is_err());
        assert!(tiny
            .evaluate(
                snapshot.view(),
                &[vec![0u8; 8]],
                BatchKernel::GroupedFourRussians { group_bits: 6 }
            )
            .is_err());
        assert!(BatchEvaluator::new(0, 1).is_err());
        assert!(BatchEvaluator::new(1, 0).is_err());
    }

    #[test]
    fn grouped_metrics_bound_answer_work_independently_of_share_weight() {
        let snapshot = Snapshot::benchmark(64, 13, 7).unwrap();
        let evaluator = evaluator();
        let zero = evaluator
            .evaluate(
                snapshot.view(),
                &[vec![0u8; 8]],
                BatchKernel::GroupedFourRussians { group_bits: 4 },
            )
            .unwrap();
        let ones = evaluator
            .evaluate(
                snapshot.view(),
                &[vec![u8::MAX; 8]],
                BatchKernel::GroupedFourRussians { group_bits: 4 },
            )
            .unwrap();
        assert_eq!(zero.metrics.answer_row_xors, ones.metrics.answer_row_xors);
        assert_eq!(
            zero.metrics.immutable_source_row_operand_reads,
            ones.metrics.immutable_source_row_operand_reads
        );
    }
}
