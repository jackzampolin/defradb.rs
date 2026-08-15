use anyhow::{bail, Context, Result};
use rand::{CryptoRng, RngCore};
use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};

use crate::snapshot::SnapshotView;

pub fn query_size(bucket_count: usize) -> usize {
    bucket_count.div_ceil(8)
}

pub fn query_shares<R: RngCore + CryptoRng>(
    bucket_index: usize,
    bucket_count: usize,
    server_count: usize,
    rng: &mut R,
) -> Result<Vec<Vec<u8>>> {
    if bucket_index >= bucket_count {
        bail!("bucket index is outside the snapshot");
    }
    if server_count < 2 {
        bail!("Dense XOR PIR requires at least two servers");
    }

    let share_size = query_size(bucket_count);
    let mut final_share = vec![0u8; share_size];
    final_share[bucket_index / 8] = 1 << (bucket_index % 8);
    let mut shares = Vec::with_capacity(server_count);
    for _ in 1..server_count {
        let mut share = vec![0u8; share_size];
        rng.fill_bytes(&mut share);
        xor_in_place(&mut final_share, &share);
        shares.push(share);
    }
    shares.push(final_share);
    Ok(shares)
}

pub fn answer(snapshot: SnapshotView<'_>, query_share: &[u8]) -> Result<Vec<u8>> {
    answer_batch(snapshot, &[query_share])
        .map(|mut answers| answers.pop().expect("one query share produces one answer"))
}

pub fn answer_batch<Q: AsRef<[u8]>>(
    snapshot: SnapshotView<'_>,
    query_shares: &[Q],
) -> Result<Vec<Vec<u8>>> {
    validate_queries(snapshot, query_shares)?;
    query_shares
        .iter()
        .map(|query| answer_set_bits_unchecked(snapshot, query.as_ref()))
        .collect()
}

fn validate_queries<Q: AsRef<[u8]>>(snapshot: SnapshotView<'_>, query_shares: &[Q]) -> Result<()> {
    let expected = query_size(snapshot.bucket_count);
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

fn answer_set_bits_unchecked(snapshot: SnapshotView<'_>, query_share: &[u8]) -> Result<Vec<u8>> {
    let rows = snapshot.rows();
    let mut answer = vec![0u8; snapshot.row_size];
    for (byte_index, query_byte) in query_share.iter().copied().enumerate() {
        let mut selected = query_byte;
        while selected != 0 {
            let bit_index = selected.trailing_zeros() as usize;
            let bucket = byte_index * 8 + bit_index;
            if bucket < snapshot.bucket_count {
                let row_start = bucket * snapshot.row_size;
                xor_row_bytes(&mut answer, &rows[row_start..row_start + snapshot.row_size]);
            }
            selected &= selected - 1;
        }
    }
    Ok(answer)
}

#[inline(always)]
fn xor_row_bytes(output: &mut [u8], row: &[u8]) {
    for (output, input) in output.iter_mut().zip(row) {
        *output ^= *input;
    }
}

/// A fixed-size worker pool for latency-sensitive PIR scans.
///
/// Keeping the pool alive avoids creating OS threads for every request. The
/// caller controls the number of workers so one PIR request cannot silently
/// monopolize every server core.
pub(crate) struct ParallelEvaluator {
    pool: ThreadPool,
    thread_count: usize,
}

impl ParallelEvaluator {
    pub(crate) fn new(thread_count: usize) -> Result<Self> {
        if thread_count == 0 {
            bail!("parallel evaluator needs at least one worker");
        }
        let pool = ThreadPoolBuilder::new()
            .num_threads(thread_count)
            .thread_name(|index| format!("pir-dense-{index}"))
            .build()
            .context("failed to create Dense XOR worker pool")?;
        Ok(Self { pool, thread_count })
    }

    pub(crate) fn answer(&self, snapshot: SnapshotView<'_>, query_share: &[u8]) -> Result<Vec<u8>> {
        validate_queries(snapshot, &[query_share])?;
        let chunks = self.thread_count * 4;
        let chunk_bytes = query_share.len().div_ceil(chunks).max(1);
        Ok(self.pool.install(|| {
            query_share
                .par_chunks(chunk_bytes)
                .enumerate()
                .map(|(chunk_index, query_chunk)| {
                    answer_query_byte_range(snapshot, query_chunk, chunk_index * chunk_bytes)
                })
                .reduce(
                    || vec![0u8; snapshot.row_size],
                    |mut left, right| {
                        xor_row_bytes(&mut left, &right);
                        left
                    },
                )
        }))
    }

    pub(crate) fn answer_batch<Q: AsRef<[u8]> + Sync>(
        &self,
        snapshot: SnapshotView<'_>,
        query_shares: &[Q],
    ) -> Result<Vec<Vec<u8>>> {
        validate_queries(snapshot, query_shares)?;
        if let [query] = query_shares {
            return self
                .answer(snapshot, query.as_ref())
                .map(|answer| vec![answer]);
        }
        self.pool.install(|| {
            query_shares
                .par_iter()
                .map(|query| answer_set_bits_unchecked(snapshot, query.as_ref()))
                .collect()
        })
    }
}

fn answer_query_byte_range(
    snapshot: SnapshotView<'_>,
    query_bytes: &[u8],
    first_query_byte: usize,
) -> Vec<u8> {
    let rows = snapshot.rows();
    let mut answer = vec![0u8; snapshot.row_size];
    for (local_byte_index, query_byte) in query_bytes.iter().copied().enumerate() {
        let mut selected = query_byte;
        while selected != 0 {
            let bit_index = selected.trailing_zeros() as usize;
            let bucket = (first_query_byte + local_byte_index) * 8 + bit_index;
            if bucket < snapshot.bucket_count {
                let row_start = bucket * snapshot.row_size;
                xor_row_bytes(&mut answer, &rows[row_start..row_start + snapshot.row_size]);
            }
            selected &= selected - 1;
        }
    }
    answer
}

pub fn combine<S: AsRef<[u8]>>(shares: &[S]) -> Result<Vec<u8>> {
    let (first, remaining) = shares.split_first().context("no answer shares supplied")?;
    let mut result = first.as_ref().to_vec();
    for share in remaining {
        if share.as_ref().len() != result.len() {
            bail!("answer share lengths differ");
        }
        xor_in_place(&mut result, share.as_ref());
    }
    Ok(result)
}

fn xor_in_place(target: &mut [u8], share: &[u8]) {
    for (target, share) in target.iter_mut().zip(share) {
        *target ^= share;
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;
    use crate::snapshot::Snapshot;

    #[test]
    fn any_server_count_recovers_the_requested_row() {
        let snapshot = Snapshot::benchmark(32, 32, 1).unwrap();
        let bucket = 7;
        for server_count in 2..=6 {
            let queries = query_shares(
                bucket,
                snapshot.manifest.bucket_count,
                server_count,
                &mut StdRng::seed_from_u64(7),
            )
            .unwrap();
            let answers = queries
                .iter()
                .map(|query| answer(snapshot.view(), query).unwrap())
                .collect::<Vec<_>>();
            assert_eq!(combine(&answers).unwrap(), snapshot.row(bucket).unwrap());
        }
    }

    #[test]
    fn repeated_queries_use_fresh_shares() {
        let mut rng = StdRng::seed_from_u64(9);
        let first = query_shares(3, 64, 3, &mut rng).unwrap();
        let second = query_shares(3, 64, 3, &mut rng).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn shares_xor_to_the_requested_unit_vector() {
        let mut rng = StdRng::seed_from_u64(11);
        for server_count in 2..=6 {
            let shares = query_shares(19, 64, server_count, &mut rng).unwrap();
            let combined = combine(&shares).unwrap();
            let mut expected = vec![0u8; query_size(64)];
            expected[19 / 8] = 1 << (19 % 8);
            assert_eq!(combined, expected);
        }
        assert!(query_shares(19, 64, 1, &mut rng).is_err());
    }

    #[test]
    fn batch_answers_match_individual_answers() {
        let snapshot = Snapshot::benchmark(64, 32, 1).unwrap();
        let mut rng = StdRng::seed_from_u64(10);
        let queries = query_shares(7, 64, 3, &mut rng).unwrap();
        let individual = queries
            .iter()
            .map(|query| answer(snapshot.view(), query).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(answer_batch(snapshot.view(), &queries).unwrap(), individual);
    }

    #[test]
    fn parallel_evaluator_matches_serial_answers() {
        for (bucket_count, row_size) in [(4, 13), (32, 32), (64, 65)] {
            let snapshot = Snapshot::benchmark(bucket_count, row_size, 2).unwrap();
            let mut rng = StdRng::seed_from_u64(bucket_count as u64);
            let queries = (0..32)
                .map(|bucket| {
                    query_shares(bucket % bucket_count, bucket_count, 2, &mut rng)
                        .unwrap()
                        .remove(0)
                })
                .collect::<Vec<_>>();
            let reference = answer_batch(snapshot.view(), &queries).unwrap();
            let parallel = ParallelEvaluator::new(2).unwrap();
            for (query, expected) in queries.iter().zip(&reference) {
                assert_eq!(parallel.answer(snapshot.view(), query).unwrap(), *expected);
            }
            assert_eq!(
                parallel.answer_batch(snapshot.view(), &queries).unwrap(),
                reference
            );
        }
    }
}
