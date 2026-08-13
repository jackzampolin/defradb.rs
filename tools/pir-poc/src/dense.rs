use anyhow::{bail, Result};
use rand::{CryptoRng, RngCore};

use crate::snapshot::{bucket_for_key, Snapshot};

pub fn query_size(bucket_count: usize) -> usize {
    bucket_count.div_ceil(8)
}

pub fn query_shares<R: RngCore + CryptoRng>(
    bucket_index: usize,
    bucket_count: usize,
    rng: &mut R,
) -> Result<(Vec<u8>, Vec<u8>)> {
    if bucket_index >= bucket_count {
        bail!("bucket index is outside the snapshot");
    }
    let mut left = vec![0u8; query_size(bucket_count)];
    rng.fill_bytes(&mut left);
    let mut right = left.clone();
    right[bucket_index / 8] ^= 1 << (bucket_index % 8);
    Ok((left, right))
}

pub fn answer(snapshot: &Snapshot, query_share: &[u8]) -> Result<Vec<u8>> {
    let expected = query_size(snapshot.manifest.bucket_count);
    if query_share.len() != expected {
        bail!(
            "query share has {} bytes, expected {expected}",
            query_share.len()
        );
    }

    let mut answer = vec![0u8; snapshot.manifest.row_size];
    for bucket in 0..snapshot.manifest.bucket_count {
        let bit = (query_share[bucket / 8] >> (bucket % 8)) & 1;
        let mask = 0u8.wrapping_sub(bit);
        let row = snapshot.row(bucket)?;
        for (output, input) in answer.iter_mut().zip(row) {
            *output ^= *input & mask;
        }
    }
    Ok(answer)
}

pub fn combine(left: &[u8], right: &[u8]) -> Result<Vec<u8>> {
    if left.len() != right.len() {
        bail!("answer share lengths differ");
    }
    Ok(left.iter().zip(right).map(|(a, b)| a ^ b).collect())
}

pub fn private_lookup<R: RngCore + CryptoRng>(
    snapshot: &Snapshot,
    key: &[u8],
    rng: &mut R,
) -> Result<Vec<Vec<u8>>> {
    let bucket = bucket_for_key(key, snapshot.manifest.bucket_count);
    let (left_query, right_query) = query_shares(bucket, snapshot.manifest.bucket_count, rng)?;
    let left = answer(snapshot, &left_query)?;
    let right = answer(snapshot, &right_query)?;
    snapshot.values_from_row(&combine(&left, &right)?, key)
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;
    use crate::snapshot::{Record, SnapshotConfig};

    #[test]
    fn two_shares_recover_only_the_requested_row() {
        let snapshot = Snapshot::build(
            vec![Record::new("alpha", "secret"), Record::new("beta", "other")],
            SnapshotConfig {
                bucket_count: 32,
                bucket_capacity: 4,
                max_key_bytes: 16,
                max_value_bytes: 32,
                source: "Test".into(),
                source_cutoff: "1".into(),
            },
        )
        .unwrap();
        let values = private_lookup(&snapshot, b"alpha", &mut StdRng::seed_from_u64(7)).unwrap();
        assert_eq!(values, vec![b"secret".to_vec()]);
    }

    #[test]
    fn repeated_queries_use_fresh_shares() {
        let mut rng = StdRng::seed_from_u64(9);
        let first = query_shares(3, 64, &mut rng).unwrap();
        let second = query_shares(3, 64, &mut rng).unwrap();
        assert_ne!(first.0, second.0);
        assert_ne!(first.1, second.1);
    }
}
