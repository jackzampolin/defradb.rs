use anyhow::{bail, Context, Result};
use rand::{seq::SliceRandom, CryptoRng, Rng, RngCore};

use crate::snapshot::SnapshotView;

pub const SERVER_COUNT: usize = 2;

/// The database indices sent to one SinglePass server.
///
/// Position `i` contains a local index into partition `i`. The wire format in
/// this POC uses one little-endian `u32` per index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerQuery {
    indices: Vec<u32>,
}

impl ServerQuery {
    pub fn indices(&self) -> &[u32] {
        &self.indices
    }

    pub fn wire_bytes(&self) -> usize {
        self.indices.len() * size_of::<u32>()
    }
}

/// A query that has changed the client permutations but has not yet updated
/// the parity hints with the two server answers.
///
/// It must be completed exactly once. If either request may have reached a
/// server and completion becomes impossible, production code must discard or
/// recover the state instead of rolling it back and reusing it.
#[derive(Debug)]
pub struct PreparedQuery {
    id: u64,
    target_partition: usize,
    hint_index: usize,
    refresh_hint_indices: Vec<usize>,
    server_queries: [ServerQuery; SERVER_COUNT],
}

impl PreparedQuery {
    pub fn server_queries(&self) -> &[ServerQuery; SERVER_COUNT] {
        &self.server_queries
    }
}

#[derive(Debug)]
struct Permutation {
    forward: Vec<u32>,
    inverse: Vec<u32>,
}

impl Permutation {
    fn random<R: RngCore + CryptoRng>(length: usize, rng: &mut R) -> Result<Self> {
        let length_u32 = u32::try_from(length).context("SinglePass partition is too large")?;
        let mut forward = (0..length_u32).collect::<Vec<_>>();
        forward.shuffle(rng);
        let mut inverse = vec![0u32; length];
        for (position, value) in forward.iter().copied().enumerate() {
            inverse[value as usize] = position as u32;
        }
        Ok(Self { forward, inverse })
    }

    fn value(&self, position: usize) -> usize {
        self.forward[position] as usize
    }

    fn position(&self, value: usize) -> usize {
        self.inverse[value] as usize
    }

    fn swap_positions(&mut self, left: usize, right: usize) {
        if left == right {
            return;
        }
        let left_value = self.forward[left];
        let right_value = self.forward[right];
        self.forward.swap(left, right);
        self.inverse[left_value as usize] = right as u32;
        self.inverse[right_value as usize] = left as u32;
    }

    fn payload_bytes(&self) -> usize {
        (self.forward.len() + self.inverse.len()) * size_of::<u32>()
    }
}

/// Persistent client preprocessing state for the two-server SinglePass PIR.
///
/// The setup phase makes one logical pass over the database to construct
/// parity hints. Online queries read only `partition_count` rows per server.
#[derive(Debug)]
pub struct ClientState {
    bucket_count: usize,
    row_size: usize,
    partition_count: usize,
    partition_len: usize,
    permutations: Vec<Permutation>,
    hints: Vec<u8>,
    next_query_id: u64,
    in_flight: Option<u64>,
}

impl ClientState {
    pub fn setup<R: RngCore + CryptoRng>(
        snapshot: SnapshotView<'_>,
        partition_count: usize,
        rng: &mut R,
    ) -> Result<Self> {
        if partition_count < 2 {
            bail!("SinglePass needs at least two database partitions");
        }
        if snapshot.bucket_count == 0 || snapshot.row_size == 0 {
            bail!("SinglePass database dimensions must be non-zero");
        }

        let partition_len = snapshot.bucket_count.div_ceil(partition_count);
        let padded_bucket_count = partition_len
            .checked_mul(partition_count)
            .context("SinglePass padded database size overflow")?;
        if padded_bucket_count > u32::MAX as usize {
            bail!("SinglePass POC supports at most u32::MAX padded rows");
        }

        let permutations = (0..partition_count)
            .map(|_| Permutation::random(partition_len, rng))
            .collect::<Result<Vec<_>>>()?;
        let hint_bytes = partition_len
            .checked_mul(snapshot.row_size)
            .context("SinglePass hint size overflow")?;
        let mut hints = vec![0u8; hint_bytes];

        // h[j] is the XOR of one permuted row from every partition. Each
        // database row contributes to exactly one hint, so setup is one pass.
        for hint_index in 0..partition_len {
            let hint_start = hint_index * snapshot.row_size;
            let hint = &mut hints[hint_start..hint_start + snapshot.row_size];
            for (partition, permutation) in permutations.iter().enumerate() {
                let local_index = permutation.value(hint_index);
                let global_index = partition * partition_len + local_index;
                if global_index < snapshot.bucket_count {
                    xor_in_place(hint, row_unchecked(snapshot, global_index));
                }
            }
        }

        Ok(Self {
            bucket_count: snapshot.bucket_count,
            row_size: snapshot.row_size,
            partition_count,
            partition_len,
            permutations,
            hints,
            next_query_id: 0,
            in_flight: None,
        })
    }

    pub fn partition_count(&self) -> usize {
        self.partition_count
    }

    pub fn partition_len(&self) -> usize {
        self.partition_len
    }

    /// Persistent payload bytes, excluding small `Vec` and struct headers.
    pub fn payload_bytes(&self) -> usize {
        self.hints.len()
            + self
                .permutations
                .iter()
                .map(Permutation::payload_bytes)
                .sum::<usize>()
    }

    pub fn hint_bytes(&self) -> usize {
        self.hints.len()
    }

    pub fn permutation_bytes(&self) -> usize {
        self.permutations
            .iter()
            .map(Permutation::payload_bytes)
            .sum()
    }

    pub fn prepare_query<R: Rng + CryptoRng>(
        &mut self,
        bucket_index: usize,
        rng: &mut R,
    ) -> Result<PreparedQuery> {
        if bucket_index >= self.bucket_count {
            bail!("SinglePass bucket index is outside the database");
        }
        if self.in_flight.is_some() {
            bail!("SinglePass client state already has a query in flight");
        }

        let target_partition = bucket_index / self.partition_len;
        let target_local_index = bucket_index % self.partition_len;
        let hint_index = self.permutations[target_partition].position(target_local_index);

        let mut punctured_indices = self
            .permutations
            .iter()
            .map(|permutation| permutation.value(hint_index) as u32)
            .collect::<Vec<_>>();
        punctured_indices[target_partition] = rng.gen_range(0..self.partition_len) as u32;

        let refresh_hint_indices = (0..self.partition_count)
            .map(|_| rng.gen_range(0..self.partition_len))
            .collect::<Vec<_>>();
        let refresh_indices = self
            .permutations
            .iter()
            .zip(&refresh_hint_indices)
            .map(|(permutation, position)| permutation.value(*position) as u32)
            .collect::<Vec<_>>();

        let id = self.next_query_id;
        let next_query_id = self
            .next_query_id
            .checked_add(1)
            .context("SinglePass query ID overflow")?;

        // The paper's show-and-shuffle step. Server 1 has just observed the
        // old values at `hint_index`; replacing them keeps future query sets
        // fresh. The target partition was punctured with an independent value
        // and is therefore not swapped.
        for (partition, refresh_position) in refresh_hint_indices.iter().copied().enumerate() {
            if partition != target_partition {
                self.permutations[partition].swap_positions(hint_index, refresh_position);
            }
        }

        self.next_query_id = next_query_id;
        self.in_flight = Some(id);
        Ok(PreparedQuery {
            id,
            target_partition,
            hint_index,
            refresh_hint_indices,
            server_queries: [
                ServerQuery {
                    indices: refresh_indices,
                },
                ServerQuery {
                    indices: punctured_indices,
                },
            ],
        })
    }

    pub fn complete_query(
        &mut self,
        prepared: PreparedQuery,
        server_answers: &[Vec<u8>],
    ) -> Result<Vec<u8>> {
        if self.in_flight != Some(prepared.id) {
            bail!("SinglePass prepared query does not match the in-flight state");
        }
        if server_answers.len() != SERVER_COUNT {
            bail!("SinglePass requires exactly two server answers");
        }
        let expected_answer_bytes = self
            .partition_count
            .checked_mul(self.row_size)
            .context("SinglePass answer size overflow")?;
        for answer in server_answers {
            if answer.len() != expected_answer_bytes {
                bail!(
                    "SinglePass answer has {} bytes, expected {expected_answer_bytes}",
                    answer.len()
                );
            }
        }

        let hint_start = prepared.hint_index * self.row_size;
        let mut recovered = self.hints[hint_start..hint_start + self.row_size].to_vec();
        for partition in 0..self.partition_count {
            if partition != prepared.target_partition {
                let answer_start = partition * self.row_size;
                xor_in_place(
                    &mut recovered,
                    &server_answers[1][answer_start..answer_start + self.row_size],
                );
            }
        }

        // Make both affected hints agree with the permutations already
        // swapped in `prepare_query`.
        for (partition, refresh_hint_index) in
            prepared.refresh_hint_indices.iter().copied().enumerate()
        {
            if partition == prepared.target_partition {
                continue;
            }
            let answer_start = partition * self.row_size;
            let refresh_hint_start = refresh_hint_index * self.row_size;
            for byte_index in 0..self.row_size {
                let delta = server_answers[0][answer_start + byte_index]
                    ^ server_answers[1][answer_start + byte_index];
                self.hints[hint_start + byte_index] ^= delta;
                self.hints[refresh_hint_start + byte_index] ^= delta;
            }
        }

        self.in_flight = None;
        Ok(recovered)
    }
}

pub fn answer(snapshot: SnapshotView<'_>, query: &ServerQuery) -> Result<Vec<u8>> {
    let partition_count = query.indices.len();
    if partition_count < 2 {
        bail!("SinglePass server query needs at least two partitions");
    }
    let partition_len = snapshot.bucket_count.div_ceil(partition_count);
    let answer_bytes = partition_count
        .checked_mul(snapshot.row_size)
        .context("SinglePass answer size overflow")?;
    let mut answer = vec![0u8; answer_bytes];
    for (partition, local_index) in query.indices.iter().copied().enumerate() {
        let local_index = local_index as usize;
        if local_index >= partition_len {
            bail!("SinglePass query contains an invalid partition index");
        }
        let global_index = partition * partition_len + local_index;
        if global_index < snapshot.bucket_count {
            let output_start = partition * snapshot.row_size;
            answer[output_start..output_start + snapshot.row_size]
                .copy_from_slice(row_unchecked(snapshot, global_index));
        }
    }
    Ok(answer)
}

fn row_unchecked(snapshot: SnapshotView<'_>, index: usize) -> &[u8] {
    let start = index * snapshot.row_size;
    &snapshot.rows()[start..start + snapshot.row_size]
}

#[inline(always)]
fn xor_in_place(target: &mut [u8], value: &[u8]) {
    for (target, value) in target.iter_mut().zip(value) {
        *target ^= value;
    }
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;
    use crate::snapshot::Snapshot;

    fn private_read(
        snapshot: &Snapshot,
        state: &mut ClientState,
        bucket: usize,
        rng: &mut StdRng,
    ) -> Result<Vec<u8>> {
        let prepared = state.prepare_query(bucket, rng)?;
        let answers = prepared
            .server_queries()
            .iter()
            .map(|query| answer(snapshot.view(), query))
            .collect::<Result<Vec<_>>>()?;
        state.complete_query(prepared, &answers)
    }

    #[test]
    fn repeated_queries_recover_rows_and_update_state() {
        let snapshot = Snapshot::benchmark(64, 37, 7).unwrap();
        let mut rng = StdRng::seed_from_u64(11);
        let mut state = ClientState::setup(snapshot.view(), 8, &mut rng).unwrap();
        for bucket in (0..64).chain([7, 7, 31, 0, 63]) {
            assert_eq!(
                private_read(&snapshot, &mut state, bucket, &mut rng).unwrap(),
                snapshot.row(bucket).unwrap()
            );
        }
    }

    #[test]
    fn padding_supports_non_divisible_database_sizes() {
        let snapshot = Snapshot::benchmark(32, 19, 9).unwrap();
        let mut rng = StdRng::seed_from_u64(13);
        let mut state = ClientState::setup(snapshot.view(), 6, &mut rng).unwrap();
        assert_eq!(state.partition_len(), 6);
        for bucket in 0..32 {
            assert_eq!(
                private_read(&snapshot, &mut state, bucket, &mut rng).unwrap(),
                snapshot.row(bucket).unwrap()
            );
        }
    }

    #[test]
    fn client_state_allows_only_one_in_flight_query() {
        let snapshot = Snapshot::benchmark(32, 32, 3).unwrap();
        let mut rng = StdRng::seed_from_u64(17);
        let mut state = ClientState::setup(snapshot.view(), 4, &mut rng).unwrap();
        let prepared = state.prepare_query(3, &mut rng).unwrap();
        assert!(state.prepare_query(4, &mut rng).is_err());
        let answers = prepared
            .server_queries()
            .iter()
            .map(|query| answer(snapshot.view(), query).unwrap())
            .collect::<Vec<_>>();
        state.complete_query(prepared, &answers).unwrap();
        assert!(state.prepare_query(4, &mut rng).is_ok());
    }

    #[test]
    fn reports_client_state_payload() {
        let snapshot = Snapshot::benchmark(64, 32, 5).unwrap();
        let mut rng = StdRng::seed_from_u64(19);
        let state = ClientState::setup(snapshot.view(), 8, &mut rng).unwrap();
        assert_eq!(state.hint_bytes(), 8 * 32);
        assert_eq!(state.permutation_bytes(), 64 * 2 * size_of::<u32>());
        assert_eq!(state.payload_bytes(), 8 * 32 + 64 * 8);
    }

    #[test]
    fn rejects_malformed_queries_and_answers() {
        let snapshot = Snapshot::benchmark(32, 32, 3).unwrap();
        let invalid_query = ServerQuery {
            indices: vec![0, 99],
        };
        assert!(answer(snapshot.view(), &invalid_query).is_err());

        let mut rng = StdRng::seed_from_u64(23);
        let mut state = ClientState::setup(snapshot.view(), 4, &mut rng).unwrap();
        let prepared = state.prepare_query(2, &mut rng).unwrap();
        assert!(state.complete_query(prepared, &[vec![0; 4 * 32]]).is_err());
    }
}
