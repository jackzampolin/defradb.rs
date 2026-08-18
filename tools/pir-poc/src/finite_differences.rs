//! Two-server finite-differences PIR research spike.
//!
//! This follows the information-theoretic construction and reference
//! implementation from Henzinger and Ragavan, "Two-Server Private Information
//! Retrieval in Sublinear Time and Quasilinear Space" (EUROCRYPT 2026).
//! Servers hold the truth table of a low-degree multilinear polynomial. Each
//! query reads a translated low-weight cloud from that preprocessed table.
//! This module is a correctness and cost spike, not audited cryptography.

use anyhow::{bail, Context, Result};
use rand::{CryptoRng, RngCore};
use rayon::prelude::*;

pub const SERVER_COUNT: usize = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parameters {
    pub record_count: usize,
    pub row_size: usize,
    pub variables_m: usize,
    pub total_degree_d: usize,
    pub capacity: usize,
    pub cloud_count: usize,
}

impl Parameters {
    pub fn new(
        record_count: usize,
        row_size: usize,
        variables_m: usize,
        total_degree_d: usize,
    ) -> Result<Self> {
        if record_count == 0 || row_size == 0 {
            bail!("finite-differences PIR needs non-zero records and row size");
        }
        if variables_m == 0 || variables_m >= usize::BITS as usize {
            bail!("finite-differences variable count is unsupported");
        }
        if total_degree_d == 0
            || total_degree_d.is_multiple_of(2)
            || total_degree_d > variables_m / 2
        {
            bail!("finite-differences degree must be odd and at most m/2");
        }
        let capacity = binomial(variables_m, total_degree_d)
            .context("finite-differences capacity overflow")?;
        if capacity < record_count {
            bail!("finite-differences parameters hold {capacity} records, need {record_count}");
        }
        let cloud_count = (0..=total_degree_d / 2).try_fold(0usize, |sum, weight| {
            sum.checked_add(
                binomial(variables_m, weight).context("finite-differences cloud size overflow")?,
            )
            .context("finite-differences cloud size overflow")
        })?;
        let parameters = Self {
            record_count,
            row_size,
            variables_m,
            total_degree_d,
            capacity,
            cloud_count,
        };
        parameters.storage_bytes()?;
        parameters.answer_bytes()?;
        Ok(parameters)
    }

    /// Non-dominated storage/response choices up to `maximum_variables`.
    pub fn pareto_variants(
        record_count: usize,
        row_size: usize,
        maximum_variables: usize,
    ) -> Result<Vec<Self>> {
        let mut variants = Vec::new();
        let mut smallest_answer = usize::MAX;
        for variables in 3..=maximum_variables.min(usize::BITS as usize - 1) {
            let candidate = (1..=variables / 2)
                .filter(|degree| degree % 2 == 1)
                .find_map(|degree| {
                    let capacity = binomial(variables, degree)?;
                    (capacity >= record_count).then_some(degree)
                });
            if let Some(degree) = candidate {
                let parameters = Self::new(record_count, row_size, variables, degree)?;
                let answer_bytes = parameters.answer_bytes()?;
                if answer_bytes < smallest_answer {
                    smallest_answer = answer_bytes;
                    variants.push(parameters);
                }
            }
        }
        if variants.is_empty() {
            bail!("no finite-differences parameters support the requested database");
        }
        Ok(variants)
    }

    pub fn encoded_entries(&self) -> usize {
        1usize << self.variables_m
    }

    pub fn storage_bytes(&self) -> Result<usize> {
        self.encoded_entries()
            .checked_mul(self.row_size)
            .context("finite-differences encoded database size overflow")
    }

    pub fn answer_bytes(&self) -> Result<usize> {
        self.cloud_count
            .checked_mul(self.row_size)
            .context("finite-differences answer size overflow")
    }

    pub fn query_bytes_per_server(&self) -> usize {
        size_of::<u64>()
    }

    pub fn encode_record_index(&self, mut index: usize) -> Result<u64> {
        if index >= self.record_count {
            bail!("finite-differences record index is out of range");
        }
        let mut encoded = 0u64;
        let mut remaining = self.total_degree_d;
        for position in 0..self.variables_m {
            if remaining == 0 {
                break;
            }
            let zero_prefix = binomial(self.variables_m - position - 1, remaining)
                .context("finite-differences index encoding overflow")?;
            if index >= zero_prefix {
                index -= zero_prefix;
                encoded |= 1u64 << position;
                remaining -= 1;
            }
        }
        Ok(encoded)
    }

    pub fn cloud(&self) -> Vec<u64> {
        let mut cloud = Vec::with_capacity(self.cloud_count);
        cloud.push(0);
        let limit = 1u64 << self.variables_m;
        for weight in 1..=self.total_degree_d / 2 {
            let mut combination = (1u64 << weight) - 1;
            while combination < limit {
                cloud.push(combination);
                let low_bit = combination & combination.wrapping_neg();
                let ripple = combination + low_bit;
                combination = (((ripple ^ combination) >> 2) / low_bit) | ripple;
            }
        }
        cloud.sort_unstable();
        debug_assert_eq!(cloud.len(), self.cloud_count);
        cloud
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientQuery {
    pub target_encoding: u64,
    pub server_queries: [u64; SERVER_COUNT],
}

#[derive(Clone, Debug)]
pub struct EncodedDatabase {
    parameters: Parameters,
    rows: Box<[u8]>,
}

impl EncodedDatabase {
    /// Encode coefficient rows into the polynomial truth table used online.
    pub fn encode(parameters: Parameters, records: &[u8]) -> Result<Self> {
        let expected = parameters
            .record_count
            .checked_mul(parameters.row_size)
            .context("finite-differences input size overflow")?;
        if records.len() != expected {
            bail!(
                "finite-differences input has {} bytes, expected {expected}",
                records.len()
            );
        }
        let mut rows = vec![0u8; parameters.storage_bytes()?];
        for record_index in 0..parameters.record_count {
            let encoded_index = parameters.encode_record_index(record_index)? as usize;
            let source = record_index * parameters.row_size;
            let target = encoded_index * parameters.row_size;
            rows[target..target + parameters.row_size]
                .copy_from_slice(&records[source..source + parameters.row_size]);
        }

        // Boolean subset zeta transform: after every variable has been folded,
        // each table row is the polynomial evaluated at that Boolean point.
        for variable in 0..parameters.variables_m {
            let lower_entries = 1usize << variable;
            let block_bytes = lower_entries * 2 * parameters.row_size;
            let lower_bytes = lower_entries * parameters.row_size;
            rows.par_chunks_mut(block_bytes).for_each(|block| {
                let (lower, upper) = block.split_at_mut(lower_bytes);
                upper
                    .iter_mut()
                    .zip(lower)
                    .for_each(|(destination, source)| *destination ^= *source);
            });
        }

        Ok(Self {
            parameters,
            rows: rows.into_boxed_slice(),
        })
    }

    pub fn parameters(&self) -> &Parameters {
        &self.parameters
    }

    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    pub fn answer(&self, cloud: &[u64], query: u64) -> Result<Vec<u8>> {
        answer(&self.parameters, &self.rows, cloud, query)
    }
}

pub fn prepare_query<R: RngCore + CryptoRng>(
    parameters: &Parameters,
    target_index: usize,
    rng: &mut R,
) -> Result<ClientQuery> {
    let target_encoding = parameters.encode_record_index(target_index)?;
    let mask = (1u64 << parameters.variables_m) - 1;
    let random_query = rng.next_u64() & mask;
    Ok(ClientQuery {
        target_encoding,
        server_queries: [random_query, random_query ^ target_encoding],
    })
}

pub fn answer(
    parameters: &Parameters,
    encoded_rows: &[u8],
    cloud: &[u64],
    query: u64,
) -> Result<Vec<u8>> {
    if encoded_rows.len() != parameters.storage_bytes()? {
        bail!("finite-differences encoded database has the wrong size");
    }
    if cloud.len() != parameters.cloud_count {
        bail!("finite-differences cloud has the wrong size");
    }
    if query >= 1u64 << parameters.variables_m {
        bail!("finite-differences server query is out of range");
    }
    let mut response = vec![0u8; parameters.answer_bytes()?];
    for (answer_index, point) in cloud.iter().copied().enumerate() {
        let row_index = (query ^ point) as usize;
        let source = row_index * parameters.row_size;
        let target = answer_index * parameters.row_size;
        response[target..target + parameters.row_size]
            .copy_from_slice(&encoded_rows[source..source + parameters.row_size]);
    }
    Ok(response)
}

pub fn recover(
    parameters: &Parameters,
    cloud: &[u64],
    query: ClientQuery,
    answers: &[Vec<u8>; SERVER_COUNT],
) -> Result<Vec<u8>> {
    let expected = parameters.answer_bytes()?;
    if cloud.len() != parameters.cloud_count
        || answers.iter().any(|answer| answer.len() != expected)
    {
        bail!("finite-differences recovery input has the wrong size");
    }
    let mut record = vec![0u8; parameters.row_size];
    for (answer_index, point) in cloud.iter().copied().enumerate() {
        if point & query.target_encoding == point {
            let start = answer_index * parameters.row_size;
            for server_answer in answers {
                for (destination, source) in record
                    .iter_mut()
                    .zip(&server_answer[start..start + parameters.row_size])
                {
                    *destination ^= *source;
                }
            }
        }
    }
    Ok(record)
}

fn binomial(n: usize, k: usize) -> Option<usize> {
    if k > n {
        return Some(0);
    }
    let k = k.min(n - k);
    let mut result = 1u128;
    for value in 1..=k {
        result = result.checked_mul((n - k + value) as u128)? / value as u128;
    }
    usize::try_from(result).ok()
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, RngCore, SeedableRng};

    use super::*;

    #[test]
    fn repeated_queries_recover_every_database_record() {
        let parameters = Parameters::new(30, 13, 7, 3).unwrap();
        let mut rng = StdRng::seed_from_u64(9);
        let mut records = vec![0u8; parameters.record_count * parameters.row_size];
        rng.fill_bytes(&mut records);
        let database = EncodedDatabase::encode(parameters.clone(), &records).unwrap();
        let cloud = parameters.cloud();
        for target in 0..parameters.record_count {
            let query = prepare_query(&parameters, target, &mut rng).unwrap();
            let answers = query
                .server_queries
                .map(|server_query| database.answer(&cloud, server_query).unwrap());
            assert_eq!(
                recover(&parameters, &cloud, query, &answers).unwrap(),
                records[target * parameters.row_size..(target + 1) * parameters.row_size]
            );
        }
    }

    #[test]
    fn four_million_record_tradeoffs_match_the_paper_cost_model() {
        let variants = Parameters::pareto_variants(1 << 22, 64, 33).unwrap();
        assert_eq!(
            variants
                .iter()
                .map(|parameters| (
                    parameters.variables_m,
                    parameters.total_degree_d,
                    parameters.storage_bytes().unwrap(),
                    parameters.answer_bytes().unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![
                (25, 11, 2 * 1024 * 1024 * 1024, 4_377_984),
                (27, 9, 8 * 1024 * 1024 * 1024, 1_334_656),
                (33, 7, 512 * 1024 * 1024 * 1024, 385_152),
            ]
        );
    }
}
