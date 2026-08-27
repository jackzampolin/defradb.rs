//! Leakage-explicit searchable-encryption POC for immutable exact lookups.
//!
//! A trusted exporter computes deterministic keyed search tokens and encrypts
//! each fixed projection before handing the index to an untrusted serving
//! sidecar. The sidecar performs an ordinary hash-table lookup without learning
//! the plaintext key or value. It still learns repeated-token equality, access
//! patterns, response volume and update timing; this is deliberately not
//! presented as a replacement for strict PIR.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use rand::rngs::StdRng;
use rand::{CryptoRng, RngCore, SeedableRng};
use serde::Serialize;

use crate::verification::{decrypt_projection, encrypt_projection};

const TOKEN_DOMAIN: &[u8] = b"defradb-pir-blind-exact-token-v1";
const GENERATION_DOMAIN: &[u8] = b"defradb-pir-blind-exact-generation-v1";
const QUERY_SAMPLES: usize = 101;
const MAX_EXECUTED_ROWS: usize = 1_000_000;

pub struct EncryptedSearchIndex {
    generation: [u8; 32],
    rows: HashMap<[u8; 32], Vec<u8>>,
}

impl EncryptedSearchIndex {
    pub fn build<R: RngCore + CryptoRng>(
        mut records: Vec<(Vec<u8>, Vec<u8>)>,
        search_key: &[u8; 32],
        data_key: &[u8; 32],
        rng: &mut R,
    ) -> Result<Self> {
        if records.is_empty() {
            bail!("encrypted search index requires at least one record");
        }
        records.sort_by(|left, right| left.0.cmp(&right.0));
        if records.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            bail!("encrypted search index contains a duplicate key");
        }
        let generation = generation_digest(&records)?;
        let mut rows = HashMap::with_capacity(records.len());
        for (key, value) in records {
            let token = search_token(search_key, &generation, &key)?;
            let encrypted = encrypt_projection(data_key, 0, &generation, &key, 0, &value, rng)?;
            if rows.insert(token, encrypted).is_some() {
                bail!("encrypted search token collision");
            }
        }
        Ok(Self { generation, rows })
    }

    pub fn lookup(&self, token: &[u8; 32]) -> Option<&[u8]> {
        self.rows.get(token).map(Vec::as_slice)
    }

    pub fn decrypt(&self, key: &[u8], envelope: &[u8], data_key: &[u8; 32]) -> Result<Vec<u8>> {
        decrypt_projection(data_key, 0, &self.generation, key, 0, envelope)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn raw_entry_bytes(&self) -> usize {
        self.rows
            .values()
            .map(|envelope| 32usize.saturating_add(envelope.len()))
            .sum()
    }

    pub fn generation(&self) -> &[u8; 32] {
        &self.generation
    }
}

#[derive(Debug, Serialize)]
pub struct EncryptedSearchReport {
    pub protocol: &'static str,
    pub rows: usize,
    pub evidence: &'static str,
    pub build_ms: f64,
    pub raw_token_and_ciphertext_bytes: usize,
    pub client_token_p50_us: f64,
    pub server_lookup_p50_us: f64,
    pub client_decrypt_p50_us: f64,
    pub upload_bytes: usize,
    pub download_bytes: usize,
    pub recovered_value: bool,
    pub missing_value_rejected: bool,
    pub repeated_query_token_is_linkable: bool,
    pub different_key_is_unlinkable_without_key_compromise: bool,
    pub server_work: &'static str,
    pub leakage: Vec<&'static str>,
    pub appropriate_when: &'static str,
    pub not_equivalent_to_pir: &'static str,
}

pub fn benchmark(rows: usize) -> Result<EncryptedSearchReport> {
    if rows == 0 || rows > MAX_EXECUTED_ROWS {
        bail!("encrypted-search execution supports 1..={MAX_EXECUTED_ROWS} rows; use the gallery scale geometry for larger datasets");
    }
    let target = rows / 2;
    let target_key = fixture_key(target);
    let target_value = (target as u64).to_le_bytes().to_vec();
    let records = (0..rows)
        .map(|ordinal| {
            (
                fixture_key(ordinal),
                (ordinal as u64).to_le_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let search_key = [0x31; 32];
    let other_search_key = [0x32; 32];
    let data_key = [0x41; 32];
    // Deterministic fixture randomness keeps benchmark builds reproducible. A
    // production exporter must seed its CSPRNG from the operating system.
    let mut rng = StdRng::seed_from_u64(0x454e_4352_5950_5445);
    let build_started = Instant::now();
    let index = EncryptedSearchIndex::build(records, &search_key, &data_key, &mut rng)?;
    let build_ms = build_started.elapsed().as_secs_f64() * 1_000.0;

    let mut client_token = Vec::with_capacity(QUERY_SAMPLES);
    let mut server_lookup = Vec::with_capacity(QUERY_SAMPLES);
    let mut client_decrypt = Vec::with_capacity(QUERY_SAMPLES);
    let mut recovered = Vec::new();
    let mut download_bytes = 0;
    for _ in 0..QUERY_SAMPLES {
        let started = Instant::now();
        let token = search_token(&search_key, index.generation(), &target_key)?;
        client_token.push(elapsed_us(started));

        let started = Instant::now();
        let envelope = index
            .lookup(&token)
            .context("encrypted search target is absent")?
            .to_vec();
        server_lookup.push(elapsed_us(started));
        download_bytes = envelope.len();

        let started = Instant::now();
        recovered = index.decrypt(&target_key, &envelope, &data_key)?;
        client_decrypt.push(elapsed_us(started));
    }

    let token = search_token(&search_key, index.generation(), &target_key)?;
    let repeated = search_token(&search_key, index.generation(), &target_key)?;
    let other = search_token(&other_search_key, index.generation(), &target_key)?;
    let missing = search_token(&search_key, index.generation(), b"definitely-absent")?;
    Ok(EncryptedSearchReport {
        protocol: "trusted-exporter blind exact index: keyed BLAKE3 token + AES-256-GCM projection",
        rows: index.len(),
        evidence: "resident executed POC; HashMap allocation overhead is excluded from raw byte accounting",
        build_ms,
        raw_token_and_ciphertext_bytes: index.raw_entry_bytes(),
        client_token_p50_us: median(&mut client_token),
        server_lookup_p50_us: median(&mut server_lookup),
        client_decrypt_p50_us: median(&mut client_decrypt),
        upload_bytes: token.len(),
        download_bytes,
        recovered_value: recovered == target_value,
        missing_value_rejected: index.lookup(&missing).is_none(),
        repeated_query_token_is_linkable: token == repeated,
        different_key_is_unlinkable_without_key_compromise: token != other,
        server_work: "one ordinary token lookup plus one encrypted fixed-row copy",
        leakage: vec![
            "the same query emits the same token, revealing the search pattern",
            "the server observes which encrypted row is returned, revealing the access pattern",
            "unpadded pages reveal response volume and result overlap",
            "updates reveal timing unless rebuilt or buffered",
            "a compromised search key permits offline token mapping",
        ],
        appropriate_when: "an independent trusted exporter encrypts an authorization-equivalent immutable projection and search/access-pattern leakage is explicitly acceptable",
        not_equivalent_to_pir: "it reduces server work from a table scan to a point lookup by accepting leakage that strict PIR hides",
    })
}

pub fn search_token(search_key: &[u8; 32], generation: &[u8; 32], key: &[u8]) -> Result<[u8; 32]> {
    let length = u64::try_from(key.len())?;
    let mut hasher = blake3::Hasher::new_keyed(search_key);
    hasher.update(TOKEN_DOMAIN);
    hasher.update(generation);
    hasher.update(&length.to_le_bytes());
    hasher.update(key);
    Ok(*hasher.finalize().as_bytes())
}

fn generation_digest(records: &[(Vec<u8>, Vec<u8>)]) -> Result<[u8; 32]> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(GENERATION_DOMAIN);
    hasher.update(&u64::try_from(records.len())?.to_le_bytes());
    for (key, value) in records {
        hasher.update(&u64::try_from(key.len())?.to_le_bytes());
        hasher.update(key);
        hasher.update(&u64::try_from(value.len())?.to_le_bytes());
        hasher.update(value);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn fixture_key(ordinal: usize) -> Vec<u8> {
    (ordinal as u64).to_le_bytes().to_vec()
}

fn elapsed_us(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000_000.0
}

fn median(samples: &mut [f64]) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_exact_search_recovers_and_authenticates() {
        let report = benchmark(128).unwrap();
        assert!(report.recovered_value);
        assert!(report.missing_value_rejected);
        assert!(report.repeated_query_token_is_linkable);
        assert!(report.different_key_is_unlinkable_without_key_compromise);
        assert_eq!(report.upload_bytes, 32);
        assert_eq!(report.download_bytes, 37);
    }

    #[test]
    fn duplicate_keys_and_unbounded_execution_are_rejected() {
        let mut rng = StdRng::seed_from_u64(1);
        assert!(EncryptedSearchIndex::build(
            vec![(b"same".to_vec(), vec![1]), (b"same".to_vec(), vec![2])],
            &[1; 32],
            &[2; 32],
            &mut rng,
        )
        .is_err());
        assert!(benchmark(MAX_EXECUTED_ROWS + 1).is_err());
    }

    #[test]
    fn search_tokens_are_scoped_to_an_index_generation() {
        let search_key = [7; 32];
        let token_a = search_token(&search_key, &[1; 32], b"same-key").unwrap();
        let token_b = search_token(&search_key, &[2; 32], b"same-key").unwrap();
        assert_ne!(token_a, token_b);
    }
}
