use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const FORMAT_VERSION: u32 = 1;
const SLOT_HEADER_SIZE: usize = 6;
const HASH_DOMAIN: &str = "defradb-pir-poc-bucket-v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    pub fn new(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        Self {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotConfig {
    pub bucket_count: usize,
    pub bucket_capacity: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub source: String,
    pub source_cutoff: String,
}

impl SnapshotConfig {
    pub fn row_size(&self) -> Result<usize> {
        let slot_size = SLOT_HEADER_SIZE
            .checked_add(self.max_key_bytes)
            .and_then(|size| size.checked_add(self.max_value_bytes))
            .context("slot size overflow")?;
        slot_size
            .checked_mul(self.bucket_capacity)
            .context("row size overflow")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Manifest {
    pub format_version: u32,
    pub source: String,
    pub source_cutoff: String,
    pub hash_domain: String,
    pub bucket_count: usize,
    pub bucket_capacity: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub row_size: usize,
    pub record_count: usize,
    pub snapshot_id: String,
}

impl Manifest {
    pub fn validate(&self) -> Result<()> {
        if self.format_version != FORMAT_VERSION {
            bail!(
                "unsupported snapshot format version {}",
                self.format_version
            );
        }
        if self.hash_domain != HASH_DOMAIN {
            bail!("unsupported snapshot hash domain");
        }
        if !self.bucket_count.is_power_of_two() {
            bail!("bucket count must be a non-zero power of two");
        }
        if self.bucket_capacity == 0 || self.max_key_bytes == 0 || self.max_value_bytes == 0 {
            bail!("capacity and key/value limits must be non-zero");
        }
        if self.max_key_bytes > u16::MAX as usize || self.max_value_bytes > u32::MAX as usize {
            bail!("manifest key/value limit cannot be represented in the row format");
        }
        let expected_row_size = SLOT_HEADER_SIZE
            .checked_add(self.max_key_bytes)
            .and_then(|size| size.checked_add(self.max_value_bytes))
            .and_then(|size| size.checked_mul(self.bucket_capacity))
            .context("manifest row size overflow")?;
        if self.row_size != expected_row_size {
            bail!("manifest row size does not match its slot layout");
        }
        let capacity = self
            .bucket_count
            .checked_mul(self.bucket_capacity)
            .context("manifest capacity overflow")?;
        if self.record_count > capacity {
            bail!("manifest record count exceeds snapshot capacity");
        }
        let id = hex::decode(&self.snapshot_id).context("snapshot ID is not hexadecimal")?;
        if id.len() != blake3::OUT_LEN {
            bail!("snapshot ID must be a BLAKE3 digest");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub manifest: Manifest,
    rows: Arc<[u8]>,
}

#[derive(Serialize)]
struct ManifestSeed<'a> {
    format_version: u32,
    source: &'a str,
    source_cutoff: &'a str,
    hash_domain: &'a str,
    bucket_count: usize,
    bucket_capacity: usize,
    max_key_bytes: usize,
    max_value_bytes: usize,
    row_size: usize,
    record_count: usize,
}

impl Snapshot {
    pub fn build(mut records: Vec<Record>, config: SnapshotConfig) -> Result<Self> {
        validate_config(&config)?;
        records.sort_by(|left, right| {
            left.key
                .cmp(&right.key)
                .then_with(|| left.value.cmp(&right.value))
        });

        let row_size = config.row_size()?;
        let total_size = row_size
            .checked_mul(config.bucket_count)
            .context("snapshot size overflow")?;
        let mut buckets = vec![Vec::<Record>::new(); config.bucket_count];
        for record in records {
            validate_record(&record, &config)?;
            let bucket = bucket_for_key(&record.key, config.bucket_count);
            if buckets[bucket].len() == config.bucket_capacity {
                bail!(
                    "bucket {bucket} overflow: capacity is {}",
                    config.bucket_capacity
                );
            }
            buckets[bucket].push(record);
        }

        let record_count = buckets.iter().map(Vec::len).sum();
        let mut rows = vec![0u8; total_size];
        let slot_size = SLOT_HEADER_SIZE + config.max_key_bytes + config.max_value_bytes;
        for (bucket_index, bucket) in buckets.iter().enumerate() {
            for (slot_index, record) in bucket.iter().enumerate() {
                let offset = bucket_index * row_size + slot_index * slot_size;
                encode_slot(&mut rows[offset..offset + slot_size], record, &config);
            }
        }

        Self::from_parts(rows, config, record_count)
    }

    pub fn benchmark(bucket_count: usize, row_size: usize, seed: u64) -> Result<Self> {
        if !bucket_count.is_power_of_two() || row_size <= SLOT_HEADER_SIZE + 1 {
            bail!(
                "benchmark row size must exceed its header and bucket count must be a power of two"
            );
        }
        let total_size = bucket_count
            .checked_mul(row_size)
            .context("benchmark snapshot size overflow")?;
        let mut rows = vec![0u8; total_size];
        let mut state = seed;
        for byte in &mut rows {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        let config = SnapshotConfig {
            bucket_count,
            bucket_capacity: 1,
            max_key_bytes: 1,
            max_value_bytes: row_size - SLOT_HEADER_SIZE - 1,
            source: "synthetic-benchmark".into(),
            source_cutoff: seed.to_string(),
        };
        let seed_manifest = ManifestSeed {
            format_version: FORMAT_VERSION,
            source: &config.source,
            source_cutoff: &config.source_cutoff,
            hash_domain: HASH_DOMAIN,
            bucket_count,
            bucket_capacity: 1,
            max_key_bytes: 1,
            max_value_bytes: config.max_value_bytes,
            row_size,
            record_count: bucket_count,
        };
        let snapshot_id = snapshot_id(&seed_manifest, &rows)?;
        Ok(Self {
            manifest: Manifest {
                format_version: FORMAT_VERSION,
                source: config.source,
                source_cutoff: config.source_cutoff,
                hash_domain: HASH_DOMAIN.into(),
                bucket_count,
                bucket_capacity: 1,
                max_key_bytes: 1,
                max_value_bytes: config.max_value_bytes,
                row_size,
                record_count: bucket_count,
                snapshot_id,
            },
            rows: rows.into(),
        })
    }

    fn from_parts(rows: Vec<u8>, config: SnapshotConfig, record_count: usize) -> Result<Self> {
        let row_size = config.row_size()?;
        let seed = ManifestSeed {
            format_version: FORMAT_VERSION,
            source: &config.source,
            source_cutoff: &config.source_cutoff,
            hash_domain: HASH_DOMAIN,
            bucket_count: config.bucket_count,
            bucket_capacity: config.bucket_capacity,
            max_key_bytes: config.max_key_bytes,
            max_value_bytes: config.max_value_bytes,
            row_size,
            record_count,
        };
        let id = snapshot_id(&seed, &rows)?;
        Ok(Self {
            manifest: Manifest {
                format_version: FORMAT_VERSION,
                source: config.source,
                source_cutoff: config.source_cutoff,
                hash_domain: HASH_DOMAIN.into(),
                bucket_count: config.bucket_count,
                bucket_capacity: config.bucket_capacity,
                max_key_bytes: config.max_key_bytes,
                max_value_bytes: config.max_value_bytes,
                row_size,
                record_count,
                snapshot_id: id,
            },
            rows: rows.into(),
        })
    }

    pub fn row(&self, index: usize) -> Result<&[u8]> {
        if index >= self.manifest.bucket_count {
            bail!("row index {index} is outside the snapshot");
        }
        let start = index * self.manifest.row_size;
        Ok(&self.rows[start..start + self.manifest.row_size])
    }

    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    pub fn values_from_row(&self, row: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>> {
        if row.len() != self.manifest.row_size {
            bail!("row size mismatch");
        }
        let slot_size =
            SLOT_HEADER_SIZE + self.manifest.max_key_bytes + self.manifest.max_value_bytes;
        let mut values = Vec::new();
        for slot in row.chunks_exact(slot_size) {
            let key_len = u16::from_le_bytes([slot[0], slot[1]]) as usize;
            let value_len = u32::from_le_bytes([slot[2], slot[3], slot[4], slot[5]]) as usize;
            if key_len == 0 && value_len == 0 {
                continue;
            }
            if key_len > self.manifest.max_key_bytes || value_len > self.manifest.max_value_bytes {
                bail!("encoded row contains invalid lengths");
            }
            let key_start = SLOT_HEADER_SIZE;
            let value_start = key_start + self.manifest.max_key_bytes;
            if &slot[key_start..key_start + key_len] == key {
                values.push(slot[value_start..value_start + value_len].to_vec());
            }
        }
        Ok(values)
    }

    pub fn save(&self, directory: &Path) -> Result<()> {
        fs::create_dir_all(directory).with_context(|| format!("create {}", directory.display()))?;
        let manifest = serde_json::to_vec_pretty(&self.manifest)?;
        fs::write(directory.join("manifest.json"), manifest)?;
        fs::write(directory.join("rows.bin"), &self.rows)?;
        Ok(())
    }

    pub fn load(directory: &Path) -> Result<Self> {
        let manifest: Manifest =
            serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)?;
        manifest.validate()?;
        let rows = fs::read(directory.join("rows.bin"))?;
        let expected_size = manifest
            .bucket_count
            .checked_mul(manifest.row_size)
            .context("snapshot size overflow")?;
        if rows.len() != expected_size {
            bail!(
                "rows.bin has {} bytes, expected {expected_size}",
                rows.len()
            );
        }
        let seed = ManifestSeed {
            format_version: manifest.format_version,
            source: &manifest.source,
            source_cutoff: &manifest.source_cutoff,
            hash_domain: &manifest.hash_domain,
            bucket_count: manifest.bucket_count,
            bucket_capacity: manifest.bucket_capacity,
            max_key_bytes: manifest.max_key_bytes,
            max_value_bytes: manifest.max_value_bytes,
            row_size: manifest.row_size,
            record_count: manifest.record_count,
        };
        let actual_id = snapshot_id(&seed, &rows)?;
        if actual_id != manifest.snapshot_id {
            bail!("snapshot content hash mismatch");
        }
        Ok(Self {
            manifest,
            rows: rows.into(),
        })
    }
}

pub fn bucket_for_key(key: &[u8], bucket_count: usize) -> usize {
    let mut hasher = blake3::Hasher::new();
    hasher.update(HASH_DOMAIN.as_bytes());
    hasher.update(key);
    let bytes = hasher.finalize();
    let prefix = u64::from_le_bytes(bytes.as_bytes()[..8].try_into().expect("fixed prefix"));
    prefix as usize & (bucket_count - 1)
}

pub fn records_from_json(
    json: &Value,
    collection: Option<&str>,
    key_field: &str,
    value_field: &str,
) -> Result<Vec<Record>> {
    let rows = match collection {
        Some(name) => json.get(name).and_then(Value::as_array),
        None => json.as_array(),
    }
    .context("expected an array of DefraDB query rows")?;

    rows.iter()
        .map(|row| {
            let key_value = row.get(key_field).context("row is missing key field")?;
            let value = row.get(value_field).context("row is missing value field")?;
            let key = key_value
                .as_str()
                .context("POC key field must be a string")?
                .as_bytes()
                .to_vec();
            let value = match value {
                Value::String(text) => text.as_bytes().to_vec(),
                other => serde_json::to_vec(other)?,
            };
            Ok(Record::new(key, value))
        })
        .collect()
}

fn validate_config(config: &SnapshotConfig) -> Result<()> {
    if !config.bucket_count.is_power_of_two() {
        bail!("bucket count must be a non-zero power of two");
    }
    if config.bucket_capacity == 0 || config.max_key_bytes == 0 || config.max_value_bytes == 0 {
        bail!("capacity and key/value limits must be non-zero");
    }
    if config.max_key_bytes > u16::MAX as usize || config.max_value_bytes > u32::MAX as usize {
        bail!("configured key/value limit cannot be represented in the row format");
    }
    config.row_size()?;
    Ok(())
}

fn validate_record(record: &Record, config: &SnapshotConfig) -> Result<()> {
    if record.key.is_empty() {
        bail!("empty keys are not supported");
    }
    if record.key.len() > config.max_key_bytes {
        bail!(
            "key is {} bytes, limit is {}",
            record.key.len(),
            config.max_key_bytes
        );
    }
    if record.value.len() > config.max_value_bytes {
        bail!(
            "value is {} bytes, limit is {}",
            record.value.len(),
            config.max_value_bytes
        );
    }
    Ok(())
}

fn encode_slot(slot: &mut [u8], record: &Record, config: &SnapshotConfig) {
    slot[..2].copy_from_slice(&(record.key.len() as u16).to_le_bytes());
    slot[2..6].copy_from_slice(&(record.value.len() as u32).to_le_bytes());
    slot[SLOT_HEADER_SIZE..SLOT_HEADER_SIZE + record.key.len()].copy_from_slice(&record.key);
    let value_start = SLOT_HEADER_SIZE + config.max_key_bytes;
    slot[value_start..value_start + record.value.len()].copy_from_slice(&record.value);
}

fn snapshot_id(seed: &ManifestSeed<'_>, rows: &[u8]) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"defradb-pir-poc-snapshot-v1");
    hasher.update(&serde_json::to_vec(seed)?);
    hasher.update(rows);
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> SnapshotConfig {
        SnapshotConfig {
            bucket_count: 16,
            bucket_capacity: 4,
            max_key_bytes: 16,
            max_value_bytes: 32,
            source: "Test".into(),
            source_cutoff: "1".into(),
        }
    }

    #[test]
    fn snapshot_is_deterministic_and_decodes_duplicates() {
        let records = vec![
            Record::new(b"alice", b"one"),
            Record::new(b"bob", b"two"),
            Record::new(b"alice", b"three"),
        ];
        let first = Snapshot::build(records.clone(), config()).unwrap();
        let second = Snapshot::build(records.into_iter().rev().collect(), config()).unwrap();
        assert_eq!(first.manifest.snapshot_id, second.manifest.snapshot_id);
        assert_eq!(first.rows(), second.rows());

        let bucket = bucket_for_key(b"alice", first.manifest.bucket_count);
        let values = first
            .values_from_row(first.row(bucket).unwrap(), b"alice")
            .unwrap();
        assert_eq!(values, vec![b"one".to_vec(), b"three".to_vec()]);
    }

    #[test]
    fn overflow_fails_closed() {
        let mut cfg = config();
        cfg.bucket_count = 1;
        cfg.bucket_capacity = 1;
        let error =
            Snapshot::build(vec![Record::new("a", "1"), Record::new("b", "2")], cfg).unwrap_err();
        assert!(error.to_string().contains("overflow"));
    }
}
