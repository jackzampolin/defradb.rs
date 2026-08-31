mod layout;

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use layout::{bucket_for_key, page_key, Manifest, SnapshotConfig, SnapshotView};
use layout::{FORMAT_VERSION, HASH_DOMAIN, PAGE_KEY_OVERHEAD, SLOT_HEADER_SIZE};

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
pub struct Snapshot {
    pub manifest: Manifest,
    rows: Arc<[u8]>,
}

/// The immutable snapshots exposed by one PIR replica.
///
/// `global` supports a tag-private lookup without disclosing a time filter.
/// Entries in `windows` support the same lookup while deliberately disclosing
/// one or more coarse window IDs so the server can scan smaller tables.
#[derive(Clone, Debug)]
pub struct SnapshotCatalog {
    global: Arc<Snapshot>,
    windows: Arc<BTreeMap<String, Arc<Snapshot>>>,
    manifest: CatalogManifest,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogManifest {
    pub global: Manifest,
    pub windows: BTreeMap<String, Manifest>,
}

impl CatalogManifest {
    pub fn validate(&self) -> Result<()> {
        self.global.validate().context("invalid global snapshot")?;
        for (window_id, manifest) in &self.windows {
            validate_window_id(window_id)?;
            manifest
                .validate()
                .with_context(|| format!("invalid snapshot for window {window_id}"))?;
        }
        Ok(())
    }
}

impl SnapshotCatalog {
    pub fn new(global: Arc<Snapshot>, windows: BTreeMap<String, Arc<Snapshot>>) -> Result<Self> {
        global
            .manifest
            .validate()
            .context("invalid global snapshot")?;
        for (window_id, snapshot) in &windows {
            validate_window_id(window_id)?;
            snapshot
                .manifest
                .validate()
                .with_context(|| format!("invalid snapshot for window {window_id}"))?;
        }
        let manifest = CatalogManifest {
            global: global.manifest.clone(),
            windows: windows
                .iter()
                .map(|(window_id, snapshot)| (window_id.clone(), snapshot.manifest.clone()))
                .collect(),
        };
        Ok(Self {
            global,
            windows: Arc::new(windows),
            manifest,
        })
    }

    pub fn global_only(global: Arc<Snapshot>) -> Result<Self> {
        Self::new(global, BTreeMap::new())
    }

    pub fn global(&self) -> &Arc<Snapshot> {
        &self.global
    }

    pub fn window(&self, window_id: &str) -> Option<&Arc<Snapshot>> {
        self.windows.get(window_id)
    }

    pub fn windows(&self) -> &BTreeMap<String, Arc<Snapshot>> {
        &self.windows
    }

    pub fn manifest(&self) -> &CatalogManifest {
        &self.manifest
    }

    /// Loads either the catalog layout or the original single-snapshot layout.
    ///
    /// Catalog layout:
    ///
    /// ```text
    /// ROOT/global/{manifest.json,rows.bin}
    /// ROOT/windows/WINDOW_ID/{manifest.json,rows.bin}
    /// ```
    pub fn load(directory: &Path) -> Result<Self> {
        let global_directory = directory.join("global");
        if !global_directory.join("manifest.json").is_file() {
            return Self::global_only(Arc::new(Snapshot::load(directory)?));
        }

        let global = Arc::new(Snapshot::load(&global_directory)?);
        let windows_directory = directory.join("windows");
        let mut windows = BTreeMap::new();
        if windows_directory.is_dir() {
            for entry in fs::read_dir(&windows_directory)
                .with_context(|| format!("read {}", windows_directory.display()))?
            {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let window_id = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("window directory name is not UTF-8"))?;
                validate_window_id(&window_id)?;
                let snapshot = Arc::new(
                    Snapshot::load(&entry.path())
                        .with_context(|| format!("load snapshot for public window {window_id}"))?,
                );
                windows.insert(window_id, snapshot);
            }
        }
        Self::new(global, windows)
    }
}

#[derive(Serialize)]
struct ManifestSeed<'a> {
    format_version: u32,
    source: &'a str,
    source_cutoff: &'a str,
    hash_domain: &'a str,
    bucket_count: usize,
    bucket_capacity: usize,
    values_per_page: usize,
    max_key_bytes: usize,
    max_value_bytes: usize,
    row_size: usize,
    record_count: usize,
    lookup_page_count: usize,
}

impl Snapshot {
    pub fn build(records: Vec<Record>, config: SnapshotConfig) -> Result<Self> {
        Self::build_encoded(records, config, 0)
    }

    pub fn build_paged(records: Vec<Record>, mut config: SnapshotConfig) -> Result<Self> {
        validate_config(&config)?;
        let mut by_key = BTreeMap::<Vec<u8>, Vec<Vec<u8>>>::new();
        for record in records {
            validate_record(&record, &config)?;
            by_key.entry(record.key).or_default().push(record.value);
        }
        for values in by_key.values_mut() {
            values.sort();
        }
        let lookup_page_count = by_key
            .values()
            .map(|values| values.len().div_ceil(config.values_per_page))
            .max()
            .unwrap_or(1);
        config.max_key_bytes = config
            .max_key_bytes
            .checked_add(PAGE_KEY_OVERHEAD)
            .context("paged key size overflow")?;

        let mut paged = Vec::new();
        for (key, values) in by_key {
            for (page, values) in values.chunks(config.values_per_page).enumerate() {
                let key = page_key(&key, page)?;
                paged.extend(values.iter().map(|value| Record::new(&key, value)));
            }
        }
        Self::build_encoded(paged, config, lookup_page_count)
    }

    fn build_encoded(
        mut records: Vec<Record>,
        config: SnapshotConfig,
        lookup_page_count: usize,
    ) -> Result<Self> {
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

        Self::from_parts(rows, config, record_count, lookup_page_count)
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
            values_per_page: 1,
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
            values_per_page: 1,
            max_key_bytes: 1,
            max_value_bytes: config.max_value_bytes,
            row_size,
            record_count: bucket_count,
            lookup_page_count: 0,
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
                values_per_page: 1,
                max_key_bytes: 1,
                max_value_bytes: config.max_value_bytes,
                row_size,
                record_count: bucket_count,
                lookup_page_count: 0,
                snapshot_id,
            },
            rows: rows.into(),
        })
    }

    #[cfg(feature = "research")]
    pub(crate) fn research_benchmark_from_rows(
        rows: Vec<u8>,
        row_size: usize,
        source_cutoff: &str,
    ) -> Result<Self> {
        if row_size <= SLOT_HEADER_SIZE + 1 || rows.len() % row_size != 0 {
            bail!("research benchmark rows must have one fixed valid row width");
        }
        let bucket_count = rows.len() / row_size;
        if !bucket_count.is_power_of_two() {
            bail!("research benchmark row count must be a power of two");
        }
        let config = SnapshotConfig {
            bucket_count,
            bucket_capacity: 1,
            values_per_page: 1,
            max_key_bytes: 1,
            max_value_bytes: row_size - SLOT_HEADER_SIZE - 1,
            source: "research-common-corpus".into(),
            source_cutoff: source_cutoff.into(),
        };
        Self::from_parts(rows, config, bucket_count, 0)
    }

    fn from_parts(
        rows: Vec<u8>,
        config: SnapshotConfig,
        record_count: usize,
        lookup_page_count: usize,
    ) -> Result<Self> {
        let row_size = config.row_size()?;
        let seed = ManifestSeed {
            format_version: FORMAT_VERSION,
            source: &config.source,
            source_cutoff: &config.source_cutoff,
            hash_domain: HASH_DOMAIN,
            bucket_count: config.bucket_count,
            bucket_capacity: config.bucket_capacity,
            values_per_page: config.values_per_page,
            max_key_bytes: config.max_key_bytes,
            max_value_bytes: config.max_value_bytes,
            row_size,
            record_count,
            lookup_page_count,
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
                values_per_page: config.values_per_page,
                max_key_bytes: config.max_key_bytes,
                max_value_bytes: config.max_value_bytes,
                row_size,
                record_count,
                lookup_page_count,
                snapshot_id: id,
            },
            rows: rows.into(),
        })
    }

    pub fn row(&self, index: usize) -> Result<&[u8]> {
        self.view().row(index)
    }

    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    pub fn view(&self) -> SnapshotView<'_> {
        SnapshotView {
            rows: &self.rows,
            bucket_count: self.manifest.bucket_count,
            row_size: self.manifest.row_size,
        }
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
            values_per_page: manifest.values_per_page,
            max_key_bytes: manifest.max_key_bytes,
            max_value_bytes: manifest.max_value_bytes,
            row_size: manifest.row_size,
            record_count: manifest.record_count,
            lookup_page_count: manifest.lookup_page_count,
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

fn validate_window_id(window_id: &str) -> Result<()> {
    if window_id.is_empty() || window_id.len() > 128 {
        bail!("window ID must contain between 1 and 128 bytes");
    }
    if window_id == "."
        || window_id == ".."
        || !window_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("window ID contains unsupported characters: {window_id}");
    }
    Ok(())
}

fn validate_config(config: &SnapshotConfig) -> Result<()> {
    if !config.bucket_count.is_power_of_two() {
        bail!("bucket count must be a non-zero power of two");
    }
    if config.bucket_capacity == 0 || config.max_key_bytes == 0 || config.max_value_bytes == 0 {
        bail!("capacity and key/value limits must be non-zero");
    }
    if config.values_per_page == 0 || config.values_per_page > config.bucket_capacity {
        bail!("values per page must be between one and the bucket capacity");
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
    hasher.update(b"defradb-pir-poc-snapshot-v2");
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
            values_per_page: 2,
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
            .manifest
            .values_from_row(first.row(bucket).unwrap(), b"alice")
            .unwrap();
        assert_eq!(values, vec![b"one".to_vec(), b"three".to_vec()]);
    }

    #[test]
    fn overflow_fails_closed() {
        let mut cfg = config();
        cfg.bucket_count = 1;
        cfg.bucket_capacity = 1;
        cfg.values_per_page = 1;
        let error =
            Snapshot::build(vec![Record::new("a", "1"), Record::new("b", "2")], cfg).unwrap_err();
        assert!(error.to_string().contains("overflow"));
    }

    #[test]
    fn paged_snapshot_supports_high_cardinality_keys_and_pads_queries() {
        let mut records = (0..9)
            .map(|index| Record::new("popular", format!("value-{index}")))
            .collect::<Vec<_>>();
        records.push(Record::new("rare", "only"));
        let mut cfg = config();
        cfg.bucket_count = 64;
        let snapshot = Snapshot::build_paged(records, cfg).unwrap();
        assert_eq!(snapshot.manifest.lookup_page_count, 5);

        let lookup_keys = snapshot.manifest.lookup_keys(b"rare").unwrap();
        assert_eq!(lookup_keys.len(), 5);
        let values = lookup_keys
            .iter()
            .flat_map(|lookup_key| {
                let bucket = bucket_for_key(lookup_key, snapshot.manifest.bucket_count);
                snapshot
                    .manifest
                    .values_from_row(snapshot.row(bucket).unwrap(), lookup_key)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![b"only".to_vec()]);
    }

    #[test]
    fn catalog_exposes_global_and_independently_sized_windows() {
        let global =
            Arc::new(Snapshot::build(vec![Record::new("tag", "global")], config()).unwrap());
        let mut window_config = config();
        window_config.bucket_count = 8;
        window_config.source_cutoff = "2026-W32".into();
        let window =
            Arc::new(Snapshot::build(vec![Record::new("tag", "window")], window_config).unwrap());
        let catalog = SnapshotCatalog::new(
            Arc::clone(&global),
            BTreeMap::from([("2026-W32".into(), Arc::clone(&window))]),
        )
        .unwrap();

        assert_eq!(
            catalog.global().manifest.snapshot_id,
            global.manifest.snapshot_id
        );
        assert_eq!(catalog.window("2026-W32").unwrap().manifest.bucket_count, 8);
        assert_eq!(catalog.manifest().windows.len(), 1);
        catalog.manifest().validate().unwrap();
    }
}
