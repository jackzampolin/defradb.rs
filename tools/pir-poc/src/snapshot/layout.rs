use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

pub(super) const FORMAT_VERSION: u32 = 2;
pub(super) const SLOT_HEADER_SIZE: usize = 6;
pub(super) const HASH_DOMAIN: &str = "defradb-pir-poc-bucket-v2";
const PAGE_KEY_DOMAIN: &[u8] = b"defradb-pir-page-v1";
pub(super) const PAGE_KEY_OVERHEAD: usize = PAGE_KEY_DOMAIN.len() + 8;

#[derive(Clone, Debug)]
pub struct SnapshotConfig {
    pub bucket_count: usize,
    pub bucket_capacity: usize,
    pub values_per_page: usize,
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
    pub values_per_page: usize,
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub row_size: usize,
    pub record_count: usize,
    pub lookup_page_count: usize,
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
        if self.values_per_page == 0 || self.values_per_page > self.bucket_capacity {
            bail!("values per page must be between one and the bucket capacity");
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

    pub fn lookup_keys(&self, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        if self.lookup_page_count == 0 {
            return Ok(vec![key.to_vec()]);
        }
        (0..self.lookup_page_count)
            .map(|page| page_key(key, page))
            .collect()
    }

    pub fn values_from_row(&self, row: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>> {
        if row.len() != self.row_size {
            bail!("answer row size mismatch");
        }
        let slot_size = SLOT_HEADER_SIZE + self.max_key_bytes + self.max_value_bytes;
        let mut values = Vec::new();
        for slot in row.chunks_exact(slot_size) {
            let key_len = u16::from_le_bytes([slot[0], slot[1]]) as usize;
            let value_len = u32::from_le_bytes([slot[2], slot[3], slot[4], slot[5]]) as usize;
            if key_len == 0 && value_len == 0 {
                continue;
            }
            if key_len > self.max_key_bytes || value_len > self.max_value_bytes {
                bail!("answer row contains invalid lengths");
            }
            let value_start = SLOT_HEADER_SIZE + self.max_key_bytes;
            if &slot[SLOT_HEADER_SIZE..SLOT_HEADER_SIZE + key_len] == key {
                values.push(slot[value_start..value_start + value_len].to_vec());
            }
        }
        Ok(values)
    }
}

#[derive(Clone, Copy)]
pub struct SnapshotView<'a> {
    pub(super) rows: &'a [u8],
    pub bucket_count: usize,
    pub row_size: usize,
}

impl<'a> SnapshotView<'a> {
    pub(crate) fn rows(&self) -> &'a [u8] {
        self.rows
    }

    pub fn row(&self, index: usize) -> Result<&'a [u8]> {
        if index >= self.bucket_count {
            bail!("row index {index} is outside the snapshot");
        }
        let start = index * self.row_size;
        Ok(&self.rows[start..start + self.row_size])
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

pub fn page_key(key: &[u8], page: usize) -> Result<Vec<u8>> {
    let key_len = u32::try_from(key.len()).context("lookup key is too long")?;
    let page = u32::try_from(page).context("lookup page index is too large")?;
    let mut encoded = Vec::with_capacity(key.len() + PAGE_KEY_OVERHEAD);
    encoded.extend_from_slice(PAGE_KEY_DOMAIN);
    encoded.extend_from_slice(&key_len.to_le_bytes());
    encoded.extend_from_slice(key);
    encoded.extend_from_slice(&page.to_le_bytes());
    Ok(encoded)
}
