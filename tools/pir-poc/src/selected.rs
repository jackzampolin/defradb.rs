//! Product-shaped storage shared by the three selected POC use cases.
//!
//! Dense XOR and the 100-decoy baseline deliberately use the same immutable
//! rows.  Strict mode evaluates a random selector share; decoy mode performs
//! visible ordinal reads.  This keeps the comparison honest and avoids a
//! second server-side index built only for the baseline.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::active_generation::{
    nullifier_order_key, ActiveGeneration, ActiveGenerationImage, ActiveGenerationLimits,
    ActiveLeaf, AuthenticatedGenerationManifest,
};
use crate::dense;
use crate::dense_batch::{BatchEvaluator, BatchKernel};
use crate::snapshot::SnapshotView;
use crate::subscription::CompactSubscriptionServer;

const STORE_FORMAT_VERSION: u32 = 1;
const STORE_DOMAIN: &[u8] = b"defradb-pir-selected-store-v1";
const STORE_MAC_DOMAIN: &[u8] = b"defradb-pir-selected-store-mac-v1";
const DIRECTORY_DOMAIN: &[u8] = b"defradb-pir-safe-ordinal-directory-v1";
const TABLE_DOMAIN: &[u8] = b"defradb-pir-selected-table-v1";
const ROW_FINGERPRINT_BYTES: usize = 16;
const ROW_HEADER_BYTES: usize = ROW_FINGERPRINT_BYTES + 4;
pub const NULLIFIER_WITNESS_BYTES: usize = 88 + 20 * 3 * 32;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(default)]
pub struct PocLimits {
    pub max_query_bytes: usize,
    pub max_response_bytes: usize,
    pub max_key_bytes: usize,
    pub max_batch_queries: usize,
    pub max_decoy_candidates: usize,
    pub max_table_bytes: usize,
    pub max_client_metadata_bytes: usize,
    pub max_transient_working_bytes: usize,
    pub max_in_flight: usize,
    pub max_subscriptions: usize,
}

impl Default for PocLimits {
    fn default() -> Self {
        Self {
            max_query_bytes: 128 * 1024 * 1024,
            max_response_bytes: 256 * 1024 * 1024,
            max_key_bytes: 4 * 1024,
            max_batch_queries: 4_096,
            max_decoy_candidates: 100,
            max_table_bytes: 2 * 1024 * 1024 * 1024,
            max_client_metadata_bytes: 256 * 1024 * 1024,
            max_transient_working_bytes: 256 * 1024 * 1024,
            max_in_flight: 4,
            max_subscriptions: 100_000,
        }
    }
}

impl PocLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_query_bytes == 0
            || self.max_response_bytes == 0
            || self.max_key_bytes == 0
            || self.max_batch_queries == 0
            || self.max_decoy_candidates == 0
            || self.max_table_bytes == 0
            || self.max_client_metadata_bytes == 0
            || self.max_transient_working_bytes == 0
            || self.max_in_flight == 0
            || self.max_subscriptions == 0
        {
            bail!("all POC admission limits must be non-zero");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DirectoryEntry {
    pub key_digest: [u8; 32],
    pub ordinal: usize,
}

/// Safe, canonical client metadata.  It is deliberately larger than PtrHash's
/// build-specific epserde image, but it can be parsed without unsafe code and
/// survives Rust/compiler upgrades.  The digest list leaks the populated key
/// set to dictionary attacks; the manifest labels that trade-off explicitly.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OrdinalDirectory {
    pub format_version: u32,
    pub row_count: usize,
    pub entries: Vec<DirectoryEntry>,
    pub digest: [u8; 32],
}

impl OrdinalDirectory {
    pub fn build(keys_in_ordinal_order: &[Vec<u8>]) -> Result<Self> {
        if keys_in_ordinal_order.is_empty() {
            bail!("ordinal directory requires at least one key");
        }
        let mut entries = keys_in_ordinal_order
            .iter()
            .enumerate()
            .map(|(ordinal, key)| DirectoryEntry {
                key_digest: directory_key_digest(key),
                ordinal,
            })
            .collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| entry.key_digest);
        if entries
            .windows(2)
            .any(|pair| pair[0].key_digest == pair[1].key_digest)
        {
            bail!("ordinal directory contains a key-digest collision");
        }
        let digest = directory_digest(keys_in_ordinal_order.len(), &entries);
        Ok(Self {
            format_version: STORE_FORMAT_VERSION,
            row_count: keys_in_ordinal_order.len(),
            entries,
            digest,
        })
    }

    pub fn validate(&self, max_metadata_bytes: usize) -> Result<()> {
        if self.format_version != STORE_FORMAT_VERSION || self.row_count == 0 {
            bail!("unsupported or empty ordinal directory");
        }
        let encoded_bytes = serde_json::to_vec(self)?.len();
        if encoded_bytes > max_metadata_bytes {
            bail!("ordinal directory exceeds client metadata admission limit");
        }
        if self.entries.len() != self.row_count
            || self
                .entries
                .windows(2)
                .any(|pair| pair[0].key_digest >= pair[1].key_digest)
        {
            bail!("ordinal directory is not a unique sorted exact mapping");
        }
        let ordinals = self
            .entries
            .iter()
            .map(|entry| entry.ordinal)
            .collect::<BTreeSet<_>>();
        if ordinals != (0..self.row_count).collect() {
            bail!("ordinal directory does not cover the compact row range");
        }
        if directory_digest(self.row_count, &self.entries) != self.digest {
            bail!("ordinal directory digest mismatch");
        }
        Ok(())
    }

    pub fn ordinal(&self, key: &[u8]) -> (usize, bool) {
        let digest = directory_key_digest(key);
        match self
            .entries
            .binary_search_by_key(&digest, |entry| entry.key_digest)
        {
            Ok(index) => (self.entries[index].ordinal, true),
            Err(_) => {
                let ordinal = usize::try_from(u64::from_le_bytes(
                    digest[..8].try_into().expect("fixed digest prefix"),
                ))
                .unwrap_or(0)
                    % self.row_count;
                (ordinal, false)
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PrivateTableManifest {
    pub name: String,
    pub generation: [u8; 32],
    pub row_count: usize,
    pub row_size: usize,
    pub values_per_row: usize,
    pub max_value_bytes: usize,
    pub query_share_bytes: usize,
    pub answer_share_bytes: usize,
    pub directory_digest: [u8; 32],
    pub client_metadata_bytes: usize,
    pub key_set_leakage: String,
    pub fixed_result_schedule: bool,
}

impl PrivateTableManifest {
    fn validate(&self, limits: &PocLimits) -> Result<()> {
        if self.row_count == 0
            || self.row_size == 0
            || self.values_per_row == 0
            || self.max_value_bytes == 0
        {
            bail!("private table dimensions must be non-zero");
        }
        if self.query_share_bytes != dense::query_size(self.row_count)
            || self.answer_share_bytes != self.row_size
        {
            bail!("private table wire dimensions are inconsistent");
        }
        if !self.fixed_result_schedule {
            bail!("selected POC tables require a fixed result schedule");
        }
        let table_bytes = self
            .row_count
            .checked_mul(self.row_size)
            .context("private table size overflow")?;
        if table_bytes > limits.max_table_bytes
            || self.query_share_bytes > limits.max_query_bytes
            || self.answer_share_bytes > limits.max_response_bytes
            || self.client_metadata_bytes > limits.max_client_metadata_bytes
        {
            bail!("private table exceeds configured admission limits");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PrivateTable {
    pub manifest: PrivateTableManifest,
    pub directory: OrdinalDirectory,
    rows: Arc<[u8]>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct PrivateTableImage {
    manifest: PrivateTableManifest,
    directory: OrdinalDirectory,
    rows_base64: String,
}

impl PrivateTable {
    pub fn build(
        name: impl Into<String>,
        mut records: Vec<(Vec<u8>, Vec<Vec<u8>>)>,
        values_per_row: usize,
        max_value_bytes: usize,
        limits: &PocLimits,
    ) -> Result<Self> {
        let name = name.into();
        if records.is_empty() || values_per_row == 0 || max_value_bytes == 0 {
            bail!("private table build dimensions must be non-zero");
        }
        records.sort_by(|left, right| left.0.cmp(&right.0));
        if records.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            bail!("private table contains duplicate keys");
        }
        for (_, values) in &records {
            if values.len() > values_per_row {
                bail!("private table record exceeds fixed result schedule");
            }
            if values.iter().any(|value| value.len() > max_value_bytes) {
                bail!("private table value exceeds fixed slot size");
            }
        }
        if records
            .iter()
            .any(|(key, _)| key.is_empty() || key.len() > limits.max_key_bytes)
        {
            bail!("private table key violates key-size admission limit");
        }
        let row_size = ROW_HEADER_BYTES
            .checked_add(
                values_per_row
                    .checked_mul(4 + max_value_bytes)
                    .context("private row slot size overflow")?,
            )
            .context("private row size overflow")?;
        let table_bytes = row_size
            .checked_mul(records.len())
            .context("private table size overflow")?;
        if table_bytes > limits.max_table_bytes {
            bail!("private table exceeds storage admission limit");
        }
        let keys = records
            .iter()
            .map(|record| record.0.clone())
            .collect::<Vec<_>>();
        let directory = OrdinalDirectory::build(&keys)?;
        directory.validate(limits.max_client_metadata_bytes)?;
        let mut rows = vec![0u8; table_bytes];
        for (ordinal, (key, values)) in records.iter().enumerate() {
            let row = &mut rows[ordinal * row_size..(ordinal + 1) * row_size];
            encode_row(row, key, values, values_per_row, max_value_bytes)?;
        }
        let generation = table_generation(
            &name,
            records.len(),
            row_size,
            values_per_row,
            max_value_bytes,
            &directory,
            &rows,
        );
        let manifest = PrivateTableManifest {
            name,
            generation,
            row_count: records.len(),
            row_size,
            values_per_row,
            max_value_bytes,
            query_share_bytes: dense::query_size(records.len()),
            answer_share_bytes: row_size,
            directory_digest: directory.digest,
            client_metadata_bytes: serde_json::to_vec(&directory)?.len(),
            key_set_leakage:
                "public stable ordinal metadata exposes populated key digests to dictionary attacks"
                    .to_owned(),
            fixed_result_schedule: true,
        };
        manifest.validate(limits)?;
        Ok(Self {
            manifest,
            directory,
            rows: rows.into(),
        })
    }

    pub fn evaluate_share(&self, query_share: &[u8], limits: &PocLimits) -> Result<Vec<u8>> {
        if query_share.len() > limits.max_query_bytes
            || query_share.len() != self.manifest.query_share_bytes
        {
            bail!("private query share violates admission limits");
        }
        dense::answer(self.view(), query_share)
    }

    pub fn evaluate_batch(
        &self,
        query_shares: &[Vec<u8>],
        limits: &PocLimits,
    ) -> Result<Vec<Vec<u8>>> {
        if query_shares.is_empty() || query_shares.len() > limits.max_batch_queries {
            bail!("private query batch violates admission limits");
        }
        let request_bytes = query_shares.iter().try_fold(0usize, |total, query| {
            total
                .checked_add(query.len())
                .context("query batch size overflow")
        })?;
        let response_bytes = query_shares
            .len()
            .checked_mul(self.manifest.row_size)
            .context("answer batch size overflow")?;
        if request_bytes > limits.max_query_bytes || response_bytes > limits.max_response_bytes {
            bail!("private query batch exceeds byte admission limits");
        }
        if query_shares
            .iter()
            .any(|query| query.len() != self.manifest.query_share_bytes)
        {
            bail!("private query share has the wrong size");
        }
        let evaluator =
            BatchEvaluator::new(limits.max_batch_queries, limits.max_transient_working_bytes)?;
        Ok(evaluator
            .evaluate(self.view(), query_shares, BatchKernel::SharedRowMajor)?
            .answers)
    }

    pub fn strict_ordinal(&self, key: &[u8]) -> (usize, bool) {
        self.directory.ordinal(key)
    }

    pub fn direct_rows(&self, keys: &[Vec<u8>], limits: &PocLimits) -> Result<Vec<Vec<u8>>> {
        if keys.is_empty() || keys.len() > limits.max_decoy_candidates {
            bail!("decoy candidate count violates admission limit");
        }
        if keys
            .iter()
            .any(|key| key.is_empty() || key.len() > limits.max_key_bytes)
        {
            bail!("decoy key violates key-size admission limit");
        }
        let response_bytes = keys
            .len()
            .checked_mul(self.manifest.row_size)
            .context("decoy response size overflow")?;
        if response_bytes > limits.max_response_bytes {
            bail!("decoy response exceeds admission limit");
        }
        keys.iter()
            .map(|key| {
                let (ordinal, _) = self.directory.ordinal(key);
                self.row(ordinal).map(ToOwned::to_owned)
            })
            .collect()
    }

    pub fn decode(&self, row: &[u8], key: &[u8]) -> Result<Option<Vec<Vec<u8>>>> {
        decode_row(
            row,
            key,
            self.manifest.values_per_row,
            self.manifest.max_value_bytes,
        )
    }

    pub fn row(&self, ordinal: usize) -> Result<&[u8]> {
        self.view().row(ordinal)
    }

    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    pub fn view(&self) -> SnapshotView<'_> {
        SnapshotView::new(&self.rows, self.manifest.row_count, self.manifest.row_size)
    }

    fn image(&self) -> PrivateTableImage {
        PrivateTableImage {
            manifest: self.manifest.clone(),
            directory: self.directory.clone(),
            rows_base64: STANDARD.encode(&self.rows),
        }
    }

    fn from_image(image: PrivateTableImage, limits: &PocLimits) -> Result<Self> {
        image.manifest.validate(limits)?;
        image.directory.validate(limits.max_client_metadata_bytes)?;
        if image.directory.digest != image.manifest.directory_digest
            || image.directory.row_count != image.manifest.row_count
        {
            bail!("private table directory does not match manifest");
        }
        let expected = image
            .manifest
            .row_count
            .checked_mul(image.manifest.row_size)
            .context("private table size overflow")?;
        let maximum_encoded = expected
            .checked_add(2)
            .and_then(|bytes| bytes.checked_div(3))
            .and_then(|groups| groups.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(4))
            .context("private table Base64 size overflow")?;
        if image.rows_base64.len() > maximum_encoded {
            bail!("private table Base64 payload exceeds its admitted decoded size");
        }
        let rows = STANDARD.decode(image.rows_base64)?;
        if rows.len() != expected {
            bail!("private table row payload has the wrong size");
        }
        let actual = table_generation(
            &image.manifest.name,
            image.manifest.row_count,
            image.manifest.row_size,
            image.manifest.values_per_row,
            image.manifest.max_value_bytes,
            &image.directory,
            &rows,
        );
        if actual != image.manifest.generation {
            bail!("private table generation digest mismatch");
        }
        Ok(Self {
            manifest: image.manifest,
            directory: image.directory,
            rows: rows.into(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct UseCaseManifest {
    pub format_version: u32,
    pub limits: PocLimits,
    pub active_generation: AuthenticatedGenerationManifest,
    pub nullifier_table: PrivateTableManifest,
    pub encrypted_tag_table: PrivateTableManifest,
    pub shinzo_bucket_count: usize,
    pub body_digest: [u8; 32],
    pub privacy_modes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthenticatedUseCaseManifest {
    pub manifest: UseCaseManifest,
    pub mac: [u8; 32],
}

impl AuthenticatedUseCaseManifest {
    pub fn verify(&self, operator_key: &[u8; 32], limits: &PocLimits) -> Result<()> {
        if self.manifest.format_version != STORE_FORMAT_VERSION {
            bail!("unsupported selected POC store version");
        }
        if &self.manifest.limits != limits {
            bail!("selected POC manifest limits do not match the serving policy");
        }
        self.manifest.limits.validate()?;
        self.manifest
            .active_generation
            .verify(operator_key, &ActiveGenerationLimits::default())?;
        self.manifest.nullifier_table.validate(limits)?;
        self.manifest.encrypted_tag_table.validate(limits)?;
        if !self.manifest.shinzo_bucket_count.is_power_of_two()
            || self.manifest.shinzo_bucket_count < 2
        {
            bail!("Shinzo DPF bucket domain must be a power of two");
        }
        let body_digest = store_body_digest(
            &self.manifest.active_generation.manifest.body_digest,
            &self.manifest.nullifier_table.generation,
            &self.manifest.encrypted_tag_table.generation,
            self.manifest.shinzo_bucket_count,
        );
        if body_digest != self.manifest.body_digest {
            bail!("selected POC manifest components do not match its body digest");
        }
        let expected = store_manifest_mac(operator_key, &self.manifest)?;
        if expected != self.mac {
            bail!("selected POC manifest authentication failed");
        }
        Ok(())
    }
}

pub struct UseCaseStore {
    pub manifest: AuthenticatedUseCaseManifest,
    pub limits: PocLimits,
    pub active_generation: Arc<ActiveGeneration>,
    pub nullifiers: Arc<PrivateTable>,
    pub encrypted_tags: Arc<PrivateTable>,
    pub shinzo: Arc<Mutex<CompactSubscriptionServer>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct UseCaseStoreImage {
    manifest: AuthenticatedUseCaseManifest,
    limits: PocLimits,
    active_generation: ActiveGenerationImage,
    nullifiers: PrivateTableImage,
    encrypted_tags: PrivateTableImage,
    shinzo_party_index: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UseCaseBuildInput {
    pub generation_height: u64,
    pub generation_root_hex: String,
    pub nullifiers: Vec<NullifierBuildRecord>,
    pub encrypted_tags: Vec<EncryptedTagBuildRecord>,
    pub shinzo_bucket_count: usize,
    #[serde(default)]
    pub limits: PocLimits,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NullifierBuildRecord {
    pub nullifier_hex: String,
    pub position: u64,
    pub witness_base64: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EncryptedTagBuildRecord {
    pub tag_base64: String,
    pub encrypted_values_base64: Vec<String>,
}

impl UseCaseStore {
    pub fn build(
        input: UseCaseBuildInput,
        operator_key: &[u8; 32],
        shinzo_party_index: usize,
    ) -> Result<Self> {
        input.limits.validate()?;
        let root = decode_array::<32>(&input.generation_root_hex, "generation root")?;
        if input.nullifiers.is_empty() || input.encrypted_tags.is_empty() {
            bail!("selected POC input requires nullifier and encrypted-tag records");
        }

        let mut nullifier_records = Vec::with_capacity(input.nullifiers.len());
        let mut leaves = Vec::with_capacity(input.nullifiers.len() + 1);
        leaves.push(ActiveLeaf {
            value: [0; 32],
            position: 0,
            next_index: 0,
            next_value: [0; 32],
            sentinel: true,
            terminal: false,
        });
        let mut decoded_nullifiers = input
            .nullifiers
            .into_iter()
            .map(|record| {
                let key = decode_array::<32>(&record.nullifier_hex, "nullifier")?;
                let witness = STANDARD.decode(record.witness_base64)?;
                if witness.len() != NULLIFIER_WITNESS_BYTES {
                    bail!("nullifier witness must use Shieldd's fixed 2008-byte schedule");
                }
                Ok((nullifier_order_key(&key)?, key, record.position, witness))
            })
            .collect::<Result<Vec<_>>>()?;
        decoded_nullifiers.sort_by_key(|record| record.0);
        for (index, (_, key, position, witness)) in decoded_nullifiers.iter().enumerate() {
            let next = decoded_nullifiers.get(index + 1);
            leaves.push(ActiveLeaf {
                value: *key,
                position: *position,
                next_index: next.map_or(0, |record| record.2),
                next_value: next.map_or([0; 32], |record| record.1),
                sentinel: false,
                terminal: next.is_none(),
            });
            nullifier_records.push((key.to_vec(), vec![witness.clone()]));
        }
        let active_generation = ActiveGeneration::build_base(
            input.generation_height,
            root,
            leaves,
            Vec::new(),
            ActiveGenerationLimits::default(),
        )?;
        let nullifiers = PrivateTable::build(
            "active-nullifier-witness",
            nullifier_records,
            1,
            NULLIFIER_WITNESS_BYTES,
            &input.limits,
        )?;

        let mut maximum_values = 1;
        let mut maximum_value_bytes = 1;
        let encrypted_tag_records = input
            .encrypted_tags
            .into_iter()
            .map(|record| {
                let key = STANDARD.decode(record.tag_base64)?;
                let values = record
                    .encrypted_values_base64
                    .into_iter()
                    .map(|value| STANDARD.decode(value).map_err(Into::into))
                    .collect::<Result<Vec<_>>>()?;
                maximum_values = maximum_values.max(values.len());
                maximum_value_bytes =
                    maximum_value_bytes.max(values.iter().map(Vec::len).max().unwrap_or(1));
                Ok((key, values))
            })
            .collect::<Result<Vec<_>>>()?;
        let encrypted_tags = PrivateTable::build(
            "encrypted-tag-projection",
            encrypted_tag_records,
            maximum_values,
            maximum_value_bytes,
            &input.limits,
        )?;

        let active_manifest = active_generation.authenticated_manifest(operator_key)?;
        let body_digest = store_body_digest(
            &active_manifest.manifest.body_digest,
            &nullifiers.manifest.generation,
            &encrypted_tags.manifest.generation,
            input.shinzo_bucket_count,
        );
        let manifest = UseCaseManifest {
            format_version: STORE_FORMAT_VERSION,
            limits: input.limits.clone(),
            active_generation: active_manifest,
            nullifier_table: nullifiers.manifest.clone(),
            encrypted_tag_table: encrypted_tags.manifest.clone(),
            shinzo_bucket_count: input.shinzo_bucket_count,
            body_digest,
            privacy_modes: BTreeMap::from([
                (
                    "strict".to_owned(),
                    "two-server information-theoretic Dense XOR target privacy; both answers required"
                        .to_owned(),
                ),
                (
                    "decoy".to_owned(),
                    "candidate-set privacy only; server sees every candidate and the configured cardinality, enabling longitudinal intersections"
                        .to_owned(),
                ),
                (
                    "shinzo-live".to_owned(),
                    "two-server computational target privacy under Compact DPF and its AES PRG"
                        .to_owned(),
                ),
            ]),
        };
        let authenticated = AuthenticatedUseCaseManifest {
            mac: store_manifest_mac(operator_key, &manifest)?,
            manifest,
        };
        authenticated.verify(operator_key, &input.limits)?;
        let shinzo = CompactSubscriptionServer::new(
            shinzo_party_index,
            authenticated.manifest.shinzo_bucket_count,
        )?;
        Ok(Self {
            manifest: authenticated,
            limits: input.limits,
            active_generation: Arc::new(active_generation),
            nullifiers: Arc::new(nullifiers),
            encrypted_tags: Arc::new(encrypted_tags),
            shinzo: Arc::new(Mutex::new(shinzo)),
        })
    }

    pub fn table(&self, use_case: TableUseCase) -> &Arc<PrivateTable> {
        match use_case {
            TableUseCase::Nullifier => &self.nullifiers,
            TableUseCase::EncryptedTag => &self.encrypted_tags,
        }
    }

    pub fn save_immutable(
        &self,
        root: &Path,
        operator_key: &[u8; 32],
        party: usize,
    ) -> Result<PathBuf> {
        self.manifest.verify(operator_key, &self.limits)?;
        if root.exists() {
            bail!("selected POC output already exists; generations are immutable");
        }
        let generation_hex = hex::encode(self.manifest.manifest.body_digest);
        let temporary = root.with_extension(format!("tmp-{}", std::process::id()));
        fs::create_dir_all(temporary.join("generations").join(&generation_hex))?;
        let image = UseCaseStoreImage {
            manifest: self.manifest.clone(),
            limits: self.limits.clone(),
            active_generation: self.active_generation.image(operator_key)?,
            nullifiers: self.nullifiers.image(),
            encrypted_tags: self.encrypted_tags.image(),
            shinzo_party_index: party,
        };
        let generation_directory = temporary.join("generations").join(&generation_hex);
        write_synced(
            &generation_directory.join("store.json"),
            &serde_json::to_vec(&image)?,
        )?;
        write_synced(&temporary.join("CURRENT"), generation_hex.as_bytes())?;
        fs::rename(&temporary, root)
            .with_context(|| format!("atomically publish selected POC store {}", root.display()))?;
        Ok(root.join("generations").join(generation_hex))
    }

    pub fn load(root: &Path, operator_key: &[u8; 32]) -> Result<Self> {
        let current = fs::read_to_string(root.join("CURRENT"))?;
        let generation = current.trim();
        if generation.len() != 64 || !generation.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            bail!("selected POC CURRENT pointer is malformed");
        }
        let path = root.join("generations").join(generation).join("store.json");
        let metadata = fs::metadata(&path)?;
        if metadata.len() > 4 * 1024 * 1024 * 1024u64 {
            bail!("selected POC store image exceeds hard load limit");
        }
        let image: UseCaseStoreImage = serde_json::from_slice(&fs::read(path)?)?;
        image.limits.validate()?;
        image.manifest.verify(operator_key, &image.limits)?;
        let active_generation =
            ActiveGeneration::from_image(image.active_generation, operator_key)?;
        let nullifiers = PrivateTable::from_image(image.nullifiers, &image.limits)?;
        let encrypted_tags = PrivateTable::from_image(image.encrypted_tags, &image.limits)?;
        let body_digest = store_body_digest(
            &active_generation.manifest.body_digest,
            &nullifiers.manifest.generation,
            &encrypted_tags.manifest.generation,
            image.manifest.manifest.shinzo_bucket_count,
        );
        if body_digest != image.manifest.manifest.body_digest
            || hex::encode(body_digest) != generation
            || image.manifest.manifest.nullifier_table != nullifiers.manifest
            || image.manifest.manifest.encrypted_tag_table != encrypted_tags.manifest
        {
            bail!("selected POC store body does not match authenticated manifest");
        }
        let shinzo = CompactSubscriptionServer::new(
            image.shinzo_party_index,
            image.manifest.manifest.shinzo_bucket_count,
        )?;
        Ok(Self {
            manifest: image.manifest,
            limits: image.limits,
            active_generation: Arc::new(active_generation),
            nullifiers: Arc::new(nullifiers),
            encrypted_tags: Arc::new(encrypted_tags),
            shinzo: Arc::new(Mutex::new(shinzo)),
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TableUseCase {
    Nullifier,
    EncryptedTag,
}

pub fn decode_table_answer(
    manifest: &PrivateTableManifest,
    row: &[u8],
    key: &[u8],
) -> Result<Option<Vec<Vec<u8>>>> {
    decode_row(row, key, manifest.values_per_row, manifest.max_value_bytes)
}

fn encode_row(
    row: &mut [u8],
    key: &[u8],
    values: &[Vec<u8>],
    values_per_row: usize,
    max_value_bytes: usize,
) -> Result<()> {
    if row.len() != ROW_HEADER_BYTES + values_per_row * (4 + max_value_bytes) {
        bail!("private table row has the wrong encoded size");
    }
    row[..ROW_FINGERPRINT_BYTES].copy_from_slice(&row_fingerprint(key));
    row[ROW_FINGERPRINT_BYTES..ROW_HEADER_BYTES]
        .copy_from_slice(&u32::try_from(values.len())?.to_le_bytes());
    for (slot, value) in values.iter().enumerate() {
        let start = ROW_HEADER_BYTES + slot * (4 + max_value_bytes);
        row[start..start + 4].copy_from_slice(&u32::try_from(value.len())?.to_le_bytes());
        row[start + 4..start + 4 + value.len()].copy_from_slice(value);
    }
    Ok(())
}

fn decode_row(
    row: &[u8],
    key: &[u8],
    values_per_row: usize,
    max_value_bytes: usize,
) -> Result<Option<Vec<Vec<u8>>>> {
    let expected = ROW_HEADER_BYTES
        .checked_add(values_per_row * (4 + max_value_bytes))
        .context("private row decode size overflow")?;
    if row.len() != expected {
        bail!("private answer row has the wrong size");
    }
    if row[..ROW_FINGERPRINT_BYTES] != row_fingerprint(key) {
        return Ok(None);
    }
    let count = usize::try_from(u32::from_le_bytes(
        row[ROW_FINGERPRINT_BYTES..ROW_HEADER_BYTES].try_into()?,
    ))?;
    if count > values_per_row {
        bail!("private answer row contains an invalid value count");
    }
    let mut values = Vec::with_capacity(count);
    for slot in 0..count {
        let start = ROW_HEADER_BYTES + slot * (4 + max_value_bytes);
        let len = usize::try_from(u32::from_le_bytes(row[start..start + 4].try_into()?))?;
        if len > max_value_bytes {
            bail!("private answer row contains an invalid value length");
        }
        values.push(row[start + 4..start + 4 + len].to_vec());
    }
    Ok(Some(values))
}

fn directory_key_digest(key: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DIRECTORY_DOMAIN);
    hasher.update(&(key.len() as u64).to_le_bytes());
    hasher.update(key);
    *hasher.finalize().as_bytes()
}

fn directory_digest(row_count: usize, entries: &[DirectoryEntry]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DIRECTORY_DOMAIN);
    hasher.update(&(row_count as u64).to_le_bytes());
    for entry in entries {
        hasher.update(&entry.key_digest);
        hasher.update(&(entry.ordinal as u64).to_le_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn row_fingerprint(key: &[u8]) -> [u8; ROW_FINGERPRINT_BYTES] {
    directory_key_digest(key)[..ROW_FINGERPRINT_BYTES]
        .try_into()
        .expect("fixed fingerprint prefix")
}

fn table_generation(
    name: &str,
    row_count: usize,
    row_size: usize,
    values_per_row: usize,
    max_value_bytes: usize,
    directory: &OrdinalDirectory,
    rows: &[u8],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(TABLE_DOMAIN);
    hasher.update(&(name.len() as u64).to_le_bytes());
    hasher.update(name.as_bytes());
    for value in [row_count, row_size, values_per_row, max_value_bytes] {
        hasher.update(&(value as u64).to_le_bytes());
    }
    hasher.update(&directory.digest);
    hasher.update(&(rows.len() as u64).to_le_bytes());
    hasher.update(rows);
    *hasher.finalize().as_bytes()
}

fn store_body_digest(
    active: &[u8; 32],
    nullifier: &[u8; 32],
    tags: &[u8; 32],
    shinzo_bucket_count: usize,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(STORE_DOMAIN);
    hasher.update(&STORE_FORMAT_VERSION.to_le_bytes());
    hasher.update(active);
    hasher.update(nullifier);
    hasher.update(tags);
    hasher.update(&(shinzo_bucket_count as u64).to_le_bytes());
    *hasher.finalize().as_bytes()
}

fn store_manifest_mac(operator_key: &[u8; 32], manifest: &UseCaseManifest) -> Result<[u8; 32]> {
    let encoded = serde_json::to_vec(manifest)?;
    let mut hasher = blake3::Hasher::new_keyed(operator_key);
    hasher.update(STORE_MAC_DOMAIN);
    hasher.update(&(encoded.len() as u64).to_le_bytes());
    hasher.update(&encoded);
    Ok(*hasher.finalize().as_bytes())
}

fn decode_array<const N: usize>(hex_value: &str, name: &str) -> Result<[u8; N]> {
    let bytes = hex::decode(hex_value).with_context(|| format!("decode {name}"))?;
    bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("{name} must be {N} bytes"))
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use rand::{rngs::StdRng, SeedableRng};

    use super::*;

    #[test]
    fn strict_and_decoy_modes_share_identical_rows() {
        let limits = PocLimits::default();
        let table = PrivateTable::build(
            "test",
            vec![
                (b"a".to_vec(), vec![b"one".to_vec()]),
                (b"b".to_vec(), vec![b"two".to_vec()]),
            ],
            1,
            8,
            &limits,
        )
        .unwrap();
        let (ordinal, present) = table.strict_ordinal(b"b");
        assert!(present);
        let shares = dense::query_shares(
            ordinal,
            table.manifest.row_count,
            2,
            &mut StdRng::seed_from_u64(7),
        )
        .unwrap();
        let answers = shares
            .iter()
            .map(|share| table.evaluate_share(share, &limits).unwrap())
            .collect::<Vec<_>>();
        let strict = dense::combine(&answers).unwrap();
        let decoy = table
            .direct_rows(&[b"a".to_vec(), b"b".to_vec()], &limits)
            .unwrap();
        assert_eq!(strict, decoy[1]);
        assert_eq!(
            table.decode(&strict, b"b").unwrap().unwrap(),
            vec![b"two".to_vec()]
        );
    }

    #[test]
    fn absent_keys_use_a_dummy_row_and_fail_the_fingerprint() {
        let limits = PocLimits::default();
        let table = PrivateTable::build(
            "test",
            vec![(b"present".to_vec(), vec![b"value".to_vec()])],
            1,
            8,
            &limits,
        )
        .unwrap();
        let (ordinal, present) = table.strict_ordinal(b"absent");
        assert!(!present);
        assert!(table
            .decode(table.row(ordinal).unwrap(), b"absent")
            .unwrap()
            .is_none());
    }

    #[test]
    fn directory_and_table_corruption_are_rejected_safely() {
        let limits = PocLimits::default();
        let table = PrivateTable::build(
            "test",
            vec![(b"key".to_vec(), vec![b"value".to_vec()])],
            1,
            8,
            &limits,
        )
        .unwrap();
        let mut image = table.image();
        image.directory.entries[0].ordinal = 99;
        assert!(PrivateTable::from_image(image, &limits).is_err());
    }

    #[test]
    fn immutable_store_round_trip_verifies_manifest_before_use() {
        let key = [5; 32];
        let mut nullifier = [0; 32];
        nullifier[0] = 2;
        let input = UseCaseBuildInput {
            generation_height: 1,
            generation_root_hex: hex::encode([1; 32]),
            nullifiers: vec![NullifierBuildRecord {
                nullifier_hex: hex::encode(nullifier),
                position: 1,
                witness_base64: STANDARD.encode(vec![3; NULLIFIER_WITNESS_BYTES]),
            }],
            encrypted_tags: vec![EncryptedTagBuildRecord {
                tag_base64: STANDARD.encode(b"tag"),
                encrypted_values_base64: vec![STANDARD.encode(b"cipher")],
            }],
            shinzo_bucket_count: 1 << 16,
            limits: PocLimits::default(),
        };
        let store = UseCaseStore::build(input, &key, 0).unwrap();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "defradb-pir-selected-{}-{unique}",
            std::process::id()
        ));
        assert!(!root.exists());
        store.save_immutable(&root, &key, 0).unwrap();
        let loaded = UseCaseStore::load(&root, &key).unwrap();
        assert_eq!(loaded.manifest, store.manifest);
        assert!(UseCaseStore::load(&root, &[6; 32]).is_err());
        std::fs::remove_dir_all(&root).unwrap();
    }
}
