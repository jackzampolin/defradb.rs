//! Exact keyword-to-page ordinals backed by the production PtrHash MPHF.
//!
//! A minimal perfect hash function maps every page key present in one immutable
//! generation to a unique compact ordinal in `0..page_count`. Dense XOR PIR can
//! therefore scan exactly one encoded row per populated page, rather than an
//! over-provisioned hash table or a Fuse retrieval table. The trade-off is public,
//! key-dependent MPHF metadata that every cold client must obtain for the exact
//! immutable generation it queries.
//!
//! An MPHF is not a membership proof: an absent key also maps to some ordinal.
//! Encoded pages carry the existing 128-bit BLAKE3 fingerprint, which the client
//! checks only after privately retrieving the selected row. A false absent-key
//! acceptance therefore requires a 128-bit fingerprint collision.

use std::{
    io::{Cursor, Read},
    mem::size_of,
};

use anyhow::{anyhow, bail, Context, Result};
use epserde::prelude::{Deserialize, Serialize};
use ptr_hash::{bucket_fn::Linear, hash::Xxh3Int, PtrHash, PtrHashParams};

use crate::{
    snapshot::{page_key, Record, SnapshotView},
    tag_pages::{
        benchmark_page_set, decode_page, encode_records, fingerprint, DecodedPage, EncodedPageSet,
        TagPageConfig,
    },
};

const LAYOUT_MAGIC: &[u8; 8] = b"DPIRMPH1";
const LAYOUT_VERSION: u32 = 1;
const KEY_HASH_DOMAIN: &[u8] = b"defradb-pir-mphf-page-key-v1";
const GENERATION_DOMAIN: &[u8] = b"defradb-pir-mphf-generation-v1";
const PUBLIC_METADATA_DOMAIN: &[u8] = b"defradb-pir-mphf-public-metadata-v1";
const MAX_BUILD_ATTEMPTS: u64 = 64;
const ABSENT_KEY_VERIFICATION_BITS: usize = 128;

/// PtrHash's exact, single-part layout with a serializable strong integer hash.
///
/// `REMAP=true` is essential: it guarantees populated rows are exactly
/// `0..page_count`, rather than PtrHash's slightly larger non-minimal range.
type ExactPtrHash = PtrHash<u64, Linear, Vec<u32>, Xxh3Int, Vec<u8>, true, true>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MphfPageManifest {
    pub layout_version: u32,
    pub document_count: usize,
    pub distinct_tag_count: usize,
    pub page_count: usize,
    pub maximum_pages_per_tag: usize,
    pub values_per_page: usize,
    pub max_value_bytes: usize,
    pub page_size: usize,
    pub key_hash_seed: u64,
    /// BLAKE3 commitment to manifest fields, serialized MPHF, and ordered rows.
    pub generation: [u8; 32],
    /// Exact epserde representation distributed as public client metadata.
    pub serialized_mphf_bytes: usize,
    /// Digest appended to the public artifact. Production clients must obtain
    /// this value from an authenticated manifest before parsing the artifact.
    pub public_metadata_digest: [u8; 32],
}

impl MphfPageManifest {
    pub fn generation_hex(&self) -> String {
        hex::encode(self.generation)
    }

    pub fn absent_key_verification_bits(&self) -> usize {
        ABSENT_KEY_VERIFICATION_BITS
    }

    /// Public generation header plus the exact serialized PtrHash structure.
    pub fn client_metadata_bytes(&self) -> usize {
        public_metadata_header_bytes()
            + self.serialized_mphf_bytes
            + self.public_metadata_digest.len()
    }

    pub fn mphf_bits_per_page(&self) -> f64 {
        self.serialized_mphf_bytes as f64 * 8.0 / self.page_count as f64
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MphfBuildMetrics {
    /// Deterministic key-hash / PtrHash attempts. Normally one.
    pub attempts: usize,
    /// Peak explicitly-owned bytes. PtrHash's transient construction workspace
    /// is unavailable through its API and is intentionally called out below.
    pub peak_tracked_bytes: usize,
    pub peak_tracking_note: &'static str,
}

pub struct MphfPageSnapshot {
    pub manifest: MphfPageManifest,
    pub build_metrics: MphfBuildMetrics,
    mphf: ExactPtrHash,
    /// Kept so the benchmark measures the real public artifact, not an estimate.
    /// Production should mmap this artifact and avoid the POC's duplicate live copy.
    serialized_mphf: Box<[u8]>,
    rows: Box<[u8]>,
}

/// Cold-client lookup state loaded from the authenticated public artifact.
/// It contains no tag membership oracle; absent inputs still map to an ordinal.
pub struct MphfClientIndex {
    pub generation: [u8; 32],
    page_count: usize,
    key_hash_seed: u64,
    mphf: ExactPtrHash,
}

impl MphfClientIndex {
    pub fn ordinal(&self, tag: &[u8], page: usize) -> Result<usize> {
        let key = page_key(tag, page)?;
        let ordinal = self.mphf.index(&hash_page_key(&key, self.key_hash_seed));
        if ordinal >= self.page_count {
            bail!("MPHF client index produced an out-of-range ordinal");
        }
        Ok(ordinal)
    }
}

impl MphfPageSnapshot {
    pub fn build(records: Vec<Record>, config: TagPageConfig) -> Result<Self> {
        let page_set = encode_records(records, &config)?;
        Self::from_page_set(&page_set, config)
    }

    pub fn benchmark(
        document_count: usize,
        distinct_tag_count: usize,
        config: TagPageConfig,
    ) -> Result<Self> {
        let page_set = benchmark_page_set(document_count, distinct_tag_count, &config)?;
        Self::from_page_set(&page_set, config)
    }

    pub(crate) fn from_page_set(page_set: &EncodedPageSet, config: TagPageConfig) -> Result<Self> {
        if page_set.pages.is_empty() {
            bail!("MPHF retrieval needs at least one encoded page");
        }
        let page_size = config.page_size()?;
        let page_count = page_set.pages.len();
        let corpus_bytes = page_set.tracked_bytes();

        let mut selected = None;
        let mut collision_peak_bytes = 0;
        for key_hash_seed in 0..MAX_BUILD_ATTEMPTS {
            let keys = page_set
                .pages
                .iter()
                .map(|page| hash_page_key(&page.key, key_hash_seed))
                .collect::<Vec<_>>();
            let mut sorted = keys.clone();
            sorted.sort_unstable();
            collision_peak_bytes = collision_peak_bytes.max(
                corpus_bytes
                    + keys.capacity() * size_of::<u64>()
                    + sorted.capacity() * size_of::<u64>(),
            );
            if sorted.windows(2).any(|pair| pair[0] == pair[1]) {
                continue;
            }
            drop(sorted);

            if let Some(mphf) = ExactPtrHash::try_new(&keys, PtrHashParams::default()) {
                selected = Some((key_hash_seed, keys, mphf));
                break;
            }
        }
        let (key_hash_seed, keys, mphf) = selected.context(
            "could not build a collision-free exact PtrHash MPHF after 64 deterministic attempts",
        )?;

        let mut serialized_mphf = Vec::new();
        // epserde's serialization API is unsafe because its byte layout mirrors
        // Rust types. The input object is owned and valid here. Loading must only
        // accept a trusted, generation-verified artifact of the same build schema.
        unsafe { mphf.serialize(&mut serialized_mphf) }
            .map_err(|error| anyhow!("serialize PtrHash public metadata: {error}"))?;

        let table_bytes = page_count
            .checked_mul(page_size)
            .context("MPHF table size overflow")?;
        let mut rows = vec![0u8; table_bytes];
        let mut occupied = vec![false; page_count];
        for ((page, key), source_index) in page_set.pages.iter().zip(&keys).zip(0..) {
            let ordinal = mphf.index(key);
            if ordinal >= page_count || occupied[ordinal] {
                bail!("PtrHash did not produce a unique compact ordinal");
            }
            occupied[ordinal] = true;
            let start = ordinal * page_size;
            if page.bytes.len() != page_size {
                bail!("encoded page {source_index} has the wrong size");
            }
            rows[start..start + page_size].copy_from_slice(&page.bytes);
        }
        if occupied.iter().any(|occupied| !occupied) {
            bail!("PtrHash compact ordinal range contains an unpopulated row");
        }

        // PtrHash does not expose transient construction allocation. Count the
        // serialized size once as a proxy for its live persistent heap and once
        // for the public artifact that this POC intentionally retains.
        let materialization_bytes = corpus_bytes
            + keys.capacity() * size_of::<u64>()
            + rows.capacity()
            + occupied.capacity() * size_of::<bool>()
            + serialized_mphf.capacity()
            + serialized_mphf.len();

        let generation = generation_digest(
            page_set,
            &config,
            page_size,
            key_hash_seed,
            &serialized_mphf,
            &rows,
        );
        let attempts = usize::try_from(key_hash_seed + 1).context("too many MPHF attempts")?;
        let mut manifest = MphfPageManifest {
            layout_version: LAYOUT_VERSION,
            document_count: page_set.document_count,
            distinct_tag_count: page_set.distinct_tag_count,
            page_count,
            maximum_pages_per_tag: page_set.maximum_pages_per_tag,
            values_per_page: config.values_per_page,
            max_value_bytes: config.max_value_bytes,
            page_size,
            key_hash_seed,
            generation,
            serialized_mphf_bytes: serialized_mphf.len(),
            public_metadata_digest: [0; 32],
        };
        let metadata_prefix = public_metadata_prefix(&manifest, &serialized_mphf);
        manifest.public_metadata_digest = public_metadata_digest(&metadata_prefix);

        Ok(Self {
            manifest,
            build_metrics: MphfBuildMetrics {
                attempts,
                peak_tracked_bytes: collision_peak_bytes.max(materialization_bytes),
                peak_tracking_note: "Deterministic owned corpus, key vectors, rows, verification bitmap, serialized public MPHF, and a serialized-size proxy for the live MPHF. Excludes PtrHash transient construction workspace, allocator metadata, code, and thread stacks.",
            },
            mphf,
            serialized_mphf: serialized_mphf.into_boxed_slice(),
            rows: rows.into_boxed_slice(),
        })
    }

    pub fn rows(&self) -> &[u8] {
        &self.rows
    }

    pub fn view(&self) -> SnapshotView<'_> {
        SnapshotView::new(
            &self.rows,
            self.manifest.page_count,
            self.manifest.page_size,
        )
    }

    pub fn row(&self, ordinal: usize) -> Result<&[u8]> {
        self.view().row(ordinal)
    }

    /// Returns the compact row ordinal for a present or absent page key.
    /// Callers must verify the retrieved row fingerprint before accepting it.
    pub fn ordinal(&self, tag: &[u8], page: usize) -> Result<usize> {
        let key = page_key(tag, page)?;
        Ok(self
            .mphf
            .index(&hash_page_key(&key, self.manifest.key_hash_seed)))
    }

    pub fn decode_retrieved_page(
        &self,
        retrieved: &[u8],
        tag: &[u8],
        page: usize,
    ) -> Result<Option<DecodedPage>> {
        if retrieved.len() != self.manifest.page_size {
            bail!("MPHF answer page has the wrong size");
        }
        let key = page_key(tag, page)?;
        if retrieved[..16] != fingerprint(&key) {
            return Ok(None);
        }
        decode_page(
            retrieved,
            self.manifest.values_per_page,
            self.manifest.max_value_bytes,
        )
        .map(Some)
    }

    pub fn public_lookup(&self, tag: &[u8]) -> Result<Vec<Vec<u8>>> {
        let first = self
            .lookup_page(tag, 0)?
            .context("tag is not present in the MPHF snapshot")?;
        let mut values = first.values;
        for page in 1..first.total_pages {
            values.extend(
                self.lookup_page(tag, page)?
                    .context("MPHF continuation page is missing")?
                    .values,
            );
        }
        Ok(values)
    }

    fn lookup_page(&self, tag: &[u8], page: usize) -> Result<Option<DecodedPage>> {
        let ordinal = self.ordinal(tag, page)?;
        self.decode_retrieved_page(self.row(ordinal)?, tag, page)
    }

    /// Immutable public artifact. It pins dimensions, key hashing, generation,
    /// and the exact epserde PtrHash bytes used to compute client ordinals. The
    /// trailing digest detects corruption but authenticates the artifact only
    /// when its expected value comes from a separately authenticated manifest.
    pub fn public_metadata(&self) -> Vec<u8> {
        let mut output = public_metadata_prefix(&self.manifest, &self.serialized_mphf);
        debug_assert_eq!(
            public_metadata_digest(&output),
            self.manifest.public_metadata_digest
        );
        output.extend_from_slice(&self.manifest.public_metadata_digest);
        debug_assert_eq!(output.len(), self.manifest.client_metadata_bytes());
        output
    }

    /// Loads the exact public PtrHash artifact as a cold client would.
    ///
    /// epserde mirrors Rust memory layouts and therefore marks deserialization
    /// unsafe. This POC accepts only the artifact it just built, checks its
    /// authenticated-manifest digest and all header fields, and then loads it.
    /// Production should replace epserde with a stable, safe, versioned format;
    /// at minimum it must authenticate and size-bound bytes before unsafe load.
    pub fn trusted_client_index(&self) -> Result<MphfClientIndex> {
        client_index_from_trusted_metadata(&self.public_metadata(), &self.manifest)
    }
}

fn public_metadata_prefix(manifest: &MphfPageManifest, serialized_mphf: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(public_metadata_header_bytes() + serialized_mphf.len());
    output.extend_from_slice(LAYOUT_MAGIC);
    output.extend_from_slice(&manifest.layout_version.to_le_bytes());
    for value in [
        manifest.document_count,
        manifest.distinct_tag_count,
        manifest.page_count,
        manifest.maximum_pages_per_tag,
        manifest.values_per_page,
        manifest.max_value_bytes,
        manifest.page_size,
    ] {
        output.extend_from_slice(&(value as u64).to_le_bytes());
    }
    output.extend_from_slice(&manifest.key_hash_seed.to_le_bytes());
    output.extend_from_slice(&manifest.generation);
    output.extend_from_slice(&(serialized_mphf.len() as u64).to_le_bytes());
    output.extend_from_slice(&(KEY_HASH_DOMAIN.len() as u16).to_le_bytes());
    output.extend_from_slice(KEY_HASH_DOMAIN);
    output.extend_from_slice(serialized_mphf);
    output
}

fn public_metadata_digest(metadata_prefix: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(PUBLIC_METADATA_DOMAIN);
    hasher.update(&(metadata_prefix.len() as u64).to_le_bytes());
    hasher.update(metadata_prefix);
    *hasher.finalize().as_bytes()
}

fn client_index_from_trusted_metadata(
    metadata: &[u8],
    expected: &MphfPageManifest,
) -> Result<MphfClientIndex> {
    let prefix_len = metadata
        .len()
        .checked_sub(32)
        .context("MPHF public metadata is truncated")?;
    let (prefix, embedded_digest) = metadata.split_at(prefix_len);
    if embedded_digest != expected.public_metadata_digest {
        bail!("MPHF public metadata digest does not match the authenticated manifest");
    }
    if public_metadata_digest(prefix) != expected.public_metadata_digest {
        bail!("MPHF public metadata failed its digest check");
    }

    let mut input = Cursor::new(prefix);
    let mut magic = [0u8; 8];
    input.read_exact(&mut magic)?;
    if &magic != LAYOUT_MAGIC {
        bail!("MPHF public metadata has the wrong magic");
    }
    let layout_version = read_u32(&mut input)?;
    if layout_version != expected.layout_version || layout_version != LAYOUT_VERSION {
        bail!("unsupported MPHF public metadata version {layout_version}");
    }
    let expected_dimensions = [
        expected.document_count,
        expected.distinct_tag_count,
        expected.page_count,
        expected.maximum_pages_per_tag,
        expected.values_per_page,
        expected.max_value_bytes,
        expected.page_size,
    ];
    for expected_value in expected_dimensions {
        if read_usize(&mut input)? != expected_value {
            bail!("MPHF public metadata dimensions do not match the authenticated manifest");
        }
    }
    let key_hash_seed = read_u64(&mut input)?;
    if key_hash_seed != expected.key_hash_seed {
        bail!("MPHF public metadata seed does not match the authenticated manifest");
    }
    let mut generation = [0u8; 32];
    input.read_exact(&mut generation)?;
    if generation != expected.generation {
        bail!("MPHF public metadata generation does not match the authenticated manifest");
    }
    let serialized_len = read_usize(&mut input)?;
    if serialized_len != expected.serialized_mphf_bytes {
        bail!("MPHF serialized index length does not match the authenticated manifest");
    }
    let domain_len = usize::from(read_u16(&mut input)?);
    if domain_len != KEY_HASH_DOMAIN.len() {
        bail!("MPHF public metadata has an unsupported key-hash domain length");
    }
    let mut key_hash_domain = vec![0u8; domain_len];
    input.read_exact(&mut key_hash_domain)?;
    if key_hash_domain != KEY_HASH_DOMAIN {
        bail!("MPHF public metadata has an unsupported key-hash domain");
    }
    let serialized_start = usize::try_from(input.position()).context("metadata offset overflow")?;
    let serialized_end = serialized_start
        .checked_add(serialized_len)
        .context("MPHF serialized index range overflow")?;
    if serialized_end != prefix.len() {
        bail!("MPHF public metadata index length does not consume the artifact");
    }

    let mut serialized_input = Cursor::new(&prefix[serialized_start..serialized_end]);
    // SAFETY: `metadata` is the POC's own just-serialized artifact. Before this
    // point it is bounded, digest-checked against the expected manifest, and its
    // schema fields are validated. epserde is not a safe untrusted wire format;
    // production must use a safe canonical encoding or retain this trust boundary.
    let mphf = unsafe { ExactPtrHash::deserialize_full(&mut serialized_input) }
        .map_err(|error| anyhow!("deserialize trusted PtrHash client metadata: {error}"))?;
    if usize::try_from(serialized_input.position()).context("metadata offset overflow")?
        != serialized_len
    {
        bail!("PtrHash deserializer did not consume the complete serialized index");
    }
    if mphf.n() != expected.page_count || mphf.max_index() != expected.page_count {
        bail!("PtrHash dimensions do not match the authenticated manifest");
    }
    Ok(MphfClientIndex {
        generation,
        page_count: expected.page_count,
        key_hash_seed,
        mphf,
    })
}

fn read_u16(input: &mut Cursor<&[u8]>) -> Result<u16> {
    let mut bytes = [0u8; 2];
    input.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(input: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut bytes = [0u8; 4];
    input.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(input: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut bytes = [0u8; 8];
    input.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_usize(input: &mut Cursor<&[u8]>) -> Result<usize> {
    usize::try_from(read_u64(input)?).context("MPHF metadata value does not fit this platform")
}

fn public_metadata_header_bytes() -> usize {
    LAYOUT_MAGIC.len()
        + size_of::<u32>()
        + 7 * size_of::<u64>()
        + size_of::<u64>()
        + 32
        + size_of::<u64>()
        + size_of::<u16>()
        + KEY_HASH_DOMAIN.len()
}

fn hash_page_key(key: &[u8], seed: u64) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(KEY_HASH_DOMAIN);
    hasher.update(&seed.to_le_bytes());
    hasher.update(key);
    u64::from_le_bytes(
        hasher.finalize().as_bytes()[..8]
            .try_into()
            .expect("fixed hash"),
    )
}

fn generation_digest(
    page_set: &EncodedPageSet,
    config: &TagPageConfig,
    page_size: usize,
    key_hash_seed: u64,
    serialized_mphf: &[u8],
    rows: &[u8],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(GENERATION_DOMAIN);
    hasher.update(&LAYOUT_VERSION.to_le_bytes());
    for value in [
        page_set.document_count,
        page_set.distinct_tag_count,
        page_set.pages.len(),
        page_set.maximum_pages_per_tag,
        config.values_per_page,
        config.max_value_bytes,
        page_size,
    ] {
        hasher.update(&(value as u64).to_le_bytes());
    }
    hasher.update(&key_hash_seed.to_le_bytes());
    hasher.update(&(serialized_mphf.len() as u64).to_le_bytes());
    hasher.update(serialized_mphf);
    hasher.update(&(rows.len() as u64).to_le_bytes());
    hasher.update(rows);
    *hasher.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use rand::{rngs::StdRng, SeedableRng};

    use super::*;
    use crate::dense;

    fn config() -> TagPageConfig {
        TagPageConfig {
            bucket_capacity: 4,
            target_load_percent: 90,
            values_per_page: 3,
            max_value_bytes: 16,
        }
    }

    #[test]
    fn exact_ordinals_are_deterministic_and_recover_all_values() {
        let records = vec![
            Record::new("music", "c"),
            Record::new("sports", "one"),
            Record::new("music", "a"),
            Record::new("music", "d"),
            Record::new("music", "b"),
        ];
        let first = MphfPageSnapshot::build(records.clone(), config()).unwrap();
        let second =
            MphfPageSnapshot::build(records.into_iter().rev().collect(), config()).unwrap();

        assert_eq!(first.rows(), second.rows());
        assert_eq!(first.public_metadata(), second.public_metadata());
        assert_eq!(first.manifest.generation, second.manifest.generation);
        assert_eq!(
            first.public_lookup(b"music").unwrap(),
            [b"a", b"b", b"c", b"d"].map(|value| value.to_vec())
        );
        assert_eq!(
            first.public_metadata().len(),
            first.manifest.client_metadata_bytes()
        );
        let public_metadata = first.public_metadata();
        assert_eq!(
            &public_metadata[public_metadata.len() - 32..],
            first.manifest.public_metadata_digest
        );
        let client = first.trusted_client_index().unwrap();
        assert_eq!(client.generation, first.manifest.generation);
        assert_eq!(
            client.ordinal(b"music", 0).unwrap(),
            first.ordinal(b"music", 0).unwrap()
        );
    }

    #[test]
    fn absent_key_is_rejected_after_private_retrieval() {
        let snapshot = MphfPageSnapshot::build(
            vec![Record::new("known", "value"), Record::new("other", "x")],
            config(),
        )
        .unwrap();
        let absent_ordinal = snapshot.ordinal(b"absent", 0).unwrap();
        assert!(snapshot
            .decode_retrieved_page(snapshot.row(absent_ordinal).unwrap(), b"absent", 0)
            .unwrap()
            .is_none());
        assert!(snapshot.public_lookup(b"absent").is_err());
        assert_eq!(snapshot.manifest.absent_key_verification_bits(), 128);
    }

    #[test]
    fn dense_two_and_three_server_queries_recover_exact_rows() {
        let snapshot = MphfPageSnapshot::benchmark(256, 64, config()).unwrap();
        let tag = crate::tag_pages::benchmark_tag(37);
        let ordinal = snapshot.ordinal(&tag, 0).unwrap();
        for server_count in [2, 3] {
            let shares = dense::query_shares(
                ordinal,
                snapshot.manifest.page_count,
                server_count,
                &mut StdRng::seed_from_u64(server_count as u64),
            )
            .unwrap();
            let answers = shares
                .iter()
                .map(|share| dense::answer(snapshot.view(), share).unwrap())
                .collect::<Vec<_>>();
            let retrieved = dense::combine(&answers).unwrap();
            assert!(snapshot
                .decode_retrieved_page(&retrieved, &tag, 0)
                .unwrap()
                .is_some());
        }
    }

    #[test]
    fn populated_keys_form_a_compact_permutation_across_corpora() {
        for distinct_tags in [1, 2, 7, 31, 127] {
            let snapshot =
                MphfPageSnapshot::benchmark(distinct_tags * 7, distinct_tags, config()).unwrap();
            let mut ordinals = Vec::new();
            for tag_index in 0..distinct_tags {
                let tag = crate::tag_pages::benchmark_tag(tag_index);
                for page in 0..snapshot.manifest.maximum_pages_per_tag {
                    ordinals.push(snapshot.ordinal(&tag, page).unwrap());
                }
            }
            ordinals.sort_unstable();
            ordinals.dedup();
            assert_eq!(
                ordinals,
                (0..snapshot.manifest.page_count).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn generation_changes_when_rows_change() {
        let first = MphfPageSnapshot::build(vec![Record::new("tag", "a")], config()).unwrap();
        let second = MphfPageSnapshot::build(vec![Record::new("tag", "b")], config()).unwrap();
        assert_ne!(first.manifest.generation, second.manifest.generation);
        assert_ne!(
            first.manifest.public_metadata_digest,
            second.manifest.public_metadata_digest
        );
    }

    #[test]
    fn corrupted_public_artifact_is_rejected_before_ptrhash_load() {
        let snapshot = MphfPageSnapshot::build(
            vec![Record::new("known", "value"), Record::new("other", "x")],
            config(),
        )
        .unwrap();
        let mut metadata = snapshot.public_metadata();
        let serialized_byte = public_metadata_header_bytes();
        metadata[serialized_byte] ^= 1;
        let error = client_index_from_trusted_metadata(&metadata, &snapshot.manifest)
            .err()
            .expect("corrupted artifact must fail validation");
        assert!(error.to_string().contains("digest check"));
    }
}
