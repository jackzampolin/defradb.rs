//! Immutable base/delta storage for Shieldd's live indexed-nullifier generation.
//!
//! The active generation changes every committed block, so rebuilding the flat
//! padded radix table measured by `bench-active-nullifier` is not acceptable.
//! This module keeps one immutable base plus geometrically merged immutable
//! deltas.  Readers pin an `Arc<ActiveGeneration>`; publication swaps that Arc
//! only after the new height, root, body digest, and operator MAC are complete.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use anyhow::{bail, Context, Result};
use poseidon377::Fq;
use serde::{Deserialize, Serialize};

pub const TREE_DEPTH: usize = 20;
pub const TREE_ARITY: usize = 4;
pub const SIBLINGS_PER_WITNESS: usize = TREE_DEPTH * (TREE_ARITY - 1);
pub const HASH_BYTES: usize = 32;
pub const LEAF_PAYLOAD_BYTES: usize = 88;
pub const NODE_PAYLOAD_BYTES: usize = 41;
const FORMAT_VERSION: u32 = 1;
const DELTA_DOMAIN: &[u8] = b"defradb-pir-active-delta-v1";
const BASE_DOMAIN: &[u8] = b"defradb-pir-active-base-v1";
const GENERATION_DOMAIN: &[u8] = b"defradb-pir-active-generation-v1";
const MANIFEST_MAC_DOMAIN: &[u8] = b"defradb-pir-active-manifest-mac-v1";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct ActiveLeaf {
    pub value: [u8; 32],
    pub position: u64,
    pub next_index: u64,
    pub next_value: [u8; 32],
    pub sentinel: bool,
    pub terminal: bool,
}

impl ActiveLeaf {
    pub fn encoded_payload_bytes(&self) -> usize {
        LEAF_PAYLOAD_BYTES
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NodeCoordinate {
    pub level: u8,
    pub position: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChangedNode {
    pub coordinate: NodeCoordinate,
    pub hash: [u8; HASH_BYTES],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveGenerationLimits {
    pub max_delta_levels: usize,
    pub max_leaf_mutations_per_block: usize,
    pub max_node_mutations_per_block: usize,
    pub max_total_payload_bytes: usize,
}

impl Default for ActiveGenerationLimits {
    fn default() -> Self {
        Self {
            max_delta_levels: 8,
            max_leaf_mutations_per_block: 65_536,
            max_node_mutations_per_block: 2_000_000,
            max_total_payload_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

impl ActiveGenerationLimits {
    fn validate(&self) -> Result<()> {
        if self.max_delta_levels == 0
            || self.max_leaf_mutations_per_block == 0
            || self.max_node_mutations_per_block == 0
            || self.max_total_payload_bytes == 0
        {
            bail!("active-generation limits must all be non-zero");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveGenerationManifest {
    pub format_version: u32,
    pub height: u64,
    pub root: [u8; 32],
    pub base_digest: [u8; 32],
    pub delta_digests: Vec<Option<[u8; 32]>>,
    pub body_digest: [u8; 32],
    pub tree_depth: usize,
    pub tree_arity: usize,
    pub fixed_predecessor_probes: usize,
    pub fixed_node_probes: usize,
}

impl ActiveGenerationManifest {
    pub fn validate(&self, limits: &ActiveGenerationLimits) -> Result<()> {
        limits.validate()?;
        if self.format_version != FORMAT_VERSION {
            bail!("unsupported active-generation format version");
        }
        if self.tree_depth != TREE_DEPTH || self.tree_arity != TREE_ARITY {
            bail!("active-generation tree shape does not match Shieldd");
        }
        if self.delta_digests.len() != limits.max_delta_levels {
            bail!("active-generation delta schedule has the wrong length");
        }
        if self.fixed_predecessor_probes != limits.max_delta_levels + 1 {
            bail!("active-generation predecessor schedule is not fixed");
        }
        let expected_node_probes = (limits.max_delta_levels + 1)
            .checked_mul(SIBLINGS_PER_WITNESS)
            .context("active-generation node schedule overflow")?;
        if self.fixed_node_probes != expected_node_probes {
            bail!("active-generation node schedule is not fixed");
        }
        Ok(())
    }
}

/// Symmetric authentication for a POC deployment.  Replicas and clients must
/// receive `operator_key` out of band.  Production can replace this field with
/// a signature without changing the canonical manifest body.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthenticatedGenerationManifest {
    pub manifest: ActiveGenerationManifest,
    pub mac: [u8; 32],
}

impl AuthenticatedGenerationManifest {
    pub fn verify(&self, operator_key: &[u8; 32], limits: &ActiveGenerationLimits) -> Result<()> {
        self.manifest.validate(limits)?;
        let expected = manifest_mac(operator_key, &self.manifest)?;
        if expected != self.mac {
            bail!("active-generation manifest authentication failed");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct GenerationDelta {
    height: u64,
    root: [u8; 32],
    leaves: Vec<ActiveLeaf>,
    nodes: Vec<ChangedNode>,
    digest: [u8; 32],
}

impl GenerationDelta {
    fn build(
        height: u64,
        root: [u8; 32],
        leaves: Vec<ActiveLeaf>,
        nodes: Vec<ChangedNode>,
    ) -> Result<Self> {
        let leaves = normalize_leaves(leaves)?;
        let nodes = normalize_nodes(nodes)?;
        let digest = delta_digest(height, &root, &leaves, &nodes);
        Ok(Self {
            height,
            root,
            leaves,
            nodes,
            digest,
        })
    }

    fn payload_bytes(&self) -> Result<usize> {
        payload_bytes(self.leaves.len(), self.nodes.len())
    }
}

#[derive(Clone, Debug)]
pub struct ActiveGeneration {
    pub manifest: ActiveGenerationManifest,
    pub limits: ActiveGenerationLimits,
    base_leaves: Arc<[ActiveLeaf]>,
    base_nodes: Arc<BTreeMap<NodeCoordinate, [u8; HASH_BYTES]>>,
    levels: Arc<[Option<Arc<GenerationDelta>>]>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveGenerationImage {
    pub authenticated_manifest: AuthenticatedGenerationManifest,
    pub limits: ActiveGenerationLimits,
    pub base_leaves: Vec<ActiveLeaf>,
    pub base_nodes: Vec<ChangedNode>,
    pub delta_levels: Vec<Option<ActiveDeltaImage>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveDeltaImage {
    pub height: u64,
    pub root: [u8; 32],
    pub leaves: Vec<ActiveLeaf>,
    pub nodes: Vec<ChangedNode>,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActiveUpdateMetrics {
    pub inserted_leaf_mutations: usize,
    pub inserted_node_mutations: usize,
    pub immutable_delta_payload_bytes: usize,
    pub occupied_delta_levels: usize,
    pub merged_levels: usize,
    pub compacted_into_base: bool,
    pub total_payload_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FixedScheduleLookup {
    pub leaf: ActiveLeaf,
    pub exact: bool,
    pub predecessor_probes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveWitness {
    pub leaf: ActiveLeaf,
    pub siblings: Vec<[u8; HASH_BYTES]>,
    pub predecessor_probes: usize,
    pub node_probes: usize,
}

impl ActiveGeneration {
    pub fn build_base(
        height: u64,
        root: [u8; 32],
        leaves: Vec<ActiveLeaf>,
        nodes: Vec<ChangedNode>,
        limits: ActiveGenerationLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let base_leaves = normalize_leaves(leaves)?;
        validate_sentinel(&base_leaves)?;
        let base_nodes = nodes_to_map(normalize_nodes(nodes)?)?;
        let levels = vec![None; limits.max_delta_levels];
        let generation = Self::from_parts(height, root, base_leaves, base_nodes, levels, limits)?;
        if generation.payload_bytes()? > generation.limits.max_total_payload_bytes {
            bail!("active-generation base exceeds configured storage limit");
        }
        Ok(generation)
    }

    pub fn apply_block(
        &self,
        height: u64,
        root: [u8; 32],
        leaf_mutations: Vec<ActiveLeaf>,
        node_mutations: Vec<ChangedNode>,
    ) -> Result<(Self, ActiveUpdateMetrics)> {
        if height <= self.manifest.height {
            bail!("active-generation block height must increase");
        }
        if leaf_mutations.is_empty() {
            bail!("active-generation block must contain at least one leaf mutation");
        }
        if leaf_mutations.len() > self.limits.max_leaf_mutations_per_block
            || node_mutations.len() > self.limits.max_node_mutations_per_block
        {
            bail!("active-generation block exceeds mutation admission limits");
        }

        let inserted_leaf_mutations = leaf_mutations.len();
        let inserted_node_mutations = node_mutations.len();
        let initial = GenerationDelta::build(height, root, leaf_mutations, node_mutations)?;
        let immutable_delta_payload_bytes = initial.payload_bytes()?;
        let mut incoming = Arc::new(initial);
        let mut levels = self.levels.to_vec();
        let mut merged_levels = 0;
        let mut compacted_into_base = false;
        let mut base_leaves = self.base_leaves.to_vec();
        let mut base_nodes = (*self.base_nodes).clone();

        for slot in &mut levels {
            match slot.take() {
                None => {
                    *slot = Some(incoming);
                    incoming = Arc::new(GenerationDelta::build(
                        height,
                        root,
                        Vec::new(),
                        Vec::new(),
                    )?);
                    break;
                }
                Some(older) => {
                    incoming = Arc::new(merge_deltas(&older, &incoming)?);
                    merged_levels += 1;
                }
            }
        }
        if !incoming.leaves.is_empty() || !incoming.nodes.is_empty() {
            base_leaves = merge_leaf_sets(&base_leaves, &incoming.leaves)?;
            for node in &incoming.nodes {
                base_nodes.insert(node.coordinate, node.hash);
            }
            compacted_into_base = true;
        }
        validate_sentinel(&base_leaves)?;

        let next = Self::from_parts(
            height,
            root,
            base_leaves,
            base_nodes,
            levels,
            self.limits.clone(),
        )?;
        let total_payload_bytes = next.payload_bytes()?;
        if total_payload_bytes > self.limits.max_total_payload_bytes {
            bail!("active-generation payload exceeds configured storage limit");
        }
        let occupied_delta_levels = next.levels.iter().filter(|level| level.is_some()).count();
        Ok((
            next,
            ActiveUpdateMetrics {
                inserted_leaf_mutations,
                inserted_node_mutations,
                immutable_delta_payload_bytes,
                occupied_delta_levels,
                merged_levels,
                compacted_into_base,
                total_payload_bytes,
            },
        ))
    }

    pub fn lookup_at_or_before(&self, target: &[u8; 32]) -> Result<FixedScheduleLookup> {
        let mut candidate =
            predecessor_in(&self.base_leaves, target, true)?.map(|leaf| (leaf, usize::MAX));
        for (level_index, level) in self.levels.iter().enumerate() {
            let level_candidate = match level {
                Some(delta) => predecessor_in(&delta.leaves, target, true)?,
                None => None,
            };
            candidate =
                select_candidate(candidate, level_candidate.map(|leaf| (leaf, level_index)))?;
        }
        let leaf = candidate
            .map(|candidate| candidate.0)
            .context("active-generation lookup has no lower sentinel")?;
        Ok(FixedScheduleLookup {
            exact: leaf.value == *target,
            leaf,
            predecessor_probes: self.limits.max_delta_levels + 1,
        })
    }

    pub fn strict_predecessor(&self, target: &[u8; 32]) -> Result<FixedScheduleLookup> {
        let mut candidate =
            predecessor_in(&self.base_leaves, target, false)?.map(|leaf| (leaf, usize::MAX));
        for (level_index, level) in self.levels.iter().enumerate() {
            let level_candidate = match level {
                Some(delta) => predecessor_in(&delta.leaves, target, false)?,
                None => None,
            };
            candidate =
                select_candidate(candidate, level_candidate.map(|leaf| (leaf, level_index)))?;
        }
        Ok(FixedScheduleLookup {
            leaf: candidate
                .map(|candidate| candidate.0)
                .context("active-generation lookup has no strict predecessor")?,
            exact: false,
            predecessor_probes: self.limits.max_delta_levels + 1,
        })
    }

    pub fn witness(&self, target: &[u8; 32]) -> Result<ActiveWitness> {
        let lookup = self.lookup_at_or_before(target)?;
        let mut position = lookup.leaf.position;
        let mut siblings = Vec::with_capacity(SIBLINGS_PER_WITNESS);
        let mut node_probes = 0;
        for level in 0..TREE_DEPTH {
            let child = position % TREE_ARITY as u64;
            let base = position / TREE_ARITY as u64 * TREE_ARITY as u64;
            for sibling in 0..TREE_ARITY as u64 {
                if sibling == child {
                    continue;
                }
                let coordinate = NodeCoordinate {
                    level: u8::try_from(level)?,
                    position: base + sibling,
                };
                let mut resolved = self
                    .base_nodes
                    .get(&coordinate)
                    .copied()
                    .map(|hash| (hash, usize::MAX));
                node_probes += 1;
                for (level_index, delta) in self.levels.iter().enumerate() {
                    node_probes += 1;
                    if let Some(hash) = delta.as_ref().and_then(|delta| {
                        delta
                            .nodes
                            .binary_search_by_key(&coordinate, |node| node.coordinate)
                            .ok()
                            .map(|index| delta.nodes[index].hash)
                    }) {
                        if resolved
                            .as_ref()
                            .is_none_or(|(_, rank)| level_index < *rank)
                        {
                            resolved = Some((hash, level_index));
                        }
                    }
                }
                siblings.push(resolved.map_or([0; HASH_BYTES], |resolved| resolved.0));
            }
            position /= TREE_ARITY as u64;
        }
        debug_assert_eq!(siblings.len(), SIBLINGS_PER_WITNESS);
        debug_assert_eq!(
            node_probes,
            (self.limits.max_delta_levels + 1) * SIBLINGS_PER_WITNESS
        );
        Ok(ActiveWitness {
            leaf: lookup.leaf,
            siblings,
            predecessor_probes: lookup.predecessor_probes,
            node_probes,
        })
    }

    pub fn authenticated_manifest(
        &self,
        operator_key: &[u8; 32],
    ) -> Result<AuthenticatedGenerationManifest> {
        Ok(AuthenticatedGenerationManifest {
            manifest: self.manifest.clone(),
            mac: manifest_mac(operator_key, &self.manifest)?,
        })
    }

    pub fn image(&self, operator_key: &[u8; 32]) -> Result<ActiveGenerationImage> {
        Ok(ActiveGenerationImage {
            authenticated_manifest: self.authenticated_manifest(operator_key)?,
            limits: self.limits.clone(),
            base_leaves: self.base_leaves.to_vec(),
            base_nodes: self
                .base_nodes
                .iter()
                .map(|(coordinate, hash)| ChangedNode {
                    coordinate: *coordinate,
                    hash: *hash,
                })
                .collect(),
            delta_levels: self
                .levels
                .iter()
                .map(|level| {
                    level.as_ref().map(|delta| ActiveDeltaImage {
                        height: delta.height,
                        root: delta.root,
                        leaves: delta.leaves.clone(),
                        nodes: delta.nodes.clone(),
                        digest: delta.digest,
                    })
                })
                .collect(),
        })
    }

    pub fn from_image(image: ActiveGenerationImage, operator_key: &[u8; 32]) -> Result<Self> {
        image
            .authenticated_manifest
            .verify(operator_key, &image.limits)?;
        if image.delta_levels.len() != image.limits.max_delta_levels {
            bail!("active-generation image has the wrong delta level count");
        }
        let mut levels = Vec::with_capacity(image.delta_levels.len());
        for level in image.delta_levels {
            let delta = match level {
                None => None,
                Some(level) => {
                    let rebuilt = GenerationDelta::build(
                        level.height,
                        level.root,
                        level.leaves,
                        level.nodes,
                    )?;
                    if rebuilt.digest != level.digest {
                        bail!("active-generation delta digest mismatch");
                    }
                    Some(Arc::new(rebuilt))
                }
            };
            levels.push(delta);
        }
        let base_leaves = normalize_leaves(image.base_leaves)?;
        validate_sentinel(&base_leaves)?;
        let base_nodes = nodes_to_map(normalize_nodes(image.base_nodes)?)?;
        let rebuilt = Self::from_parts(
            image.authenticated_manifest.manifest.height,
            image.authenticated_manifest.manifest.root,
            base_leaves,
            base_nodes,
            levels,
            image.limits,
        )?;
        if rebuilt.manifest != image.authenticated_manifest.manifest {
            bail!("active-generation body does not match authenticated manifest");
        }
        if rebuilt.payload_bytes()? > rebuilt.limits.max_total_payload_bytes {
            bail!("active-generation image exceeds configured storage limit");
        }
        Ok(rebuilt)
    }

    pub fn payload_bytes(&self) -> Result<usize> {
        let mut total = payload_bytes(self.base_leaves.len(), self.base_nodes.len())?;
        for delta in self.levels.iter().flatten() {
            total = total
                .checked_add(delta.payload_bytes()?)
                .context("active-generation payload overflow")?;
        }
        Ok(total)
    }

    fn from_parts(
        height: u64,
        root: [u8; 32],
        base_leaves: Vec<ActiveLeaf>,
        base_nodes: BTreeMap<NodeCoordinate, [u8; HASH_BYTES]>,
        levels: Vec<Option<Arc<GenerationDelta>>>,
        limits: ActiveGenerationLimits,
    ) -> Result<Self> {
        if levels.len() != limits.max_delta_levels {
            bail!("active-generation level count does not match limits");
        }
        let base_digest = base_digest(&base_leaves, &base_nodes);
        let delta_digests = levels
            .iter()
            .map(|level| level.as_ref().map(|delta| delta.digest))
            .collect::<Vec<_>>();
        let body_digest = generation_digest(height, &root, &base_digest, &delta_digests);
        let manifest = ActiveGenerationManifest {
            format_version: FORMAT_VERSION,
            height,
            root,
            base_digest,
            delta_digests,
            body_digest,
            tree_depth: TREE_DEPTH,
            tree_arity: TREE_ARITY,
            fixed_predecessor_probes: limits.max_delta_levels + 1,
            fixed_node_probes: (limits.max_delta_levels + 1) * SIBLINGS_PER_WITNESS,
        };
        manifest.validate(&limits)?;
        Ok(Self {
            manifest,
            limits,
            base_leaves: base_leaves.into(),
            base_nodes: Arc::new(base_nodes),
            levels: levels.into(),
        })
    }
}

/// Readers never observe a partially built generation.  Construct and verify a
/// new `ActiveGeneration`, then replace the pinned Arc under one short lock.
#[derive(Clone, Debug)]
pub struct ActiveGenerationPublisher {
    current: Arc<RwLock<Arc<ActiveGeneration>>>,
}

impl ActiveGenerationPublisher {
    pub fn new(initial: ActiveGeneration) -> Self {
        Self {
            current: Arc::new(RwLock::new(Arc::new(initial))),
        }
    }

    pub fn pin(&self) -> Result<Arc<ActiveGeneration>> {
        self.current
            .read()
            .map(|generation| Arc::clone(&generation))
            .map_err(|_| anyhow::anyhow!("active-generation publication lock is poisoned"))
    }

    pub fn publish(
        &self,
        next: ActiveGeneration,
        authenticated: &AuthenticatedGenerationManifest,
        operator_key: &[u8; 32],
    ) -> Result<()> {
        authenticated.verify(operator_key, &next.limits)?;
        if authenticated.manifest != next.manifest {
            bail!("published active generation does not match authenticated manifest");
        }
        let current = self.pin()?;
        if next.manifest.height <= current.manifest.height {
            bail!("active-generation publication must advance height");
        }
        *self
            .current
            .write()
            .map_err(|_| anyhow::anyhow!("active-generation publication lock is poisoned"))? =
            Arc::new(next);
        Ok(())
    }
}

/// Returns the same fixed-width big-endian ordering key used by Shieldd's
/// `FqOrdKey`. Nullifiers are encoded little-endian on the wire, so comparing
/// their encoded bytes directly is not field order.
pub(crate) fn nullifier_order_key(value: &[u8; 32]) -> Result<[u8; 32]> {
    let field = Fq::from_bytes_checked(value).map_err(|_| {
        anyhow::anyhow!("active-generation nullifier is not a canonical field element")
    })?;
    let mut key = field.to_bytes();
    key.reverse();
    Ok(key)
}

fn normalize_leaves(leaves: Vec<ActiveLeaf>) -> Result<Vec<ActiveLeaf>> {
    let mut by_value = BTreeMap::new();
    for leaf in leaves {
        by_value.insert(nullifier_order_key(&leaf.value)?, leaf);
    }
    Ok(by_value.into_values().collect())
}

fn normalize_nodes(mut nodes: Vec<ChangedNode>) -> Result<Vec<ChangedNode>> {
    if nodes
        .iter()
        .any(|node| usize::from(node.coordinate.level) >= TREE_DEPTH)
    {
        bail!("active-generation node level exceeds Shieldd depth");
    }
    let mut by_coordinate = BTreeMap::new();
    for node in nodes.drain(..) {
        by_coordinate.insert(node.coordinate, node);
    }
    Ok(by_coordinate.into_values().collect())
}

fn nodes_to_map(nodes: Vec<ChangedNode>) -> Result<BTreeMap<NodeCoordinate, [u8; HASH_BYTES]>> {
    let mut output = BTreeMap::new();
    for node in nodes {
        if output.insert(node.coordinate, node.hash).is_some() {
            bail!("active-generation contains a duplicate node coordinate");
        }
    }
    Ok(output)
}

fn validate_sentinel(leaves: &[ActiveLeaf]) -> Result<()> {
    let sentinel = leaves
        .first()
        .context("active-generation base must contain a lower sentinel")?;
    if sentinel.value != [0; 32] || !sentinel.sentinel {
        bail!("active-generation first leaf must be the zero lower sentinel");
    }
    Ok(())
}

fn merge_deltas(older: &GenerationDelta, newer: &GenerationDelta) -> Result<GenerationDelta> {
    let leaves = merge_leaf_sets(&older.leaves, &newer.leaves)?;
    let mut nodes = older
        .nodes
        .iter()
        .map(|node| (node.coordinate, *node))
        .collect::<BTreeMap<_, _>>();
    for node in &newer.nodes {
        nodes.insert(node.coordinate, *node);
    }
    GenerationDelta::build(
        newer.height,
        newer.root,
        leaves,
        nodes.into_values().collect(),
    )
}

fn merge_leaf_sets(older: &[ActiveLeaf], newer: &[ActiveLeaf]) -> Result<Vec<ActiveLeaf>> {
    let mut leaves = older
        .iter()
        .map(|leaf| Ok((nullifier_order_key(&leaf.value)?, *leaf)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    for leaf in newer {
        leaves.insert(nullifier_order_key(&leaf.value)?, *leaf);
    }
    Ok(leaves.into_values().collect())
}

fn predecessor_in(
    leaves: &[ActiveLeaf],
    target: &[u8; 32],
    include_equal: bool,
) -> Result<Option<ActiveLeaf>> {
    let target = nullifier_order_key(target)?;
    let mut left = 0;
    let mut right = leaves.len();
    while left < right {
        let middle = left + (right - left) / 2;
        let candidate = nullifier_order_key(&leaves[middle].value)?;
        if candidate < target || (include_equal && candidate == target) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    Ok(left.checked_sub(1).map(|index| leaves[index]))
}

fn select_candidate(
    left: Option<(ActiveLeaf, usize)>,
    right: Option<(ActiveLeaf, usize)>,
) -> Result<Option<(ActiveLeaf, usize)>> {
    Ok(match (left, right) {
        (None, candidate) | (candidate, None) => candidate,
        (Some(left), Some(right))
            if nullifier_order_key(&right.0.value)? > nullifier_order_key(&left.0.value)?
                || (right.0.value == left.0.value && right.1 < left.1) =>
        {
            Some(right)
        }
        (Some(left), Some(_)) => Some(left),
    })
}

fn payload_bytes(leaves: usize, nodes: usize) -> Result<usize> {
    leaves
        .checked_mul(LEAF_PAYLOAD_BYTES)
        .and_then(|bytes| bytes.checked_add(nodes.checked_mul(NODE_PAYLOAD_BYTES)?))
        .context("active-generation payload byte count overflow")
}

fn delta_digest(
    height: u64,
    root: &[u8; 32],
    leaves: &[ActiveLeaf],
    nodes: &[ChangedNode],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DELTA_DOMAIN);
    hasher.update(&height.to_le_bytes());
    hasher.update(root);
    hash_leaves(&mut hasher, leaves);
    hash_nodes(&mut hasher, nodes.iter().copied());
    *hasher.finalize().as_bytes()
}

fn base_digest(
    leaves: &[ActiveLeaf],
    nodes: &BTreeMap<NodeCoordinate, [u8; HASH_BYTES]>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(BASE_DOMAIN);
    hash_leaves(&mut hasher, leaves);
    hash_nodes(
        &mut hasher,
        nodes.iter().map(|(coordinate, hash)| ChangedNode {
            coordinate: *coordinate,
            hash: *hash,
        }),
    );
    *hasher.finalize().as_bytes()
}

fn hash_leaves(hasher: &mut blake3::Hasher, leaves: &[ActiveLeaf]) {
    hasher.update(&(leaves.len() as u64).to_le_bytes());
    for leaf in leaves {
        hasher.update(&leaf.value);
        hasher.update(&leaf.position.to_le_bytes());
        hasher.update(&leaf.next_index.to_le_bytes());
        hasher.update(&leaf.next_value);
        hasher.update(&[leaf.sentinel as u8, leaf.terminal as u8]);
    }
}

fn hash_nodes(hasher: &mut blake3::Hasher, nodes: impl IntoIterator<Item = ChangedNode>) {
    let nodes = nodes.into_iter().collect::<Vec<_>>();
    hasher.update(&(nodes.len() as u64).to_le_bytes());
    for node in nodes {
        hasher.update(&[node.coordinate.level]);
        hasher.update(&node.coordinate.position.to_le_bytes());
        hasher.update(&node.hash);
    }
}

fn generation_digest(
    height: u64,
    root: &[u8; 32],
    base_digest: &[u8; 32],
    delta_digests: &[Option<[u8; 32]>],
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(GENERATION_DOMAIN);
    hasher.update(&FORMAT_VERSION.to_le_bytes());
    hasher.update(&height.to_le_bytes());
    hasher.update(root);
    hasher.update(base_digest);
    hasher.update(&(delta_digests.len() as u64).to_le_bytes());
    for digest in delta_digests {
        hasher.update(&[digest.is_some() as u8]);
        hasher.update(&digest.unwrap_or([0; 32]));
    }
    *hasher.finalize().as_bytes()
}

fn manifest_mac(operator_key: &[u8; 32], manifest: &ActiveGenerationManifest) -> Result<[u8; 32]> {
    let encoded = serde_json::to_vec(manifest)?;
    let mut hasher = blake3::Hasher::new_keyed(operator_key);
    hasher.update(MANIFEST_MAC_DOMAIN);
    hasher.update(&(encoded.len() as u64).to_le_bytes());
    hasher.update(&encoded);
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [7; 32];

    fn value(value: u64) -> [u8; 32] {
        Fq::from(value).to_bytes()
    }

    fn leaf(value: u64, position: u64) -> ActiveLeaf {
        ActiveLeaf {
            value: self::value(value),
            position,
            next_index: 0,
            next_value: [0; 32],
            sentinel: value == 0,
            terminal: false,
        }
    }

    fn base(levels: usize) -> ActiveGeneration {
        let limits = ActiveGenerationLimits {
            max_delta_levels: levels,
            ..ActiveGenerationLimits::default()
        };
        ActiveGeneration::build_base(1, [1; 32], vec![leaf(0, 0), leaf(20, 1)], vec![], limits)
            .unwrap()
    }

    #[test]
    fn deltas_merge_geometrically_and_keep_newest_values() {
        let generation = base(2);
        let (generation, first) = generation
            .apply_block(2, [2; 32], vec![leaf(10, 2)], vec![])
            .unwrap();
        assert_eq!(first.occupied_delta_levels, 1);
        let (generation, second) = generation
            .apply_block(3, [3; 32], vec![leaf(15, 3)], vec![])
            .unwrap();
        assert_eq!(second.merged_levels, 1);
        assert_eq!(
            generation
                .lookup_at_or_before(&value(17))
                .unwrap()
                .leaf
                .position,
            3
        );
        let (generation, _) = generation
            .apply_block(4, [4; 32], vec![leaf(10, 22)], vec![])
            .unwrap();
        assert_eq!(
            generation
                .lookup_at_or_before(&value(10))
                .unwrap()
                .leaf
                .position,
            22
        );
    }

    #[test]
    fn overflow_compacts_into_a_new_immutable_base() {
        let generation = base(1);
        let (generation, _) = generation
            .apply_block(2, [2; 32], vec![leaf(5, 2)], vec![])
            .unwrap();
        let pinned = Arc::new(generation.clone());
        let (next, metrics) = generation
            .apply_block(3, [3; 32], vec![leaf(7, 3)], vec![])
            .unwrap();
        assert!(metrics.compacted_into_base);
        assert_eq!(
            next.lookup_at_or_before(&value(8)).unwrap().leaf.value,
            value(7)
        );
        assert_eq!(
            pinned.lookup_at_or_before(&value(8)).unwrap().leaf.value,
            value(5)
        );
    }

    #[test]
    fn witness_uses_one_fixed_schedule_for_empty_and_populated_levels() {
        let generation = base(3);
        let witness = generation.witness(&value(20)).unwrap();
        assert_eq!(witness.predecessor_probes, 4);
        assert_eq!(witness.siblings.len(), SIBLINGS_PER_WITNESS);
        assert_eq!(witness.node_probes, 4 * SIBLINGS_PER_WITNESS);
    }

    #[test]
    fn newest_copy_on_write_node_wins_across_delta_levels() {
        let coordinate = NodeCoordinate {
            level: 0,
            position: 0,
        };
        let generation = base(2);
        let (generation, _) = generation
            .apply_block(
                2,
                [2; 32],
                vec![leaf(5, 2)],
                vec![ChangedNode {
                    coordinate,
                    hash: [2; 32],
                }],
            )
            .unwrap();
        let (generation, _) = generation
            .apply_block(
                3,
                [3; 32],
                vec![leaf(7, 3)],
                vec![ChangedNode {
                    coordinate,
                    hash: [3; 32],
                }],
            )
            .unwrap();
        let (generation, _) = generation
            .apply_block(
                4,
                [4; 32],
                vec![leaf(9, 4)],
                vec![ChangedNode {
                    coordinate,
                    hash: [4; 32],
                }],
            )
            .unwrap();
        assert_eq!(generation.witness(&value(20)).unwrap().siblings[0], [4; 32]);
    }

    #[test]
    fn authenticated_image_rejects_corruption_and_stale_publication() {
        let generation = base(2);
        let image = generation.image(&KEY).unwrap();
        assert_eq!(
            ActiveGeneration::from_image(image.clone(), &KEY)
                .unwrap()
                .manifest,
            generation.manifest
        );
        let mut corrupt = image;
        corrupt.base_leaves[1].position = 999;
        assert!(ActiveGeneration::from_image(corrupt, &KEY).is_err());

        let publisher = ActiveGenerationPublisher::new(generation.clone());
        let (next, _) = generation
            .apply_block(2, [2; 32], vec![leaf(5, 2)], vec![])
            .unwrap();
        let authenticated = next.authenticated_manifest(&KEY).unwrap();
        publisher.publish(next, &authenticated, &KEY).unwrap();
        assert!(publisher
            .publish(
                generation.clone(),
                &generation.authenticated_manifest(&KEY).unwrap(),
                &KEY
            )
            .is_err());
    }

    #[test]
    fn predecessor_uses_shieldd_field_order_not_little_endian_wire_order() {
        let generation = ActiveGeneration::build_base(
            1,
            [1; 32],
            vec![leaf(0, 0), leaf(1, 1), leaf(255, 2), leaf(256, 3)],
            vec![],
            ActiveGenerationLimits::default(),
        )
        .unwrap();

        assert_eq!(
            generation
                .strict_predecessor(&value(256))
                .unwrap()
                .leaf
                .value,
            value(255)
        );
        assert_eq!(
            generation
                .lookup_at_or_before(&value(256))
                .unwrap()
                .leaf
                .value,
            value(256)
        );
    }
}
