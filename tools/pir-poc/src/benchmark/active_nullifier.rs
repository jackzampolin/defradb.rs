//! Live active-generation benchmark shaped after Shieldd's indexed tree.
//!
//! This benchmarks the mutable PIR sidecar, not Shieldd consensus code or its
//! Poseidon implementation. The topology is Shieldd's depth-20 quaternary
//! tree, including sequential leaf positions and predecessor proofs.

use std::collections::{BTreeSet, HashSet};
use std::hint::black_box;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use rand::{rngs::StdRng, SeedableRng};
use serde::Serialize;

use super::Profile;
use crate::{
    active_generation::{
        ActiveGeneration, ActiveGenerationLimits, ActiveLeaf, ChangedNode, NodeCoordinate,
    },
    dense,
    dense_batch::{BatchEvaluator, BatchKernel},
    snapshot::SnapshotView,
};

const DEPTH: usize = 20;
const ARITY: usize = 4;
const HASH_BYTES: usize = 32;
const POSITION_BYTES: usize = 8;
const LEAF_BYTES: usize = 80;
const BUCKET_ENTRY_BYTES: usize = POSITION_BYTES + LEAF_BYTES;
const BUCKET_HEADER_BYTES: usize = 16;
const BUCKET_CAPACITY: usize = 384;
const SERVER_COUNT: usize = 2;
const DECOY_COUNT: usize = 100;
const WITNESS_BYTES: usize = BUCKET_ENTRY_BYTES + DEPTH * (ARITY - 1) * HASH_BYTES;

#[derive(Clone, Copy, Debug)]
struct Scale {
    profile: &'static str,
    initial_leaves: usize,
    inserted_leaves: usize,
    radix_buckets: usize,
    query_samples: usize,
}

impl Scale {
    fn new(profile: Profile) -> Self {
        match profile {
            Profile::Quick => Self {
                profile: "quick",
                initial_leaves: 1 << 18,
                inserted_leaves: 1 << 12,
                radix_buckets: 1 << 10,
                query_samples: 3,
            },
            Profile::Full => Self {
                profile: "full",
                initial_leaves: 1 << 20,
                inserted_leaves: 32_768,
                radix_buckets: 1 << 12,
                query_samples: 5,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OrderedLeaf {
    value: [u8; 32],
    position: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ActiveNullifierReport {
    pub schema: &'static str,
    pub profile: &'static str,
    pub shieldd_shape: ShielddShape,
    pub sidecar: SidecarShape,
    pub build: BuildMeasurement,
    pub committed_block_update: UpdateMeasurement,
    pub strict_private_query: QueryMeasurement,
    pub decoy_100_query: QueryMeasurement,
    pub comparison: QueryComparison,
    pub caveats: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ShielddShape {
    pub active_generation_initial_nullifiers: usize,
    pub committed_block_insertions: usize,
    pub active_generation_final_nullifiers: usize,
    pub implicit_lower_sentinel_leaves: usize,
    pub tree_depth: usize,
    pub tree_arity: usize,
    pub siblings_per_witness: usize,
    pub generation_capacity: u64,
    pub benchmark_query: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct SidecarShape {
    pub predecessor_index: &'static str,
    pub radix_buckets: usize,
    pub bucket_capacity: usize,
    pub maximum_bucket_occupancy: usize,
    pub bucket_row_bytes: usize,
    pub radix_table_bytes_per_replica: usize,
    pub quaternary_node_bytes_per_replica: usize,
    pub total_bytes_per_replica: usize,
    pub deployed_two_replica_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct BuildMeasurement {
    pub initial_sidecar_build_ms: f64,
    pub initial_radix_bytes: usize,
    pub initial_node_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct UpdateMeasurement {
    pub measured_update_ms: f64,
    pub inserted_leaves: usize,
    pub uncoalesced_shieldd_leaf_mutations: usize,
    pub uncoalesced_shieldd_path_node_mutations_upper_bound: usize,
    pub coalesced_radix_rows_rewritten: usize,
    pub coalesced_changed_leaf_records: usize,
    pub coalesced_tree_nodes_rewritten: usize,
    pub logical_bytes_written_per_replica: usize,
    pub logical_bytes_written_two_replicas: usize,
    pub update_amplification_over_32_byte_nullifiers: f64,
    pub minimum_lsm_delta_payload_bytes_per_replica: usize,
    pub flat_update_over_minimum_lsm_delta: f64,
    pub immutable_delta_build_p50_ms: f64,
    pub immutable_delta_payload_bytes_per_replica: usize,
    pub immutable_delta_amplification_over_32_byte_nullifiers: f64,
    pub flat_update_over_implemented_delta: f64,
    pub flat_update_time_over_implemented_delta: f64,
    pub fixed_delta_query_levels: usize,
    pub publication: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryMeasurement {
    pub protocol: &'static str,
    pub privacy: &'static str,
    pub query_samples: usize,
    pub server_p50_ms: f64,
    pub client_p50_ms: f64,
    pub upload_bytes: usize,
    pub download_bytes: usize,
    pub aggregate_logical_source_bytes: usize,
    pub server_storage_bytes: usize,
    pub returned_witnesses: usize,
    pub client_processed_witnesses: usize,
    pub result_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueryComparison {
    pub strict_server_over_decoy: f64,
    pub decoy_download_over_strict: f64,
    pub conclusion: &'static str,
}

pub fn run(profile: Profile) -> Result<ActiveNullifierReport> {
    let scale = Scale::new(profile);
    if !scale.radix_buckets.is_power_of_two() {
        bail!("active-nullifier radix bucket count must be a power of two");
    }
    let bucket_row_bytes = bucket_row_bytes()?;

    let initial_values = (1..=scale.initial_leaves)
        .map(|position| OrderedLeaf {
            value: synthetic_nullifier(position as u64, 0),
            position: position as u64,
        })
        .collect::<Vec<_>>();
    let new_values = (0..scale.inserted_leaves)
        .map(|offset| OrderedLeaf {
            value: synthetic_nullifier(offset as u64, 1),
            position: (scale.initial_leaves + offset + 1) as u64,
        })
        .collect::<Vec<_>>();

    let build_started = Instant::now();
    let mut buckets = distribute(initial_values, scale.radix_buckets)?;
    let (initial_last_before, initial_first_after) = boundary_entries(&buckets);
    let mut radix_rows = encode_all_buckets(
        &buckets,
        &initial_last_before,
        &initial_first_after,
        bucket_row_bytes,
    )?;
    let initial_leaf_count = scale
        .initial_leaves
        .checked_add(1)
        .context("active-nullifier sentinel count overflow")?;
    let mut node_planes = build_node_planes(initial_leaf_count, 0)?;
    let build_elapsed = build_started.elapsed();
    let initial_radix_bytes = radix_rows.len();
    let initial_node_bytes = node_planes.iter().map(Vec::len).sum::<usize>();

    let update_started = Instant::now();
    let mut inserted_buckets = BTreeSet::new();
    for leaf in &new_values {
        let bucket = bucket_for(&leaf.value, scale.radix_buckets);
        buckets[bucket].push(*leaf);
        inserted_buckets.insert(bucket);
    }
    for &bucket in &inserted_buckets {
        buckets[bucket].sort_unstable_by_key(|leaf| leaf.value);
        if buckets[bucket].len() > BUCKET_CAPACITY {
            bail!(
                "active-nullifier radix bucket {bucket} overflowed: {} > {BUCKET_CAPACITY}",
                buckets[bucket].len()
            );
        }
    }
    let (last_before, first_after) = boundary_entries(&buckets);
    let touched_buckets = affected_bucket_rows(&buckets, &inserted_buckets);
    for &bucket in &touched_buckets {
        let encoded = encode_bucket(
            bucket,
            &buckets,
            &last_before,
            &first_after,
            bucket_row_bytes,
        )?;
        let start = bucket * bucket_row_bytes;
        radix_rows[start..start + bucket_row_bytes].copy_from_slice(&encoded);
    }

    let mut changed_leaf_positions = HashSet::with_capacity(scale.inserted_leaves * 2);
    for leaf in &new_values {
        changed_leaf_positions.insert(leaf.position);
        let predecessor = strict_predecessor_for(&buckets, &last_before, &leaf.value)?;
        changed_leaf_positions.insert(predecessor.position);
    }
    let changed_nodes = changed_tree_nodes(&changed_leaf_positions);
    let final_leaf_count = scale
        .initial_leaves
        .checked_add(scale.inserted_leaves)
        .and_then(|count| count.checked_add(1))
        .context("active-nullifier final leaf count overflow")?;
    resize_node_planes(&mut node_planes, final_leaf_count)?;
    for &(level, position) in &changed_nodes {
        if level >= DEPTH {
            continue;
        }
        let plane = &mut node_planes[level];
        let start = usize::try_from(position)?
            .checked_mul(HASH_BYTES)
            .context("active-nullifier changed node offset overflow")?;
        if start + HASH_BYTES <= plane.len() {
            plane[start..start + HASH_BYTES]
                .copy_from_slice(node_hash(level, position, 1).as_bytes());
        }
    }
    let update_elapsed = update_started.elapsed();

    let maximum_bucket_occupancy = buckets.iter().map(Vec::len).max().unwrap_or_default();
    let radix_bytes = radix_rows.len();
    let node_bytes = node_planes.iter().map(Vec::len).sum::<usize>();
    let bytes_written_per_replica = touched_buckets
        .len()
        .checked_mul(bucket_row_bytes)
        .and_then(|bytes| bytes.checked_add(changed_nodes.len() * HASH_BYTES))
        .context("active-nullifier update byte count overflow")?;
    let minimum_lsm_delta_payload = changed_leaf_positions
        .len()
        .checked_mul(BUCKET_ENTRY_BYTES)
        .and_then(|bytes| bytes.checked_add(changed_nodes.len() * HASH_BYTES))
        .context("active-nullifier LSM delta payload overflow")?;

    // Exercise the production-shaped immutable delta implementation on the
    // exact coalesced mutation sets.  The large base is already represented by
    // `radix_rows`/`node_planes`; a sentinel-only base avoids allocating a
    // duplicate copy solely to time construction of this block delta.
    let delta_base = ActiveGeneration::build_base(
        1,
        [1; 32],
        vec![ActiveLeaf {
            value: [0; 32],
            position: 0,
            next_index: 0,
            next_value: [0; 32],
            sentinel: true,
            terminal: false,
        }],
        Vec::new(),
        ActiveGenerationLimits::default(),
    )?;
    let mut delta_leaves = buckets
        .iter()
        .flatten()
        .filter(|leaf| changed_leaf_positions.contains(&leaf.position))
        .map(|leaf| ActiveLeaf {
            value: leaf.value,
            position: leaf.position,
            next_index: 0,
            next_value: [0; 32],
            sentinel: false,
            terminal: false,
        })
        .collect::<Vec<_>>();
    if changed_leaf_positions.contains(&0) {
        delta_leaves.push(ActiveLeaf {
            value: [0; 32],
            position: 0,
            next_index: 0,
            next_value: [0; 32],
            sentinel: true,
            terminal: false,
        });
    }
    let delta_nodes = changed_nodes
        .iter()
        .filter(|(level, _)| *level < DEPTH)
        .map(|(level, position)| ChangedNode {
            coordinate: NodeCoordinate {
                level: u8::try_from(*level).expect("Shieldd level fits u8"),
                position: *position,
            },
            hash: *node_hash(*level, *position, 1).as_bytes(),
        })
        .collect::<Vec<_>>();
    let mut delta_samples = Vec::with_capacity(scale.query_samples);
    let mut delta_result = None;
    for _ in 0..scale.query_samples {
        let sample_leaves = delta_leaves.clone();
        let sample_nodes = delta_nodes.clone();
        let delta_started = Instant::now();
        let result = delta_base.apply_block(2, [2; 32], sample_leaves, sample_nodes)?;
        delta_samples.push(delta_started.elapsed());
        delta_result = Some(result);
    }
    delta_samples.sort_unstable();
    let delta_elapsed = delta_samples[delta_samples.len() / 2];
    let (delta_generation, delta_metrics) =
        delta_result.context("active-generation delta benchmark produced no sample")?;
    let authenticated_delta = delta_generation.authenticated_manifest(&[7; 32])?;
    authenticated_delta.verify(&[7; 32], &delta_generation.limits)?;

    let mut target = synthetic_nullifier(0x6162_7365_6e74, 2);
    while contains_value(&buckets, &target) {
        target = *blake3::hash(&target).as_bytes();
        target[0] &= 0x1f;
    }
    let strict = benchmark_strict_query(
        &radix_rows,
        &buckets,
        &last_before,
        &node_planes,
        target,
        scale.query_samples,
    )?;
    let decoy = benchmark_decoys(
        &radix_rows,
        &buckets,
        &last_before,
        &node_planes,
        target,
        scale.query_samples,
    )?;

    Ok(ActiveNullifierReport {
        schema: "defradb-pir-shieldd-active-generation-v1",
        profile: scale.profile,
        shieldd_shape: ShielddShape {
            active_generation_initial_nullifiers: scale.initial_leaves,
            committed_block_insertions: scale.inserted_leaves,
            active_generation_final_nullifiers: scale.initial_leaves + scale.inserted_leaves,
            implicit_lower_sentinel_leaves: 1,
            tree_depth: DEPTH,
            tree_arity: ARITY,
            siblings_per_witness: DEPTH * (ARITY - 1),
            generation_capacity: 1u64 << 40,
            benchmark_query: "absent nullifier -> linked predecessor leaf plus fixed 20-layer quaternary witness",
        },
        sidecar: SidecarShape {
            predecessor_index: "fixed high-bit radix bucket with carry-in predecessor and padded full Shieldd-shaped leaves",
            radix_buckets: scale.radix_buckets,
            bucket_capacity: BUCKET_CAPACITY,
            maximum_bucket_occupancy,
            bucket_row_bytes,
            radix_table_bytes_per_replica: radix_bytes,
            quaternary_node_bytes_per_replica: node_bytes,
            total_bytes_per_replica: radix_bytes + node_bytes,
            deployed_two_replica_bytes: 2 * (radix_bytes + node_bytes),
        },
        build: BuildMeasurement {
            initial_sidecar_build_ms: millis(build_elapsed),
            initial_radix_bytes,
            initial_node_bytes,
        },
        committed_block_update: UpdateMeasurement {
            measured_update_ms: millis(update_elapsed),
            inserted_leaves: scale.inserted_leaves,
            uncoalesced_shieldd_leaf_mutations: 2 * scale.inserted_leaves,
            uncoalesced_shieldd_path_node_mutations_upper_bound: 2
                * scale.inserted_leaves
                * (DEPTH + 1),
            coalesced_radix_rows_rewritten: touched_buckets.len(),
            coalesced_changed_leaf_records: changed_leaf_positions.len(),
            coalesced_tree_nodes_rewritten: changed_nodes.len(),
            logical_bytes_written_per_replica: bytes_written_per_replica,
            logical_bytes_written_two_replicas: 2 * bytes_written_per_replica,
            update_amplification_over_32_byte_nullifiers: bytes_written_per_replica as f64
                / (scale.inserted_leaves * 32) as f64,
            minimum_lsm_delta_payload_bytes_per_replica: minimum_lsm_delta_payload,
            flat_update_over_minimum_lsm_delta: bytes_written_per_replica as f64
                / minimum_lsm_delta_payload as f64,
            immutable_delta_build_p50_ms: millis(delta_elapsed),
            immutable_delta_payload_bytes_per_replica: delta_metrics
                .immutable_delta_payload_bytes,
            immutable_delta_amplification_over_32_byte_nullifiers: delta_metrics
                .immutable_delta_payload_bytes as f64
                / (scale.inserted_leaves * 32) as f64,
            flat_update_over_implemented_delta: bytes_written_per_replica as f64
                / delta_metrics.immutable_delta_payload_bytes as f64,
            flat_update_time_over_implemented_delta: update_elapsed.as_secs_f64()
                / delta_elapsed.as_secs_f64(),
            fixed_delta_query_levels: delta_generation.limits.max_delta_levels + 1,
            publication: "build changed rows/nodes off the serving generation, bind them to the committed Shieldd height/root, then atomically publish a new immutable manifest",
        },
        strict_private_query: strict.clone(),
        decoy_100_query: decoy.clone(),
        comparison: QueryComparison {
            strict_server_over_decoy: strict.server_p50_ms / decoy.server_p50_ms,
            decoy_download_over_strict: decoy.download_bytes as f64
                / strict.download_bytes as f64,
            conclusion: "the benchmark rejects a flat rewritten radix table for maximum-size random blocks; use an immutable base plus tiered predecessor/node deltas, while strict PIR still spends substantially more server work than 100 visible candidates",
        },
        caveats: vec![
            "this reproduces Shieldd's index topology and mutation coordinates but uses deterministic BLAKE3 row fixtures, not Shieldd's Poseidon377 implementation",
            "the flat comparison mutates staging buffers; the implemented immutable delta is separately built, authenticated, and published through a generation-pinned Arc",
            "radix occupancy uses synthetic canonical-order values with three leading field slack bits cleared; activate only after measuring real Shieldd nullifier distributions",
            "the path stage is 20 level-specific batches of three ordinary Dense selectors; a production TreePIR-style path protocol remains an optimization target",
            "server p50 is summed sequential in-process replica elapsed time and excludes storage I/O, transport, TLS, queues, cycles, and energy",
            "100 decoys provide candidate-set privacy only and the client processes only its target witness",
        ],
    })
}

fn synthetic_nullifier(index: u64, generation: u8) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"defradb-pir-shieldd-active-nullifier-v1");
    hasher.update(&[generation]);
    hasher.update(&index.to_le_bytes());
    let mut value = *hasher.finalize().as_bytes();
    // BLS12-377 Fq encodings have leading slack bits. Clearing three makes the
    // radix distribution representative without importing Shieldd consensus.
    value[0] &= 0x1f;
    value
}

fn bucket_for(value: &[u8; 32], bucket_count: usize) -> usize {
    let prefix_bits = bucket_count.trailing_zeros() as usize;
    let mut result = 0usize;
    for bit in 0..prefix_bits {
        let source_bit = bit + 3;
        let byte = source_bit / 8;
        let shift = 7 - source_bit % 8;
        result = (result << 1) | ((value[byte] >> shift) & 1) as usize;
    }
    result
}

fn distribute(values: Vec<OrderedLeaf>, bucket_count: usize) -> Result<Vec<Vec<OrderedLeaf>>> {
    let mut buckets = vec![Vec::new(); bucket_count];
    for leaf in values {
        buckets[bucket_for(&leaf.value, bucket_count)].push(leaf);
    }
    for (bucket_index, bucket) in buckets.iter_mut().enumerate() {
        bucket.sort_unstable_by_key(|leaf| leaf.value);
        if bucket.len() > BUCKET_CAPACITY {
            bail!(
                "active-nullifier radix bucket {bucket_index} overflowed: {} > {BUCKET_CAPACITY}",
                bucket.len()
            );
        }
    }
    Ok(buckets)
}

fn boundary_entries(
    buckets: &[Vec<OrderedLeaf>],
) -> (Vec<Option<OrderedLeaf>>, Vec<Option<OrderedLeaf>>) {
    let mut last_before = vec![None; buckets.len()];
    let mut previous = None;
    for (index, bucket) in buckets.iter().enumerate() {
        last_before[index] = previous;
        if let Some(last) = bucket.last() {
            previous = Some(*last);
        }
    }
    let mut first_after = vec![None; buckets.len()];
    let mut next = None;
    for (index, bucket) in buckets.iter().enumerate().rev() {
        first_after[index] = next;
        if let Some(first) = bucket.first() {
            next = Some(*first);
        }
    }
    (last_before, first_after)
}

fn bucket_row_bytes() -> Result<usize> {
    BUCKET_CAPACITY
        .checked_add(1)
        .and_then(|entries| entries.checked_mul(BUCKET_ENTRY_BYTES))
        .and_then(|entries| entries.checked_add(BUCKET_HEADER_BYTES))
        .context("active-nullifier bucket row size overflow")
}

fn encode_all_buckets(
    buckets: &[Vec<OrderedLeaf>],
    last_before: &[Option<OrderedLeaf>],
    first_after: &[Option<OrderedLeaf>],
    row_bytes: usize,
) -> Result<Vec<u8>> {
    let mut rows = vec![0u8; buckets.len() * row_bytes];
    for bucket in 0..buckets.len() {
        let encoded = encode_bucket(bucket, buckets, last_before, first_after, row_bytes)?;
        rows[bucket * row_bytes..(bucket + 1) * row_bytes].copy_from_slice(&encoded);
    }
    Ok(rows)
}

fn encode_bucket(
    bucket_index: usize,
    buckets: &[Vec<OrderedLeaf>],
    last_before: &[Option<OrderedLeaf>],
    first_after: &[Option<OrderedLeaf>],
    row_bytes: usize,
) -> Result<Vec<u8>> {
    let bucket = &buckets[bucket_index];
    if bucket.len() > BUCKET_CAPACITY {
        bail!("active-nullifier bucket exceeds its fixed capacity");
    }
    let mut row = vec![0u8; row_bytes];
    row[..4].copy_from_slice(&(bucket.len() as u32).to_le_bytes());
    row[4..8].copy_from_slice(&(bucket_index as u32).to_le_bytes());
    let carry = last_before[bucket_index];
    let carry_successor = bucket.first().copied().or(first_after[bucket_index]);
    encode_leaf_slot(
        &mut row[BUCKET_HEADER_BYTES..BUCKET_HEADER_BYTES + BUCKET_ENTRY_BYTES],
        carry,
        carry_successor,
        carry.is_none(),
    );
    for (index, leaf) in bucket.iter().copied().enumerate() {
        let successor = bucket.get(index + 1).copied().or(first_after[bucket_index]);
        let start = BUCKET_HEADER_BYTES + (index + 1) * BUCKET_ENTRY_BYTES;
        encode_leaf_slot(
            &mut row[start..start + BUCKET_ENTRY_BYTES],
            Some(leaf),
            successor,
            false,
        );
    }
    Ok(row)
}

fn encode_leaf_slot(
    output: &mut [u8],
    leaf: Option<OrderedLeaf>,
    successor: Option<OrderedLeaf>,
    sentinel: bool,
) {
    let leaf = leaf.unwrap_or(OrderedLeaf {
        value: [0u8; 32],
        position: 0,
    });
    output[..8].copy_from_slice(&leaf.position.to_le_bytes());
    output[8..40].copy_from_slice(&leaf.value);
    let successor_position = successor.map_or(0, |next| next.position);
    let successor_value = successor.map_or([0u8; 32], |next| next.value);
    output[40..48].copy_from_slice(&successor_position.to_le_bytes());
    output[48..80].copy_from_slice(&successor_value);
    let flags = (sentinel as u64) | ((successor.is_none() as u64) << 1);
    output[80..88].copy_from_slice(&flags.to_le_bytes());
}

fn affected_bucket_rows(
    buckets: &[Vec<OrderedLeaf>],
    inserted: &BTreeSet<usize>,
) -> BTreeSet<usize> {
    let mut touched = BTreeSet::new();
    for &bucket in inserted {
        touched.insert(bucket);
        if bucket > 0 {
            let mut previous = bucket - 1;
            touched.insert(previous);
            while previous > 0 && buckets[previous].is_empty() {
                previous -= 1;
                touched.insert(previous);
            }
        }
        if bucket + 1 < buckets.len() {
            let mut next = bucket + 1;
            touched.insert(next);
            while next + 1 < buckets.len() && buckets[next].is_empty() {
                next += 1;
                touched.insert(next);
            }
        }
    }
    touched
}

fn contains_value(buckets: &[Vec<OrderedLeaf>], value: &[u8; 32]) -> bool {
    let bucket = bucket_for(value, buckets.len());
    buckets[bucket]
        .binary_search_by_key(value, |leaf| leaf.value)
        .is_ok()
}

fn predecessor_for(
    buckets: &[Vec<OrderedLeaf>],
    last_before: &[Option<OrderedLeaf>],
    value: &[u8; 32],
) -> Result<OrderedLeaf> {
    let bucket = bucket_for(value, buckets.len());
    match buckets[bucket].binary_search_by_key(value, |leaf| leaf.value) {
        Ok(index) => Ok(buckets[bucket][index]),
        Err(0) => Ok(last_before[bucket].unwrap_or(OrderedLeaf {
            value: [0u8; 32],
            position: 0,
        })),
        Err(index) => Ok(buckets[bucket][index - 1]),
    }
}

fn strict_predecessor_for(
    buckets: &[Vec<OrderedLeaf>],
    last_before: &[Option<OrderedLeaf>],
    value: &[u8; 32],
) -> Result<OrderedLeaf> {
    let bucket = bucket_for(value, buckets.len());
    let index = match buckets[bucket].binary_search_by_key(value, |leaf| leaf.value) {
        Ok(index) | Err(index) => index,
    };
    if index == 0 {
        Ok(last_before[bucket].unwrap_or(OrderedLeaf {
            value: [0u8; 32],
            position: 0,
        }))
    } else {
        Ok(buckets[bucket][index - 1])
    }
}

fn decode_predecessor_slot(
    row: &[u8],
    value: &[u8; 32],
) -> Result<(OrderedLeaf, bool, [u8; BUCKET_ENTRY_BYTES])> {
    let count = usize::try_from(u32::from_le_bytes(row[..4].try_into()?))?;
    if count > BUCKET_CAPACITY || row.len() != bucket_row_bytes()? {
        bail!("active-nullifier recovered radix row is malformed");
    }
    let carry = &row[BUCKET_HEADER_BYTES..BUCKET_HEADER_BYTES + BUCKET_ENTRY_BYTES];
    let mut predecessor = decode_leaf_slot(carry)?;
    let mut predecessor_slot: [u8; BUCKET_ENTRY_BYTES] = carry.try_into()?;
    let mut exact = false;
    for index in 0..count {
        let start = BUCKET_HEADER_BYTES + (index + 1) * BUCKET_ENTRY_BYTES;
        let leaf = decode_leaf_slot(&row[start..start + BUCKET_ENTRY_BYTES])?;
        match leaf.value.cmp(value) {
            std::cmp::Ordering::Less => {
                predecessor = leaf;
                predecessor_slot.copy_from_slice(&row[start..start + BUCKET_ENTRY_BYTES]);
            }
            std::cmp::Ordering::Equal => {
                predecessor = leaf;
                predecessor_slot.copy_from_slice(&row[start..start + BUCKET_ENTRY_BYTES]);
                exact = true;
                break;
            }
            std::cmp::Ordering::Greater => break,
        }
    }
    Ok((predecessor, exact, predecessor_slot))
}

fn decode_leaf_slot(slot: &[u8]) -> Result<OrderedLeaf> {
    Ok(OrderedLeaf {
        position: u64::from_le_bytes(slot[..8].try_into()?),
        value: slot[8..40].try_into()?,
    })
}

fn plane_row_counts(leaf_count: usize) -> Vec<usize> {
    let mut counts = Vec::with_capacity(DEPTH);
    let mut current = leaf_count;
    for _ in 0..DEPTH {
        counts.push(current.div_ceil(ARITY).max(1) * ARITY);
        current = current.div_ceil(ARITY);
    }
    counts
}

fn build_node_planes(leaf_count: usize, version: u8) -> Result<Vec<Vec<u8>>> {
    plane_row_counts(leaf_count)
        .into_iter()
        .enumerate()
        .map(|(level, rows)| {
            let mut plane = vec![0u8; rows * HASH_BYTES];
            let actual = leaf_count.div_ceil(ARITY.pow(level as u32));
            for position in 0..actual {
                let start = position * HASH_BYTES;
                plane[start..start + HASH_BYTES]
                    .copy_from_slice(node_hash(level, position as u64, version).as_bytes());
            }
            Ok(plane)
        })
        .collect()
}

fn resize_node_planes(planes: &mut [Vec<u8>], leaf_count: usize) -> Result<()> {
    for (plane, rows) in planes.iter_mut().zip(plane_row_counts(leaf_count)) {
        plane.resize(
            rows.checked_mul(HASH_BYTES)
                .context("active-nullifier node plane resize overflow")?,
            0,
        );
    }
    Ok(())
}

fn node_hash(level: usize, position: u64, version: u8) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"defradb-pir-shieldd-node-fixture-v1");
    hasher.update(&(level as u64).to_le_bytes());
    hasher.update(&position.to_le_bytes());
    hasher.update(&[version]);
    hasher.finalize()
}

fn changed_tree_nodes(changed_leaves: &HashSet<u64>) -> HashSet<(usize, u64)> {
    let mut changed = HashSet::with_capacity(changed_leaves.len() * (DEPTH + 1));
    for &leaf in changed_leaves {
        let mut position = leaf;
        for level in 0..=DEPTH {
            changed.insert((level, position));
            position /= ARITY as u64;
        }
    }
    changed
}

fn witness_from_planes(
    planes: &[Vec<u8>],
    leaf: OrderedLeaf,
    encoded_leaf: &[u8; BUCKET_ENTRY_BYTES],
) -> Result<Vec<u8>> {
    let mut witness = Vec::with_capacity(WITNESS_BYTES);
    witness.extend_from_slice(encoded_leaf);
    let mut position = usize::try_from(leaf.position)?;
    for plane in planes {
        let base = position / ARITY * ARITY;
        let child = position % ARITY;
        for sibling in 0..ARITY {
            if sibling == child {
                continue;
            }
            let start = (base + sibling) * HASH_BYTES;
            witness.extend_from_slice(&plane[start..start + HASH_BYTES]);
        }
        position /= ARITY;
    }
    Ok(witness)
}

fn benchmark_strict_query(
    radix_rows: &[u8],
    buckets: &[Vec<OrderedLeaf>],
    last_before: &[Option<OrderedLeaf>],
    node_planes: &[Vec<u8>],
    target: [u8; 32],
    samples: usize,
) -> Result<QueryMeasurement> {
    let row_bytes = bucket_row_bytes()?;
    let radix_view = SnapshotView::new(radix_rows, buckets.len(), row_bytes);
    let expected_predecessor = predecessor_for(buckets, last_before, &target)?;
    let expected_bucket = bucket_for(&target, buckets.len());
    let expected_row = radix_view.row(expected_bucket)?;
    let (_, _, expected_leaf_slot) = decode_predecessor_slot(expected_row, &target)?;
    let expected_witness =
        witness_from_planes(node_planes, expected_predecessor, &expected_leaf_slot)?;
    let mut server_samples = Vec::with_capacity(samples);
    let mut client_samples = Vec::with_capacity(samples);
    let mut logical_samples = Vec::with_capacity(samples);
    let mut upload_bytes = 0usize;
    let mut download_bytes = 0usize;

    for sample in 0..samples {
        let mut rng = StdRng::seed_from_u64(0x4143_5449_5645_0000 ^ sample as u64);
        let client_started = Instant::now();
        let bucket = bucket_for(&target, buckets.len());
        let radix_shares = dense::query_shares(bucket, buckets.len(), SERVER_COUNT, &mut rng)?;
        let radix_query_elapsed = client_started.elapsed();
        let evaluator = BatchEvaluator::new(1, row_bytes)?;
        let mut server_elapsed = Duration::ZERO;
        let mut logical_bytes = 0usize;
        let mut radix_answers = Vec::with_capacity(SERVER_COUNT);
        for share in &radix_shares {
            let started = Instant::now();
            let evaluated = evaluator.evaluate(
                radix_view,
                std::slice::from_ref(share),
                BatchKernel::SharedRowMajor,
            )?;
            server_elapsed += started.elapsed();
            logical_bytes += evaluated.metrics.immutable_source_operand_bytes;
            radix_answers.push(evaluated.answers[0].clone());
        }
        let combine_started = Instant::now();
        let radix_row =
            dense::combine(&radix_answers.iter().map(Vec::as_slice).collect::<Vec<_>>())?;
        let (predecessor, exact, recovered_leaf_slot) =
            decode_predecessor_slot(&radix_row, &target)?;
        if exact || predecessor != expected_predecessor {
            bail!("active-nullifier private predecessor lookup was incorrect");
        }
        let radix_combine_elapsed = combine_started.elapsed();

        let path_query_started = Instant::now();
        let mut current = usize::try_from(predecessor.position)?;
        let mut per_level_queries = Vec::with_capacity(DEPTH);
        for plane in node_planes {
            let rows = plane.len() / HASH_BYTES;
            let base = current / ARITY * ARITY;
            let child = current % ARITY;
            let ordinals = (0..ARITY)
                .filter(|sibling| *sibling != child)
                .map(|sibling| base + sibling)
                .collect::<Vec<_>>();
            let mut server_queries = (0..SERVER_COUNT)
                .map(|_| Vec::with_capacity(ARITY - 1))
                .collect::<Vec<_>>();
            for ordinal in ordinals {
                for (server, share) in dense::query_shares(ordinal, rows, SERVER_COUNT, &mut rng)?
                    .into_iter()
                    .enumerate()
                {
                    server_queries[server].push(share);
                }
            }
            per_level_queries.push(server_queries);
            current /= ARITY;
        }
        let path_query_elapsed = path_query_started.elapsed();
        let mut recovered_witness = Vec::with_capacity(WITNESS_BYTES);
        let mut leaf_slot = [0u8; BUCKET_ENTRY_BYTES];
        encode_leaf_slot(
            &mut leaf_slot,
            Some(predecessor),
            None,
            predecessor.position == 0,
        );
        recovered_witness.extend_from_slice(&leaf_slot);

        let path_combine_started = Instant::now();
        for (plane, server_queries) in node_planes.iter().zip(&per_level_queries) {
            let view = SnapshotView::new(plane, plane.len() / HASH_BYTES, HASH_BYTES);
            let evaluator = BatchEvaluator::new(ARITY - 1, (ARITY - 1) * HASH_BYTES)?;
            let mut server_answers = Vec::with_capacity(SERVER_COUNT);
            for queries in server_queries {
                let started = Instant::now();
                let evaluated = evaluator.evaluate(view, queries, BatchKernel::SharedRowMajor)?;
                server_elapsed += started.elapsed();
                logical_bytes += evaluated.metrics.immutable_source_operand_bytes;
                server_answers.push(evaluated.answers);
            }
            for answer_index in 0..ARITY - 1 {
                recovered_witness.extend_from_slice(&dense::combine(
                    &server_answers
                        .iter()
                        .map(|answers| answers[answer_index].as_slice())
                        .collect::<Vec<_>>(),
                )?);
            }
        }
        let path_combine_elapsed = path_combine_started.elapsed();
        recovered_witness[..BUCKET_ENTRY_BYTES].copy_from_slice(&recovered_leaf_slot);
        if recovered_witness != expected_witness {
            bail!("active-nullifier private path retrieval was incorrect");
        }
        black_box(&recovered_witness);
        server_samples.push(server_elapsed);
        client_samples.push(
            radix_query_elapsed + radix_combine_elapsed + path_query_elapsed + path_combine_elapsed,
        );
        logical_samples.push(logical_bytes);
        upload_bytes = 2 * dense::query_size(buckets.len())
            + node_planes
                .iter()
                .map(|plane| 2 * (ARITY - 1) * dense::query_size(plane.len() / HASH_BYTES))
                .sum::<usize>();
        download_bytes = 2 * row_bytes + 2 * DEPTH * (ARITY - 1) * HASH_BYTES;
    }

    let storage_per_replica = radix_rows.len() + node_planes.iter().map(Vec::len).sum::<usize>();
    Ok(QueryMeasurement {
        protocol: "two-server live radix predecessor + level-specific Dense quaternary path",
        privacy: "information-theoretic target privacy if one replica does not collude; current generation/root are public",
        query_samples: samples,
        server_p50_ms: millis(median_duration(&mut server_samples)),
        client_p50_ms: millis(median_duration(&mut client_samples)),
        upload_bytes,
        download_bytes,
        aggregate_logical_source_bytes: median_usize(&mut logical_samples),
        server_storage_bytes: SERVER_COUNT * storage_per_replica,
        returned_witnesses: 1,
        client_processed_witnesses: 1,
        result_bytes: WITNESS_BYTES,
    })
}

fn benchmark_decoys(
    radix_rows: &[u8],
    buckets: &[Vec<OrderedLeaf>],
    last_before: &[Option<OrderedLeaf>],
    node_planes: &[Vec<u8>],
    target: [u8; 32],
    samples: usize,
) -> Result<QueryMeasurement> {
    let mut candidates = (0..DECOY_COUNT - 1)
        .map(|index| synthetic_nullifier(index as u64, 3))
        .collect::<Vec<_>>();
    candidates.push(target);
    candidates.rotate_left(37);
    let target_index = candidates
        .iter()
        .position(|candidate| candidate == &target)
        .context("active-nullifier decoy schedule omitted target")?;
    let expected_target = predecessor_for(buckets, last_before, &target)?;
    let row_bytes = bucket_row_bytes()?;
    let target_bucket = bucket_for(&target, buckets.len());
    let (_, _, target_leaf_slot) = decode_predecessor_slot(
        &radix_rows[target_bucket * row_bytes..(target_bucket + 1) * row_bytes],
        &target,
    )?;
    let expected_witness = witness_from_planes(node_planes, expected_target, &target_leaf_slot)?;
    let mut server_samples = Vec::with_capacity(samples);
    let mut client_samples = Vec::with_capacity(samples);
    for _ in 0..samples {
        let server_started = Instant::now();
        let mut responses = Vec::with_capacity(DECOY_COUNT * WITNESS_BYTES);
        for candidate in &candidates {
            let bucket = bucket_for(candidate, buckets.len());
            let (predecessor, _, leaf_slot) = decode_predecessor_slot(
                &radix_rows[bucket * row_bytes..(bucket + 1) * row_bytes],
                candidate,
            )?;
            responses.extend_from_slice(&witness_from_planes(
                node_planes,
                predecessor,
                &leaf_slot,
            )?);
        }
        server_samples.push(server_started.elapsed());
        let client_started = Instant::now();
        let start = target_index * WITNESS_BYTES;
        if responses[start..start + WITNESS_BYTES] != expected_witness {
            bail!("active-nullifier target decoy witness was incorrect");
        }
        black_box(&responses[start..start + WITNESS_BYTES]);
        client_samples.push(client_started.elapsed());
    }
    let storage_per_replica =
        buckets.len() * bucket_row_bytes()? + node_planes.iter().map(Vec::len).sum::<usize>();
    Ok(QueryMeasurement {
        protocol: "one-server public ordered index with 100 visible nullifier candidates",
        privacy:
            "candidate-set privacy only; equality, popularity, and longitudinal intersections leak",
        query_samples: samples,
        server_p50_ms: millis(median_duration(&mut server_samples)),
        client_p50_ms: millis(median_duration(&mut client_samples)),
        upload_bytes: DECOY_COUNT * 32,
        download_bytes: DECOY_COUNT * WITNESS_BYTES,
        aggregate_logical_source_bytes: DECOY_COUNT * WITNESS_BYTES,
        server_storage_bytes: storage_per_replica,
        returned_witnesses: DECOY_COUNT,
        client_processed_witnesses: 1,
        result_bytes: WITNESS_BYTES,
    })
}

fn median_duration(values: &mut [Duration]) -> Duration {
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_usize(values: &mut [usize]) -> usize {
    values.sort_unstable();
    values[values.len() / 2]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn radix_buckets_preserve_order_and_predecessors() {
        let buckets = distribute(
            (1..=4_096)
                .map(|position| OrderedLeaf {
                    value: synthetic_nullifier(position, 9),
                    position,
                })
                .collect(),
            64,
        )
        .unwrap();
        let (last_before, first_after) = boundary_entries(&buckets);
        let rows = encode_all_buckets(
            &buckets,
            &last_before,
            &first_after,
            bucket_row_bytes().unwrap(),
        )
        .unwrap();
        let mut target = synthetic_nullifier(99_999, 9);
        while contains_value(&buckets, &target) {
            target = *blake3::hash(&target).as_bytes();
            target[0] &= 0x1f;
        }
        let bucket = bucket_for(&target, buckets.len());
        let row_bytes = bucket_row_bytes().unwrap();
        let (decoded, exact, _) =
            decode_predecessor_slot(&rows[bucket * row_bytes..(bucket + 1) * row_bytes], &target)
                .unwrap();
        assert!(!exact);
        assert_eq!(
            decoded,
            predecessor_for(&buckets, &last_before, &target).unwrap()
        );
        let exact_leaf = buckets.iter().find_map(|bucket| bucket.get(1)).unwrap();
        let strict = strict_predecessor_for(&buckets, &last_before, &exact_leaf.value).unwrap();
        assert!(strict.value < exact_leaf.value);
    }

    #[test]
    fn shieldd_topology_has_fixed_twenty_layer_witness() {
        assert_eq!(1u64 << (DEPTH * 2), 1u64 << 40);
        assert_eq!(DEPTH * (ARITY - 1), 60);
        assert_eq!(WITNESS_BYTES, 2_008);
        let planes = build_node_planes(1_001, 0).unwrap();
        assert_eq!(planes.len(), DEPTH);
        let leaf = OrderedLeaf {
            value: synthetic_nullifier(7, 7),
            position: 1_000,
        };
        let mut leaf_slot = [0u8; BUCKET_ENTRY_BYTES];
        encode_leaf_slot(&mut leaf_slot, Some(leaf), None, false);
        assert_eq!(
            witness_from_planes(&planes, leaf, &leaf_slot)
                .unwrap()
                .len(),
            WITNESS_BYTES
        );
    }
}
