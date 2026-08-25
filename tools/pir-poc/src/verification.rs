//! End-to-end verification for values reconstructed by PIR.
//!
//! Dense XOR proves nothing about the returned bytes.  The client must still
//! authenticate projections and verify state witnesses against a root obtained
//! independently from the serving replicas.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use anyhow::{bail, ensure, Context, Result};
use poseidon377::Fq;
use rand::{CryptoRng, RngCore};

use crate::active_generation::{TREE_ARITY, TREE_DEPTH};
use crate::selected::NULLIFIER_WITNESS_BYTES;

const LEAF_BYTES: usize = 88;
const SIBLINGS_PER_LEVEL: usize = TREE_ARITY - 1;
const PROJECTION_VERSION: u8 = 1;
const PROJECTION_NONCE_BYTES: usize = 12;
const PROJECTION_OVERHEAD: usize = 1 + PROJECTION_NONCE_BYTES + 16;
const PROJECTION_AAD_DOMAIN: &[u8] = b"defradb-pir-encrypted-projection-v1";

static LEAF_DOMAIN: OnceLock<Fq> = OnceLock::new();
static ZERO_HASHES: OnceLock<Vec<Fq>> = OnceLock::new();
type EncodedWitnessFixtures = ([u8; 32], Vec<(u64, Vec<u8>)>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct IndexedLeaf {
    value: [u8; 32],
    next_index: u64,
    next_value: [u8; 32],
    is_lower_sentinel: bool,
    is_terminal: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IndexedWitness {
    position: u64,
    leaf: IndexedLeaf,
    auth_path: Vec<[[u8; 32]; SIBLINGS_PER_LEVEL]>,
}

/// Verifies either a membership path (leaf equals `nullifier`) or a Shieldd
/// indexed-tree non-membership path (leaf is its strict predecessor).
pub fn verify_nullifier_witness(
    nullifier: &[u8; 32],
    witness: &[u8],
    expected_root: &[u8; 32],
) -> Result<()> {
    let witness = decode_witness(witness)?;
    let target = checked_fq(nullifier, "nullifier")?;
    let root = witness_root(&witness)?;
    ensure!(root == *expected_root, "indexed nullifier root mismatch");
    if witness.leaf.value == *nullifier && !witness.leaf.is_lower_sentinel {
        return Ok(());
    }
    if !witness.leaf.is_lower_sentinel {
        ensure!(
            field_order(checked_fq(&witness.leaf.value, "leaf value")?) < field_order(target),
            "nullifier is not above its claimed predecessor"
        );
    }
    if !witness.leaf.is_terminal {
        ensure!(
            field_order(target)
                < field_order(checked_fq(&witness.leaf.next_value, "successor value")?),
            "nullifier is not below its claimed successor"
        );
    }
    Ok(())
}

/// Encrypts one projection value.  Associated data prevents a valid value
/// from being replayed into another generation, tag, or result slot.
pub fn encrypt_projection<R: RngCore + CryptoRng>(
    key: &[u8; 32],
    generation_height: u64,
    generation_root: &[u8; 32],
    tag: &[u8],
    slot: usize,
    plaintext: &[u8],
    rng: &mut R,
) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key).expect("AES-256 keys have a fixed length");
    let mut nonce = [0u8; PROJECTION_NONCE_BYTES];
    rng.fill_bytes(&mut nonce);
    let aad = projection_aad(generation_height, generation_root, tag, slot)?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| anyhow::anyhow!("projection encryption failed"))?;
    let mut envelope = Vec::with_capacity(PROJECTION_OVERHEAD + plaintext.len());
    envelope.push(PROJECTION_VERSION);
    envelope.extend_from_slice(&nonce);
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

pub fn decrypt_projection(
    key: &[u8; 32],
    generation_height: u64,
    generation_root: &[u8; 32],
    tag: &[u8],
    slot: usize,
    envelope: &[u8],
) -> Result<Vec<u8>> {
    if envelope.len() < PROJECTION_OVERHEAD || envelope[0] != PROJECTION_VERSION {
        bail!("encrypted projection has an unsupported envelope");
    }
    let cipher = Aes256Gcm::new_from_slice(key).expect("AES-256 keys have a fixed length");
    let aad = projection_aad(generation_height, generation_root, tag, slot)?;
    cipher
        .decrypt(
            Nonce::from_slice(&envelope[1..1 + PROJECTION_NONCE_BYTES]),
            Payload {
                msg: &envelope[1 + PROJECTION_NONCE_BYTES..],
                aad: &aad,
            },
        )
        .map_err(|_| anyhow::anyhow!("encrypted projection authentication failed"))
}

pub fn decrypt_projection_values(
    key: &[u8; 32],
    generation_height: u64,
    generation_root: &[u8; 32],
    tag: &[u8],
    values: &[Vec<u8>],
) -> Result<Vec<Vec<u8>>> {
    values
        .iter()
        .enumerate()
        .map(|(slot, value)| {
            decrypt_projection(key, generation_height, generation_root, tag, slot, value)
        })
        .collect()
}

fn projection_aad(
    generation_height: u64,
    generation_root: &[u8; 32],
    tag: &[u8],
    slot: usize,
) -> Result<Vec<u8>> {
    let mut aad = Vec::with_capacity(PROJECTION_AAD_DOMAIN.len() + 56 + tag.len());
    aad.extend_from_slice(PROJECTION_AAD_DOMAIN);
    aad.extend_from_slice(&generation_height.to_le_bytes());
    aad.extend_from_slice(generation_root);
    aad.extend_from_slice(&u64::try_from(tag.len())?.to_le_bytes());
    aad.extend_from_slice(tag);
    aad.extend_from_slice(&u64::try_from(slot)?.to_le_bytes());
    Ok(aad)
}

fn decode_witness(bytes: &[u8]) -> Result<IndexedWitness> {
    if bytes.len() != NULLIFIER_WITNESS_BYTES {
        bail!("indexed nullifier witness must be {NULLIFIER_WITNESS_BYTES} bytes");
    }
    let position = u64::from_le_bytes(bytes[0..8].try_into().expect("fixed slice"));
    ensure!(
        position < 1u64 << (TREE_DEPTH * 2),
        "leaf position exceeds Shieldd capacity"
    );
    let value = bytes[8..40].try_into().expect("fixed slice");
    let next_index = u64::from_le_bytes(bytes[40..48].try_into().expect("fixed slice"));
    let next_value = bytes[48..80].try_into().expect("fixed slice");
    let flags = u64::from_le_bytes(bytes[80..88].try_into().expect("fixed slice"));
    ensure!(
        flags & !3 == 0,
        "indexed nullifier witness has unknown flags"
    );
    let leaf = IndexedLeaf {
        value,
        next_index,
        next_value,
        is_lower_sentinel: flags & 1 != 0,
        is_terminal: flags & 2 != 0,
    };
    validate_leaf(position, &leaf)?;
    let mut auth_path = Vec::with_capacity(TREE_DEPTH);
    for level in 0..TREE_DEPTH {
        let start = LEAF_BYTES + level * SIBLINGS_PER_LEVEL * 32;
        let mut siblings = [[0u8; 32]; SIBLINGS_PER_LEVEL];
        for (index, sibling) in siblings.iter_mut().enumerate() {
            let offset = start + index * 32;
            sibling.copy_from_slice(&bytes[offset..offset + 32]);
            checked_fq(sibling, "path sibling")?;
        }
        auth_path.push(siblings);
    }
    Ok(IndexedWitness {
        position,
        leaf,
        auth_path,
    })
}

fn validate_leaf(position: u64, leaf: &IndexedLeaf) -> Result<()> {
    let value = checked_fq(&leaf.value, "leaf value")?;
    let next = checked_fq(&leaf.next_value, "leaf successor")?;
    ensure!(
        leaf.is_lower_sentinel == (position == 0),
        "only the lower sentinel may occupy position zero"
    );
    ensure!(
        !leaf.is_lower_sentinel || value == Fq::from(0u64),
        "lower sentinel has a nonzero value"
    );
    ensure!(
        !leaf.is_terminal || (leaf.next_index == 0 && next == Fq::from(0u64)),
        "terminal indexed leaf has a successor"
    );
    ensure!(
        leaf.is_terminal || (leaf.next_index > 0 && leaf.next_index < 1u64 << (TREE_DEPTH * 2)),
        "indexed leaf successor position is invalid"
    );
    Ok(())
}

fn witness_root(witness: &IndexedWitness) -> Result<[u8; 32]> {
    let mut current = leaf_commitment(&witness.leaf)?;
    let mut position = witness.position;
    for layer in &witness.auth_path {
        let siblings = layer
            .iter()
            .map(|sibling| checked_fq(sibling, "path sibling"))
            .collect::<Result<Vec<_>>>()?;
        let mut children = [Fq::from(0u64); TREE_ARITY];
        let selected = usize::try_from(position % TREE_ARITY as u64)?;
        let mut sibling = 0;
        for (index, child) in children.iter_mut().enumerate() {
            if index == selected {
                *child = current;
            } else {
                *child = siblings[sibling];
                sibling += 1;
            }
        }
        current = hash_children(children);
        position /= TREE_ARITY as u64;
    }
    Ok(current.to_bytes())
}

fn leaf_commitment(leaf: &IndexedLeaf) -> Result<Fq> {
    Ok(poseidon377::hash_5(
        leaf_domain(),
        (
            checked_fq(&leaf.value, "leaf value")?,
            Fq::from(leaf.next_index),
            checked_fq(&leaf.next_value, "leaf successor")?,
            Fq::from(leaf.is_lower_sentinel as u64),
            Fq::from(leaf.is_terminal as u64),
        ),
    ))
}

fn leaf_domain() -> &'static Fq {
    LEAF_DOMAIN.get_or_init(|| {
        Fq::from_le_bytes_mod_order(
            blake2b_simd::blake2b(b"shieldd.nullifier.imt.leaf.v1").as_bytes(),
        )
    })
}

fn zero_hashes() -> &'static [Fq] {
    ZERO_HASHES.get_or_init(|| {
        let mut hashes = Vec::with_capacity(TREE_DEPTH + 1);
        hashes.push(Fq::from(0u64));
        for level in 1..=TREE_DEPTH {
            hashes.push(hash_children([hashes[level - 1]; TREE_ARITY]));
        }
        hashes
    })
}

fn hash_children(children: [Fq; TREE_ARITY]) -> Fq {
    poseidon377::hash_4(
        &Fq::from(0u64),
        (children[0], children[1], children[2], children[3]),
    )
}

fn checked_fq(bytes: &[u8; 32], label: &str) -> Result<Fq> {
    Fq::from_bytes_checked(bytes).map_err(|_| anyhow::anyhow!("invalid {label} field encoding"))
}

fn field_order(value: Fq) -> [u8; 32] {
    let mut ordered = value.to_bytes();
    ordered.reverse();
    ordered
}

/// Builds small canonical Shieldd membership fixtures for the executable demo.
/// Production builders ingest witnesses emitted by Shieldd instead.
pub(crate) fn build_demo_witnesses(values: &[[u8; 32]]) -> Result<EncodedWitnessFixtures> {
    if values.is_empty() {
        bail!("demo witness tree requires at least one value");
    }
    let mut ordered = values.to_vec();
    for value in &ordered {
        checked_fq(value, "demo nullifier")?;
    }
    ordered.sort_by_key(|value| {
        field_order(checked_fq(value, "validated demo nullifier").expect("validated above"))
    });
    if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("demo witness tree contains duplicate values");
    }
    let mut leaves = Vec::with_capacity(ordered.len() + 1);
    leaves.push(IndexedLeaf {
        value: [0; 32],
        next_index: 1,
        next_value: ordered[0],
        is_lower_sentinel: true,
        is_terminal: false,
    });
    for (index, value) in ordered.iter().enumerate() {
        let next = ordered.get(index + 1);
        leaves.push(IndexedLeaf {
            value: *value,
            next_index: next.map_or(0, |_| u64::try_from(index + 2).expect("small fixture")),
            next_value: next.copied().unwrap_or([0; 32]),
            is_lower_sentinel: false,
            is_terminal: next.is_none(),
        });
    }

    let mut levels = Vec::<BTreeMap<u64, Fq>>::with_capacity(TREE_DEPTH + 1);
    let mut leaf_hashes = BTreeMap::new();
    for (position, leaf) in leaves.iter().enumerate() {
        leaf_hashes.insert(u64::try_from(position)?, leaf_commitment(leaf)?);
    }
    levels.push(leaf_hashes);
    for level in 0..TREE_DEPTH {
        let parent_positions = levels[level]
            .keys()
            .map(|position| position / TREE_ARITY as u64)
            .collect::<BTreeSet<_>>();
        let mut parents = BTreeMap::new();
        for parent in parent_positions {
            let children = std::array::from_fn(|child| {
                levels[level]
                    .get(&(parent * TREE_ARITY as u64 + child as u64))
                    .copied()
                    .unwrap_or(zero_hashes()[level])
            });
            parents.insert(parent, hash_children(children));
        }
        levels.push(parents);
    }
    let root = levels[TREE_DEPTH]
        .get(&0)
        .copied()
        .unwrap_or(zero_hashes()[TREE_DEPTH])
        .to_bytes();

    let mut output = Vec::with_capacity(ordered.len());
    for (leaf_index, leaf) in leaves.iter().enumerate().skip(1) {
        let position = u64::try_from(leaf_index)?;
        let mut cursor = position;
        let mut auth_path = Vec::with_capacity(TREE_DEPTH);
        for (level, hashes) in levels.iter().enumerate().take(TREE_DEPTH) {
            let selected = cursor % TREE_ARITY as u64;
            let base = cursor / TREE_ARITY as u64 * TREE_ARITY as u64;
            let siblings = (0..TREE_ARITY as u64)
                .filter(|child| *child != selected)
                .map(|child| {
                    hashes
                        .get(&(base + child))
                        .copied()
                        .unwrap_or(zero_hashes()[level])
                        .to_bytes()
                })
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid demo path width"))?;
            auth_path.push(siblings);
            cursor /= TREE_ARITY as u64;
        }
        let witness = IndexedWitness {
            position,
            leaf: *leaf,
            auth_path,
        };
        let encoded = encode_witness(&witness)?;
        verify_nullifier_witness(&leaf.value, &encoded, &root)
            .context("generated Shieldd demo witness did not verify")?;
        output.push((position, encoded));
    }
    Ok((root, output))
}

fn encode_witness(witness: &IndexedWitness) -> Result<Vec<u8>> {
    let mut encoded = Vec::with_capacity(NULLIFIER_WITNESS_BYTES);
    encoded.extend_from_slice(&witness.position.to_le_bytes());
    encoded.extend_from_slice(&witness.leaf.value);
    encoded.extend_from_slice(&witness.leaf.next_index.to_le_bytes());
    encoded.extend_from_slice(&witness.leaf.next_value);
    let flags = witness.leaf.is_lower_sentinel as u64 | (witness.leaf.is_terminal as u64) << 1;
    encoded.extend_from_slice(&flags.to_le_bytes());
    for layer in &witness.auth_path {
        for sibling in layer {
            encoded.extend_from_slice(sibling);
        }
    }
    ensure!(
        encoded.len() == NULLIFIER_WITNESS_BYTES,
        "encoded witness size mismatch"
    );
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn shieldd_membership_path_and_projection_are_verified() {
        let values = [Fq::from(7u64).to_bytes(), Fq::from(11u64).to_bytes()];
        let (root, witnesses) = build_demo_witnesses(&values).unwrap();
        verify_nullifier_witness(&values[0], &witnesses[0].1, &root).unwrap();
        let mut tampered = witnesses[0].1.clone();
        *tampered.last_mut().unwrap() ^= 1;
        assert!(verify_nullifier_witness(&values[0], &tampered, &root).is_err());

        let key = [9u8; 32];
        let mut rng = StdRng::seed_from_u64(5);
        let encrypted =
            encrypt_projection(&key, 42, &root, b"tag", 0, b"payload", &mut rng).unwrap();
        assert_eq!(
            decrypt_projection(&key, 42, &root, b"tag", 0, &encrypted).unwrap(),
            b"payload"
        );
        assert!(decrypt_projection(&key, 42, &root, b"other", 0, &encrypted).is_err());
    }
}
