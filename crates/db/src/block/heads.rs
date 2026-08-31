//! The collection head set: the tips of a branchable collection's DAG.
//!
//! A head is a stored block that nothing else names as a parent. Maintaining
//! that set by deleting a parent's head key when a child appears is the obvious
//! encoding and it is the one this module replaces: two writers appending
//! concurrently observe the same parent and both issue the same delete, which
//! is a write-write conflict the engine refuses at every isolation level. The
//! set stops forming siblings, which is a change to the data structure and not
//! to its speed.
//!
//! Here a writer only ever writes keys that name its own block: its head key,
//! and one `/cs/{collection}/{parent}/{child}` marker per parent. Two writers
//! therefore cannot write the same key, and the head set becomes a query: a
//! stored head key is live exactly when no marker names it.
//!
//! `proofs/tla/HeadSet.tla` checks both strategies, with
//! `MC_HeadSet_Red_EagerDelete.cfg` failing on the delete and
//! `MC_HeadSet_Green.cfg` passing on this one. `proofs/lean/HeadSet/Core.lean`
//! carries the algebra: `derived_writeSets_disjoint` for why nothing collides,
//! and `applyDerived_parents_not_head` for why the answer is unchanged.
//!
//! # Cost
//!
//! Markers are garbage once their parent's head key goes, which is inherent:
//! every conflict-free set carries tombstones. [`prune_superseded_heads`]
//! reclaims them in a transaction of its own, where the only thing it can
//! collide with is another prune. Left alone the headstore would grow one key
//! per mutation and each append would scan all of them, so the prune is not
//! optional on a device with a small memory budget.

use cid::Cid;
use datastore::NamespaceView;
use storage::corekv::{IterOptions, Iterator, Reader, Result};
use storage::keys::headstore::{HeadstoreColKey, HeadstoreColSuperseded};

/// What one pass over the head prefix found.
#[derive(Debug, Default)]
pub struct CollectionHeads {
    /// The live tips, ascending by CID text.
    pub live: Vec<Cid>,
    /// The highest priority among the live tips, or 0 when there are none.
    pub max_priority: u64,
    /// Head keys that a marker superseded. These are reclaimable, and their
    /// count is what decides whether a prune is worth a transaction.
    pub superseded: usize,
}

/// The collection's live heads.
///
/// Walks the head prefix and the marker prefix together. Both are sorted by
/// the CID text they carry, so one pass over each answers the whole question
/// and neither range is ever held in memory.
pub async fn live_collection_heads<R: Reader + ?Sized>(
    reader: &R,
    collection_short_id: u32,
) -> Result<CollectionHeads> {
    let head_prefix = HeadstoreColKey::collection_prefix(collection_short_id);
    let head_prefix_len = head_prefix.len();
    let marker_prefix = HeadstoreColSuperseded::collection_prefix(collection_short_id);
    let marker_prefix_len = marker_prefix.len();

    let mut head_iter = reader
        .iterator(IterOptions::new().with_prefix(head_prefix))
        .await?;
    let mut marker_iter = reader
        .iterator(
            IterOptions::new()
                .with_prefix(marker_prefix)
                .with_keys_only(true),
        )
        .await?;

    let mut found = CollectionHeads::default();
    // One buffer, refilled per marker, rather than a `Vec` per marker.
    let mut parent = Vec::new();
    let mut have_parent = next_parent(marker_iter.as_mut(), marker_prefix_len, &mut parent).await?;

    while let Some(pair) = head_iter.next().await? {
        let cid_text = &pair.key[head_prefix_len..];
        // Markers below this head belong to heads already passed.
        while have_parent && parent.as_slice() < cid_text {
            have_parent = next_parent(marker_iter.as_mut(), marker_prefix_len, &mut parent).await?;
        }
        if have_parent && parent.as_slice() == cid_text {
            found.superseded += 1;
            continue;
        }
        let Ok(text) = std::str::from_utf8(cid_text) else {
            continue;
        };
        let Ok(cid) = text.parse::<Cid>() else {
            continue;
        };
        found.max_priority = found
            .max_priority
            .max(crate::block::builder::decode_priority_varint(&pair.value));
        found.live.push(cid);
    }

    head_iter.close().await?;
    marker_iter.close().await?;
    Ok(found)
}

/// Advance to the next marker and load the head it names into `parent`.
///
/// Returns false once the marker range is exhausted.
async fn next_parent(
    iter: &mut dyn Iterator,
    prefix_len: usize,
    parent: &mut Vec<u8>,
) -> Result<bool> {
    while let Some(pair) = iter.next().await? {
        // The key is /cs/{collection}/{parent}/{child}.
        let rest = &pair.key[prefix_len..];
        let Some(sep) = rest.iter().position(|byte| *byte == b'/') else {
            continue;
        };
        parent.clear();
        parent.extend_from_slice(&rest[..sep]);
        return Ok(true);
    }
    Ok(false)
}

/// Record that `child` superseded each of `parents`.
///
/// Every key written names `child`, so this is safe to run concurrently with
/// any other writer: `HeadSet.derived_writeSets_disjoint`.
pub async fn record_supersedes(
    headstore: &NamespaceView,
    collection_short_id: u32,
    parents: &[Cid],
    child: Cid,
) -> Result<()> {
    for parent in parents {
        let marker = HeadstoreColSuperseded::new(collection_short_id, *parent, child);
        headstore
            .set(&storage::corekv::Key::bytes(&marker), &[])
            .await?;
    }
    Ok(())
}

/// What one prune pass reclaimed.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PruneOutcome {
    /// Head keys removed. Each came with its markers.
    pub heads_removed: usize,
    /// Marker keys removed.
    pub markers_removed: usize,
    /// True when the pass stopped on its key budget rather than on the end of
    /// the range, so a caller can see that more is left rather than read the
    /// result as "clean".
    pub more_remaining: bool,
}

/// Delete superseded head keys, each together with the markers against it.
///
/// A head key and its markers go in one transaction, so no reader can observe
/// a head whose markers were already removed. Deleting them separately would
/// resurrect the head, which is why `max_keys` bounds a *pass* and not a
/// single head's key group: a group wider than the budget still leaves whole,
/// on a pass of its own.
///
/// A marker whose parent has no head key is left alone. It is not necessarily
/// garbage: a block replicated ahead of its parent records the marker first,
/// and the parent's head key arrives afterwards.
///
/// Runs in whatever transaction it is given, and that must not be a
/// transaction doing anything else: it writes keys another prune would also
/// write, so it is the one path here that can conflict. Losing is harmless,
/// the next pass repeats the work.
pub async fn prune_superseded_heads(
    headstore: &NamespaceView,
    collection_short_id: u32,
    max_keys: usize,
) -> Result<PruneOutcome> {
    let head_prefix = HeadstoreColKey::collection_prefix(collection_short_id);
    let head_prefix_len = head_prefix.len();
    let marker_prefix = HeadstoreColSuperseded::collection_prefix(collection_short_id);
    let marker_prefix_len = marker_prefix.len();

    let mut head_iter = headstore
        .iterator(IterOptions::new().with_prefix(head_prefix))
        .await?;
    let mut marker_iter = headstore
        .iterator(
            IterOptions::new()
                .with_prefix(marker_prefix)
                .with_keys_only(true),
        )
        .await?;

    // Collected rather than deleted in place: deleting from under an iterator
    // that is rebuilt from its last key would skip entries. The budget is what
    // keeps this bounded.
    let mut doomed: Vec<Vec<u8>> = Vec::new();
    let mut outcome = PruneOutcome::default();
    let mut marker: Option<(Vec<u8>, Vec<u8>)> =
        next_marker(marker_iter.as_mut(), marker_prefix_len).await?;

    while let Some(pair) = head_iter.next().await? {
        let cid_text = &pair.key[head_prefix_len..];
        while marker
            .as_ref()
            .is_some_and(|(parent, _)| parent.as_slice() < cid_text)
        {
            marker = next_marker(marker_iter.as_mut(), marker_prefix_len).await?;
        }
        if marker
            .as_ref()
            .is_none_or(|(parent, _)| parent.as_slice() != cid_text)
        {
            continue;
        }
        // Every marker against this head, so the group leaves together.
        let mut group = vec![pair.key.clone()];
        while let Some((parent, key)) = marker.take() {
            if parent.as_slice() != cid_text {
                marker = Some((parent, key));
                break;
            }
            group.push(key);
            marker = next_marker(marker_iter.as_mut(), marker_prefix_len).await?;
        }
        // A group always leaves whole, so one larger than the whole budget
        // still goes on an otherwise empty pass. Deferring it instead would
        // defer it forever: every later pass would meet the same group first
        // and stop in the same place.
        if !doomed.is_empty() && doomed.len() + group.len() > max_keys {
            outcome.more_remaining = true;
            break;
        }
        outcome.heads_removed += 1;
        outcome.markers_removed += group.len() - 1;
        doomed.extend(group);
    }

    head_iter.close().await?;
    marker_iter.close().await?;

    for key in doomed {
        headstore.delete(&key).await?;
    }
    Ok(outcome)
}

/// The next marker as (head it names, whole key).
async fn next_marker(
    iter: &mut dyn Iterator,
    prefix_len: usize,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    while let Some(pair) = iter.next().await? {
        let rest = &pair.key[prefix_len..];
        let Some(sep) = rest.iter().position(|byte| *byte == b'/') else {
            continue;
        };
        return Ok(Some((rest[..sep].to_vec(), pair.key)));
    }
    Ok(None)
}
