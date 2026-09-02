//! The IVF_FLAT acceptance property: at `nprobe == nlist`, the scan is exact.
//!
//! There is no quantization in this engine at all, so probing every list
//! makes it identical to [`Flat`], not merely close: same ids, same
//! distances, same order, ties included. Any divergence here is a bug in the
//! scan, not a recall tradeoff, which is why it gets its own file rather than
//! one assertion inside `ivfflat_engine.rs`. Checked over several randomized
//! corpus shapes rather than one fixture, mirroring how the rest of this
//! suite varies seeds instead of reaching for `proptest`.
//!
//! That byte-identical claim holds for a *live* corpus. With tombstones
//! present it narrows to a subset: `iterate_aux`'s visitor is synchronous, so
//! a candidate's liveness can only be checked once the per-list scan hands
//! control back, after the top-k heap has already filled on raw distance.
//! IVF-PQ's `search_lists` has the identical shape for the identical reason.
//! A heap slot a dead entry wins is a slot a live one further out never gets
//! to compete for, so the surviving hits can be fewer than `k` even when `k`
//! live documents exist in the probed lists; what stays provably true is that
//! every survivor is one `Flat` would also return, at the same distance.

use db::index::vector::engine::ann::VectorIndexEngine;
use db::index::vector::engine::flat::Flat;
use db::index::vector::engine::ivfflat::IvfFlat;
use db::index::vector::engine::ivfflat::IvfFlatParams;
use db::index::vector::store::MemoryNodeStore;
use db::index::vector::store::NodeId;
use defra_core::vector::Metric;

async fn built(nlist: u32, seed: u64, vectors: &[Vec<f32>]) -> IvfFlat<MemoryNodeStore> {
    let params = IvfFlatParams {
        nlist,
        nprobe: nlist,
        ..IvfFlatParams::default()
    };
    let mut index = IvfFlat::try_new(MemoryNodeStore::new(), Metric::Cosine, params, seed)
        .expect("cosine partitions soundly");
    for (i, vector) in vectors.iter().enumerate() {
        index.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }
    index.build().await.unwrap();
    index
}

async fn flat_of(vectors: &[Vec<f32>]) -> Flat<MemoryNodeStore> {
    let mut flat = Flat::new(MemoryNodeStore::new(), Metric::Cosine);
    for (i, vector) in vectors.iter().enumerate() {
        flat.insert(NodeId(i as u64 + 1), vector).await.unwrap();
    }
    flat
}

/// `(seed, corpus, dimensions, clusters, nlist, k)`. Deliberately varied
/// shapes: `nlist` above, below and equal to the true cluster count, a
/// single-list corpus, and a single-document corpus, so a bug that only shows
/// up when a list is empty or when `nlist` does not match reality cannot hide
/// behind one lucky configuration.
const CASES: &[(u64, usize, usize, usize, u32, usize)] = &[
    (0x0001, 200, 8, 5, 5, 10),
    (0x0002, 500, 16, 12, 12, 10),
    (0x0003, 300, 4, 3, 7, 5),
    (0x0004, 150, 32, 6, 6, 20),
    (0x0005, 1, 8, 1, 1, 5),
    (0x0006, 64, 8, 1, 1, 10),
    (0x0007, 800, 16, 20, 3, 15),
];

#[tokio::test]
async fn nprobe_equal_to_nlist_matches_flat_exactly() {
    for &(seed, count, dimensions, clusters, nlist, k) in CASES {
        let mut corpus = crate::support::Corpus::new(seed);
        let vectors = corpus.clustered(count, dimensions, clusters, 0.15);

        let index = built(nlist, seed ^ 0xF1A7, &vectors).await;
        let flat = flat_of(&vectors).await;

        let mut queries = crate::support::Corpus::new(seed ^ 0xC0FF_EE00);
        for q in 0..25 {
            let query = queries.vector(dimensions);
            let from_ivf = index.search(query.as_slice(), k, None).await.unwrap();
            let from_flat = flat.search(query.as_slice(), k, None).await.unwrap();
            assert_eq!(
                from_ivf, from_flat,
                "seed={seed:#x} count={count} dims={dimensions} clusters={clusters} \
                 nlist={nlist} query={q}: ids, distances or order diverged from Flat"
            );

            // And a corpus vector itself, so a tie against its own bucket-mate
            // is exercised, not only a random unseen query.
            let self_query = &vectors[q % vectors.len()];
            let from_ivf = index.search(self_query.as_slice(), k, None).await.unwrap();
            let from_flat = flat.search(self_query.as_slice(), k, None).await.unwrap();
            assert_eq!(
                from_ivf, from_flat,
                "seed={seed:#x} corpus-vector query {q}: ids, distances or order diverged"
            );
        }
    }
}

/// A deleted document must never come back, and every document IVF_FLAT does
/// return must be one `Flat` would also return, at the identical distance:
/// the strongest claim that holds once tombstones are in play, proved in the
/// module doc above. Full list equality is not that claim; see
/// `ivfflat_engine::a_selective_filter_can_return_fewer_than_k_when_the_probed_lists_run_out`
/// for the same "fewer than k survives" shape under a filter instead of a
/// delete.
#[tokio::test]
async fn deletes_never_resurrect_and_every_survivor_matches_flat() {
    for &(seed, count, dimensions, clusters, nlist, k) in CASES {
        let mut corpus = crate::support::Corpus::new(seed ^ 0x5EED);
        let vectors = corpus.clustered(count, dimensions, clusters, 0.15);

        let mut index = built(nlist, seed ^ 0xF1A7, &vectors).await;
        let mut flat = flat_of(&vectors).await;

        let deleted: std::collections::HashSet<u64> =
            (1..=vectors.len() as u64).step_by(3).collect();
        for &id in &deleted {
            index.delete(NodeId(id)).await.unwrap();
            flat.delete(NodeId(id)).await.unwrap();
        }

        let mut queries = crate::support::Corpus::new(seed ^ 0xBEEF);
        for _ in 0..10 {
            let query = queries.vector(dimensions);
            let from_ivf = index.search(query.as_slice(), k, None).await.unwrap();
            let from_flat = flat.search(query.as_slice(), k, None).await.unwrap();

            assert!(from_ivf.len() <= k, "seed={seed:#x}: returned more than k");
            assert!(
                from_ivf.iter().all(|hit| !deleted.contains(&hit.id.0)),
                "seed={seed:#x}: a deleted document was returned: {from_ivf:?}"
            );
            assert!(
                from_ivf.iter().all(|hit| from_flat.contains(hit)),
                "seed={seed:#x}: IVF_FLAT returned a hit Flat would not: \
                 ivf={from_ivf:?} flat={from_flat:?}"
            );
        }
    }
}
