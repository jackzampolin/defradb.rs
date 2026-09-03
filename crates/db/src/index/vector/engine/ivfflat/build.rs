//! Training and the build that follows it.

use super::IvfFlat;
use crate::index::error::{Error, Result};
use crate::index::vector::engine::ann::Sampler;
use crate::index::vector::engine::ivf::{self, TrainedState};
use crate::index::vector::quantize::Reservoir;
use crate::index::vector::store::{NodeId, VectorNodeStore};

/// What a build did, so a caller can report it rather than guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildReport {
    pub sampled: usize,
    pub sample_bytes: usize,
    pub indexed: u64,
    pub state: TrainedState,
}

impl<S: VectorNodeStore> IvfFlat<S> {
    /// Vectors held, tombstones excluded.
    pub async fn live_count(&self) -> Result<u64> {
        let mut count = 0u64;
        self.store()
            .iterate_nodes(|_| {
                count += 1;
                Ok(())
            })
            .await?;
        Ok(count)
    }

    /// Whether at least `wanted` live vectors are stored, counting no further.
    ///
    /// See [`IvfPq::live_count_at_least`](super::super::ivfpq::IvfPq::live_count_at_least)
    /// for why returning an error from the visitor is what stops the walk
    /// early.
    pub async fn live_count_at_least(&self, wanted: u64) -> Result<bool> {
        if wanted == 0 {
            return Ok(true);
        }
        let mut count = 0u64;
        match self
            .store()
            .iterate_nodes(|_| {
                count += 1;
                if count < wanted {
                    Ok(())
                } else {
                    Err(Error::Other(
                        "vector index: live_count_at_least stopped early".into(),
                    ))
                }
            })
            .await
        {
            Ok(()) => Ok(false),
            Err(_) if count >= wanted => Ok(true),
            Err(err) => Err(err),
        }
    }

    /// Whether there are enough vectors to fit the configured lists.
    pub async fn should_build(&self) -> Result<bool> {
        if self.is_trained().await? {
            return Ok(false);
        }
        self.live_count_at_least(self.params().resolved_train_threshold())
            .await
    }

    /// Trains from a byte-bounded sample and writes centroids and inverted
    /// lists. Cheaper to build than IVF-PQ: one k-means fit, no residual
    /// pass, no codebook training, no quantizer round trip.
    ///
    /// Only the list assignment travels between the read pass that computes
    /// it and the write pass that stores it, never the vector: at
    /// `dimensions` in the hundreds that is the difference between a few
    /// bytes per document and a second corpus resident in memory. The vector
    /// itself is re-read from the node it is already durable in, the same
    /// read `insert` does on an update; reading and writing the store cannot
    /// interleave in one pass in any case, since a write needs `&mut` access
    /// the read's borrow is still holding.
    pub async fn build(&mut self) -> Result<BuildReport> {
        let live = self.live_count().await?;
        if live == 0 {
            return Err(Error::Other(
                "vector index: nothing to train an IVF_FLAT build on".into(),
            ));
        }

        let dimensions = self.first_width().await?;
        let nlist = self.params().resolved_nlist(live);

        let mut reservoir =
            Reservoir::new(dimensions, self.params().sample_bytes as usize, self.seed());
        self.store()
            .iterate_nodes(|node| {
                reservoir.offer(&node.vector);
                Ok(())
            })
            .await?;

        let sampled = reservoir.len();
        let sample_bytes = reservoir.resident_bytes();
        let coarse =
            ivf::fit_centroids(reservoir.as_flat(), dimensions, nlist as usize, self.seed())?;
        drop(reservoir);

        for index in 0..coarse.k {
            let bytes = ivf::encode_vector(coarse.get(index));
            self.store_mut()
                .put_aux(ivf::CENTROID, &(index as u32).to_be_bytes(), &bytes)
                .await?;
        }

        let state = TrainedState {
            nlist: coarse.k as u32,
            dimensions: dimensions as u32,
        };

        let mut assignments: Vec<(u32, NodeId)> = Vec::new();
        self.store()
            .iterate_nodes(|node| {
                let (list, _) = coarse.nearest(&node.vector);
                assignments.push((list as u32, node.id));
                Ok(())
            })
            .await?;

        let indexed = assignments.len() as u64;
        for (list, id) in assignments {
            let node =
                self.store().get_node(id).await?.ok_or_else(|| {
                    Error::Other("vector index: a just-sampled node is gone".into())
                })?;
            self.store_mut()
                .put_aux(
                    ivf::LIST,
                    &ivf::list_key(list, id),
                    &ivf::encode_vector(&node.vector),
                )
                .await?;
        }

        self.store_mut()
            .put_aux(ivf::STATE, b"", &ivf::encode_state(&state))
            .await?;

        Ok(BuildReport {
            sampled,
            sample_bytes,
            indexed,
            state,
        })
    }

    /// The list a vector belongs to.
    pub(super) async fn assign(&self, state: &TrainedState, vector: &[f32]) -> Result<u32> {
        let centroids = self.trained_centroids(state).await?;
        let (list, _) = centroids.nearest(vector);
        Ok(list as u32)
    }

    async fn first_width(&self) -> Result<usize> {
        let mut width = 0usize;
        self.store()
            .iterate_nodes(|node| {
                if width == 0 {
                    width = node.vector.len();
                }
                Ok(())
            })
            .await?;
        if width == 0 {
            return Err(Error::Other(
                "vector index: no stored vector to take a width from".into(),
            ));
        }
        Ok(width)
    }
}
