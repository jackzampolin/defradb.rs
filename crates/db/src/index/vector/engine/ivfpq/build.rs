//! Training and the build that follows it.

use super::codec::{self, TrainedState};
use super::IvfPq;
use crate::index::error::{Error, Result};
use crate::index::vector::engine::ann::{Centroids, Clusterer, Quantizer, Sampler};
use crate::index::vector::quantize::{KMeans, ProductQuantizer, Reservoir};
use crate::index::vector::store::{NodeId, VectorNodeStore};

/// What a build did, so a caller can report it rather than guess.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuildReport {
    pub sampled: usize,
    pub sample_bytes: usize,
    pub indexed: u64,
    pub state: TrainedState,
}

impl<S: VectorNodeStore> IvfPq<S> {
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

    /// Whether there are enough vectors to fit the configured lists.
    pub async fn should_build(&self) -> Result<bool> {
        if self.is_trained().await? {
            return Ok(false);
        }
        let live = self.live_count().await?;
        Ok(live >= self.params().train_threshold(live).max(1))
    }

    /// Trains from a byte-bounded sample and writes centroids, codebooks and
    /// inverted lists.
    ///
    /// Two streaming passes over the nodes and nothing else resident: the
    /// sample, the centroids and the codebooks, each bounded by configuration
    /// rather than by the corpus.
    pub async fn build(&mut self) -> Result<BuildReport> {
        let live = self.live_count().await?;
        if live == 0 {
            return Err(Error::Other(
                "vector index: nothing to train an IVF-PQ build on".into(),
            ));
        }

        let dimensions = self.first_width().await?;
        let nlist = self.params().resolved_nlist(live);
        let m = self.params().resolved_m(dimensions);

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
        let clusterer = KMeans::new(self.seed());
        let (coarse, _) = clusterer.fit(reservoir.as_flat(), dimensions, nlist as usize);
        if coarse.k == 0 {
            return Err(Error::Other(
                "vector index: the training sample produced no centroids".into(),
            ));
        }

        let residuals = residuals_of(reservoir.as_flat(), dimensions, &coarse);
        let quantizer = ProductQuantizer::train(&clusterer, &residuals, dimensions, m)?;
        drop(residuals);

        for index in 0..coarse.k {
            let bytes = codec::encode_vector(coarse.get(index));
            self.store_mut()
                .put_aux(codec::CENTROID, &(index as u32).to_be_bytes(), &bytes)
                .await?;
        }
        for (sub, book) in quantizer.books().iter().enumerate() {
            let bytes = codec::encode_centroids(book);
            self.store_mut()
                .put_aux(codec::CODEBOOK, &(sub as u32).to_be_bytes(), &bytes)
                .await?;
        }

        let state = TrainedState {
            nlist: coarse.k as u32,
            m: quantizer.m() as u32,
            dimensions: dimensions as u32,
        };

        let mut assignments: Vec<(u32, NodeId, Vec<u8>)> = Vec::new();
        let mut code = vec![0u8; quantizer.code_len()];
        let mut residual = vec![0.0f32; dimensions];
        self.store()
            .iterate_nodes(|node| {
                let (list, _) = coarse.nearest(&node.vector);
                subtract_into(&node.vector, coarse.get(list), &mut residual);
                quantizer.encode(&residual, &mut code);
                assignments.push((list as u32, node.id, code.clone()));
                Ok(())
            })
            .await?;

        let indexed = assignments.len() as u64;
        for (list, id, code) in assignments {
            self.store_mut()
                .put_aux(codec::LIST, &codec::list_key(list, id), &code)
                .await?;
        }

        self.store_mut()
            .put_aux(codec::STATE, b"", &codec::encode_state(&state))
            .await?;

        Ok(BuildReport {
            sampled,
            sample_bytes,
            indexed,
            state,
        })
    }

    /// The list a vector belongs to, and its code.
    pub(super) async fn assign(
        &self,
        state: &TrainedState,
        vector: &[f32],
    ) -> Result<(u32, Vec<u8>)> {
        let (coarse, quantizer) = self.trained_parts(state).await?;
        let (list, _) = coarse.nearest(vector);
        let mut residual = vec![0.0f32; state.dimensions as usize];
        subtract_into(vector, coarse.get(list), &mut residual);
        let mut code = vec![0u8; quantizer.code_len()];
        quantizer.encode(&residual, &mut code);
        Ok((list as u32, code))
    }

    pub(super) async fn load_coarse_centroids(&self, state: &TrainedState) -> Result<Centroids> {
        let mut values = Vec::with_capacity(state.nlist as usize * state.dimensions as usize);
        for index in 0..state.nlist {
            let bytes = self
                .store()
                .get_aux(codec::CENTROID, &index.to_be_bytes())
                .await?
                .ok_or_else(|| {
                    Error::Other(format!("vector index: centroid {index} is missing"))
                })?;
            values.extend_from_slice(&codec::decode_vector(&bytes)?);
        }
        Ok(Centroids {
            k: state.nlist as usize,
            dimensions: state.dimensions as usize,
            values,
        })
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

fn subtract_into(vector: &[f32], centroid: &[f32], out: &mut [f32]) {
    for (slot, (v, c)) in out.iter_mut().zip(vector.iter().zip(centroid)) {
        *slot = v - c;
    }
}

fn residuals_of(sample: &[f32], dimensions: usize, coarse: &Centroids) -> Vec<f32> {
    let mut residuals = vec![0.0f32; sample.len()];
    for (point, slot) in sample
        .chunks_exact(dimensions)
        .zip(residuals.chunks_exact_mut(dimensions))
    {
        let (nearest, _) = coarse.nearest(point);
        subtract_into(point, coarse.get(nearest), slot);
    }
    residuals
}
