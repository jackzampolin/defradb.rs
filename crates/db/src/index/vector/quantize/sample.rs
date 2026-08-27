//! A bounded training sample drawn from a stream.

use crate::index::vector::engine::ann::Sampler;

/// Reservoir sampling: every vector in the stream has the same chance of ending
/// up in the sample, whatever the stream's length, and the stream is read once.
pub struct Reservoir {
    dimensions: usize,
    capacity: usize,
    vectors: Vec<f32>,
    seen: u64,
    state: u64,
}

impl Reservoir {
    /// `budget_bytes` caps the resident sample. A budget too small for even one
    /// vector still holds one, so training never sees an empty reservoir on a
    /// non-empty stream.
    pub fn new(dimensions: usize, budget_bytes: usize, seed: u64) -> Self {
        let per_vector = dimensions.max(1) * size_of::<f32>();
        let capacity = (budget_bytes / per_vector).max(1);
        Self {
            dimensions,
            capacity,
            vectors: Vec::with_capacity(capacity.min(1024) * dimensions),
            seen: 0,
            state: seed,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn push(&mut self, vector: &[f32]) {
        if vector.len() != self.dimensions {
            return;
        }
        self.seen += 1;

        if self.vectors.len() < self.capacity * self.dimensions {
            self.vectors.extend_from_slice(vector);
            return;
        }

        let slot = (self.next_u64() % self.seen) as usize;
        if slot < self.capacity {
            let at = slot * self.dimensions;
            self.vectors[at..at + self.dimensions].copy_from_slice(vector);
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn into_flat(self) -> Vec<f32> {
        self.vectors
    }
}

impl Sampler for Reservoir {
    /// Wrong-width vectors are ignored rather than corrupting the flat buffer.
    fn offer(&mut self, vector: &[f32]) {
        self.push(vector)
    }

    fn as_flat(&self) -> &[f32] {
        &self.vectors
    }

    fn len(&self) -> usize {
        if self.dimensions == 0 {
            return 0;
        }
        self.vectors.len() / self.dimensions
    }

    fn seen(&self) -> u64 {
        self.seen
    }

    fn resident_bytes(&self) -> usize {
        self.vectors.len() * size_of::<f32>()
    }
}
