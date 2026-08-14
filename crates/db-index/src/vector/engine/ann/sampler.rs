//! Drawing a bounded training set from a stream.

/// Bounded in **bytes**: the same 100,000 vectors are 6 MB at 16 dimensions and
/// 300 MB at 768, so a count does not bound what actually fails.
pub trait Sampler {
    fn offer(&mut self, vector: &[f32]);

    /// The sample as one flat `len() * dimensions` buffer.
    fn as_flat(&self) -> &[f32];

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Vectors offered, which exceeds `len` once the budget is reached.
    fn seen(&self) -> u64;

    fn resident_bytes(&self) -> usize;
}
